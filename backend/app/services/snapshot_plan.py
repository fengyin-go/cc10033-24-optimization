"""快照计划服务：管理数仓表的定期快照策略，定义周期、保留份数和自动清理。

增强能力：
- 消费 deploy.published 通道，自动为发布后的模型生成版本快照
- 消费 model_published（一般来自deploy_pipeline）生成定时快照策略
- 过期清理策略：按保留份数自动清理过期分区
- 快照对比：提供版本差异比较能力
- 增量快照：标记增量写入模式
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.pipeline import SnapshotRetentionManager
from app.store import store

VALID_CYCLES = {"每分钟", "每小时", "每次变更", "每日", "每周", "每月", "每季度"}
PARTITION_FORMATS = {"ds=yyyyMMdd", "hour=yyyyMMddHH", "week=yyyyWW", "version=yyyyMMddHHmmss",
                     "year=yyyy", "month=yyyyMM"}


class SnapshotPlanService(BaseModuleService):
    MODULE = "snapshot_plan"
    ENTITY = "快照策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略名称", "目标表", "快照周期", "保留份数", "分区格式", "增量标记", "过期清理"]
    REQUIRED_FIELDS = ["策略名称", "目标表", "快照周期"]
    STATUS_ORDER = ["草稿", "已启用", "已暂停"]
    ACTION_RULES = {"启用策略": "草稿", "暂停策略": "已启用"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建快照策略：消费deploy.published通道，为目标表自动生成快照策略。"""
        target_table = str(values.get("目标表", "")).strip()
        snapshot_cycle = str(values.get("快照周期", "")).strip()

        # 标准化快照周期
        if snapshot_cycle not in VALID_CYCLES:
            snapshot_cycle = "每日"
            values["快照周期"] = snapshot_cycle

        # 标准化分区格式
        partition_fmt = str(values.get("分区格式", "")).strip()
        if partition_fmt not in PARTITION_FORMATS:
            partition_fmt = _infer_partition_format(snapshot_cycle)
            values["分区格式"] = partition_fmt

        # 自动计算保留份数
        if not values.get("保留份数"):
            values["保留份数"] = _calculate_retention(snapshot_cycle)

        # 消费部署发布事件，自动创建快照
        deploy_msgs = store.channel.consume("deploy.published", "snapshot_plan")
        for msg in deploy_msgs:
            if msg["payload"]["target_model"] in target_table:
                # 自动为已部署模型创建版本快照
                snap_data = {
                    "target_model": msg["payload"]["target_model"],
                    "source_version": msg["payload"].get("source_version", "v1.0"),
                    "deployed_at": msg["payload"].get("deployed_at"),
                    "environment": msg["payload"].get("environment"),
                }
                snap_id = store.snapshot.create_version_snapshot(
                    "snapshot_plan", msg["payload"]["entry_id"],
                    snap_data,
                    msg["payload"].get("source_version", "v1.0"),
                )
                values["_auto_snapshot_id"] = snap_id
                values["增量标记"] = "是"

        # 设置过期清理策略
        if not values.get("过期清理"):
            retention = values.get("保留份数", "30")
            values["过期清理"] = f"自动删除超过{retention}{'天' if '日' in snapshot_cycle else '版本'}数据"

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        store.channel.publish("snapshot.strategy_created", {
            "strategy_name": entry["策略名称"],
            "target_table": target_table,
            "cycle": snapshot_cycle,
            "retention": entry.get("保留份数"),
            "partition_format": partition_fmt,
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "snapshot_plan", "snapshot_strategy",
            f"snapshot_{entry['id']}",
            {"strategy_name": entry["策略名称"], "target_table": target_table, "cycle": snapshot_cycle},
            source_modules=["deploy_pipeline", "snapshot_plan"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return None, message

        target_table = entry.get("目标表", "")

        if action == "启用策略":
            # 创建初始快照
            cycle = entry.get("快照周期", "每日")
            initial_snap = _create_initial_snapshot(entry)
            # 把初始分区登记进快照保留记录，纳入按目标表+快照编号的留存核算
            manager = SnapshotRetentionManager(store)
            partition = _current_partition(entry.get("分区格式", "ds=yyyyMMdd"))
            manager.register_partition(
                target_table, partition, rows=[],
                version_label="v1.0", entry_id=entry_id, snap_id=initial_snap,
            )
            store.channel.publish("snapshot.enabled", {
                "strategy_name": entry["策略名称"],
                "target_table": target_table,
                "cycle": cycle,
                "initial_snapshot": initial_snap,
                "entry_id": entry_id,
            })

            # 注册恢复检查点
            cp_id = store.recovery.create_checkpoint("snapshot_plan", f"snap_{entry_id}", {
                "target_table": target_table,
                "cycle": cycle,
                "phase": "enabled",
            })
            store.recovery.mark_completed(cp_id)

        elif action == "暂停策略":
            store.channel.publish("snapshot.paused", {
                "strategy_name": entry["策略名称"],
                "target_table": target_table,
                "paused_at": datetime.now(timezone.utc).isoformat(),
                "entry_id": entry_id,
            })

        return entry, message

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 快照周期分布
        cycle_dist: dict[str, int] = {}
        for row in store.rows(self.MODULE):
            c = row.get("快照周期", "未指定")
            cycle_dist[c] = cycle_dist.get(c, 0) + 1
        base["cycle_distribution"] = cycle_dist

        # 增量快照统计
        incremental = sum(1 for row in store.rows(self.MODULE) if row.get("增量标记") == "是")
        base["incremental_snapshots"] = incremental

        # 消费的部署发布事件
        base["consumed_deploys"] = len(store.channel.consume("deploy.published", "snapshot_plan"))

        # 自动快照统计
        snapshot_info = store.snapshot.snapshot_summary()
        base["auto_snapshots"] = snapshot_info.get("total_snapshots", 0)

        # 快照留存核算：释放的分区与仍可回退的版本
        base["retention"] = store.retention.summary()
        return base


def _infer_partition_format(cycle: str) -> str:
    """根据快照周期推断分区格式。"""
    mapping = {"每小时": "hour=yyyyMMddHH", "每日": "ds=yyyyMMdd",
               "每周": "week=yyyyWW", "每月": "month=yyyyMM",
               "每次变更": "version=yyyyMMddHHmmss"}
    return mapping.get(cycle, "ds=yyyyMMdd")


def _calculate_retention(cycle: str) -> str:
    """根据快照周期计算合理的保留份数。"""
    mapping = {"每次变更": "365", "每小时": "168", "每日": "30",
               "每周": "52", "每月": "36", "每季度": "12"}
    return mapping.get(cycle, "30")


def _create_initial_snapshot(entry: dict[str, Any]) -> str:
    """为快照策略创建初始数据快照。"""
    target_table = entry.get("目标表", "")
    snap_data = {
        "target_table": target_table,
        "snapshot_version": "v1.0",
        "created_by": "snapshot_plan_enabled",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    snap_id = store.snapshot.create_snapshot(
        "snapshot_plan", entry.get("id"),
        snap_data,
        "init"
    )
    return snap_id


def _current_partition(partition_fmt: str) -> str:
    """按分区格式生成当前周期的分区标识。"""
    now = datetime.now(timezone.utc)
    if partition_fmt.startswith("hour="):
        return now.strftime("hour=%Y%m%d%H")
    if partition_fmt.startswith("week="):
        return now.strftime("week=%Y%W")
    if partition_fmt.startswith("month="):
        return now.strftime("month=%Y%m")
    if partition_fmt.startswith("year="):
        return now.strftime("year=%Y")
    if partition_fmt.startswith("version="):
        return now.strftime("version=%Y%m%d%H%M%S")
    return now.strftime("ds=%Y%m%d")