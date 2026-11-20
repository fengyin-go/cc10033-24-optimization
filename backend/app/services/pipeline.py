"""数据流管线服务：提供维度建模核心业务操作。
贯通链路：
  dim_definition → dim_attr → scd_handler → surrogate_key → slow_change_log
  fact_design → measure_config → star_query → agg_summary
  etl_mapping → deploy_pipeline → model_validate
  hierarchy_build → dim_bridge → cube_builder
  conform_dim → dim_definition 关联约束
  snapshot_plan → data_vault → model_lineage
  date_dim_gen → fact_design 外键关联
  snapshot_plan 留存清理 ↔ 回滚协调（SnapshotRetentionManager）
"""
from __future__ import annotations

import time
import uuid
from typing import Any, Optional

from app.store import SnapshotReadError

# ---------------------------------------------------------------------------
# 1. SCD 类型 2 处理
# ---------------------------------------------------------------------------


def scd_type2_handle(dimension_key: str, new_attrs: dict[str, Any], store: Any) -> dict[str, Any]:
    """对某个维度执行 SCD Type 2 变更：旧记录失效，新记录生效。

    1. 查找该维度当前生效记录（current_marker=1）
    2. 标记旧记录失效（current_marker=0, end_date=now）
    3. 插入新记录（current_marker=1, start_date=now）
    """
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    result: dict[str, Any] = {"dimension_key": dimension_key, "action": "scd2_change"}

    # 在内存 store 中查找属于该维度的记录
    dim_table = store.rows("dim_definition")
    old_records = [
        r for r in dim_table
        if str(r.get("维度编码", "")) == dimension_key and str(r.get("current_marker", "1")) == "1"
    ]

    expired_count = 0
    for old in old_records:
        old["current_marker"] = "0"
        old["失效时间"] = now_str
        old["status"] = "已停用"
        expired_count += 1

    # 插入新记录
    new_id = max((int(r.get("id", 0)) for r in dim_table), default=0) + 1
    new_row = {
        "id": new_id,
        "维度编码": dimension_key,
        "status": "已发布",
        "pending": True,
        "abnormal": False,
    }
    # 合并新属性
    new_row.update({k: str(v) for k, v in new_attrs.items()})
    new_row["current_marker"] = "1"
    new_row["生效时间"] = now_str
    new_row["失效时间"] = "9999-12-31 23:59:59"
    dim_table.append(new_row)

    # 记入变更日志
    log_table = store.rows("slow_change_log")
    log_id = max((int(r.get("id", 0)) for r in log_table), default=0) + 1
    changed_fields = ", ".join(new_attrs.keys())
    log_table.append({
        "id": log_id,
        "变更ID": f"SCD-{uuid.uuid4().hex[:8].upper()}",
        "目标维度": dimension_key,
        "变更字段": changed_fields,
        "旧值": str([r.get(f, "") for f in new_attrs for r in old_records if r.get(f)]) if old_records else "N/A",
        "新值": str(list(new_attrs.values())),
        "变更时间": now_str,
        "变更原因": f"SCD Type 2 变更: {changed_fields}",
        "status": "已记录",
        "pending": True,
        "abnormal": False,
    })

    result["expired_count"] = expired_count
    result["new_id"] = new_id
    result["log_id"] = log_id
    return result


# ---------------------------------------------------------------------------
# 2. 代理键分配与冲突处理
# ---------------------------------------------------------------------------

class SurrogateKeyAllocator:
    """代理键生成器：支持顺序递增与键段预分配，检测冲突并重新分配。"""

    def __init__(self, store: Any) -> None:
        self._store = store
        self._counters: dict[str, int] = {}
        self._assigned: dict[str, set[int]] = {}

    def allocate(self, dimension_key: str, strategy: str = "sequence") -> int:
        """为指定维度分配一个代理键。

        顺序模式：从 counter 递增
        键段模式：从 seed 数据中读取初始值和步长
        冲突检测：已分配的键不回退重用
        """
        table = self._store.rows("surrogate_key")
        configs = [r for r in table if str(r.get("目标维度", "")) == dimension_key]

        if strategy == "sequence":
            if dimension_key not in self._counters:
                start = 1
                for cfg in configs:
                    try:
                        start = int(cfg.get("初始值", "1"))
                    except (ValueError, TypeError):
                        start = 1
                self._counters[dimension_key] = start
                self._assigned.setdefault(dimension_key, set())

            key = self._counters[dimension_key]
            self._counters[dimension_key] += 1
            self._assigned[dimension_key].add(key)
            return key

        elif strategy == "segment":
            step = 1000
            for cfg in configs:
                try:
                    step = int(cfg.get("步长", "1000"))
                except (ValueError, TypeError):
                    pass
            if dimension_key not in self._counters:
                self._counters[dimension_key] = 10000
            key = self._counters[dimension_key]
            self._counters[dimension_key] += step
            self._assigned.setdefault(dimension_key, set()).add(key)
            return key

        return 0

    def handle_conflict(self, dimension_key: str, conflicting_key: int) -> int:
        """发生代理键冲突时的处理：将冲突键标记后重新分配下一个键。"""
        if dimension_key not in self._counters:
            self._counters[dimension_key] = 100000
        new_key = self._counters[dimension_key]
        self._counters[dimension_key] += 1
        return new_key

    def get_assigned_keys(self, dimension_key: str) -> list[int]:
        return sorted(self._assigned.get(dimension_key, set()))


# ---------------------------------------------------------------------------
# 3. 事实表分区写入与维度外键完整性校验
# ---------------------------------------------------------------------------


def fact_partition_write(
    fact_key: str,
    records: list[dict[str, Any]],
    partition_field: str,
    dim_fk_fields: dict[str, str],
    store: Any,
) -> dict[str, Any]:
    """向事实表写入记录，同时校验维度外键完整性。

    Args:
        fact_key: 事实表标识
        records: 待写入的事实记录列表
        partition_field: 分区字段名
        dim_fk_fields: 维度外键映射 {field_name: dim_key}
        store: 数据仓库实例

    Returns:
        {wrote: int, violations: list, partitions: dict}
    """
    result: dict[str, Any] = {"wrote": 0, "violations": [], "partitions": {}}

    # 收集维度有效 keys
    dim_table = store.rows("dim_definition")
    valid_dim_keys: dict[str, set[str]] = {}
    for field_name, dim_key in dim_fk_fields.items():
        valid = {
            str(r.get("维度编码", ""))
            for r in dim_table
            if str(r.get("current_marker", "1")) == "1"
        }
        valid_dim_keys[field_name] = valid

    fact_table = store.rows("fact_design")
    max_id = max((int(r.get("id", 0)) for r in fact_table), default=0)

    for rec in records:
        violations = []
        for field_name, dim_key in dim_fk_fields.items():
            fk_value = str(rec.get(field_name, ""))
            valid_keys = valid_dim_keys.get(field_name, set())
            if fk_value and fk_value not in valid_keys:
                violations.append({
                    "field": field_name,
                    "value": fk_value,
                    "reason": f"维度 {dim_key} 不存在有效键值 {fk_value}",
                })

        if violations:
            result["violations"].extend(violations)
            continue

        max_id += 1
        partition_value = str(rec.get(partition_field, "default"))
        result["partitions"].setdefault(partition_value, 0)
        result["partitions"][partition_value] += 1

        row = {
            "id": max_id,
            "事实名称": fact_key,
            "status": "已设计",
            "pending": True,
            "abnormal": False,
        }
        row.update({k: str(v) for k, v in rec.items()})
        fact_table.append(row)
        result["wrote"] += 1

    return result


# ---------------------------------------------------------------------------
# 4. 汇总表构建与增量刷新
# ---------------------------------------------------------------------------

class AggregateBuilder:
    """汇总表构建器：基于事实表上卷维度、聚合度量。支持全量构建与增量刷新。"""

    def __init__(self, store: Any) -> None:
        self._store = store
        self._watermarks: dict[str, int] = {}

    def build(self, agg_name: str, source_fact: str, rollup_dims: list[str],
              agg_measures: list[tuple[str, str]], grain: str) -> dict[str, Any]:
        """全量构建汇总表。

        Args:
            agg_name: 汇总表名称
            source_fact: 源事实表名称
            rollup_dims: 上卷维度列表
            agg_measures: 聚合度量 [(measure_name, agg_func)]
            grain: 汇总粒度
        """
        fact_table = self._store.rows("fact_design")
        source_rows = [r for r in fact_table if str(r.get("事实名称", "")) == source_fact]

        result = self._compute_aggregation(source_rows, rollup_dims, agg_measures)

        # 写入汇总表
        agg_table = self._store.rows("agg_summary")
        max_id = max((int(r.get("id", 0)) for r in agg_table), default=0)
        for group_key, measures in result.items():
            max_id += 1
            row = {
                "id": max_id,
                "汇总名称": agg_name,
                "源事实表": source_fact,
                "汇总维度": ", ".join(rollup_dims),
                "汇总度量": str(measures),
                "汇总粒度": grain,
                "刷新周期": "T+1",
                "增量标记": "full",
                "status": "已构建",
                "pending": False,
                "abnormal": False,
            }
            agg_table.append(row)

        # 更新水位
        self._watermarks[agg_name] = max_id
        return {"built_groups": len(result), "total_rows": max_id, "mode": "full"}

    def refresh_incremental(self, agg_name: str, source_fact: str,
                            rollup_dims: list[str], agg_measures: list[tuple[str, str]],
                            since_id: int) -> dict[str, Any]:
        """增量刷新汇总表。

        只处理 id > since_id 的新增事实记录，追加汇总结果。
        """
        fact_table = self._store.rows("fact_design")
        new_rows = [
            r for r in fact_table
            if str(r.get("事实名称", "")) == source_fact
            and int(r.get("id", 0)) > since_id
        ]

        if not new_rows:
            return {"built_groups": 0, "new_rows": 0, "mode": "incremental"}

        result = self._compute_aggregation(new_rows, rollup_dims, agg_measures)

        agg_table = self._store.rows("agg_summary")
        max_id = max((int(r.get("id", 0)) for r in agg_table), default=0)
        for group_key, measures in result.items():
            max_id += 1
            row = {
                "id": max_id,
                "汇总名称": agg_name,
                "源事实表": source_fact,
                "汇总维度": ", ".join(rollup_dims),
                "汇总度量": str(measures),
                "增量标记": "incremental",
                "status": "已刷新",
                "pending": False,
                "abnormal": False,
            }
            agg_table.append(row)

        self._watermarks[agg_name] = max_id
        return {"built_groups": len(result), "new_rows": len(new_rows), "mode": "incremental"}

    def _compute_aggregation(
        self,
        rows: list[dict[str, Any]],
        rollup_dims: list[str],
        agg_measures: list[tuple[str, str]],
    ) -> dict[str, dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        for row in rows:
            key_parts = [str(row.get(d, "N/A")) for d in rollup_dims]
            key = "|".join(key_parts)
            if key not in groups:
                groups[key] = {d: row.get(d, "") for d in rollup_dims}
                for m_name, _ in agg_measures:
                    groups[key][f"_{m_name}_cnt"] = 0.0
                    groups[key][f"_{m_name}_sum"] = 0.0
                groups[key]["_row_count"] = 0

            groups[key]["_row_count"] += 1
            for m_name, agg_func in agg_measures:
                try:
                    raw_val = float(str(row.get(m_name, "0")).replace(",", ""))
                except (ValueError, TypeError):
                    raw_val = 0.0

                if agg_func in ("sum", "SUM"):
                    groups[key][f"_{m_name}_sum"] += raw_val
                if agg_func in ("count", "COUNT"):
                    groups[key][f"_{m_name}_cnt"] += 1
                if agg_func in ("avg", "AVG"):
                    groups[key][f"_{m_name}_cnt"] += 1
                    groups[key][f"_{m_name}_sum"] += raw_val

            # 计算 AVG
            for m_name, agg_func in agg_measures:
                if agg_func in ("avg", "AVG") and groups[key][f"_{m_name}_cnt"] > 0:
                    groups[key][f"{m_name}_avg"] = (
                        groups[key][f"_{m_name}_sum"] / groups[key][f"_{m_name}_cnt"]
                    )

        return groups


# ---------------------------------------------------------------------------
# 5. 模型血缘影响分析
# ---------------------------------------------------------------------------


def model_lineage_impact(
    changed_model: str,
    changed_fields: list[str],
    store: Any,
) -> dict[str, Any]:
    """分析模型变更对下游的影响范围。

    遍历 model_lineage 表，找到所有下游依赖，按影响级别分类。
    返回下游模型列表和受影响字段。
    """
    lineage_table = store.rows("model_lineage")
    affected: list[dict[str, Any]] = []

    for rel in lineage_table:
        upstream = str(rel.get("上游模型", ""))
        if upstream == changed_model or any(
            f in str(rel.get("依赖字段", "")) for f in changed_fields
        ):
            impact = {
                "relationship_id": rel.get("关系ID", rel.get("id", "")),
                "downstream": rel.get("下游模型", ""),
                "dependency": rel.get("依赖字段", ""),
                "type": rel.get("关联类型", "direct"),
                "level": rel.get("影响级别", "medium"),
                "current_status": rel.get("更新状态", rel.get("status", "")),
            }

            # 更新影响状态
            rel["status"] = "变更中"
            rel["更新状态"] = f"受 {changed_model} 变更影响"
            affected.append(impact)

    # 计算影响摘要
    severe = [a for a in affected if a.get("level") == "high"]
    moderate = [a for a in affected if a.get("level") == "medium"]

    return {
        "changed_model": changed_model,
        "changed_fields": changed_fields,
        "affected_count": len(affected),
        "severe_impact": len(severe),
        "moderate_impact": len(moderate),
        "downstream_models": list({a["downstream"] for a in affected}),
        "details": affected,
    }


# ---------------------------------------------------------------------------
# 6. 维度定义关联约束（一致性维度）
# ---------------------------------------------------------------------------


def conform_dim_constrain(
    conform_name: str,
    required_dim_keys: list[str],
    store: Any,
) -> dict[str, Any]:
    """将一致性维度标准与维度定义做关联约束校验。

    检查所有 dim_definition 记录是否符合一致性维度标准。
    """
    dim_table = store.rows("dim_definition")
    conform_table = store.rows("conform_dim")

    conform_entry = None
    for c in conform_table:
        if str(c.get("标准名称", "")) == conform_name:
            conform_entry = c
            break

    if not conform_entry:
        return {"error": f"未找到一致性维度 {conform_name}"}

    violations: list[dict[str, Any]] = []
    passed: list[str] = []

    for dim in dim_table:
        dim_key = str(dim.get("维度编码", ""))
        if dim_key in required_dim_keys:
            dim_name = str(dim.get("维度名称", ""))
            standard_key = str(conform_entry.get("标准键值", ""))
            if dim_name and standard_key and dim_name != standard_key:
                violations.append({
                    "dim_key": dim_key,
                    "dim_name": dim_name,
                    "expected": standard_key,
                    "reason": f"维度名称与一致性标准 {conform_name} 不一致",
                })
            else:
                passed.append(dim_key)

    return {
        "conform_name": conform_name,
        "total_checked": len(required_dim_keys),
        "passed": len(passed),
        "violations": len(violations),
        "violation_details": violations,
    }


# ---------------------------------------------------------------------------
# 7. 日期维度 → 事实设计外键关联
# ---------------------------------------------------------------------------


def date_dim_to_fact_fk(
    fact_name: str,
    date_fk_field: str,
    store: Any,
) -> dict[str, Any]:
    """将日期维度表的外键关联到事实表中。

    为指定事实表添加日期维度外键约束，记录在 date_dim_gen 模块中。
    """
    fact_table = store.rows("fact_design")
    date_table = store.rows("date_dim_gen")

    # 查找目标事实表
    target_facts = [r for r in fact_table if str(r.get("事实名称", "")) == fact_name]
    if not target_facts:
        return {"error": f"未找到事实表 {fact_name}"}

    # 可用的日期键
    date_keys = [str(r.get("日期键", "")) for r in date_table if r.get("日期键")]

    for fact in target_facts:
        existing_dims = str(fact.get("关联维度", ""))
        if date_fk_field not in existing_dims:
            fact["关联维度"] = f"{existing_dims},{date_fk_field}" if existing_dims else date_fk_field

    return {
        "fact_name": fact_name,
        "date_fk_field": date_fk_field,
        "linked_facts": len(target_facts),
        "available_date_keys": len(date_keys),
    }


# ---------------------------------------------------------------------------
# 8. 快照留存清理与回滚协调
# ---------------------------------------------------------------------------


class SnapshotRetentionManager:
    """快照留存与回滚协调器：解决过期清理与回滚并发冲突。

    - 留存按 (目标表, 快照编号) 核算：清理只释放核算出的超量编号，
      不按日期范围删除，按策略重复清理也不会误删相邻日期的新分区
    - 分区超出留存周期也须先核对回滚任务的有效引用，被引用分区本轮跳过
    - 回滚读取目标表关联的快照保留记录判定回写资格；
      自有快照已释放时，回退到同一目标表仍存的兼容快照
    - 清理留下实际释放的编号，回滚留下实际引用的编号；
      回滚确认后实际使用的快照编号与分区必须还能查到，且快照可读
    """

    def __init__(self, store: Any) -> None:
        self._store = store

    # ------------------------------------------------------------------
    # 登记快照分区
    # ------------------------------------------------------------------
    def register_partition(
        self,
        target_table: str,
        partition: str,
        rows: list[dict[str, Any]] | None = None,
        *,
        version_label: str = "v1.0",
        compat_group: str | None = None,
        module: str = "snapshot_plan",
        entry_id: int = 0,
        snap_id: str | None = None,
    ) -> dict[str, Any]:
        """登记快照分区：创建引擎快照并写入快照保留记录，返回留存记录。"""
        ledger = self._store.retention
        snapshot_no = ledger.next_snapshot_no(target_table)
        if snap_id is None:
            snap_id = self._store.snapshot.create_snapshot(module, entry_id, {
                "target_table": target_table,
                "partition": partition,
                "snapshot_no": snapshot_no,
                "rows": [dict(r) for r in (rows or [])],
            }, version_label)
        record = ledger.register(
            target_table=target_table,
            snapshot_no=snapshot_no,
            partition=partition,
            snap_id=snap_id,
            version_label=version_label,
            compat_group=compat_group or version_label,
        )
        self._store.channel.publish("snapshot.partition_registered", {
            "target_table": target_table,
            "partition": partition,
            "snapshot_no": snapshot_no,
            "record_id": record["record_id"],
        })
        return record

    # ------------------------------------------------------------------
    # 过期清理：按保留份数核算释放
    # ------------------------------------------------------------------
    def cleanup_expired(
        self,
        target_table: str,
        retention_count: int | str,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        """按保留份数清理目标表超量的旧分区。

        只按快照编号顺序核算超量的现存 active 记录并逐条释放：
        已释放记录不会重复释放，后登记的新分区（包括相邻日期的回填分区）
        编号更新，不会被按日期范围误删。释放前先核对回滚任务有效引用。
        """
        ledger = self._store.retention
        run_task = task_id or "cleanup"
        try:
            keep = max(0, int(retention_count))
        except (TypeError, ValueError):
            keep = 0

        retained = ledger.for_table(target_table, states=set(ledger.RETAINED_STATES))
        excess = max(0, len(retained) - keep)
        candidates = retained[:excess]  # 最旧的快照编号先进入清理范围

        released: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for record in candidates:
            refs = ledger.valid_rollback_refs(record)
            if refs:
                skipped.append({
                    "snapshot_no": record["snapshot_no"],
                    "partition": record["partition"],
                    "refs": [t["task_id"] for t in refs],
                })
                continue
            ledger.mark_released(record["record_id"], released_by=run_task)
            released.append(record)

        kept = [r["snapshot_no"] for r in ledger.for_table(
            target_table, states=set(ledger.RETAINED_STATES))]
        run = ledger.log_cleanup(
            target_table=target_table,
            task_id=run_task,
            released=released,
            skipped_referenced=skipped,
            kept_snapshot_nos=kept,
        )
        self._store.channel.publish("snapshot.cleanup_done", {
            "target_table": target_table,
            "run_id": run["run_id"],
            "released_snapshot_nos": run["released_snapshot_nos"],
            "skipped": len(skipped),
        })
        return {
            "run_id": run["run_id"],
            "target_table": target_table,
            "released_snapshot_nos": run["released_snapshot_nos"],
            "released_partitions": run["released_partitions"],
            "skipped_referenced": skipped,
            "kept_snapshot_nos": kept,
        }

    # ------------------------------------------------------------------
    # 回滚：请求校验与确认回写
    # ------------------------------------------------------------------
    def request_rollback(
        self,
        target_table: str,
        partition: str,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        """回滚请求校验：读取目标表关联的快照保留记录，判定回写资格。

        有资格则登记任务对快照编号的引用（供清理核对），
        无可用版本则任务阻塞、分区维持待回退。
        """
        ledger = self._store.retention
        task = ledger.create_rollback_task(target_table, partition, task_id=task_id)
        record = ledger.find_by_partition(target_table, partition)
        resolution = self._resolve_snapshot(record, target_table)
        if resolution is None:
            ledger.update_task(
                task["task_id"], status="blocked", attempts=1,
                last_error="无可用快照版本",
            )
            self._store.recovery.log_retry(
                "snapshot_plan", task["task_id"], 1, "无可用快照版本", False)
        else:
            used_rec, snap_no = resolution
            ledger.add_rollback_ref(used_rec["record_id"], task["task_id"])
            if record is not None and record["record_id"] != used_rec["record_id"]:
                ledger.add_rollback_ref(record["record_id"], task["task_id"])
            ledger.update_task(
                task["task_id"], status="eligible", attempts=1,
                planned_snapshot_no=snap_no,
            )
        return dict(ledger.get_task(task["task_id"]))

    def confirm_rollback(self, task_id: str) -> dict[str, Any]:
        """回滚确认：校验实际使用的快照编号与分区仍可查到且快照可读，然后回写分区。

        校验失败时任务阻塞、分区维持待回退，可修复后重试。
        """
        ledger = self._store.retention
        task = ledger.get_task(task_id)
        if task is None:
            return {"ok": False, "status": "missing_task",
                    "message": f"回滚任务 {task_id} 不存在"}
        if task["status"] == "rolled_back":
            return {"ok": True, "status": "rolled_back", "task_id": task_id,
                    "used_snapshot_no": task["used_snapshot_no"],
                    "partition": task["partition"], "idempotent": True}

        target_table = task["target_table"]
        partition = task["partition"]
        record = ledger.find_by_partition(target_table, partition)
        resolution = self._resolve_snapshot(
            record, target_table,
            prefer_snapshot_no=task.get("planned_snapshot_no"),
        )
        if resolution is None:
            return self._block_task(task, "无可用快照版本或快照不可读")

        used_rec, snap_no = resolution
        try:
            snap = self._store.snapshot.read_snapshot(used_rec["snap_id"])
        except SnapshotReadError as exc:
            return self._block_task(task, str(exc))

        rows = [dict(r) for r in snap["data"].get("rows", [])]
        written = self._write_back_partition(target_table, partition, rows)

        # 留痕：回滚实际引用的快照编号
        ledger.add_rollback_ref(used_rec["record_id"], task_id)
        if record is not None and record["record_id"] != used_rec["record_id"]:
            ledger.add_rollback_ref(record["record_id"], task_id)
            ledger.mark_rolled_back(
                record["record_id"], task_id=task_id, used_snapshot_no=snap_no)
        elif record is not None:
            ledger.mark_rolled_back(
                record["record_id"], task_id=task_id, used_snapshot_no=snap_no)
        ledger.update_task(
            task_id, status="rolled_back", attempts=task["attempts"] + 1,
            used_snapshot_no=snap_no, last_error=None,
        )
        self._store.recovery.log_retry(
            "snapshot_plan", task_id, task["attempts"] + 1, "回滚确认", True)
        self._store.channel.publish("snapshot.rollback_confirmed", {
            "target_table": target_table,
            "partition": partition,
            "used_snapshot_no": snap_no,
            "task_id": task_id,
        })
        return {
            "ok": True,
            "status": "rolled_back",
            "task_id": task_id,
            "used_snapshot_no": snap_no,
            "partition": partition,
            "written_rows": written,
        }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def retention_records(self, target_table: str) -> list[dict[str, Any]]:
        """目标表关联的全部快照保留记录（按快照编号顺序）。"""
        return self._store.retention.for_table(target_table)

    def pending_rollback_partitions(self, target_table: str | None = None) -> list[str]:
        """仍待回退的分区（存在未终结回滚任务）。"""
        return self._store.retention.pending_rollback_partitions(target_table)

    def rollback_task(self, task_id: str) -> dict[str, Any] | None:
        return self._store.retention.get_task(task_id)

    def cleanup_history(self, target_table: str | None = None) -> list[dict[str, Any]]:
        return self._store.retention.cleanup_history(target_table)

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------
    def _resolve_snapshot(
        self,
        record: dict[str, Any] | None,
        target_table: str,
        prefer_snapshot_no: str | None = None,
    ) -> tuple[dict[str, Any], str] | None:
        """判定回写资格并选定实际使用的快照编号。

        顺序：请求阶段已计划的编号 → 分区自有仍可回退版本 → 同表兼容快照。
        分区从未登记（缺失）时不予回写，维持待回退。
        """
        ledger = self._store.retention
        if record is None:
            return None
        retained = set(ledger.RETAINED_STATES)
        if prefer_snapshot_no:
            preferred = ledger.find_by_snapshot_no(target_table, prefer_snapshot_no)
            if (preferred is not None and preferred["state"] in retained
                    and self._store.snapshot.is_readable(preferred["snap_id"])):
                return preferred, preferred["snapshot_no"]
        if record["state"] in retained and self._store.snapshot.is_readable(record["snap_id"]):
            return record, record["snapshot_no"]
        compat_group = record.get("compat_group")
        for cand in ledger.for_table(target_table, states=retained):
            if cand["record_id"] == record["record_id"]:
                continue
            if compat_group and cand.get("compat_group") != compat_group:
                continue
            if self._store.snapshot.is_readable(cand["snap_id"]):
                return cand, cand["snapshot_no"]
        return None

    def _block_task(self, task: dict[str, Any], reason: str) -> dict[str, Any]:
        """回滚受阻：任务阻塞并记录重试，分区维持待回退。"""
        ledger = self._store.retention
        attempts = task["attempts"] + 1
        ledger.update_task(
            task["task_id"], status="blocked", attempts=attempts, last_error=reason)
        self._store.recovery.log_retry(
            "snapshot_plan", task["task_id"], attempts, reason, False)
        return {
            "ok": False,
            "status": "pending_rollback",
            "task_id": task["task_id"],
            "partition": task["partition"],
            "reason": reason,
            "pending_rollback_partitions": ledger.pending_rollback_partitions(
                task["target_table"]),
        }

    def _write_back_partition(
        self,
        target_table: str,
        partition: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """把快照中的行回写到目标表的指定分区。"""
        table = self._store.rows(target_table)
        kept = [r for r in table if r.get("partition") != partition]
        now_str = time.strftime("%Y-%m-%d %H:%M:%S")
        for row in rows:
            restored = dict(row)
            restored["partition"] = partition
            restored["_rolled_back_at"] = now_str
            kept.append(restored)
        table[:] = kept
        return len(rows)