"""内存数据仓库：跨模块数据通道、失败恢复、快照引擎和产物注册。

各业务模块产生的数据产物通过 DataChannel 发布，下游模块消费；
FailureRecovery 管理断点恢复；SnapshotEngine 提供版本快照能力。
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS


class DataChannel:
    """跨模块数据通道：上游模块发布数据产物，下游模块通过 topic 消费。"""

    def __init__(self) -> None:
        self._channels: dict[str, list[dict[str, Any]]] = {}
        self._consumers: dict[str, list[str]] = {}

    def publish(self, topic: str, payload: dict[str, Any]) -> str:
        """将数据产物发布到指定 topic，返回消息ID。"""
        msg_id = str(uuid.uuid4())[:12]
        entry = {
            "msg_id": msg_id,
            "topic": topic,
            "payload": payload,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "consumed_by": [],
            "expired": False,
        }
        self._channels.setdefault(topic, []).append(entry)
        return msg_id

    def consume(self, topic: str, consumer: str) -> list[dict[str, Any]]:
        """消费指定 topic 上未被当前消费者消费过的消息，标记已消费。"""
        channel = self._channels.get(topic, [])
        results: list[dict[str, Any]] = []
        for entry in channel:
            if consumer not in entry["consumed_by"] and not entry["expired"]:
                entry["consumed_by"].append(consumer)
                results.append(entry)
        return results

    def consume_latest(self, topic: str, consumer: str) -> dict[str, Any] | None:
        """消费指定 topic 上最新一条未被消费的消息。"""
        channel = self._channels.get(topic, [])
        for entry in reversed(channel):
            if consumer not in entry["consumed_by"] and not entry["expired"]:
                entry["consumed_by"].append(consumer)
                return entry
        return None

    def topic_snapshot(self, topic: str) -> list[dict[str, Any]]:
        """获取 topic 上所有消息的快照。"""
        return list(self._channels.get(topic, []))

    def expire_topic(self, topic: str) -> int:
        """过期指定 topic 上所有未消费消息，返回过期数量。"""
        count = 0
        for entry in self._channels.get(topic, []):
            if not entry["expired"]:
                entry["expired"] = True
                count += 1
        return count

    def list_topics(self) -> list[str]:
        return sorted(self._channels)

    def topic_stats(self) -> dict[str, dict[str, int]]:
        stats: dict[str, dict[str, int]] = {}
        for topic, entries in self._channels.items():
            consumed = sum(1 for e in entries if len(e["consumed_by"]) > 0)
            pending = len(entries) - consumed
            expired = sum(1 for e in entries if e["expired"])
            stats[topic] = {"total": len(entries), "consumed": consumed, "pending": pending, "expired": expired}
        return stats


class FailureRecovery:
    """失败恢复管理器：检查点管理、重试计数和失败链路追踪。"""

    def __init__(self) -> None:
        self._checkpoints: dict[str, dict[str, Any]] = {}
        self._retry_logs: dict[str, list[dict[str, Any]]] = {}
        self._failure_chains: dict[str, list[dict[str, Any]]] = {}

    def create_checkpoint(self, module: str, task_id: str, state: dict[str, Any]) -> str:
        """创建处理检查点，返回检查点ID。"""
        cp_id = f"{module}-{task_id}-{uuid.uuid4().hex[:8]}"
        self._checkpoints[cp_id] = {
            "cp_id": cp_id,
            "module": module,
            "task_id": task_id,
            "state": dict(state),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "restored_at": None,
            "restore_count": 0,
            "completed": False,
        }
        return cp_id

    def get_checkpoint(self, cp_id: str) -> dict[str, Any] | None:
        return self._checkpoints.get(cp_id)

    def update_checkpoint(self, cp_id: str, state: dict[str, Any]) -> bool:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return False
        cp["state"].update(state)
        return True

    def mark_completed(self, cp_id: str) -> bool:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return False
        cp["completed"] = True
        return True

    def restore_checkpoint(self, cp_id: str) -> dict[str, Any] | None:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return None
        cp["restored_at"] = datetime.now(timezone.utc).isoformat()
        cp["restore_count"] += 1
        return dict(cp)

    def log_retry(self, module: str, task_id: str, attempt: int, reason: str, success: bool) -> None:
        self._retry_logs.setdefault(module, [])
        self._retry_logs[module].append({
            "task_id": task_id,
            "attempt": attempt,
            "reason": reason,
            "success": success,
            "logged_at": datetime.now(timezone.utc).isoformat(),
        })

    def retry_history(self, module: str) -> list[dict[str, Any]]:
        return self._retry_logs.get(module, [])

    def log_failure_chain(self, chain_id: str, step: str, module: str, error: str, upstream: str | None = None) -> None:
        self._failure_chains.setdefault(chain_id, [])
        self._failure_chains[chain_id].append({
            "step": step,
            "module": module,
            "error": error,
            "upstream": upstream,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def chain_summary(self, chain_id: str) -> list[dict[str, Any]]:
        return self._failure_chains.get(chain_id, [])

    def recovery_summary(self) -> dict[str, Any]:
        completed = sum(1 for cp in self._checkpoints.values() if cp["completed"])
        pending = len(self._checkpoints) - completed
        total_retries = sum(len(logs) for logs in self._retry_logs.values())
        total_chains = len(self._failure_chains)
        return {
            "checkpoints": {"total": len(self._checkpoints), "completed": completed, "pending": pending},
            "retries": total_retries,
            "failure_chains": total_chains,
        }


class SnapshotReadError(RuntimeError):
    """快照读取异常：快照不存在、已被清理释放或当前不可读。"""


class SnapshotEngine:
    """版本快照引擎：创建、比较和管理数据快照。"""

    def __init__(self) -> None:
        self._snapshots: dict[str, dict[str, Any]] = {}
        self._diff_logs: dict[str, dict[str, Any]] = {}
        self._unreadable: set[str] = set()

    def create_snapshot(self, module: str, entry_id: int, data: dict[str, Any], version_label: str = "") -> str:
        """创建数据快照，返回快照ID。"""
        data_str = str(sorted(data.items()))
        content_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
        snap_id = f"{module}-{entry_id}-v{version_label}-{uuid.uuid4().hex[:8]}"
        self._snapshots[snap_id] = {
            "snap_id": snap_id,
            "module": module,
            "entry_id": entry_id,
            "version_label": version_label,
            "data": dict(data),
            "content_hash": content_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "parent_snap_id": None,
            "retention_policy": "keep_forever",
        }
        return snap_id

    def create_version_snapshot(self, module: str, entry_id: int, data: dict[str, Any],
                                version_label: str, parent_snap_id: str | None = None) -> str:
        """创建带父子关系的版本快照，用于模型发布版本管理。"""
        data_str = str(sorted(data.items()))
        content_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
        snap_id = f"{module}-{entry_id}-{version_label}-{uuid.uuid4().hex[:8]}"
        self._snapshots[snap_id] = {
            "snap_id": snap_id,
            "module": module,
            "entry_id": entry_id,
            "version_label": version_label,
            "data": dict(data),
            "content_hash": content_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "parent_snap_id": parent_snap_id,
            "retention_policy": "keep_forever",
        }
        return snap_id

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshots.get(snap_id)

    def mark_unreadable(self, snap_id: str) -> None:
        """把快照标记为不可读，用于模拟存储侧读取异常。"""
        self._unreadable.add(snap_id)

    def clear_unreadable(self, snap_id: str) -> None:
        """解除快照的不可读标记，模拟读取异常恢复。"""
        self._unreadable.discard(snap_id)

    def read_snapshot(self, snap_id: str) -> dict[str, Any]:
        """严格读取快照：不存在或不可读时抛出 SnapshotReadError。"""
        if snap_id in self._unreadable:
            raise SnapshotReadError(f"快照 {snap_id} 读取异常：当前不可读")
        snap = self._snapshots.get(snap_id)
        if snap is None:
            raise SnapshotReadError(f"快照 {snap_id} 不存在或已释放")
        return snap

    def is_readable(self, snap_id: str) -> bool:
        """快照当前是否可读（存在且未被标记不可读）。"""
        try:
            self.read_snapshot(snap_id)
            return True
        except SnapshotReadError:
            return False

    def list_snapshots(self, module: str, entry_id: int) -> list[dict[str, Any]]:
        return [
            s for s in self._snapshots.values()
            if s["module"] == module and s["entry_id"] == entry_id
        ]

    def diff_snapshots(self, snap_id_a: str, snap_id_b: str) -> dict[str, Any] | None:
        snap_a = self._snapshots.get(snap_id_a)
        snap_b = self._snapshots.get(snap_id_b)
        if not snap_a or not snap_b:
            return None
        keys_a = set(snap_a["data"])
        keys_b = set(snap_b["data"])
        added = {k: snap_b["data"][k] for k in keys_b - keys_a}
        removed = {k: snap_a["data"][k] for k in keys_a - keys_b}
        modified = {}
        for k in keys_a & keys_b:
            if snap_a["data"][k] != snap_b["data"][k]:
                modified[k] = {"old": snap_a["data"][k], "new": snap_b["data"][k]}
        diff_id = f"diff-{uuid.uuid4().hex[:8]}"
        result = {
            "diff_id": diff_id,
            "snap_a": snap_id_a,
            "snap_b": snap_id_b,
            "added_fields": added,
            "removed_fields": removed,
            "modified_fields": modified,
            "total_changes": len(added) + len(removed) + len(modified),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._diff_logs[diff_id] = result
        return result

    def rollback_to_snapshot(self, module: str, entry_id: int, snap_id: str, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        """将数据回滚到指定快照版本。"""
        snap = self._snapshots.get(snap_id)
        if not snap:
            return None
        for row in rows:
            if row.get("id") == entry_id:
                for k, v in snap["data"].items():
                    row[k] = v
                row["_rollback_to"] = snap_id
                row["_rollback_at"] = datetime.now(timezone.utc).isoformat()
                return row
        return None

    def snapshot_summary(self) -> dict[str, Any]:
        by_module: dict[str, int] = {}
        for s in self._snapshots.values():
            by_module[s["module"]] = by_module.get(s["module"], 0) + 1
        return {"total_snapshots": len(self._snapshots), "by_module": by_module, "total_diffs": len(self._diff_logs)}


TERMINAL_TASK_STATES = {"rolled_back", "cancelled"}


def _partition_date(partition: str) -> str:
    """从分区标识中提取日期部分，如 ds=20261031 -> 20261031。"""
    text = str(partition)
    if "=" in text:
        text = text.split("=", 1)[1]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits or str(partition)


class SnapshotRetentionLedger:
    """快照保留记录：按目标表与快照编号核算留存。

    - 每条记录对应一个日期分区的一版快照，状态在
      active（仍可回退）/ released（已释放）/ rolled_back（已回退）间流转
    - 清理把实际释放的快照编号记入记录，回滚把实际引用的编号记入记录
    - 回滚任务登记在册，清理释放前据此核对有效引用
    """

    RECORD_STATES = ("active", "released", "rolled_back")
    # 仍占用留存份额、仍可作为回滚来源的状态（已释放除外）
    RETAINED_STATES = ("active", "rolled_back")

    def __init__(self) -> None:
        self._records: dict[str, dict[str, Any]] = {}
        self._record_seq = 0
        self._snap_seq: dict[str, int] = {}
        self._cleanup_log: list[dict[str, Any]] = []
        self._cleanup_seq = 0
        self._rollback_tasks: dict[str, dict[str, Any]] = {}
        self._task_seq = 0

    # ------------------------------------------------------------------
    # 留存记录
    # ------------------------------------------------------------------
    def next_snapshot_no(self, target_table: str) -> str:
        """按目标表递增分配快照编号。"""
        seq = self._snap_seq.get(target_table, 0) + 1
        self._snap_seq[target_table] = seq
        return f"SNAP-{seq:04d}"

    def register(self, *, target_table: str, snapshot_no: str, partition: str,
                 snap_id: str, version_label: str = "", compat_group: str = "") -> dict[str, Any]:
        """登记一条快照保留记录，初始状态为 active（仍可回退）。"""
        self._record_seq += 1
        record_id = f"RET-{self._record_seq:06d}"
        record = {
            "record_id": record_id,
            "target_table": target_table,
            "snapshot_no": snapshot_no,
            "partition": partition,
            "partition_date": _partition_date(partition),
            "snap_id": snap_id,
            "version_label": version_label,
            "compat_group": compat_group or version_label,
            "state": "active",
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "released_at": None,
            "released_by": None,
            "rolled_back_at": None,
            "used_by_rollback": None,
            "used_snapshot_no": None,
            "rollback_refs": [],
            "seq": self._record_seq,
        }
        self._records[record_id] = record
        return record

    def get(self, record_id: str) -> dict[str, Any] | None:
        return self._records.get(record_id)

    def for_table(self, target_table: str, states: set[str] | None = None) -> list[dict[str, Any]]:
        """按快照编号顺序返回目标表的留存记录。"""
        records = [r for r in self._records.values() if r["target_table"] == target_table]
        if states:
            records = [r for r in records if r["state"] in states]
        return sorted(records, key=lambda r: r["seq"])

    def find_by_snapshot_no(self, target_table: str, snapshot_no: str) -> dict[str, Any] | None:
        for record in self.for_table(target_table):
            if record["snapshot_no"] == snapshot_no:
                return record
        return None

    def find_by_partition(self, target_table: str, partition: str) -> dict[str, Any] | None:
        """返回目标表指定分区最新登记的一条留存记录。"""
        matches = [r for r in self.for_table(target_table) if r["partition"] == partition]
        return matches[-1] if matches else None

    def mark_released(self, record_id: str, released_by: str) -> dict[str, Any] | None:
        record = self._records.get(record_id)
        if record is None:
            return None
        record["state"] = "released"
        record["released_at"] = datetime.now(timezone.utc).isoformat()
        record["released_by"] = released_by
        return record

    def mark_rolled_back(self, record_id: str, *, task_id: str, used_snapshot_no: str) -> dict[str, Any] | None:
        record = self._records.get(record_id)
        if record is None:
            return None
        record["state"] = "rolled_back"
        record["rolled_back_at"] = datetime.now(timezone.utc).isoformat()
        record["used_by_rollback"] = task_id
        record["used_snapshot_no"] = used_snapshot_no
        return record

    def add_rollback_ref(self, record_id: str, task_id: str) -> bool:
        record = self._records.get(record_id)
        if record is None:
            return False
        if task_id not in record["rollback_refs"]:
            record["rollback_refs"].append(task_id)
        return True

    # ------------------------------------------------------------------
    # 回滚任务
    # ------------------------------------------------------------------
    def create_rollback_task(self, target_table: str, partition: str,
                             task_id: str | None = None) -> dict[str, Any]:
        self._task_seq += 1
        task_id = task_id or f"RB-{self._task_seq:04d}"
        task = {
            "task_id": task_id,
            "target_table": target_table,
            "partition": partition,
            "status": "pending",
            "planned_snapshot_no": None,
            "used_snapshot_no": None,
            "attempts": 0,
            "last_error": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": None,
        }
        self._rollback_tasks[task_id] = task
        return task

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        return self._rollback_tasks.get(task_id)

    def update_task(self, task_id: str, **fields: Any) -> dict[str, Any] | None:
        task = self._rollback_tasks.get(task_id)
        if task is None:
            return None
        task.update(fields)
        task["updated_at"] = datetime.now(timezone.utc).isoformat()
        return task

    def tasks_for(self, target_table: str | None = None,
                  statuses: set[str] | None = None) -> list[dict[str, Any]]:
        tasks = list(self._rollback_tasks.values())
        if target_table:
            tasks = [t for t in tasks if t["target_table"] == target_table]
        if statuses:
            tasks = [t for t in tasks if t["status"] in statuses]
        return tasks

    def valid_rollback_refs(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        """记录上的有效回滚引用：未终结任务显式引用或指向同一分区。"""
        refs: list[dict[str, Any]] = []
        for task in self._rollback_tasks.values():
            if task["status"] in TERMINAL_TASK_STATES:
                continue
            if task["task_id"] in record["rollback_refs"]:
                refs.append(task)
            elif (task["target_table"] == record["target_table"]
                  and task["partition"] == record["partition"]):
                refs.append(task)
        return refs

    def pending_rollback_partitions(self, target_table: str | None = None) -> list[str]:
        """仍待回退的分区：存在未终结回滚任务的分区。"""
        parts = {
            task["partition"]
            for task in self._rollback_tasks.values()
            if task["status"] not in TERMINAL_TASK_STATES
            and (target_table is None or task["target_table"] == target_table)
        }
        return sorted(parts)

    # ------------------------------------------------------------------
    # 清理日志与汇总
    # ------------------------------------------------------------------
    def log_cleanup(self, *, target_table: str, task_id: str,
                    released: list[dict[str, Any]],
                    skipped_referenced: list[dict[str, Any]],
                    kept_snapshot_nos: list[str]) -> dict[str, Any]:
        """记录一次清理实际释放的快照编号与分区。"""
        self._cleanup_seq += 1
        entry = {
            "run_id": f"CLN-{self._cleanup_seq:04d}",
            "target_table": target_table,
            "task_id": task_id,
            "released_snapshot_nos": [r["snapshot_no"] for r in released],
            "released_partitions": [r["partition"] for r in released],
            "skipped_referenced": skipped_referenced,
            "kept_snapshot_nos": list(kept_snapshot_nos),
            "ran_at": datetime.now(timezone.utc).isoformat(),
        }
        self._cleanup_log.append(entry)
        return entry

    def cleanup_history(self, target_table: str | None = None) -> list[dict[str, Any]]:
        if target_table is None:
            return list(self._cleanup_log)
        return [e for e in self._cleanup_log if e["target_table"] == target_table]

    def summary(self, target_table: str | None = None) -> dict[str, Any]:
        records = [
            r for r in self._records.values()
            if target_table is None or r["target_table"] == target_table
        ]
        by_state: dict[str, list[str]] = {}
        for record in records:
            by_state.setdefault(record["state"], []).append(record["snapshot_no"])
        return {
            "total": len(records),
            "by_state": by_state,
            "released_partitions": sorted(
                r["partition"] for r in records if r["state"] == "released"
            ),
            "rollback_tasks": len(self._rollback_tasks),
            "cleanup_runs": len(self._cleanup_log),
            "pending_rollback": self.pending_rollback_partitions(target_table),
        }


class ArtifactRegistry:
    """数据产物注册表：追踪各模块产出的数据工件。"""

    def __init__(self) -> None:
        self._artifacts: dict[str, dict[str, Any]] = {}

    def register(self, module: str, artifact_type: str, artifact_id: str,
                 metadata: dict[str, Any], source_modules: list[str] | None = None) -> str:
        """注册数据产物，返回注册ID。"""
        reg_id = f"{module}-{artifact_type}-{artifact_id}"
        self._artifacts[reg_id] = {
            "reg_id": reg_id,
            "module": module,
            "artifact_type": artifact_type,
            "artifact_id": artifact_id,
            "metadata": dict(metadata),
            "source_modules": source_modules or [module],
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "consumed_by": [],
        }
        return reg_id

    def get_artifact(self, reg_id: str) -> dict[str, Any] | None:
        return self._artifacts.get(reg_id)

    def list_by_module(self, module: str) -> list[dict[str, Any]]:
        return [a for a in self._artifacts.values() if a["module"] == module]

    def mark_consumed(self, reg_id: str, consumer: str) -> bool:
        a = self._artifacts.get(reg_id)
        if a is None:
            return False
        a["consumed_by"].append(consumer)
        return True

    def list_all(self) -> list[dict[str, Any]]:
        return list(self._artifacts.values())

    def by_type(self, artifact_type: str) -> list[dict[str, Any]]:
        return [a for a in self._artifacts.values() if a["artifact_type"] == artifact_type]

    def artifact_summary(self) -> dict[str, Any]:
        by_type: dict[str, int] = {}
        by_module: dict[str, int] = {}
        for a in self._artifacts.values():
            by_type[a["artifact_type"]] = by_type.get(a["artifact_type"], 0) + 1
            by_module[a["module"]] = by_module.get(a["module"], 0) + 1
        return {"total": len(self._artifacts), "by_type": by_type, "by_module": by_module}


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self.channel = DataChannel()
        self.recovery = FailureRecovery()
        self.snapshot = SnapshotEngine()
        self.retention = SnapshotRetentionLedger()
        self.artifacts = ArtifactRegistry()

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def insert(self, module: str, entry: dict[str, Any]) -> None:
        self.rows(module).append(entry)

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        channel_stats = self.channel.topic_stats()
        recovery = self.recovery.recovery_summary()
        snapshot_info = self.snapshot.snapshot_summary()
        artifact_info = self.artifacts.artifact_summary()
        retention_info = self.retention.summary()
        return {
            "cards": cards,
            "modules": modules,
            "data_channels": {"topics": len(channel_stats), "stats": channel_stats},
            "recovery": recovery,
            "snapshots": snapshot_info,
            "artifacts": artifact_info,
            "retention": retention_info,
        }


store = Store()