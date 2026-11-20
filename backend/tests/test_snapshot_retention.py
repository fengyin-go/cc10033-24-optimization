"""快照留存与回滚协调测试：覆盖过期清理与回滚并发冲突场景。

场景对应：
- 按目标表与快照编号核算留存，释放分区与可回退版本记入快照保留记录
- 分区超出留存周期先核对回滚任务有效引用
- 重复清理不误删相邻日期的新分区
- 回滚读取目标表关联的留存记录判定回写资格
- 回滚确认后实际使用的快照编号和分区仍可查到，缺失分区维持待回退
- 待回滚分区与过期清理冲突时回退到同表兼容快照
- 月末分区进入清理范围后触发同表回滚，校对留存记录与回写分区
- 兼容快照读取异常后重试，缺少可用版本时维持待回退
"""
from __future__ import annotations

import pytest

from app.store import Store, SnapshotReadError
from app.services.pipeline import SnapshotRetentionManager

TABLE = "dwd_sales_detail"


@pytest.fixture
def store() -> Store:
    """每个测试使用独立 store 实例。"""
    return Store()


@pytest.fixture
def manager(store: Store) -> SnapshotRetentionManager:
    return SnapshotRetentionManager(store)


def _register_daily(manager: SnapshotRetentionManager, dates: list[str],
                    target_table: str = TABLE, compat_group: str = "v1") -> list[dict]:
    """按日期登记一批日快照分区，返回留存记录列表。"""
    records = []
    for i, day in enumerate(dates, start=1):
        records.append(manager.register_partition(
            target_table, f"ds={day}",
            rows=[{"order_id": f"OD-{day}-{i}", "amount": i * 100}],
            version_label="v1.0", compat_group=compat_group,
        ))
    return records


# ======================================================================
# Section A: 留存核算与过期清理
# ======================================================================


class TestRetentionCleanup:
    """按目标表与快照编号核算留存；释放与可回退版本记入快照保留记录。"""

    def test_cleanup_releases_oldest_beyond_retention(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """超出保留份数的最旧快照编号被释放，新编号保留。"""
        records = _register_daily(manager, ["20261028", "20261029", "20261030", "20261031"])

        result = manager.cleanup_expired(TABLE, retention_count=2, task_id="CLN-T1")

        assert result["released_snapshot_nos"] == ["SNAP-0001", "SNAP-0002"]
        assert result["released_partitions"] == ["ds=20261028", "ds=20261029"]
        assert result["kept_snapshot_nos"] == ["SNAP-0003", "SNAP-0004"]

        # 释放的日期分区记入快照保留记录
        released = [r for r in manager.retention_records(TABLE) if r["state"] == "released"]
        assert {r["partition"] for r in released} == {"ds=20261028", "ds=20261029"}
        for rec in released:
            assert rec["released_by"] == "CLN-T1"
            assert rec["released_at"] is not None

        # 仍可回退的版本同样记入快照保留记录
        active = [r for r in manager.retention_records(TABLE) if r["state"] == "active"]
        assert {r["snapshot_no"] for r in active} == {"SNAP-0003", "SNAP-0004"}

    def test_cleanup_is_accounted_by_snapshot_no_not_date_range(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """重复清理只按快照编号核算，不误删相邻日期的新分区。"""
        _register_daily(manager, ["20261029", "20261031"])
        first = manager.cleanup_expired(TABLE, retention_count=1, task_id="CLN-R1")
        assert first["released_snapshot_nos"] == ["SNAP-0001"]

        # 相邻日期的新分区（10-30 回填，晚于 10-31 登记）在清理后到达
        backfill = manager.register_partition(
            TABLE, "ds=20261030",
            rows=[{"order_id": "OD-backfill", "amount": 1}],
            version_label="v1.0", compat_group="v1",
        )
        assert backfill["snapshot_no"] == "SNAP-0003"

        # 第二次清理：保留 1 份，应释放 SNAP-0002(ds=20261031)，新回填分区必须存活
        second = manager.cleanup_expired(TABLE, retention_count=1, task_id="CLN-R2")
        assert second["released_snapshot_nos"] == ["SNAP-0002"]
        assert second["kept_snapshot_nos"] == ["SNAP-0003"]

        backfill_rec = store.retention.find_by_snapshot_no(TABLE, "SNAP-0003")
        assert backfill_rec is not None
        assert backfill_rec["state"] == "active"
        assert backfill_rec["partition"] == "ds=20261030"

        # 第三次清理：无超量，幂等不再释放任何编号
        third = manager.cleanup_expired(TABLE, retention_count=1, task_id="CLN-R3")
        assert third["released_snapshot_nos"] == []
        assert third["kept_snapshot_nos"] == ["SNAP-0003"]

    def test_cleanup_skips_partitions_with_valid_rollback_refs(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """分区超出留存周期但被回滚任务有效引用时，本轮清理跳过。"""
        _register_daily(manager, ["20261029", "20261030", "20261031"])

        # 回滚任务引用最旧分区（此时它仍可回退）
        task = manager.request_rollback(TABLE, "ds=20261029", task_id="RB-HOLD")
        assert task["status"] == "eligible"

        # 保留 1 份：SNAP-0001、SNAP-0002 都超量，但 SNAP-0001 被有效引用
        result = manager.cleanup_expired(TABLE, retention_count=1, task_id="CLN-H1")
        assert result["released_snapshot_nos"] == ["SNAP-0002"]
        assert result["skipped_referenced"] == [
            {"snapshot_no": "SNAP-0001", "partition": "ds=20261029", "refs": ["RB-HOLD"]}
        ]
        held = store.retention.find_by_snapshot_no(TABLE, "SNAP-0001")
        assert held is not None and held["state"] == "active"

        # 回滚确认后引用终结，再次清理即可释放
        confirmed = manager.confirm_rollback("RB-HOLD")
        assert confirmed["status"] == "rolled_back"
        again = manager.cleanup_expired(TABLE, retention_count=1, task_id="CLN-H2")
        assert "SNAP-0001" in again["released_snapshot_nos"]

    def test_cleanup_log_keeps_released_numbers(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """清理在清理日志中留下实际释放的快照编号。"""
        _register_daily(manager, ["20261028", "20261029", "20261030"])
        manager.cleanup_expired(TABLE, retention_count=1, task_id="CLN-LOG")

        history = manager.cleanup_history(TABLE)
        assert len(history) == 1
        assert history[0]["released_snapshot_nos"] == ["SNAP-0001", "SNAP-0002"]
        assert history[0]["task_id"] == "CLN-LOG"


# ======================================================================
# Section B: 回滚资格判定
# ======================================================================


class TestRollbackEligibility:
    """回滚读取目标表关联的快照保留记录判定回写资格。"""

    def test_rollback_active_partition_uses_own_snapshot(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """分区仍可回退时，回滚使用自有快照编号并回写分区。"""
        _register_daily(manager, ["20261030", "20261031"])
        # 当前分区数据被污染
        store.rows(TABLE).append({"partition": "ds=20261031", "order_id": "BAD", "amount": -1})

        task = manager.request_rollback(TABLE, "ds=20261031", task_id="RB-OWN")
        assert task["status"] == "eligible"
        assert task["planned_snapshot_no"] == "SNAP-0002"

        result = manager.confirm_rollback("RB-OWN")
        assert result["status"] == "rolled_back"
        assert result["used_snapshot_no"] == "SNAP-0002"
        assert result["written_rows"] == 1

        # 回写分区：污染行被快照数据替换
        rows = [r for r in store.rows(TABLE) if r.get("partition") == "ds=20261031"]
        assert len(rows) == 1
        assert rows[0]["order_id"] == "OD-20261031-2"

        # 回滚在留存记录中留下实际引用的编号
        rec = store.retention.find_by_partition(TABLE, "ds=20261031")
        assert rec is not None
        assert rec["state"] == "rolled_back"
        assert rec["used_snapshot_no"] == "SNAP-0002"
        assert rec["used_by_rollback"] == "RB-OWN"

    def test_rollback_released_partition_falls_back_to_compatible_snapshot(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """待回滚分区已被过期清理释放，但同表仍存兼容快照时可回退。"""
        _register_daily(manager, ["20261029", "20261030", "20261031"])
        manager.cleanup_expired(TABLE, retention_count=1, task_id="CLN-C1")
        # ds=20261029 的自有快照已被清理释放
        released = store.retention.find_by_partition(TABLE, "ds=20261029")
        assert released is not None and released["state"] == "released"

        task = manager.request_rollback(TABLE, "ds=20261029", task_id="RB-COMPAT")
        assert task["status"] == "eligible"
        # 回退到同目标表仍存的兼容快照
        assert task["planned_snapshot_no"] == "SNAP-0003"

        result = manager.confirm_rollback("RB-COMPAT")
        assert result["status"] == "rolled_back"
        assert result["used_snapshot_no"] == "SNAP-0003"

        # 目标分区记录标记已回退，兼容记录仍保留实际引用
        target_rec = store.retention.find_by_partition(TABLE, "ds=20261029")
        assert target_rec is not None and target_rec["state"] == "rolled_back"
        assert target_rec["used_snapshot_no"] == "SNAP-0003"
        compat_rec = store.retention.find_by_snapshot_no(TABLE, "SNAP-0003")
        assert compat_rec is not None
        assert "RB-COMPAT" in compat_rec["rollback_refs"]

    def test_rollback_missing_partition_stays_pending(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """缺失分区（从未登记快照）维持待回退。"""
        _register_daily(manager, ["20261031"])

        task = manager.request_rollback(TABLE, "ds=20261001", task_id="RB-MISS")
        assert task["status"] == "blocked"

        result = manager.confirm_rollback("RB-MISS")
        assert result["status"] == "pending_rollback"
        assert "ds=20261001" in manager.pending_rollback_partitions(TABLE)


# ======================================================================
# Section C: 月末分区清理与回滚校对
# ======================================================================


class TestMonthEndCleanupThenRollback:
    """月末分区进入清理范围后触发同一目标表回滚，校对留存记录与回写分区。"""

    def test_month_end_partition_released_then_rolled_back_via_compatible(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        month_end = "ds=20261031"
        records = _register_daily(
            manager, ["20261029", "20261030", "20261031", "20261101", "20261102"])

        # 保留 2 份：月末分区 ds=20261031 进入清理范围并被释放
        cleanup = manager.cleanup_expired(TABLE, retention_count=2, task_id="CLN-ME")
        assert month_end in cleanup["released_partitions"]
        assert cleanup["released_snapshot_nos"] == ["SNAP-0001", "SNAP-0002", "SNAP-0003"]
        month_end_rec = store.retention.find_by_partition(TABLE, month_end)
        assert month_end_rec is not None and month_end_rec["state"] == "released"

        # 触发同一目标表的回滚：月末分区自有快照已释放，回退到兼容快照
        task = manager.request_rollback(TABLE, month_end, task_id="RB-ME")
        assert task["status"] == "eligible"
        planned = task["planned_snapshot_no"]
        assert planned in {"SNAP-0004", "SNAP-0005"}

        result = manager.confirm_rollback("RB-ME")
        assert result["status"] == "rolled_back"
        used_no = result["used_snapshot_no"]
        assert used_no == planned

        # 校对留存记录：实际使用的快照编号和分区必须还能查到
        used_rec = store.retention.find_by_snapshot_no(TABLE, used_no)
        assert used_rec is not None
        partition_rec = store.retention.find_by_partition(TABLE, month_end)
        assert partition_rec is not None
        assert partition_rec["state"] == "rolled_back"
        assert partition_rec["used_snapshot_no"] == used_no

        # 回滚成功所引用的快照须可读
        snap = store.snapshot.read_snapshot(used_rec["snap_id"])
        assert snap["data"]["partition"] == used_rec["partition"]

        # 校对回写分区：月末分区数据已按兼容快照回写
        written = [r for r in store.rows(TABLE) if r.get("partition") == month_end]
        assert len(written) == 1
        assert written[0]["order_id"].startswith("OD-2026110")
        assert "_rolled_back_at" in written[0]

        # 回滚任务留下实际引用的编号
        finished = manager.rollback_task("RB-ME")
        assert finished is not None
        assert finished["status"] == "rolled_back"
        assert finished["used_snapshot_no"] == used_no

    def test_month_end_records_summary_tracks_states(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """留存汇总按状态列出快照编号，释放与可回退版本可查。"""
        _register_daily(manager, ["20261030", "20261031", "20261101"])
        manager.cleanup_expired(TABLE, retention_count=1, task_id="CLN-S")

        summary = store.retention.summary(TABLE)
        assert summary["by_state"]["released"] == ["SNAP-0001", "SNAP-0002"]
        assert summary["by_state"]["active"] == ["SNAP-0003"]
        assert summary["released_partitions"] == ["ds=20261030", "ds=20261031"]


# ======================================================================
# Section D: 兼容快照读取异常与重试
# ======================================================================


class TestCompatibleSnapshotReadException:
    """模拟兼容快照读取异常后重试；缺少可用版本时维持待回退。"""

    def _setup_conflict(self, manager: SnapshotRetentionManager) -> dict:
        """月末分区被清理释放，仅剩一版兼容快照。"""
        records = _register_daily(manager, ["20261031", "20261101"])
        manager.cleanup_expired(TABLE, retention_count=1, task_id="CLN-EX")
        return records[1]  # 兼容快照记录 SNAP-0002

    def test_read_exception_blocks_then_retry_succeeds(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        compat_rec = self._setup_conflict(manager)

        task = manager.request_rollback(TABLE, "ds=20261031", task_id="RB-EX")
        assert task["status"] == "eligible"
        assert task["planned_snapshot_no"] == "SNAP-0002"

        # 模拟兼容快照读取异常
        store.snapshot.mark_unreadable(compat_rec["snap_id"])
        blocked = manager.confirm_rollback("RB-EX")
        assert blocked["status"] == "pending_rollback"
        assert "ds=20261031" in blocked["pending_rollback_partitions"]
        assert "ds=20261031" in manager.pending_rollback_partitions(TABLE)

        # 异常期间重试仍维持待回退
        still_blocked = manager.confirm_rollback("RB-EX")
        assert still_blocked["status"] == "pending_rollback"

        # 读取异常恢复后重试成功
        store.snapshot.clear_unreadable(compat_rec["snap_id"])
        retried = manager.confirm_rollback("RB-EX")
        assert retried["status"] == "rolled_back"
        assert retried["used_snapshot_no"] == "SNAP-0002"
        assert manager.pending_rollback_partitions(TABLE) == []

        # 重试过程留有恢复日志
        retries = store.recovery.retry_history("snapshot_plan")
        assert any(not r["success"] for r in retries)
        assert any(r["success"] for r in retries)

    def test_no_usable_version_keeps_partition_pending(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """同一目标表缺少可用版本时，分区维持待回退。"""
        records = _register_daily(manager, ["20261031", "20261101"])
        # 全部快照都被清理释放（保留 0 份）
        manager.cleanup_expired(TABLE, retention_count=0, task_id="CLN-ALL")

        task = manager.request_rollback(TABLE, "ds=20261031", task_id="RB-NONE")
        assert task["status"] == "blocked"
        assert task["last_error"] is not None

        result = manager.confirm_rollback("RB-NONE")
        assert result["status"] == "pending_rollback"
        assert manager.pending_rollback_partitions(TABLE) == ["ds=20261031"]

        # 即使唯一留存版本可读异常，也维持待回退而非误写
        _register_daily(manager, ["20261102"])
        newest = store.retention.find_by_snapshot_no(TABLE, "SNAP-0003")
        assert newest is not None
        store.snapshot.mark_unreadable(newest["snap_id"])
        again = manager.confirm_rollback("RB-NONE")
        assert again["status"] == "pending_rollback"
        assert "ds=20261031" in manager.pending_rollback_partitions(TABLE)

    def test_released_snapshot_raises_on_strict_read(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """严格读取不存在或不可读的快照抛出 SnapshotReadError。"""
        with pytest.raises(SnapshotReadError):
            store.snapshot.read_snapshot("snap-not-exist")
        rec = _register_daily(manager, ["20261031"])[0]
        store.snapshot.mark_unreadable(rec["snap_id"])
        with pytest.raises(SnapshotReadError):
            store.snapshot.read_snapshot(rec["snap_id"])
        store.snapshot.clear_unreadable(rec["snap_id"])
        assert store.snapshot.is_readable(rec["snap_id"])


# ======================================================================
# Section E: 留痕与可读性
# ======================================================================


class TestAuditTrail:
    """清理和回滚各自留下实际引用或释放的编号。"""

    def test_rollback_leaves_referenced_snapshot_no(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        _register_daily(manager, ["20261030", "20261031"])
        manager.request_rollback(TABLE, "ds=20261031", task_id="RB-AUDIT")
        manager.confirm_rollback("RB-AUDIT")

        task = manager.rollback_task("RB-AUDIT")
        assert task is not None
        assert task["planned_snapshot_no"] == "SNAP-0002"
        assert task["used_snapshot_no"] == "SNAP-0002"

        rec = store.retention.find_by_snapshot_no(TABLE, "SNAP-0002")
        assert rec is not None
        assert "RB-AUDIT" in rec["rollback_refs"]
        assert rec["used_by_rollback"] == "RB-AUDIT"

    def test_successful_rollback_snapshot_remains_readable_and_queryable(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """回滚确认后，实际使用的快照编号和分区必须还能查到且快照可读。"""
        _register_daily(manager, ["20261029", "20261030", "20261031"])
        manager.cleanup_expired(TABLE, retention_count=1, task_id="CLN-Q")
        manager.request_rollback(TABLE, "ds=20261029", task_id="RB-Q")
        result = manager.confirm_rollback("RB-Q")
        assert result["status"] == "rolled_back"
        used_no = result["used_snapshot_no"]

        # 快照编号仍可查到
        used_rec = store.retention.find_by_snapshot_no(TABLE, used_no)
        assert used_rec is not None
        # 分区仍可查到
        part_rec = store.retention.find_by_partition(TABLE, "ds=20261029")
        assert part_rec is not None
        # 引用的快照可读
        assert store.snapshot.is_readable(used_rec["snap_id"])
        snap = store.snapshot.read_snapshot(used_rec["snap_id"])
        assert snap["data"]["snapshot_no"] == used_no

    def test_confirm_rollback_is_idempotent(
        self, store: Store, manager: SnapshotRetentionManager
    ) -> None:
        """已确认的回滚任务重复确认返回同一结果，不重复回写。"""
        _register_daily(manager, ["20261031"])
        manager.request_rollback(TABLE, "ds=20261031", task_id="RB-IDEM")
        first = manager.confirm_rollback("RB-IDEM")
        assert first["status"] == "rolled_back"
        second = manager.confirm_rollback("RB-IDEM")
        assert second["status"] == "rolled_back"
        assert second["used_snapshot_no"] == first["used_snapshot_no"]
        assert second.get("idempotent") is True

    def test_confirm_unknown_task(self, manager: SnapshotRetentionManager) -> None:
        result = manager.confirm_rollback("RB-UNKNOWN")
        assert result["ok"] is False
        assert result["status"] == "missing_task"
