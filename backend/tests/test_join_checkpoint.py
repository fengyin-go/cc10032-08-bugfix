"""序号8 回归测试：双流关联聚合口径、关联结果版本、检查点引用完整性。

覆盖：
  - 同键同窗口多事件全部参与关联与聚合（不再只剩最后一条）
  - 跨窗口同键拆分为独立窗口结果
  - 并发重复执行只产生一个结果版本、记录数不增加
  - 检查点核对键数/事件数/窗口边界，拒绝只覆盖部分事件的快照
  - 检查点详情分页读取，跨页面用同一筛选条件定位同一结果版本
  - 事务失败回滚到原记录
"""
from __future__ import annotations

import copy
import threading

import pytest

from app.pipeline import (
    aggregate_and_sink_join_results,
    create_checkpoint_snapshot,
    perform_stream_join,
)
from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.join_stream import JoinStreamService
from app.store import store

BASE_TS = 1_700_000_000.0
JOIN_NAME = "流关联样例1"
CHECKPOINT_CONFIG = "序号8检查点配置"


@pytest.fixture(autouse=True)
def _clean_join_state():
    """每个用例从干净的结果表开始，互不影响。"""
    for table in ("join_result", "join_result_version"):
        store.rows(table).clear()
    store._join_versions.pop(JOIN_NAME, None)
    # 关联落库产生的聚合任务行和检查点测试行不留给下一个用例
    store._tables["aggregate_sink"] = [
        r for r in store.rows("aggregate_sink")
        if not str(r.get("源指标集", "")).startswith("join://")
        and "_join_dedupe_key" not in r
    ]
    store._tables["checkpoint_mgr"] = [
        r for r in store.rows("checkpoint_mgr")
        if r.get("配置名称") != CHECKPOINT_CONFIG
    ]
    store.rows("checkpoint_mgr").append({
        "id": 9001,
        "配置名称": CHECKPOINT_CONFIG,
        "间隔时间": "30",
        "超时时长": "60",
        "最大并发": "3",
        "对齐策略": "exactly_once",
        "保留数量": 100,
        "外部化路径": "/data/checkpoints/serial8",
        "status": "已启用",
        "pending": False,
        "abnormal": False,
    })
    yield
    store._tables["checkpoint_mgr"] = [
        r for r in store.rows("checkpoint_mgr")
        if r.get("配置名称") != CHECKPOINT_CONFIG
    ]


def _event(event_id: str, user_id: str, offset: float, amount: float = 1.0) -> dict:
    return {
        "event_id": event_id,
        "payload": {"user_id": user_id, "amount": amount},
        "event_time": BASE_TS + offset,
        "source": "test",
    }


def _dual_streams() -> tuple[list[dict], list[dict]]:
    """同一个用户在同一窗口内多事件，另一个用户跨到下一个窗口。"""
    left = [
        _event("l1", "u1", 1),
        _event("l2", "u1", 2),
        _event("l3", "u1", 3),
        _event("l4", "u2", 4),
        _event("l5", "u2", 14),
    ]
    right = [
        _event("r1", "u1", 1.5),
        _event("r2", "u1", 2.5),
        _event("r3", "u2", 4.5),
        _event("r4", "u2", 15),
    ]
    return left, right


def _enable_checkpoint_config() -> str:
    config = next(
        row for row in store.rows("checkpoint_mgr")
        if row.get("配置名称") == CHECKPOINT_CONFIG
    )
    config["status"] = "已启用"
    return config["配置名称"]


# ---------------------------------------------------------------------------
# 聚合口径
# ---------------------------------------------------------------------------

class TestJoinAggregationScope:
    def test_all_events_of_same_key_and_window_are_kept(self):
        """同一用户同一窗口的多条事件全部参与关联，不再只剩最后一条。"""
        left, right = _dual_streams()
        results = perform_stream_join(left, right, "user_id", 10.0, "inner")

        u1 = next(r for r in results if r.left_key == "u1")
        assert u1.left_key == "u1"
        assert len(u1.left_events) == 3
        assert len(u1.right_events) == 2
        assert u1.event_count == 5
        assert u1.pair_count == 6

    def test_same_key_across_windows_is_split(self):
        """同一关联键跨两个相邻窗口时拆成独立窗口结果。"""
        left, right = _dual_streams()
        results = perform_stream_join(left, right, "user_id", 10.0, "inner")
        u2_windows = {
            (r.window_start, r.window_end) for r in results if r.left_key == "u2"
        }
        assert len(u2_windows) == 2
        event_total = sum(r.event_count for r in results if r.left_key == "u2")
        assert event_total == 4

    def test_aggregate_counts_every_event(self):
        """聚合事件数等于所有合格事件数，而不是键数的 1:1。"""
        left, right = _dual_streams()
        results = perform_stream_join(left, right, "user_id", 10.0, "inner")
        agg = aggregate_and_sink_join_results(results, "聚合落库样例1")

        assert agg["key_count"] == agg["join_total"] == 3
        assert agg["event_count"] == len(left) + len(right) == 9
        assert agg["event_count"] > agg["join_total"]

    def test_aggregate_is_idempotent(self):
        """同一批关联结果重复落库不追加记录。"""
        left, right = _dual_streams()
        results = perform_stream_join(left, right, "user_id", 10.0, "inner")
        before = len(store.rows("aggregate_sink"))
        first = aggregate_and_sink_join_results(results, "聚合落库样例1")
        after_first = len(store.rows("aggregate_sink"))
        second = aggregate_and_sink_join_results(results, "聚合落库样例1")
        assert first["_join_dedupe_key"] == second["_join_dedupe_key"]
        assert after_first == before + 1
        assert len(store.rows("aggregate_sink")) == after_first

    def test_unqualified_events_are_skipped(self):
        """缺少关联键或事件时间的记录不参与匹配。"""
        left = [_event("l1", "u1", 1), {"event_id": "bad", "event_time": BASE_TS + 2}]
        right = [_event("r1", "u1", 1.5)]
        results = perform_stream_join(left, right, "user_id", 10.0, "inner")
        assert sum(r.event_count for r in results) == 2


# ---------------------------------------------------------------------------
# 关联结果版本 / 幂等
# ---------------------------------------------------------------------------

class TestJoinResultVersion:
    def test_repeat_evaluation_keeps_one_version(self):
        left, right = _dual_streams()
        service = JoinStreamService()
        first = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        rows_after_first = len(store.rows("join_result"))
        second = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)

        assert first["result_version"] == second["result_version"]
        assert second["idempotent"] is True
        assert len(store.rows("join_result")) == rows_after_first
        assert len(store.rows("join_result_version")) == 1
        assert second["event_count"] == 9

    def test_concurrent_evaluation_returns_single_version(self):
        """并发查询只保留一个版本：多线程同时执行不产生重复记录。"""
        left, right = _dual_streams()
        service = JoinStreamService()
        results: list[dict] = []

        def run() -> None:
            results.append(service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10))

        threads = [threading.Thread(target=run) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(results) == 8
        assert {r["result_version"] for r in results} == {1}
        assert len(store.rows("join_result_version")) == 1
        assert len(store.rows("join_result")) == 3

    def test_new_events_create_new_version_and_keep_old(self):
        left, right = _dual_streams()
        service = JoinStreamService()
        first = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        second = service.evaluate_join_window(
            JOIN_NAME, left + [_event("l6", "u1", 4)], right, window_sec=10,
        )
        assert second["result_version"] == first["result_version"] + 1
        assert second["event_count"] == first["event_count"] + 1

        old = service.list_join_result(
            JOIN_NAME, result_version=first["result_version"],
        )
        assert old["event_count"] == first["event_count"]
        new = service.list_join_result(JOIN_NAME)
        assert new["event_count"] == second["event_count"]
        # 默认读取只返回当前版本，同一窗口不会同时出现新旧两版
        assert {r["version"] for r in new["records"]} == {second["result_version"]}

    def test_window_only_rerun_keeps_other_windows(self):
        left, right = _dual_streams()
        service = JoinStreamService()
        first = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        window_key = first["window_keys"][0]
        rerun = service.evaluate_join_window(
            JOIN_NAME, left, right, window_sec=10, window_key=window_key,
        )
        assert rerun["result_version"] == first["result_version"]
        assert rerun["key_count"] == first["key_count"]


# ---------------------------------------------------------------------------
# 检查点引用完整性
# ---------------------------------------------------------------------------

class TestCheckpointJoinReference:
    def test_checkpoint_reference_matches_result(self):
        left, right = _dual_streams()
        service = JoinStreamService()
        evaluated = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)

        config_name = _enable_checkpoint_config()
        created = CheckpointMgrService().create_checkpoint(
            config_name, {"join_agg": {"count": 3}},
            join_name=JOIN_NAME, result_version=evaluated["result_version"],
        )
        assert created["ok"] is True
        reference = created["join_reference"]
        assert reference["key_count"] == evaluated["key_count"] == 3
        assert reference["event_count"] == evaluated["event_count"] == 9
        assert len(reference["windows"]) == 2

    def test_partial_result_rejected(self):
        """结果只覆盖部分事件时，检查点拒绝保存。"""
        left, right = _dual_streams()
        service = JoinStreamService()
        evaluated = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)

        # 模拟并发覆盖/写入中断：版本索引还在，但结果行只剩一个键
        version_rows = [
            r for r in store.rows("join_result")
            if r["version"] == evaluated["result_version"]
        ]
        store.rows("join_result").remove(version_rows[-1])

        config_name = _enable_checkpoint_config()
        created = CheckpointMgrService().create_checkpoint(
            config_name, {"join_agg": {"count": 3}},
            join_name=JOIN_NAME, result_version=evaluated["result_version"],
        )
        assert created["ok"] is False
        assert any("不一致" in issue for issue in created["issues"])
        assert not any(r.get("checkpoint_id") for r in store.rows("checkpoint_mgr")
                       if r.get("join_reference"))

    def test_pipeline_checkpoint_refuses_partial_result(self):
        left, right = _dual_streams()
        service = JoinStreamService()
        evaluated = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        version_rows = [
            r for r in store.rows("join_result")
            if r["version"] == evaluated["result_version"]
        ]
        store.rows("join_result").remove(version_rows[0])

        with pytest.raises(ValueError):
            create_checkpoint_snapshot(
                "join_stream_agg", {"count": 1},
                join_name=JOIN_NAME, result_version=evaluated["result_version"],
            )

    def test_restore_detects_tampered_reference(self):
        left, right = _dual_streams()
        service = JoinStreamService()
        evaluated = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        config_name = _enable_checkpoint_config()
        checker = CheckpointMgrService()
        created = checker.create_checkpoint(
            config_name, {"join_agg": {"count": 3}},
            join_name=JOIN_NAME, result_version=evaluated["result_version"],
        )
        assert checker.restore_state(created["checkpoint_id"])["ok"] is True

        version_rows = [
            r for r in store.rows("join_result")
            if r["version"] == evaluated["result_version"]
        ]
        store.rows("join_result").remove(version_rows[0])

        restored = checker.restore_state(created["checkpoint_id"])
        assert restored["ok"] is False
        assert restored["validation"]["issues"]

    def test_old_version_survives_new_version(self):
        """新版本覆盖当前结果后，检查点引用的旧版本仍可核对与恢复。"""
        left, right = _dual_streams()
        service = JoinStreamService()
        first = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        config_name = _enable_checkpoint_config()
        checker = CheckpointMgrService()
        created = checker.create_checkpoint(
            config_name, {"join_agg": {"count": 3}},
            join_name=JOIN_NAME, result_version=first["result_version"],
        )
        service.evaluate_join_window(
            JOIN_NAME, left + [_event("l7", "u1", 5)], right, window_sec=10,
        )
        check = checker.verify_join_reference(JOIN_NAME, first["result_version"])
        assert check["ok"] is True
        assert check["actual"]["event_count"] == first["event_count"]
        assert checker.restore_state(created["checkpoint_id"])["ok"] is True


# ---------------------------------------------------------------------------
# 分页与跨页面定位
# ---------------------------------------------------------------------------

class TestCheckpointPagination:
    def test_snapshots_paginate_and_filter(self):
        left, right = _dual_streams()
        service = JoinStreamService()
        evaluated = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        config_name = _enable_checkpoint_config()
        config = next(
            r for r in store.rows("checkpoint_mgr") if r.get("配置名称") == config_name
        )
        config["保留数量"] = 100
        checker = CheckpointMgrService()
        for _ in range(5):
            checkpoint = checker.create_checkpoint(
                config_name, {"join_agg": {"count": 3}},
                join_name=JOIN_NAME, result_version=evaluated["result_version"],
            )
            assert checkpoint["ok"]

        page1 = checker.list_checkpoint_snapshots(join_name=JOIN_NAME, page=1, size=2)
        page2 = checker.list_checkpoint_snapshots(join_name=JOIN_NAME, page=2, size=2)
        assert page1["total"] == 5
        assert len(page1["items"]) == 2
        assert len(page2["items"]) == 2
        ids = {item["checkpoint_id"] for item in page1["items"]} & {
            item["checkpoint_id"] for item in page2["items"]
        }
        assert ids == set()
        for item in page1["items"]:
            assert item["result_version"] == evaluated["result_version"]
            assert item["event_count"] == evaluated["event_count"]

    def test_join_windows_carry_event_counts(self):
        """流关联页面按窗口查看事件数，与检查点引用的统计一致。"""
        left, right = _dual_streams()
        service = JoinStreamService()
        evaluated = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        listed = service.list_join_result(JOIN_NAME)
        assert len(listed["windows"]) == 2
        assert sum(w["event_count"] for w in listed["windows"]) == evaluated["event_count"]
        for window in listed["windows"]:
            assert window["window_end"] > window["window_start"]


# ---------------------------------------------------------------------------
# 聚合落库同步
# ---------------------------------------------------------------------------

class TestAggregateSinkSync:
    def test_evaluate_updates_single_sink_row(self):
        """关联评估把统计同步到聚合落库，重复执行只刷新同一条记录。"""
        left, right = _dual_streams()
        service = JoinStreamService()
        service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        rows_before = [
            r for r in store.rows("aggregate_sink")
            if str(r.get("源指标集", "")).startswith("join://" + JOIN_NAME)
        ]
        assert len(rows_before) == 1
        assert rows_before[0]["event_count"] == 9

        service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        rows_after = [
            r for r in store.rows("aggregate_sink")
            if str(r.get("源指标集", "")).startswith("join://" + JOIN_NAME)
        ]
        assert len(rows_after) == 1
        assert rows_after[0]["event_count"] == 9
        assert rows_after[0]["result_version"] == 1


# ---------------------------------------------------------------------------
# 事务失败回滚
# ---------------------------------------------------------------------------

class TestTransactionRollback:
    def test_failed_evaluation_restores_original_rows(self):
        left, right = _dual_streams()
        service = JoinStreamService()
        first = service.evaluate_join_window(JOIN_NAME, left, right, window_sec=10)
        rows_before = copy.deepcopy(store.rows("join_result"))
        versions_before = copy.deepcopy(store.rows("join_result_version"))

        with pytest.raises(RuntimeError):
            with store.transaction():
                store.rows("join_result").clear()
                store.rows("join_result_version").clear()
                store.next_join_version(JOIN_NAME)
                raise RuntimeError("模拟写入失败")

        assert store.rows("join_result") == rows_before
        assert store.rows("join_result_version") == versions_before
        assert service.latest_result_version(JOIN_NAME) == first["result_version"]
