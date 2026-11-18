"""检查点领域服务：管理流式任务的检查点策略。

数据流角色: replay_engine → checkpoint_mgr（消费者+生产者）→ restore_plan（消费者）
增强能力: 检查点创建/快照/恢复/对齐、状态校验、外部化存储路径管理、多检查点一致性验证

关联结果引用（序号8）:
  - 检查点快照只记录对"关联结果数据某个版本"的引用，不自行复制一份
    可能缺事件的状态。
  - 保存前读取关联结果数据，核对键数、事件数和窗口边界；引用版本与实
    际结果不一致（只覆盖部分事件）时拒绝保存。
  - 详情页按快照分页读取，每个快照都能回溯到完整结果集合的版本。
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

RESULT_TABLE = "join_result"
VERSION_TABLE = "join_result_version"


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class CheckpointMgrService(BaseModuleService):
    MODULE = "checkpoint_mgr"
    ENTITY = "检查点配置"
    LIST_FIELDS = ["配置名称", "间隔时间", "超时时长", "最大并发", "对齐策略", "保留数量", "外部化路径"]
    REQUIRED_FIELDS = ["配置名称", "间隔时间", "超时时长"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用检查点": "草稿", "停用检查点": "已启用"}
    NEGATIVE_ACTIONS = ["停用检查点"]

    # ---- 关联结果引用校验 ----

    @staticmethod
    def read_join_result(join_name: str, result_version: int) -> dict[str, Any]:
        """读取关联结果数据中某个版本的完整集合（键、事件、窗口边界）。"""
        rows = [
            r for r in store.rows(RESULT_TABLE)
            if r.get("join_name") == join_name
            and _as_int(r.get("version")) == _as_int(result_version)
        ]
        windows = sorted({
            (round(_as_float(r.get("window_start")), 6),
             round(_as_float(r.get("window_end")), 6))
            for r in rows
        })
        return {
            "join_name": join_name,
            "result_version": _as_int(result_version),
            "rows": rows,
            "key_count": len(rows),
            "event_count": sum(_as_int(r.get("event_count")) for r in rows),
            "left_event_count": sum(_as_int(r.get("left_event_count")) for r in rows),
            "right_event_count": sum(_as_int(r.get("right_event_count")) for r in rows),
            "matched": sum(1 for r in rows if r.get("matched")),
            "windows": [
                {"window_start": start, "window_end": end}
                for start, end in windows
            ],
        }

    @staticmethod
    def read_join_version(join_name: str, result_version: int) -> dict[str, Any] | None:
        for v in store.rows(VERSION_TABLE):
            if (
                v.get("join_name") == join_name
                and _as_int(v.get("result_version")) == _as_int(result_version)
            ):
                return v
        return None

    def verify_join_reference(
        self, join_name: str, result_version: int,
        expected: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """核对检查点引用的结果版本：键数、事件数、窗口边界必须与结果数据一致。

        除了版本索引里记录的统计值，还要与实际行逐窗口比对，避免版本索引
        写着全量、结果表里却只有部分事件。
        """
        version_entry = self.read_join_version(join_name, result_version)
        actual = self.read_join_result(join_name, result_version)
        issues: list[str] = []

        if version_entry is None:
            issues.append(f"结果版本 {join_name}#{result_version} 不存在")
        if actual["key_count"] == 0:
            issues.append(f"结果版本 {join_name}#{result_version} 没有任何关联结果记录")

        if version_entry is not None:
            for field in ("key_count", "event_count", "matched"):
                if _as_int(version_entry.get(field)) != actual[field]:
                    issues.append(
                        f"{field} 不一致：版本索引 {version_entry.get(field)}，"
                        f"结果数据 {actual[field]}"
                    )
            expected_windows = {
                (round(_as_float(w.get("window_start")), 6),
                 round(_as_float(w.get("window_end")), 6))
                for w in version_entry.get("window_keys_detail", [])
            }
            if expected_windows != {
                (w["window_start"], w["window_end"]) for w in actual["windows"]
            }:
                issues.append("窗口边界与结果版本记录的窗口集合不一致")

        if expected is not None:
            for field in ("key_count", "event_count"):
                if field in expected and _as_int(expected.get(field)) != actual[field]:
                    issues.append(
                        f"待保存快照的{field} {expected.get(field)} 与结果数据 {actual[field]} 不一致"
                    )
            if "windows" in expected and expected["windows"] is not None:
                want = {
                    (round(_as_float(w.get("window_start")), 6),
                     round(_as_float(w.get("window_end")), 6))
                    for w in expected["windows"]
                }
                if want != {
                    (w["window_start"], w["window_end"]) for w in actual["windows"]
                }:
                    issues.append("待保存快照的窗口边界与结果数据不一致")

        return {
            "ok": len(issues) == 0,
            "complete": len(issues) == 0,
            "join_name": join_name,
            "result_version": _as_int(result_version),
            "actual": actual,
            "issues": issues,
        }

    # ---- 跨模块数据管理 ----

    def create_checkpoint(
        self, config_name: str, operator_states: dict[str, Any],
        *, join_name: str | None = None, result_version: int | None = None,
    ) -> dict[str, Any]:
        """创建算子状态检查点快照。

        为指定的检查点配置生成带有状态快照的检查点记录，
        存储到 store 中供 replay_engine 消费。指定 join_name 时必须带上
        结果版本，并在保存前核对结果集合是否完整。
        """
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"检查点配置「{config_name}」不存在"}
        if config.get("status") != "已启用":
            return {"ok": False, "error": f"配置「{config_name}」未启用"}

        # 先拿当前的关联结果版本，写入时再核对一次，避免并发覆盖后引用错版本
        reference = None
        if join_name is not None:
            with store.locked():
                if result_version is None:
                    result_version = self._latest_join_version(join_name)
                validation = self.verify_join_reference(join_name, result_version)
                if not validation["ok"]:
                    return {
                        "ok": False,
                        "error": "关联结果数据不完整，拒绝保存检查点快照",
                        "issues": validation["issues"],
                    }
                actual = validation["actual"]
                reference = {
                    "join_name": join_name,
                    "result_version": actual["result_version"],
                    "key_count": actual["key_count"],
                    "event_count": actual["event_count"],
                    "matched": actual["matched"],
                    "windows": actual["windows"],
                }

        try:
            interval = float(config.get("间隔时间", "30"))
        except (ValueError, TypeError):
            interval = 30.0

        cp_id = hashlib.md5(
            f"{config_name}_{time.time()}".encode()
        ).hexdigest()[:12]

        with store.transaction():
            # 在同一个事务里复核结果引用：并发的关联重算若已覆盖该版本，
            # 这里会失败并整体回滚，不会留下引用错版本的快照。
            if join_name is not None:
                recheck = self.verify_join_reference(
                    join_name, reference["result_version"], expected=reference,
                )
                if not recheck["ok"]:
                    return {
                        "ok": False,
                        "error": "关联结果数据在保存前被改写，检查点快照未保存",
                        "issues": recheck["issues"],
                    }
            snapshot = {
                "id": cp_id,
                "配置名称": config_name,
                "间隔时间": str(interval),
                "超时时长": config.get("超时时长", "60"),
                "最大并发": config.get("最大并发", "3"),
                "对齐策略": config.get("对齐策略", "exactly_once"),
                "保留数量": int(config.get("保留数量", 5)),
                "外部化路径": config.get("外部化路径", "/data/checkpoints"),
                "status": "已启用",
                "pending": False,
                "abnormal": False,
                "checkpoint_id": cp_id,
                "operator_states": copy.deepcopy(operator_states),
                "snapshot_ts": time.time(),
                "_created_at": time.time(),
            }
            if reference is not None:
                snapshot["join_reference"] = reference
                snapshot["result_version"] = reference["result_version"]
                snapshot["key_count"] = reference["key_count"]
                snapshot["event_count"] = reference["event_count"]
                snapshot["windows"] = reference["windows"]
            store.rows(self.MODULE).append(snapshot)

            # 清理旧检查点（保留最近 N 个）
            self._prune_old_checkpoints(config_name)

        return {
            "ok": True,
            "checkpoint_id": cp_id,
            "config": config_name,
            "operators": list(operator_states.keys()),
            "interval": interval,
            "join_reference": reference,
        }

    def _latest_join_version(self, join_name: str) -> int:
        versions = [
            _as_int(v.get("result_version"))
            for v in store.rows(VERSION_TABLE)
            if v.get("join_name") == join_name
        ]
        return max(versions, default=0)

    def _prune_old_checkpoints(self, config_name: str) -> None:
        """清理超过保留数量的旧检查点。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        max_keep = int((config or {}).get("保留数量", 5))

        snapshots = [
            r for r in store.rows(self.MODULE)
            if r.get("配置名称") == config_name and r.get("checkpoint_id")
        ]
        snapshots.sort(key=lambda s: s.get("snapshot_ts", 0), reverse=True)
        for s in snapshots[max_keep:]:
            s["status"] = "已停用"
            s["_pruned"] = True

    def restore_state(self, checkpoint_id: str) -> dict[str, Any]:
        """从检查点恢复算子状态，验证完整性后返回。

        如果快照引用了关联结果数据，还要核对引用版本是否仍完整：
        结果被后续版本覆盖导致引用对不上时，恢复失败而不是静默返回半个集合。
        """
        snapshot = None
        for r in store.rows(self.MODULE):
            if r.get("checkpoint_id") == checkpoint_id:
                snapshot = r
                break
        if snapshot is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}

        states = snapshot.get("operator_states", {})
        # 验证状态完整性
        validation = self._validate_state_structure(states)

        issues = list(validation["issues"])
        reference = snapshot.get("join_reference")
        if reference:
            check = self.verify_join_reference(
                reference.get("join_name", ""),
                _as_int(reference.get("result_version")),
                expected=reference,
            )
            issues.extend(check["issues"])
            validation["referenced_version"] = check["actual"]["result_version"]
            validation["referenced_event_count"] = check["actual"]["event_count"]

        ok = validation["valid"] and len(issues) == 0
        return {
            "ok": ok,
            "checkpoint_id": checkpoint_id,
            "config": snapshot.get("配置名称", ""),
            "snapshot_ts": snapshot.get("snapshot_ts", 0),
            "result_version": snapshot.get("result_version"),
            "join_reference": reference,
            "operator_count": len(states),
            "operators": list(states.keys()),
            "states": states,
            "validation": {**validation, "valid": ok, "issues": issues},
        }

    @staticmethod
    def _validate_state_structure(states: dict[str, Any]) -> dict[str, Any]:
        """验证状态数据结构完整性。"""
        issues = []
        if not isinstance(states, dict):
            return {"valid": False, "issues": ["状态结构不是 dict 类型"]}
        for op_name, op_state in states.items():
            if op_state is None:
                issues.append(f"算子「{op_name}」状态为 None")
            elif isinstance(op_state, dict) and len(op_state) == 0:
                issues.append(f"算子「{op_name}」状态为空字典")
        return {"valid": len(issues) == 0, "issues": issues}

    def verify_multi_checkpoint_consistency(
        self, config_name: str,
    ) -> dict[str, Any]:
        """验证同一配置下多个检查点之间的状态一致性。"""
        snapshots = [
            r for r in store.rows(self.MODULE)
            if r.get("配置名称") == config_name and r.get("checkpoint_id")
        ]
        snapshots.sort(key=lambda s: s.get("snapshot_ts", 0))

        if len(snapshots) < 2:
            return {"ok": True, "consistent": True, "message": "检查点不足2个，无需比较"}

        baseline = snapshots[0].get("operator_states", {})
        inconsistencies = []
        versions = set()
        for i, s in enumerate(snapshots[1:], 1):
            current = s.get("operator_states", {})
            for op, state in current.items():
                if op not in baseline:
                    inconsistencies.append(f"检查点{i}: 新增算子「{op}」")
                elif state != baseline.get(op):
                    inconsistencies.append(f"检查点{i}: 算子「{op}」状态变更")
            reference = s.get("join_reference") or {}
            if reference:
                versions.add(_as_int(reference.get("result_version")))
                check = self.verify_join_reference(
                    reference.get("join_name", ""),
                    _as_int(reference.get("result_version")),
                    expected=reference,
                )
                inconsistencies.extend(
                    f"检查点{i}: {issue}" for issue in check["issues"]
                )

        return {
            "ok": True,
            "config": config_name,
            "snapshot_count": len(snapshots),
            "result_versions": sorted(versions),
            "consistent": len(inconsistencies) == 0,
            "inconsistencies": inconsistencies,
        }

    def list_checkpoints_for_replay(self) -> list[dict[str, Any]]:
        """列出可用于回放引擎消费的检查点。"""
        available = [
            r for r in store.rows(self.MODULE)
            if r.get("checkpoint_id") and r.get("status") == "已启用"
        ]
        result = []
        for cp in available:
            states = cp.get("operator_states", {})
            result.append({
                "checkpoint_id": cp.get("checkpoint_id"),
                "config": cp.get("配置名称", ""),
                "operator_count": len(states),
                "operators": list(states.keys()),
                "snapshot_ts": cp.get("snapshot_ts", 0),
                "external_path": cp.get("外部化路径", ""),
                "join_reference": cp.get("join_reference"),
            })
        return result

    def list_checkpoint_snapshots(
        self,
        *,
        join_name: str | None = None,
        result_version: int | None = None,
        page: int = 1,
        size: int = 20,
    ) -> dict[str, Any]:
        """检查点详情页的分页读取：每个快照带自己引用的结果版本统计。

        只返回真正是快照的记录（有 checkpoint_id），已清理的旧快照保留
        记录但标记为已停用，页面可据此区分"可用快照"和"历史快照"。
        """
        with store.locked():
            rows = [
                r for r in store.rows(self.MODULE)
                if r.get("checkpoint_id")
            ]
            if join_name:
                rows = [
                    r for r in rows
                    if (r.get("join_reference") or {}).get("join_name") == join_name
                ]
            if result_version is not None:
                rows = [
                    r for r in rows
                    if _as_int(r.get("result_version")) == _as_int(result_version)
                ]
            rows.sort(key=lambda r: r.get("snapshot_ts", 0), reverse=True)

            total = len(rows)
            page = max(int(page), 1)
            size = max(min(int(size), 200), 1)
            start = (page - 1) * size
            items = []
            for r in rows[start:start + size]:
                reference = r.get("join_reference") or {}
                check = None
                if reference:
                    check = self.verify_join_reference(
                        reference.get("join_name", ""),
                        _as_int(reference.get("result_version")),
                        expected=reference,
                    )
                items.append({
                    "checkpoint_id": r.get("checkpoint_id"),
                    "id": r.get("id"),
                    "config": r.get("配置名称", ""),
                    "status": r.get("status", ""),
                    "pruned": bool(r.get("_pruned")),
                    "snapshot_ts": r.get("snapshot_ts", 0),
                    "join_name": reference.get("join_name"),
                    "result_version": r.get("result_version"),
                    "key_count": r.get("key_count", 0),
                    "event_count": r.get("event_count", 0),
                    "windows": r.get("windows", []),
                    "complete": check["ok"] if check else None,
                    "issues": check["issues"] if check else [],
                })
            return {"items": items, "total": total, "page": page, "size": size}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_snapshots"] = sum(1 for r in rows if r.get("checkpoint_id"))
        base["available_snapshots"] = sum(
            1 for r in rows
            if r.get("checkpoint_id") and r.get("status") == "已启用"
        )
        base["pruned_snapshots"] = sum(1 for r in rows if r.get("_pruned"))
        return base