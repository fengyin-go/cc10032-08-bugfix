"""流关联领域服务：定义两条流之间的关联计算配置。

增强能力: 关联类型校验、时间窗口比对、左/右流源校验、关联输出到聚合落库

聚合口径（序号8）:
  - 读取双流事件时先按事件窗口分桶，同一关联键在同一窗口内的所有合格事件
    都进入匹配（不是字典覆盖，只保留后到的一条）。
  - 匹配结果落成带版本的关联结果数据（join_result），同一关联配置 + 窗口 +
    关联键在同一个版本内唯一；重复执行复用版本、不增加记录。
  - 每次执行覆盖本次读到的窗口，并沿用其它窗口的当前结果，所以每个版本
    都是一份完整集合，检查点只引用该版本对应的事件集合。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

RESULT_TABLE = "join_result"
VERSION_TABLE = "join_result_version"


def event_key(event: dict[str, Any], join_field: str) -> str:
    """取关联键：优先 payload，其次事件顶层字段。"""
    payload = event.get("payload") or {}
    return str(payload.get(join_field, event.get(join_field, "")))


def event_time_of(event: dict[str, Any]) -> float | None:
    try:
        value = float(event.get("event_time"))
    except (TypeError, ValueError):
        return None
    return value


def event_window_bucket(event: dict[str, Any], window_sec: float) -> int:
    """按事件时间对齐到窗口起点，返回可比较的窗口桶序号。"""
    event_time = event_time_of(event)
    if event_time is None:
        return 0
    return int(event_time // _safe_window_sec(window_sec))


def _safe_window_sec(window_sec: Any) -> float:
    try:
        size = float(window_sec)
    except (TypeError, ValueError):
        size = 10.0
    return size if size > 0 else 10.0


def window_bounds(bucket: int, window_sec: float) -> tuple[float, float]:
    size = _safe_window_sec(window_sec)
    start = bucket * size
    return start, start + size


def group_window_events(
    left_events: list[dict], right_events: list[dict],
    join_field: str, window_sec: float,
) -> list[dict[str, Any]]:
    """把双流事件读成按窗口分组的关联输入。

    每个事件都保留在所属窗口里，同一关联键的多个事件不会互相覆盖；
    没有合法关联键或没有事件时间的事件视为不合格，直接跳过。
    """
    groups: dict[tuple[float, float, str], dict[str, list[dict[str, Any]]]] = {}
    for side, events in (("left", left_events), ("right", right_events)):
        for event in events:
            key = event_key(event, join_field)
            if not key or event_time_of(event) is None:
                continue
            bucket = event_window_bucket(event, window_sec)
            ws, we = window_bounds(bucket, window_sec)
            group = groups.setdefault((ws, we, key), {"left": [], "right": []})
            group[side].append(event)

    inputs: list[dict[str, Any]] = []
    for (ws, we, key), sides in groups.items():
        inputs.append({
            "key": key,
            "window_start": ws,
            "window_end": we,
            "left_events": sides["left"],
            "right_events": sides["right"],
        })
    inputs.sort(key=lambda g: (g["window_start"], g["key"]))
    return inputs


class JoinStreamService(BaseModuleService):
    MODULE = "join_stream"
    ENTITY = "关联配置"
    LIST_FIELDS = ["配置名称", "左流源", "右流源", "关联字段", "关联类型", "窗口区间", "输出模式"]
    REQUIRED_FIELDS = ["配置名称", "左流源", "右流源"]
    STATUS_ORDER = ["草稿", "运行中", "已停止"]
    ACTION_RULES = {"启动关联": "草稿", "停止关联": "运行中"}
    NEGATIVE_ACTIONS = []

    def validate_source_availability(
        self, left_source: str, right_source: str,
    ) -> dict[str, Any]:
        """校验左右流源是否可用（存在且未停止/异常断连）。"""
        left_ok = False
        for r in store.rows("stream_source"):
            if r.get("源端名称") == left_source and r.get("status") not in ("已下线", "异常断连"):
                left_ok = True
                break

        right_ok = False
        for r in store.rows("stream_source"):
            if r.get("源端名称") == right_source and r.get("status") not in ("已下线", "异常断连"):
                right_ok = True
                break

        return {
            "ok": left_ok and right_ok,
            "left": {"source": left_source, "available": left_ok},
            "right": {"source": right_source, "available": right_ok},
        }

    # ---- 关联配置读取 ----

    def _find_config(self, join_name: str) -> dict[str, Any] | None:
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == join_name:
                return r
        return None

    @staticmethod
    def _window_sec(join_cfg: dict[str, Any]) -> float:
        return _safe_window_sec(join_cfg.get("窗口区间", "10"))

    @staticmethod
    def _join_field(join_cfg: dict[str, Any]) -> str:
        """关联字段配置非法（例如仍是占位文案）时退回到 user_id。"""
        field = str(join_cfg.get("关联字段") or "").strip()
        return field or "user_id"

    def _resolve_join_field(
        self, join_cfg: dict[str, Any],
        left_events: list[dict], right_events: list[dict],
    ) -> str:
        """配置字段在双流事件里取不到键时，回落到 user_id（样例配置常见）。"""
        field = self._join_field(join_cfg)
        events = list(left_events) + list(right_events)
        if not events:
            return field
        if any(event_key(e, field) for e in events):
            return field
        if any(event_key(e, "user_id") for e in events):
            return "user_id"
        return field

    # ---- 核心：读取双流事件、关联匹配、生成关联结果数据 ----

    def read_stream_events(
        self, left_events: list[dict], right_events: list[dict],
        join_field: str, window_sec: float,
    ) -> list[dict[str, Any]]:
        """把双流事件读成按窗口分组的关联输入（保留同键同窗口的全部事件）。"""
        return group_window_events(left_events, right_events, join_field, window_sec)

    def build_join_results(
        self, join_name: str, left_events: list[dict], right_events: list[dict],
        *, join_field: str | None = None, window_sec: float | None = None,
        join_type: str | None = None,
    ) -> dict[str, Any]:
        """关联匹配并生成关联结果数据（纯计算，不写 store）。

        同一关联键在同一窗口内的所有合格事件两两配对；内关联只保留
        两侧都有事件的键，左/右外关联保留单边事件但不算匹配。
        join_field / window_sec / join_type 可显式覆盖配置值（供管道和接口调用）。
        """
        join_cfg = self._find_config(join_name)
        if join_cfg is None:
            return {"ok": False, "error": f"关联配置「{join_name}」不存在"}

        join_type = self._normalize_join_type(join_type or join_cfg.get("关联类型", "inner"))
        join_field = join_field or self._resolve_join_field(
            join_cfg, left_events, right_events,
        )
        window_sec = self._window_sec(join_cfg) if window_sec is None else _safe_window_sec(window_sec)

        inputs = self.read_stream_events(left_events, right_events, join_field, window_sec)

        records: list[dict[str, Any]] = []
        for item in inputs:
            left = item["left_events"]
            right = item["right_events"]
            matched = bool(left) and bool(right)
            if join_type == "inner" and not matched:
                continue
            if join_type == "left" and not left:
                continue
            if join_type == "right" and not right:
                continue
            records.append({
                "join_name": join_name,
                "key": item["key"],
                "window_start": item["window_start"],
                "window_end": item["window_end"],
                "join_type": join_type,
                "join_field": join_field,
                "matched": matched,
                "left_events": left,
                "right_events": right,
                "left_event_count": len(left),
                "right_event_count": len(right),
                "event_count": len(left) + len(right),
                "pair_count": len(left) * len(right) if matched else 0,
            })

        return {
            "ok": True,
            "join_config": join_name,
            "type": join_type,
            "join_field": join_field,
            "window_sec": window_sec,
            "records": records,
            "key_count": len(records),
            "event_count": sum(r["event_count"] for r in records),
            "matched": sum(1 for r in records if r["matched"]),
        }

    def evaluate_join_window(
        self, join_name: str, left_events: list[dict], right_events: list[dict],
        window_key: str | None = None,
        *, join_field: str | None = None, window_sec: float | None = None,
        join_type: str | None = None,
    ) -> dict[str, Any]:
        """在时间窗口内评估双流关联，并把结果写成可被检查点引用的版本。

        window_key 可选：只重算并覆盖指定窗口（形如 w_<start>_<end>），
        不传则覆盖本次读取覆盖到的全部窗口。同一批事件重复执行只更新
        同一个结果版本，不会追加记录，也不会产生新的版本号。
        """
        built = self.build_join_results(
            join_name, left_events, right_events,
            join_field=join_field, window_sec=window_sec, join_type=join_type,
        )
        with store.transaction():
            return self._evaluate_locked(built, window_key)

    def _evaluate_locked(
        self, built: dict[str, Any], window_key: str | None,
    ) -> dict[str, Any]:
        """在事务里落结果：读到的当前集合和写入的新集合不会被并发请求插队。"""
        join_name = str(built.get("join_config"))
        if not built.get("ok"):
            return built

        target_windows = {
            (r["window_start"], r["window_end"]) for r in built["records"]
        }
        if window_key:
            bounds = self._parse_window_key(window_key)
            if bounds is None:
                return {"ok": False, "error": f"窗口「{window_key}」格式无效"}
            target_windows &= {bounds}
        pending_records = [
            r for r in built["records"]
            if (r["window_start"], r["window_end"]) in target_windows
        ]
        if not pending_records:
            return {
                "ok": True,
                "join_config": join_name,
                "type": built["type"],
                "result_version": None,
                "window_keys": [],
                "total_keys": 0,
                "key_count": 0,
                "event_count": 0,
                "total_events": 0,
                "matched": 0,
                "match_rate": 0.0,
                "message": "该窗口没有合格关联事件，未生成结果版本",
            }

        current_rows = [
            r for r in store.rows(RESULT_TABLE)
            if r.get("join_name") == join_name and r.get("current", True)
        ]
        # 本次重算覆盖的窗口用新结果，其它窗口沿用当前版本，
        # 因此每个版本都是"全部窗口的完整集合"，检查点不会引用半个结果。
        carried = [
            dict(r) for r in current_rows
            if (r["window_start"], r["window_end"]) not in target_windows
        ]
        snapshot_content = carried + [dict(r) for r in pending_records]
        reused_version = self._reusable_version(current_rows, snapshot_content)
        if reused_version is not None:
            version = reused_version
            version_entry = self._version_entry(join_name, version) or {}
            result_rows = [dict(r) for r in current_rows]
        else:
            version = store.next_join_version(join_name)
            for row in current_rows:
                row["current"] = False
            now = time.time()
            snapshot_rows = []
            for record in snapshot_content:
                fingerprint = self._record_fingerprint(record, version)
                row = dict(record)
                row.update({
                    "id": fingerprint,
                    "result_id": fingerprint,
                    "version": version,
                    "current": True,
                    "snapshot_ts": now,
                })
                snapshot_rows.append(row)
            store.rows(RESULT_TABLE).extend(snapshot_rows)
            version_entry = self._version_payload(
                join_name, version, snapshot_rows, now,
            )
            store.rows(VERSION_TABLE).append(version_entry)
            result_rows = snapshot_rows

        matched_count = sum(1 for r in result_rows if r["matched"])
        self._sync_aggregate_sink(join_name, result_rows, version)
        return {
            "ok": True,
            "join_config": join_name,
            "type": built["type"],
            "result_version": version,
            "window_keys": version_entry.get("window_keys", []),
            "windows": version_entry.get("window_keys_detail", []),
            "total_keys": len(result_rows),
            "key_count": len(result_rows),
            "updated_key_count": built["key_count"],
            "event_count": sum(r["event_count"] for r in result_rows),
            "updated_event_count": built["event_count"],
            "total_events": sum(r["event_count"] for r in result_rows),
            "matched": matched_count,
            "match_rate": matched_count / max(len(result_rows), 1),
            "idempotent": reused_version is not None,
        }

    @classmethod
    def _version_payload(
        cls, join_name: str, version: int, records: list[dict[str, Any]],
        snapshot_ts: float,
    ) -> dict[str, Any]:
        return {
            "result_version": version,
            "join_name": join_name,
            "window_keys": sorted({
                cls._window_key(r["window_start"], r["window_end"])
                for r in records
            }),
            "window_keys_detail": [
                {"window_start": start, "window_end": end}
                for start, end in sorted({
                    (r["window_start"], r["window_end"]) for r in records
                })
            ],
            "key_count": len(records),
            "event_count": sum(r["event_count"] for r in records),
            "matched": sum(1 for r in records if r["matched"]),
            "left_event_count": sum(r["left_event_count"] for r in records),
            "right_event_count": sum(r["right_event_count"] for r in records),
            "signature": cls._content_signature(records),
            "snapshot_ts": snapshot_ts,
        }

    @classmethod
    def _version_entry(cls, join_name: str, version: int) -> dict[str, Any] | None:
        for v in store.rows(VERSION_TABLE):
            if (
                v.get("join_name") == join_name
                and int(v.get("result_version", 0)) == int(version)
            ):
                return v
        return None

    @classmethod
    def _reusable_version(
        cls, existing: list[dict[str, Any]], pending: list[dict[str, Any]],
    ) -> int | None:
        """内容与当前版本完全相同才复用版本，否则分配新版本。

        复用只发生在"同一批事件重复执行"时：结果行数、键、窗口和事件 ID
        都没变，就不新增记录、也不递增版本号。
        """
        if not existing:
            return None
        if cls._content_signature(existing) != cls._content_signature(pending):
            return None
        versions = {int(r.get("version", 0)) for r in existing}
        if len(versions) != 1:
            return None
        return versions.pop()

    @staticmethod
    def _content_signature(records: list[dict[str, Any]]) -> str:
        parts = []
        for r in records:
            parts.append("|".join([
                str(r.get("key", "")),
                str(int(float(r.get("window_start", 0)))),
                str(int(float(r.get("window_end", 0)))),
                "1" if r.get("matched") else "0",
                str(r.get("left_event_count", 0)),
                str(r.get("right_event_count", 0)),
                ",".join(str(e.get("event_id", "")) for e in r.get("left_events", [])),
                ",".join(str(e.get("event_id", "")) for e in r.get("right_events", [])),
            ]))
        return hashlib.md5("\n".join(sorted(parts)).encode()).hexdigest()

    @staticmethod
    def _normalize_join_type(join_type: Any) -> str:
        value = str(join_type or "inner").strip().lower()
        if value in ("left_outer", "left_only", "leftjoin"):
            return "left"
        if value in ("right_outer", "rightjoin"):
            return "right"
        if value in ("left", "right", "outer", "full"):
            return value
        return "inner"

    @staticmethod
    def _sync_aggregate_sink(
        join_name: str, records: list[dict[str, Any]], version: int,
    ) -> None:
        """把当前版本的统计同步到聚合落库任务，口径与关联结果数据一致。

        每个关联配置对应一条落库记录，重复执行只刷新统计，不追加记录。
        """
        matched = sum(1 for r in records if r["matched"])
        event_count = sum(int(r.get("event_count", 0)) for r in records)
        windows = sorted({
            (r["window_start"], r["window_end"]) for r in records
        })
        payload = {
            "join_total": len(records),
            "key_count": len(records),
            "event_count": event_count,
            "matched": matched,
            "unmatched": len(records) - matched,
            "match_rate": matched / max(len(records), 1),
            "result_version": version,
            "window_count": len(windows),
            "join_updated_at": time.time(),
        }
        rows = store.rows("aggregate_sink")
        target = None
        for row in rows:
            if row.get("源指标集") == f"join://{join_name}":
                target = row
                break
        if target is None:
            target = {
                "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
                "任务名称": f"join_{join_name}",
                "源指标集": f"join://{join_name}",
                "聚合粒度": "window",
                "输出表名": f"table_{join_name}",
                "落库频率": "60",
                "覆盖模式": "append",
                "分区键": "key",
                "status": "运行中",
                "pending": True,
                "abnormal": False,
            }
            rows.append(target)
        target.update(payload)

    @staticmethod
    def _window_key(start: float, end: float) -> str:
        return f"w_{int(start)}_{int(end)}"

    @staticmethod
    def _parse_window_key(window_key: str) -> tuple[float, float] | None:
        parts = window_key.split("_")
        if len(parts) != 3 or parts[0] != "w":
            return None
        try:
            return float(parts[1]), float(parts[2])
        except ValueError:
            return None

    @staticmethod
    def _record_fingerprint(record: dict[str, Any], version: int | None = None) -> str:
        """结果行主键：关联配置 + 窗口 + 关联键（同一版本内唯一）。"""
        raw = "|".join([
            str(record.get("join_name", "")),
            str(int(float(record.get("window_start", 0)))),
            str(int(float(record.get("window_end", 0)))),
            str(record.get("key", "")),
            "" if version is None else str(int(version)),
        ])
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    # ---- 结果查询 ----

    def latest_result_version(self, join_name: str) -> int:
        versions = [
            int(v.get("result_version", 0))
            for v in store.rows(VERSION_TABLE)
            if v.get("join_name") == join_name
        ]
        return max(versions, default=0)

    def list_join_result(
        self, join_name: str, window_key: str | None = None,
        result_version: int | None = None,
    ) -> dict[str, Any]:
        """按关联配置/窗口/版本读取关联结果数据，返回稳定的一份结果。

        不指定版本时只返回当前版本（latest current rows），同一窗口在
        重复执行后不会同时出现新旧两版记录；版本号是旧版本时仍能完整读出
        当时的集合，供检查点详情页回看。
        """
        with store.locked():
            rows = [
                r for r in store.rows(RESULT_TABLE)
                if r.get("join_name") == join_name
            ]
            if window_key:
                bounds = self._parse_window_key(window_key)
                if bounds is None:
                    return {"ok": False, "error": f"窗口「{window_key}」格式无效"}
                rows = [
                    r for r in rows
                    if (r["window_start"], r["window_end"]) == bounds
                ]
            latest = self.latest_result_version(join_name)
            if result_version is not None:
                rows = [r for r in rows if int(r.get("version", 0)) == int(result_version)]
            else:
                rows = [
                    r for r in rows
                    if int(r.get("version", 0)) == latest and r.get("current", True)
                ]

            by_window: dict[str, dict[str, Any]] = {}
            for r in rows:
                wk = self._window_key(r["window_start"], r["window_end"])
                slot = by_window.setdefault(wk, {
                    "window_key": wk,
                    "window_start": r["window_start"],
                    "window_end": r["window_end"],
                    "key_count": 0,
                    "event_count": 0,
                    "matched": 0,
                    "versions": set(),
                })
                slot["key_count"] += 1
                slot["event_count"] += int(r.get("event_count", 0))
                slot["matched"] += 1 if r.get("matched") else 0
                slot["versions"].add(int(r.get("version", 0)))
            windows = []
            for slot in sorted(by_window.values(), key=lambda s: s["window_start"]):
                slot["versions"] = sorted(slot["versions"])
                windows.append(slot)

            return {
                "ok": True,
                "join_config": join_name,
                "result_version": result_version if result_version is not None else latest,
                "latest_version": latest,
                "key_count": sum(w["key_count"] for w in windows),
                "event_count": sum(w["event_count"] for w in windows),
                "matched": sum(w["matched"] for w in windows),
                "windows": windows,
                "records": [dict(r) for r in rows],
            }

    def list_result_versions(self, join_name: str | None = None) -> list[dict[str, Any]]:
        versions = [
            dict(v) for v in store.rows(VERSION_TABLE)
            if join_name is None or v.get("join_name") == join_name
        ]
        versions.sort(key=lambda v: (str(v.get("join_name", "")), int(v.get("result_version", 0))))
        return versions

    def list_join_statistics(self) -> list[dict[str, Any]]:
        """列出所有流关联的统计信息（口径与关联结果数据一致）。"""
        result = []
        for r in store.rows(self.MODULE):
            if r.get("id", 0) < 100:
                name = r.get("配置名称", "")
                results = [
                    x for x in store.rows(RESULT_TABLE)
                    if x.get("join_name") == name and x.get("current", True)
                ]
                result.append({
                    "join": name,
                    "left_src": r.get("左流源", ""),
                    "right_src": r.get("右流源", ""),
                    "type": r.get("关联类型", ""),
                    "status": r.get("status", ""),
                    "windows": len({
                        (x["window_start"], x["window_end"]) for x in results
                    }),
                    "keys": len(results),
                    "events": sum(int(x.get("event_count", 0)) for x in results),
                    "result_version": self.latest_result_version(name),
                })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        return base