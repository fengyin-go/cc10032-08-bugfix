"""序号8 专属接口：流关联结果与检查点快照的跨页面定位。

通用资源路由只负责配置的查询/登记/动作；这里补上关联结果数据按窗口、
按版本读取，以及检查点快照分页与引用核对，方便两个页面用同一组筛选
条件定位到同一份结果。
"""
from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.pipeline import simulate_stream_batch
from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.join_stream import JoinStreamService
from app.store import store

router = APIRouter(prefix="/api", tags=["流关联与检查点"])

join_service = JoinStreamService()
checkpoint_service = CheckpointMgrService()


def _join_config_by_id(entry_id: int) -> dict[str, Any]:
    entry = store.find(join_service.MODULE, entry_id)
    if entry is None or not entry.get("配置名称"):
        raise HTTPException(status_code=404, detail=f"关联配置 {entry_id} 不存在或已归档")
    return entry


# ---- 流关联结果 ----

@router.get("/join_stream/results", response_model=dict)
def list_join_results(
    join_name: str | None = Query(default=None),
    window_key: str | None = Query(default=None),
    result_version: int | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    """按关联配置/窗口/结果版本读取关联结果数据。

    不指定版本时返回当前版本；同一批事件重复执行不会多出版本或记录。
    """
    names = [join_name] if join_name else [
        r.get("配置名称", "")
        for r in store.rows(join_service.MODULE)
        if r.get("id", 0) < 100 and r.get("配置名称")
    ]
    payloads = []
    for name in names:
        payload = join_service.list_join_result(
            name, window_key=window_key, result_version=result_version,
        )
        if not payload.get("ok"):
            raise HTTPException(status_code=400, detail=payload.get("error"))
        payloads.append(payload)

    windows = [
        {**window, "join_name": payload["join_config"]}
        for payload in payloads
        for window in payload["windows"]
    ]
    windows.sort(key=lambda w: (str(w["join_name"]), w["window_start"]))
    total = len(windows)
    start = (page - 1) * size
    effective_version = result_version
    if effective_version is None and payloads:
        effective_version = payloads[0]["result_version"]
    return {
        "items": windows[start:start + size],
        "total": total,
        "page": page,
        "size": size,
        "result_version": effective_version,
        "latest_version": payloads[0]["latest_version"] if payloads else 0,
        "key_count": sum(p["key_count"] for p in payloads),
        "event_count": sum(p["event_count"] for p in payloads),
    }


@router.get("/join_stream/result-versions", response_model=dict)
def list_join_result_versions(
    join_name: str | None = Query(default=None),
) -> dict[str, Any]:
    versions = join_service.list_result_versions(join_name)
    return {"items": versions, "total": len(versions)}


@router.get("/join_stream/{entry_id}/results", response_model=dict)
def get_join_results(
    entry_id: int,
    window_key: str | None = Query(default=None),
    result_version: int | None = Query(default=None),
) -> dict[str, Any]:
    entry = _join_config_by_id(entry_id)
    payload = join_service.list_join_result(
        entry["配置名称"], window_key=window_key, result_version=result_version,
    )
    if not payload.get("ok"):
        raise HTTPException(status_code=400, detail=payload.get("error"))
    return payload


@router.post("/join_stream/{entry_id}/evaluate", response_model=dict)
def evaluate_join(
    entry_id: int,
    window_key: str | None = Query(default=None),
    window_sec: float | None = Query(default=None),
    batch_size: int = Query(default=10, ge=1, le=200),
    base_ts: float | None = Query(default=None),
) -> dict[str, Any]:
    """读取双流事件、执行关联匹配并写入关联结果数据。

    事件从左右流源读取（无源端配置时按源端名称生成模拟批次）。
    base_ts 显式指定批次起点时，重复调用读到的是同一批事件，结果幂等：
    记录数和版本号不增加；不指定则按当前时间读新批次。
    """
    entry = _join_config_by_id(entry_id)
    left_source = entry.get("左流源", "")
    right_source = entry.get("右流源", "")
    left_events = simulate_stream_batch(left_source, batch_size=batch_size, base_ts=base_ts)
    right_events = simulate_stream_batch(right_source, batch_size=batch_size, base_ts=base_ts)
    result = join_service.evaluate_join_window(
        entry["配置名称"], left_events, right_events,
        window_key=window_key, window_sec=window_sec,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    result["timestamp"] = time.time()
    return result


# ---- 检查点快照 ----

@router.get("/checkpoint_mgr/snapshots", response_model=dict)
def list_checkpoint_snapshots(
    join_name: str | None = Query(default=None),
    result_version: int | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    """检查点详情页分页读取快照，并标注每个快照引用的结果版本是否完整。"""
    return checkpoint_service.list_checkpoint_snapshots(
        join_name=join_name, result_version=result_version, page=page, size=size,
    )


@router.get("/checkpoint_mgr/snapshots/{checkpoint_id}", response_model=dict)
def get_checkpoint_snapshot(checkpoint_id: str) -> dict[str, Any]:
    snapshot = None
    for row in store.rows(checkpoint_service.MODULE):
        if row.get("checkpoint_id") == checkpoint_id:
            snapshot = row
            break
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"检查点「{checkpoint_id}」不存在")

    reference = snapshot.get("join_reference")
    payload: dict[str, Any] = {
        "checkpoint_id": checkpoint_id,
        "config": snapshot.get("配置名称", ""),
        "status": snapshot.get("status", ""),
        "snapshot_ts": snapshot.get("snapshot_ts", 0),
        "operator_states": snapshot.get("operator_states", {}),
        "join_reference": reference,
    }
    if reference:
        payload["check"] = checkpoint_service.verify_join_reference(
            reference.get("join_name", ""),
            int(reference.get("result_version", 0)),
            expected=reference,
        )
        payload["result"] = join_service.list_join_result(
            reference.get("join_name", ""),
            result_version=int(reference.get("result_version", 0)),
        )
    return payload


@router.post("/checkpoint_mgr/snapshots", response_model=dict)
def create_checkpoint_snapshot(
    config_name: str = Query(...),
    join_name: str | None = Query(default=None),
    result_version: int | None = Query(default=None),
) -> dict[str, Any]:
    """按关联结果版本创建检查点快照；结果集合不完整时拒绝保存。"""
    result = checkpoint_service.create_checkpoint(
        config_name,
        {"join_aggregate": {"result_version": result_version or "latest"}},
        join_name=join_name, result_version=result_version,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result
