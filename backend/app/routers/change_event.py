"""变更事件事务包接口：Binlog 摄取、事件消费、事务归并视图、事务包导出与回放。

路由固定在通用资源路由 /api/{module} 之前注册，避免被动态路径抢先匹配。
所有业务判断都落在 change_event / binlog_parser 领域服务内，路由层只做协议适配。
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.pipeline import pipeline_bus
from app.services import service_for
from app.services.binlog_parser import BinlogParserService
from app.services.change_event import ChangeEventService

router = APIRouter(prefix="/api/change_event", tags=["变更事件事务包"])


class IngestRowsPayload(BaseModel):
    config_id: int | None = None
    rows: list[dict[str, Any]] = Field(default_factory=list)


class ConsumePayload(BaseModel):
    batch_size: int = 100


class ReplayPayload(BaseModel):
    package: dict[str, Any]


def _change_service() -> ChangeEventService:
    service = service_for("change_event")
    assert isinstance(service, ChangeEventService)
    return service


def _parser_service() -> BinlogParserService:
    service = service_for("binlog_parser")
    assert isinstance(service, BinlogParserService)
    return service


@router.post("/ingest")
def ingest_binlog_rows(payload: IngestRowsPayload) -> dict[str, Any]:
    """接收一批原始 Binlog 行（含变更成员与 XID/COMMIT 边界），经解析推入管道。

    变更行携带 binlog_file/binlog_pos；边界行登记事务提交坐标，
    两者来自同一 Binlog 流，保证成员与提交位点口径一致。
    """
    parser = _parser_service()
    if payload.config_id is not None:
        config = parser.get_entry(payload.config_id)
        if config is None:
            raise HTTPException(status_code=404, detail=f"解析配置 {payload.config_id} 不存在")
    else:
        rows = parser.list_entries(page=1, size=1)[0]
        if not rows:
            raise HTTPException(status_code=400, detail="没有可用的Binlog解析配置，请先登记或指定 config_id")
        config = rows[0]
    summary = parser.parse_batch(config, payload.rows)
    return {"ok": True, "message": "Binlog 行已解析入队", "summary": summary,
            "queue_depth": len(pipeline_bus.event_queue)}


@router.post("/consume")
def consume_events(payload: ConsumePayload) -> dict[str, Any]:
    """消费管道中的变更事件：落库变更记录并按事务归并。"""
    service = _change_service()
    entries = service.consume_pipeline_events(batch_size=payload.batch_size)
    return {
        "ok": True,
        "message": f"已消费并归并 {len(entries)} 条变更事件",
        "consumed": len(entries),
        "entries": [
            {
                "事件ID": e.get("事件ID"), "事务ID": e.get("事务ID"),
                "binlog_file": e.get("binlog_file"), "binlog_pos": e.get("binlog_pos"),
                "member_key": e.get("member_key"), "tx_key": e.get("tx_key"),
            }
            for e in entries
        ],
        "queue_depth": len(pipeline_bus.event_queue),
    }


@router.get("/transactions")
def list_transactions(status: str | None = None) -> dict[str, Any]:
    """事务归并视图：与事务包导出共用同一数据源和来源位点。"""
    return _change_service().list_transactions(status_filter=status)


@router.get("/transactions/{tx_key}")
def transaction_detail(tx_key: str) -> dict[str, Any]:
    """打开事务详情：复核成员、起始坐标与事务提交位置。"""
    detail = _change_service().get_transaction_detail(tx_key)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"事务 {tx_key} 不存在")
    return detail


@router.get("/export-batches")
def export_batches() -> dict[str, Any]:
    """导出批次登记：复核每轮文件实际囊括的事务范围。"""
    return {"items": _change_service().list_export_batches(),
            "total": len(pipeline_bus.export_batches)}


@router.get("/replay-runs")
def replay_runs() -> dict[str, Any]:
    """回放导入历史：核对重复导入的幂等结果。"""
    return {"items": _change_service().list_replay_runs(),
            "total": len(pipeline_bus.replay_runs)}


@router.get("/tx-package/download")
def download_tx_package() -> StreamingResponse:
    """下载可回放事务数据包（JSON）。

    仅完整 COMMITTED 事务进入回放集合；未闭合事务在 open_transactions 显式标注。
    """
    service = _change_service()
    package, filename = service.export_replay_package()
    body = service.serialize_package(package)
    return StreamingResponse(
        iter([body]),
        media_type="application/json; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Export-Batch-No": str(package.get("export_batch_no", "")),
        },
    )


@router.get("/tx-package/preview")
def preview_tx_package() -> dict[str, Any]:
    """预览即将导出的事务包内容（不落导出批次），供前端详情复核。"""
    return _change_service().build_replay_package()


@router.post("/replay")
def replay_package(payload: ReplayPayload) -> dict[str, Any]:
    """导入回放事务包：只写入从未应用的事务，重复导入整体跳过。"""
    return _change_service().replay_package(payload.package)
