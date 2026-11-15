"""FastAPI application: DST-safe airport traffic ingestion.

Endpoints
---------
GET  /health         liveness probe
POST /upload         back-fill one CSV batch (atomic accept / whole-batch reject)
GET  /traffic        determined UTC details + pending sample ids
POST /purge          purge settled raw readings up to a truncation number
GET  /purge          current purge watermark / retained boundary / history
GET  /batches        batch audit trail (rejections survive rollback)
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .csv_utils import CsvValidationError, parse_counter_csv
from .db import audit_connect, connect, init_db
from .service import (
    ConflictError,
    IngestionError,
    PurgeError,
    UnresolvableTimelineError,
    ingest_batch,
    list_audit,
    purge_prefix,
    purge_status,
    query_traffic,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Two independent connections: the audit connection is autocommit so that
    # rejection records persist even when a writer transaction rolls back.
    app.state.db = connect()
    app.state.audit_db = audit_connect()
    init_db(app.state.db)
    yield
    app.state.db.close()
    app.state.audit_db.close()


app = FastAPI(
    title="机场入口客流 DST 回拨入仓服务",
    description=(
        "接收 America/New_York 本地时刻（无偏移）的补传 CSV。回拨当天 "
        "01:00–02:00 的钟面时刻对应两个 UTC，本服务只发布在所有合法时间线中 "
        "UTC 都唯一的记录，其余列为待定采样号。已结算的早期采样可按截断号 "
        "清退：截断号只前移，清退后含已清退号的批次整批拒收，剩余读数的 "
        "UTC 判定与后续补传合法性保持不变。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.exception_handler(IngestionError)
async def _ingestion_error_handler(_: Request, exc: IngestionError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"accepted": False, "error": exc.reason},
    )


@app.exception_handler(PurgeError)
async def _purge_error_handler(_: Request, exc: PurgeError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"purged": False, "error": exc.reason},
    )


@app.exception_handler(CsvValidationError)
async def _csv_error_handler(_: Request, exc: CsvValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={"accepted": False, "error": f"invalid CSV: {exc}"},
    )


@app.get("/health", tags=["运维"])
def health() -> dict:
    return {"status": "ok"}


@app.post(
    "/upload",
    tags=["补传"],
    summary="补传一批 CSV（采样号, 本地时刻, 通过人数）",
    responses={
        200: {"description": "批次全部入库，或与已有内容完全相同而幂等跳过"},
        400: {"description": "CSV 格式错误"},
        409: {"description": "同号异值冲突，整批拒收"},
        422: {"description": "合并后排不出合法 UTC 时间线，整批拒收"},
    },
)
async def upload(request: Request, file: UploadFile = File(..., description="UTF-8 CSV 文件")) -> dict:
    content = await file.read()
    records = parse_counter_csv(content)
    result = ingest_batch(
        request.app.state.db,
        records,
        audit_conn=request.app.state.audit_db,
    )
    return {
        "accepted": True,
        "batch_ref": result.batch_ref,
        "record_count": result.record_count,
        "inserted_sample_ids": result.inserted,
        "skipped_duplicate_sample_ids": result.skipped_duplicates,
    }


@app.get(
    "/traffic",
    tags=["查询"],
    summary="查询客流明细（UTC 唯一的记录）与待定采样号",
)
def traffic(request: Request) -> dict:
    return query_traffic(request.app.state.db)


@app.get("/batches", tags=["运维"], summary="批次受理审计记录（含被拒收批次）")
def batches(request: Request) -> dict:
    return {"batches": list_audit(request.app.state.db)}


class PurgeRequest(BaseModel):
    truncate_upto_sample_id: int = Field(
        ...,
        ge=1,
        description="截断号：删除所有 sample_id ≤ 该值的原始读数；只能比当前截断号更大",
    )


@app.post(
    "/purge",
    tags=["清退"],
    summary="按截断号清退已结算的早期采样（删除 sample_id ≤ 截断号的读数）",
    responses={
        200: {"description": "清退完成，返回清退数量与新截断号"},
        409: {"description": "截断号未前移（不大于当前截断号），未做任何删除"},
        422: {"description": "请求体校验失败"},
    },
)
def purge(request: Request, body: PurgeRequest) -> dict:
    result = purge_prefix(request.app.state.db, body.truncate_upto_sample_id)
    return {
        "purged": True,
        "deleted_count": result.deleted_count,
        "watermark": result.watermark,
        "boundary_sample_id": result.boundary_sample_id,
        "boundary_utc": result.boundary_utc,
    }


@app.get(
    "/purge",
    tags=["清退"],
    summary="当前清退状态：截断号、保留的时间线边界与清退历史",
)
def purge_state(request: Request) -> dict:
    return purge_status(request.app.state.db)
