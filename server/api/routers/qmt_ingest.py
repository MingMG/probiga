# -*- coding: utf-8 -*-
"""Authenticated Windows-QMT result ingestion into the Linux-owned database."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from server.api.qmt_linux_ingest import (
    QmtLinuxIngestError,
    build_plan,
    commit_result,
)
from server.api.qmt_strategy_results import (
    QmtStrategyResultError,
    prepare_strategy_inputs,
    commit_strategy_result,
)
from server.common.config import get_ai_bridge_config
from server.common.qmt_linux_ingest_protocol import (
    MAX_COMMIT_BYTES,
    MAX_PLAN_BYTES,
    QmtLinuxIngestProtocolError,
    decode_object,
    signed_response,
    verify_request_headers,
)


router = APIRouter(prefix="/qmt-ingest", tags=["qmt-ingest"])


async def _signed_body(request: Request, *, limit: int) -> tuple[dict, str]:
    declared = request.headers.get("Content-Length", "").strip()
    if declared:
        try:
            if int(declared) < 1 or int(declared) > limit:
                raise HTTPException(status_code=413, detail="QMT ingestion body is too large")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid Content-Length") from exc
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            raise HTTPException(status_code=413, detail="QMT ingestion body is too large")
    try:
        payload = decode_object(bytes(body), limit=limit)
        secret = str(get_ai_bridge_config().get("token") or "")
        verify_request_headers(
            secret,
            payload,
            timestamp=request.headers.get("X-ProBigA-QMT-Ingest-Time"),
            nonce=request.headers.get("X-ProBigA-QMT-Ingest-Nonce"),
            signature=request.headers.get("X-ProBigA-QMT-Ingest-Signature"),
        )
    except QmtLinuxIngestProtocolError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    return payload, secret


async def _execute(request: Request, *, limit: int, operation):
    payload, secret = await _signed_body(request, limit=limit)
    try:
        result = await run_in_threadpool(operation, payload)
    except (QmtLinuxIngestError, QmtLinuxIngestProtocolError, QmtStrategyResultError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return signed_response(secret, result)


@router.post("/plan")
async def plan(request: Request):
    return await _execute(request, limit=MAX_PLAN_BYTES, operation=build_plan)


@router.post("/commit")
async def commit(request: Request):
    return await _execute(request, limit=MAX_COMMIT_BYTES, operation=commit_result)


@router.post("/strategy-inputs")
async def strategy_inputs(request: Request):
    return await _execute(request, limit=MAX_PLAN_BYTES, operation=prepare_strategy_inputs)


@router.post("/strategy-results")
async def strategy_results(request: Request):
    return await _execute(request, limit=MAX_COMMIT_BYTES, operation=commit_strategy_result)
