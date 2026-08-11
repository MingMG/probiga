# -*- coding: utf-8 -*-
from __future__ import annotations

import secrets
from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field, field_validator

from server.ai_bridge.service import (
    claim_job,
    complete_job,
    create_job,
    get_job,
    list_jobs,
    update_progress,
)
from server.api.routers._engine import get_engine
from server.common.config import get_ai_bridge_config

router = APIRouter(prefix="/ai-bridge", tags=["ai-bridge"])


class QuestionCreate(BaseModel):
    channel: Literal["stock", "general"]
    question: str = Field(min_length=1, max_length=8000)

    @field_validator("question")
    @classmethod
    def question_must_contain_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("question cannot be blank")
        return value


class WorkerClaim(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)


class WorkerProgress(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    provider_attempt: Literal["codex_gpt", "deepseek_web"]


class WorkerComplete(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    status: Literal["completed", "failed"]
    answer: str | None = Field(default=None, max_length=200000)
    source: Literal["codex_gpt", "deepseek_web"] | None = None
    source_label: str | None = Field(default=None, max_length=80)
    error_message: str | None = Field(default=None, max_length=1000)


def _owner_user_id(request: Request) -> int:
    user = getattr(request.state, "auth_user", None)
    return int(getattr(user, "id", 0) or 0)


def _require_worker_token(supplied: str | None) -> dict[str, object]:
    config = get_ai_bridge_config()
    expected = str(config.get("token") or "")
    if not expected:
        raise HTTPException(status_code=503, detail="AI bridge worker is not configured")
    if not supplied or not secrets.compare_digest(supplied.strip(), expected):
        raise HTTPException(status_code=401, detail="Invalid AI bridge worker token")
    return config


@router.post("/questions", status_code=202)
def submit_question(payload: QuestionCreate, request: Request):
    job = create_job(
        get_engine(),
        owner_user_id=_owner_user_id(request),
        channel=payload.channel,
        question=payload.question,
    )
    return {"status": "ok", "job": job}


@router.get("/questions")
def recent_questions(
    request: Request,
    channel: Literal["stock", "general"] | None = None,
    limit: int = Query(default=20, ge=1, le=50),
):
    jobs = list_jobs(
        get_engine(),
        owner_user_id=_owner_user_id(request),
        channel=channel,
        limit=limit,
    )
    return {"status": "ok", "jobs": jobs}


@router.get("/questions/{request_id}")
def question_status(request_id: str, request: Request):
    job = get_job(
        get_engine(),
        owner_user_id=_owner_user_id(request),
        request_uid=request_id,
    )
    if job is None:
        raise HTTPException(status_code=404, detail="Question not found")
    return {"status": "ok", "job": job}


@router.post("/worker/claim")
def worker_claim(
    payload: WorkerClaim,
    x_probiga_ai_bridge_token: str | None = Header(default=None),
):
    config = _require_worker_token(x_probiga_ai_bridge_token)
    job = claim_job(
        get_engine(),
        worker_id=payload.worker_id,
        lease_seconds=int(config["lease_seconds"]),
    )
    return {"status": "ok", "job": job}


@router.post("/worker/{request_id}/progress")
def worker_progress(
    request_id: str,
    payload: WorkerProgress,
    x_probiga_ai_bridge_token: str | None = Header(default=None),
):
    config = _require_worker_token(x_probiga_ai_bridge_token)
    updated = update_progress(
        get_engine(),
        request_uid=request_id,
        worker_id=payload.worker_id,
        provider_attempt=payload.provider_attempt,
        lease_seconds=int(config["lease_seconds"]),
    )
    if not updated:
        raise HTTPException(status_code=409, detail="Job lease is no longer owned by this worker")
    return {"status": "ok"}


@router.post("/worker/{request_id}/complete")
def worker_complete(
    request_id: str,
    payload: WorkerComplete,
    x_probiga_ai_bridge_token: str | None = Header(default=None),
):
    _require_worker_token(x_probiga_ai_bridge_token)
    try:
        updated = complete_job(
            get_engine(),
            request_uid=request_id,
            worker_id=payload.worker_id,
            status=payload.status,
            answer=payload.answer,
            source=payload.source,
            source_label=payload.source_label,
            error_message=payload.error_message,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if not updated:
        raise HTTPException(status_code=409, detail="Job lease is no longer owned by this worker")
    return {"status": "ok"}

