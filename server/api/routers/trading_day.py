"""Authenticated personal observation plans, separate from trading authority."""
from __future__ import annotations

from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, StrictInt

from server.common.trading_day_store import (
    JournalConflict, JournalStoreError, TradingDayStore, journal_runtime_root,
)


router = APIRouter(prefix="/trading-day", tags=["trading-day"])


class PlanInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)
    stock_code: str = Field(pattern=r"^[0-9]{6}$")
    stock_name: str = Field(default="", max_length=80)
    theme: str = Field(default="", max_length=160)
    reason: str = Field(default="", max_length=1200)
    trigger: str = Field(default="", max_length=1200)
    invalidation: str = Field(default="", max_length=1200)
    source_as_of: str = Field(default="", max_length=40)
    source_run_uid: str = Field(default="", max_length=160)
    status: Literal["WATCHING", "WAITING", "PAUSED", "REVIEWED"] = "WATCHING"
    note: str = Field(default="", max_length=2000)


class ReviewInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)
    text: str = Field(max_length=12000)


class JournalInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    revision: Annotated[StrictInt, Field(ge=0, le=2**53 - 1)]
    plans: list[PlanInput] = Field(max_length=100)
    review: ReviewInput


def _account_id(request: Request) -> int:
    user = getattr(request.state, "auth_user", None)
    user_id = getattr(user, "id", None)
    if type(user_id) is not int or user_id <= 0 or getattr(user, "is_active", False) is not True:
        raise HTTPException(status_code=401, detail={"error": "account_session_required", "message": "请使用账户登录后保存个人观察计划。"})
    return user_id


def _store() -> TradingDayStore:
    return TradingDayStore(journal_runtime_root())


def _unavailable() -> HTTPException:
    return HTTPException(status_code=503, detail={"error": "journal_store_unavailable", "message": "个人观察记录暂时不可读写，请稍后重试；尚未确认保存。"})


@router.get("/journal")
def read_journal(request: Request, response: Response, trade_date: date = Query()):
    user_id = _account_id(request)
    response.headers["Cache-Control"] = "private, no-store"
    try:
        return _store().read(user_id, trade_date.isoformat())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (JournalStoreError, OSError) as exc:
        raise _unavailable() from exc


@router.put("/journal")
def save_journal(request: Request, response: Response, body: JournalInput, trade_date: date = Query()):
    user_id = _account_id(request)
    response.headers["Cache-Control"] = "private, no-store"
    try:
        return _store().save(user_id, trade_date.isoformat(), body.model_dump())
    except JournalConflict as exc:
        raise HTTPException(status_code=409, detail={"error": "journal_revision_conflict", "revision": exc.revision, "message": "记录已在其他页面更新，请重新读取后合并修改。"}) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (JournalStoreError, OSError) as exc:
        raise _unavailable() from exc
