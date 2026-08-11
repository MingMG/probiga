# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from integrations.wecom.webhook import WeComWebhookError, send_markdown
from server.api.commentary_utils import (
    build_rule_checks,
    build_verdict,
    parse_commentary_text,
    project_feasibility_summary,
)
from server.api.routers._engine import get_engine
from server.common.config import get_wecom_webhook
from server.common.minute_data import minute_source_info
from server.common.scheduler_tasks import (
    DEFAULT_SCHEDULER_COLUMNS,
    ensure_scheduler_columns as ensure_shared_scheduler_columns,
    update_scheduler_tasks,
    upsert_scheduler_task as shared_upsert_scheduler_task,
)
from server.common.sql_reader import read_sql_rows
from server.engine.data_loader import StockDataLoader

router = APIRouter(tags=["commentary"])
logger = logging.getLogger(__name__)

PROFILE_TABLE = "st_commentary_profiles"
SCRIPT_PATH = "tools/run_commentary_watch.py"
SCHEDULER_COLUMNS = DEFAULT_SCHEDULER_COLUMNS


class CommentaryAssessRequest(BaseModel):
    text: str = Field(..., description="原始股评文本")
    phase: Literal["premarket", "intraday"] = Field(default="premarket")
    reference_date: str | None = Field(default=None, description="股评发布日期，如 2026-06-13")
    as_of_date: str | None = Field(default=None, description="评估日期，默认今天")


class CommentaryProfileBody(BaseModel):
    id: int | None = None
    profile_name: str = Field(..., min_length=1, max_length=120)
    text: str = Field(..., min_length=1)
    reference_date: str | None = None
    phase: Literal["premarket", "intraday"] = "premarket"
    cron_time: str = Field(default="08:55", min_length=4, max_length=5)
    enabled: bool = True
    push_enabled: bool = True
    webhook_kind: Literal["default", "briefing", "news"] = "briefing"


def _read_sql(sql: str, params: dict | None = None) -> list[dict]:
    return read_sql_rows(get_engine(), sql, params, context="commentary")


def _exec_sql(sql: str, params: dict | None = None) -> None:
    with get_engine().begin() as conn:
        conn.execute(text(sql), params or {})


def _table_columns(table_name: str) -> set[str]:
    with get_engine().connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchall()
    return {str(row[0]) for row in rows}


def _ensure_profile_table() -> None:
    _exec_sql(
        f"""
        CREATE TABLE IF NOT EXISTS `{PROFILE_TABLE}` (
          `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
          `profile_name` VARCHAR(120) NOT NULL,
          `commentary_text` MEDIUMTEXT NOT NULL,
          `reference_date` DATE DEFAULT NULL,
          `phase` VARCHAR(16) NOT NULL DEFAULT 'premarket',
          `cron_time` VARCHAR(5) NOT NULL DEFAULT '08:55',
          `enabled` TINYINT NOT NULL DEFAULT 1,
          `push_enabled` TINYINT NOT NULL DEFAULT 1,
          `webhook_kind` VARCHAR(16) NOT NULL DEFAULT 'briefing',
          `last_run_at` DATETIME DEFAULT NULL,
          `last_run_status` VARCHAR(32) DEFAULT '',
          `last_push_at` DATETIME DEFAULT NULL,
          `last_push_status` VARCHAR(32) DEFAULT '',
          `created_at` DATETIME DEFAULT NULL,
          `updated_at` DATETIME DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def _ensure_scheduler_columns() -> set[str]:
    return ensure_shared_scheduler_columns(get_engine(), column_definitions=SCHEDULER_COLUMNS)


def _profile_task_type(profile_id: int) -> str:
    return f"commentary_watch_{int(profile_id)}"


def _profile_task_name(profile_name: str) -> str:
    short = (profile_name or "").strip()[:32] or "未命名股评"
    return f"股评监控-{short}"


def _upsert_scheduler_task(payload: dict[str, Any], *, task_type: str) -> dict[str, Any]:
    return shared_upsert_scheduler_task(
        get_engine(),
        payload,
        lookup_where="task_type = :task_type",
        lookup_params={"task_type": task_type},
        update_exclude={"task_type"},
        forced_values={"task_type": task_type},
        column_definitions=SCHEDULER_COLUMNS,
    )


def _toggle_scheduler_task(task_type: str, enabled: bool) -> None:
    try:
        update_scheduler_tasks(
            get_engine(),
            {"enabled": 1 if enabled else 0},
            lookup_where="task_type = :task_type",
            lookup_params={"task_type": task_type},
        )
    except Exception:
        logger.debug("Failed to update commentary scheduler enabled flag.", exc_info=True)


def _latest_trade_date(as_of_date: str | None = None) -> str:
    sql = "SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type = 1"
    params: dict[str, str] = {}
    if as_of_date:
        sql += " AND trade_date <= :d"
        params["d"] = as_of_date[:10]
    rows = _read_sql(sql, params)
    if rows and rows[0].get("d"):
        return str(rows[0]["d"])[:10]
    return (as_of_date or date.today().isoformat())[:10]


def _load_daily_bars(stock_code: str, as_of_date: str) -> list[dict]:
    rows = _read_sql(
        """
        SELECT trade_date, open, close, high, low, volume, amount, change_pct
        FROM sm_stock_kline
        WHERE stock_code = :c
          AND k_type = 1
          AND trade_date <= :d
        ORDER BY trade_date DESC
        LIMIT 40
        """,
        {"c": stock_code, "d": as_of_date[:10]},
    )
    rows.reverse()
    return rows


def _pick_anchor_bar(rows: list[dict], anchor_dates: list[str], fallback_date: str | None) -> dict | None:
    targets = anchor_dates[:] if anchor_dates else []
    if fallback_date:
        targets.append(fallback_date[:10])
    for target in targets:
        for row in rows:
            if str(row.get("trade_date", ""))[:10] >= target[:10]:
                return row
    return rows[0] if rows else None


def _load_news_items(stock_code: str, stock_name: str) -> list[dict]:
    like_name = f"%{stock_name}%"
    like_code = f"%{stock_code}%"
    news_rows = _read_sql(
        """
        SELECT source, title, content, publish_time
        FROM st_news_flash
        WHERE title LIKE :name OR content LIKE :name OR stocks LIKE :code
        ORDER BY publish_time DESC
        LIMIT 5
        """,
        {"name": like_name, "code": like_code},
    )
    notice_rows = _read_sql(
        """
        SELECT 'notice' AS source, title, column_name AS content, notice_date AS publish_time
        FROM si_notice_eastmoney
        WHERE stock_code = :code
          AND association_validated = 1
        ORDER BY notice_date DESC
        LIMIT 3
        """,
        {"code": stock_code},
    )
    merged = news_rows + notice_rows
    merged.sort(key=lambda row: str(row.get("publish_time") or ""), reverse=True)
    return merged[:5]


def _assess_one(
    item: dict,
    *,
    phase: str,
    trade_date: str,
    loader: StockDataLoader,
) -> dict:
    payload = loader.load_full_data(
        item["stock_code"],
        trade_date=None if phase == "intraday" else trade_date,
        use_realtime=(phase == "intraday"),
    )
    market = payload.get("market") or {}
    technical = payload.get("technical") or {}
    capital = payload.get("capital") or {}
    bars = _load_daily_bars(item["stock_code"], trade_date)
    anchor_bar = _pick_anchor_bar(bars, item.get("anchor_dates") or [], trade_date)
    latest_bar = bars[-1] if bars else {}
    news_items = _load_news_items(item["stock_code"], item["stock_name"])

    current_price = market.get("price")
    ma = technical.get("ma") or {}
    checks = build_rule_checks(
        phase=phase,
        current_price=float(current_price) if current_price is not None else None,
        ma5=ma.get("ma5"),
        ma10=ma.get("ma10"),
        support=technical.get("support"),
        anchor_low=float(anchor_bar.get("low")) if anchor_bar and anchor_bar.get("low") is not None else None,
        anchor_volume=float(anchor_bar.get("volume")) if anchor_bar and anchor_bar.get("volume") is not None else None,
        latest_volume=float(latest_bar.get("volume")) if latest_bar and latest_bar.get("volume") is not None else None,
        news_count=len(news_items),
    )
    verdict = build_verdict(checks)

    return {
        "index": item["index"],
        "stock_code": item["stock_code"],
        "stock_name": item["stock_name"],
        "sector": item.get("sector") or "",
        "description": item.get("description") or "",
        "logic_tags": item.get("logic_tags") or [],
        "anchor_dates": item.get("anchor_dates") or [],
        "trade_date": trade_date,
        "current": {
            "price": market.get("price"),
            "change_pct": market.get("change_pct"),
            "turnover_ratio": market.get("turnover_ratio"),
            "volume_ratio": market.get("volume_ratio"),
            "main_net_inflow_wan": ((capital.get("today") or {}).get("main_net_inflow")),
            "flow_5d_wan": capital.get("flow_5d"),
            "trend": (technical.get("trend") or {}).get("short"),
            "support": technical.get("support"),
            "resistance": technical.get("resistance"),
        },
        "anchor": {
            "trade_date": str((anchor_bar or {}).get("trade_date") or "")[:10],
            "open": (anchor_bar or {}).get("open"),
            "close": (anchor_bar or {}).get("close"),
            "high": (anchor_bar or {}).get("high"),
            "low": (anchor_bar or {}).get("low"),
            "change_pct": (anchor_bar or {}).get("change_pct"),
            "volume": (anchor_bar or {}).get("volume"),
        },
        "news": {
            "count": len(news_items),
            "items": news_items,
        },
        "checks": checks,
        "verdict": verdict,
    }


def _assess_commentary_core(req: CommentaryAssessRequest) -> dict[str, Any]:
    parsed = parse_commentary_text(
        req.text,
        reference_date=req.reference_date,
    )
    phase = req.phase
    trade_date = _latest_trade_date(req.as_of_date)
    loader = StockDataLoader()

    items = []
    for item in parsed["items"]:
        try:
            items.append(_assess_one(item, phase=phase, trade_date=trade_date, loader=loader))
        except Exception as exc:
            items.append({
                "index": item["index"],
                "stock_code": item["stock_code"],
                "stock_name": item["stock_name"],
                "sector": item.get("sector") or "",
                "description": item.get("description") or "",
                "error": str(exc),
            })

    return {
        "reference_date": parsed["reference_date"],
        "phase": phase,
        "trade_date": trade_date,
        "project_feasibility": project_feasibility_summary(minute_source_info()),
        "items": items,
        "total": len(items),
    }


def _format_push_markdown(profile_name: str, result: dict[str, Any]) -> str:
    lines = [
        f"## 股评监控 | {profile_name}",
        f"> 评估模式: {result.get('phase')} | 交易日: {result.get('trade_date')} | 参考日: {result.get('reference_date')}",
    ]
    items = result.get("items") or []
    if not items:
        lines.append("暂无可解析的股票条目。")
    for item in items[:10]:
        if item.get("error"):
            lines.append(f"\n**{item.get('stock_name','-')}({item.get('stock_code','-')})**")
            lines.append(f"> 评估失败: {item['error'][:80]}")
            continue
        verdict = item.get("verdict") or {}
        current = item.get("current") or {}
        lines.append(
            f"\n**{item.get('index')}. {item.get('stock_name')}({item.get('stock_code')})** "
            f"<font color=\"{'warning' if verdict.get('status') in {'TRACK','WATCH'} else 'comment'}\">{verdict.get('status','-')}</font>"
        )
        lines.append(
            f"现价 {current.get('price','-')} / 涨跌 {current.get('change_pct','-')}% / "
            f"量比 {current.get('volume_ratio','-')} / 5日资金 {current.get('flow_5d_wan','-')}"
        )
        lines.append(verdict.get("summary") or "")
        check_parts = []
        for check in item.get("checks") or []:
            if check.get("status") == "info":
                continue
            prefix = {
                "pass": "通过",
                "warn": "观察",
                "fail": "失效",
            }.get(check.get("status"), check.get("status"))
            check_parts.append(f"{check.get('label')}:{prefix}")
        if check_parts:
            lines.append("> " + " | ".join(check_parts[:5]))
    content = "\n".join(lines).strip()
    if len(content) > 3600:
        content = content[:3600] + "\n\n> 内容过长，已截断。"
    return content


def _serialize_profile(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "profile_name": row.get("profile_name") or "",
        "text": row.get("commentary_text") or "",
        "reference_date": str(row.get("reference_date") or "")[:10] or None,
        "phase": row.get("phase") or "premarket",
        "cron_time": row.get("cron_time") or "08:55",
        "enabled": bool(int(row.get("enabled") or 0)),
        "push_enabled": bool(int(row.get("push_enabled") or 0)),
        "webhook_kind": row.get("webhook_kind") or "briefing",
        "last_run_at": row.get("last_run_at"),
        "last_run_status": row.get("last_run_status") or "",
        "last_push_at": row.get("last_push_at"),
        "last_push_status": row.get("last_push_status") or "",
    }


def _profile_row(profile_id: int) -> dict[str, Any]:
    _ensure_profile_table()
    rows = _read_sql(f"SELECT * FROM `{PROFILE_TABLE}` WHERE id = :id LIMIT 1", {"id": profile_id})
    if not rows:
        raise HTTPException(status_code=404, detail="profile not found")
    return rows[0]


def _profile_to_request(profile: dict[str, Any], *, as_of_date: str | None = None) -> CommentaryAssessRequest:
    return CommentaryAssessRequest(
        text=profile.get("commentary_text") or "",
        reference_date=str(profile.get("reference_date") or "")[:10] or None,
        phase=profile.get("phase") or "premarket",
        as_of_date=as_of_date,
    )


def _sync_profile_task(profile: dict[str, Any]) -> dict[str, Any]:
    profile_id = int(profile["id"])
    task_type = _profile_task_type(profile_id)
    payload = {
        "task_name": _profile_task_name(profile.get("profile_name") or ""),
        "task_type": task_type,
        "group_name": "资讯公告",
        "script_path": SCRIPT_PATH,
        "script_args": f"--profile-id {profile_id} --push",
        "cron_time": profile.get("cron_time") or "08:55",
        "interval_minutes": 0,
        "enabled": 1 if int(profile.get("enabled") or 0) else 0,
        "description": "按保存的股评文本执行盘前/盘中校验，并可推送企业微信。",
        "sort_order": 96 + profile_id,
        "date_param": "",
    }
    return _upsert_scheduler_task(payload, task_type=task_type)


def _run_profile_assessment(profile_id: int, *, push: bool = False, as_of_date: str | None = None) -> dict[str, Any]:
    profile = _profile_row(profile_id)
    result = _assess_commentary_core(_profile_to_request(profile, as_of_date=as_of_date))
    status = "success"
    push_result: dict[str, Any] | None = None

    _exec_sql(
        f"UPDATE `{PROFILE_TABLE}` SET last_run_at = NOW(), last_run_status = :s WHERE id = :id",
        {"s": status, "id": profile_id},
    )

    if push:
        webhook_kind = profile.get("webhook_kind") or "briefing"
        webhook_url = get_wecom_webhook(webhook_kind, required=False)
        if not webhook_url:
            push_result = {"success": False, "error": f"未配置 {webhook_kind} webhook"}
        else:
            content = _format_push_markdown(profile.get("profile_name") or "", result)
            try:
                send_markdown(webhook_url, content)
                push_result = {"success": True}
                _exec_sql(
                    f"UPDATE `{PROFILE_TABLE}` SET last_push_at = NOW(), last_push_status = 'success' WHERE id = :id",
                    {"id": profile_id},
                )
            except WeComWebhookError as exc:
                push_result = {"success": False, "error": str(exc)}
                _exec_sql(
                    f"UPDATE `{PROFILE_TABLE}` SET last_push_at = NOW(), last_push_status = :s WHERE id = :id",
                    {"id": profile_id, "s": f"failed:{str(exc)[:120]}"},
                )

    return {
        "profile": _serialize_profile(_profile_row(profile_id)),
        "result": result,
        "push": push_result,
    }


@router.post("/commentary/assess")
def assess_commentary(req: CommentaryAssessRequest):
    return _assess_commentary_core(req)


@router.get("/commentary/profiles")
def list_commentary_profiles():
    _ensure_profile_table()
    rows = _read_sql(f"SELECT * FROM `{PROFILE_TABLE}` ORDER BY updated_at DESC, id DESC")
    task_rows = []
    try:
        task_rows = _read_sql(
            "SELECT id, task_type, enabled, cron_time, last_run_status, last_run_at FROM st_scheduled_tasks "
            "WHERE task_type LIKE 'commentary_watch_%'"
        )
    except Exception:
        task_rows = []
    task_map = {row.get("task_type"): row for row in task_rows}
    data = []
    for row in rows:
        item = _serialize_profile(row)
        task = task_map.get(_profile_task_type(item["id"])) or {}
        item["task"] = task
        data.append(item)
    return {"data": data, "total": len(data)}


@router.post("/commentary/profiles")
def save_commentary_profile(body: CommentaryProfileBody):
    _ensure_profile_table()
    if body.id:
        _exec_sql(
            f"""
            UPDATE `{PROFILE_TABLE}`
            SET profile_name = :profile_name,
                commentary_text = :text,
                reference_date = :reference_date,
                phase = :phase,
                cron_time = :cron_time,
                enabled = :enabled,
                push_enabled = :push_enabled,
                webhook_kind = :webhook_kind,
                updated_at = NOW()
            WHERE id = :id
            """,
            {
                "id": body.id,
                "profile_name": body.profile_name.strip(),
                "text": body.text.strip(),
                "reference_date": body.reference_date or None,
                "phase": body.phase,
                "cron_time": body.cron_time,
                "enabled": 1 if body.enabled else 0,
                "push_enabled": 1 if body.push_enabled else 0,
                "webhook_kind": body.webhook_kind,
            },
        )
        profile_id = body.id
        action = "updated"
    else:
        with get_engine().begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO `{PROFILE_TABLE}`
                    (profile_name, commentary_text, reference_date, phase, cron_time, enabled, push_enabled, webhook_kind, created_at, updated_at)
                    VALUES (:profile_name, :text, :reference_date, :phase, :cron_time, :enabled, :push_enabled, :webhook_kind, NOW(), NOW())
                    """
                ),
                {
                    "profile_name": body.profile_name.strip(),
                    "text": body.text.strip(),
                    "reference_date": body.reference_date or None,
                    "phase": body.phase,
                    "cron_time": body.cron_time,
                    "enabled": 1 if body.enabled else 0,
                    "push_enabled": 1 if body.push_enabled else 0,
                    "webhook_kind": body.webhook_kind,
                },
            )
            profile_id = int(conn.execute(text("SELECT LAST_INSERT_ID()")).scalar() or 0)
        action = "inserted"

    profile = _profile_row(profile_id)
    _toggle_scheduler_task(_profile_task_type(profile_id), bool(int(profile.get("enabled") or 0)))
    return {"success": True, "action": action, "profile": _serialize_profile(profile)}


@router.post("/commentary/profiles/{profile_id}/toggle")
def toggle_commentary_profile(profile_id: int):
    profile = _profile_row(profile_id)
    enabled = 0 if int(profile.get("enabled") or 0) else 1
    _exec_sql(
        f"UPDATE `{PROFILE_TABLE}` SET enabled = :e, updated_at = NOW() WHERE id = :id",
        {"e": enabled, "id": profile_id},
    )
    _toggle_scheduler_task(_profile_task_type(profile_id), bool(enabled))
    return {"success": True, "enabled": bool(enabled)}


@router.post("/commentary/profiles/{profile_id}/task/ensure")
def ensure_commentary_profile_task(profile_id: int):
    profile = _profile_row(profile_id)
    result = _sync_profile_task(profile)
    return {"success": True, **result}


@router.post("/commentary/profiles/{profile_id}/run")
def run_commentary_profile(
    profile_id: int,
    push: bool = Query(default=False),
    as_of_date: str = Query(default=""),
):
    return _run_profile_assessment(profile_id, push=push, as_of_date=as_of_date or None)
