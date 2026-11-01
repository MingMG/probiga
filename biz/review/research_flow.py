"""Publication flow for the shared, evidence-bound market research artifact.

The existing digest row is the durable publication unit. A morning update never
recalculates yesterday's facts or replaces its frozen review.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.common.batch_db import read_records

TZ = ZoneInfo("Asia/Shanghai")
JSON_FIELDS = ("quality_json", "market_structure_json", "industry_rotation_json", "factor_validation_json")


def artifact_hash(value: dict) -> str:
    def normalise(item):
        if isinstance(item, (datetime, date)):
            return item.isoformat()
        if isinstance(item, Decimal):
            return int(item) if item == item.to_integral_value() else float(item)
        if hasattr(item, "item"):
            return item.item()
        raise TypeError(type(item).__name__)
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), default=normalise,
                                     allow_nan=False).encode()).hexdigest()


def decode_digest(row: dict) -> dict:
    result = dict(row)
    result["review_date"] = str(result["review_date"])[:10]
    for key in JSON_FIELDS:
        value = result.get(key) or {}
        result[key] = json.loads(value) if isinstance(value, str) else value
    return result


def assert_digest(digest: dict, target: str) -> None:
    quality = digest.get("quality_json") or {}
    dates = quality.get("source_dates") or {}
    if any(str(value or "")[:10] != target for value in
           (digest.get("review_date"), quality.get("target_date"), dates.get("target_bars"))):
        raise RuntimeError("DATA_BLOCKED stage=research_input: input dates do not match target")
    if digest.get("publish_status") != "ready":
        raise RuntimeError("DATA_BLOCKED stage=research_input: " +
                           "; ".join(map(str, quality.get("errors") or ["digest not ready"])))


def next_session(engine, review_date: str) -> str:
    rows = read_records(text("SELECT /*+ MAX_EXECUTION_TIME(15000) */ MIN(trade_date) AS d FROM si_trade_calendar "
                             "WHERE trade_status=1 AND trade_date>:d"), engine,
                        params={"d": review_date})
    target = str(rows[0].get("d") or "")[:10] if rows else ""
    if not target or target <= review_date:
        raise RuntimeError("DATA_BLOCKED stage=research_calendar: next session unavailable")
    return target


def load_digest(engine, review_date: str, *, required: bool = True) -> dict | None:
    rows = read_records(text("SELECT /*+ MAX_EXECUTION_TIME(15000) */ * FROM st_quant_review_digest "
                             "WHERE review_date=:d AND adjust_type=0"), engine,
                        params={"d": review_date})
    if not rows:
        if required:
            raise RuntimeError("DATA_BLOCKED stage=previous_review: exact review is missing")
        return None
    return decode_digest(rows[0])


def collect_context(engine, review_date: str, *, now: datetime | None = None,
                    overnight: bool = False) -> dict:
    """Bound all observations to a declared Shanghai cutoff, without DB writes."""
    current = now or datetime.now(TZ)
    current = current.replace(tzinfo=TZ) if current.tzinfo is None else current.astimezone(TZ)
    cutoff = current if overnight else min(current, datetime.combine(
        datetime.fromisoformat(review_date).date(), time(23, 59, 59), TZ))
    context: dict[str, Any] = {"cutoff_at": cutoff.isoformat(), "source": "ProBigA"}
    # Independent optional observations cannot upgrade the gated stock coverage.
    for name, sql, params in (
        ("indices", "SELECT index_code, trade_date, open, close, high, low, amount, change_pct FROM sm_index_kline "
         "WHERE k_type=1 AND trade_date=:d AND index_code IN "
         "('000001','399001','399006','000688','899050')", {"d": review_date}),
        ("events", "SELECT source, title, content, publish_time, first_seen_at FROM st_news_flash "
         "WHERE publish_time>=:start AND publish_time<=:cutoff AND first_seen_at<=:cutoff "
         "ORDER BY publish_time DESC LIMIT 50",
         {"start": datetime.fromisoformat(review_date), "cutoff": cutoff.replace(tzinfo=None)}),
    ):
        try:
            sql = sql.replace("SELECT ", "SELECT /*+ MAX_EXECUTION_TIME(15000) */ ", 1)
            rows = read_records(text(sql), engine, params=params)
            context[name] = {"status": "OBSERVED" if rows else "UNKNOWN",
                             "source": "sm_index_kline" if name == "indices" else "st_news_flash",
                             "rows": rows}
        except Exception as exc:
            context[name] = {"status": "UNKNOWN", "reason": type(exc).__name__}
    if overnight:
        # Reuse captured source timestamps. Public refresh jobs own acquisition;
        # a report is a read-only consumer, not a second collector.
        try:
            rows = read_records(text(
                "SELECT /*+ MAX_EXECUTION_TIME(15000) */ source, symbol, display_name, price, change_pct, previous_close, "
                "market_time, captured_at, availability FROM st_external_market_context "
                "WHERE captured_at>=:start AND captured_at<=:cutoff "
                "ORDER BY captured_at DESC LIMIT 100"), engine,
                params={"start": cutoff.replace(tzinfo=None) - timedelta(days=3),
                        "cutoff": cutoff.replace(tzinfo=None)})
            latest = {}
            for row in rows:
                latest.setdefault(row["symbol"], row)
            fresh = 0
            baseline_close = datetime.combine(datetime.fromisoformat(review_date).date(), time(15), TZ)
            for row in latest.values():
                try:
                    market_time = datetime.fromisoformat(str(row.get("market_time") or ""))
                    market_time = (market_time.replace(tzinfo=TZ) if market_time.tzinfo is None
                                   else market_time.astimezone(TZ))
                    if market_time <= baseline_close:
                        row["availability"] = "STALE"
                        row["freshness_reason"] = "quote_not_after_baseline_close"
                    elif market_time > cutoff:
                        row["availability"] = "MISSING"
                        row["freshness_reason"] = "quote_after_report_cutoff"
                    elif row.get("availability") == "available":
                        fresh += 1
                except (TypeError, ValueError):
                    row["availability"] = "MISSING"
                    row["freshness_reason"] = "quote_time_unverified"
                if row.get("availability") in {"STALE", "MISSING"}:
                    for field in ("price", "change_pct", "previous_close"):
                        row.pop(field, None)
            context["external_market"] = {"status": "OBSERVED" if fresh else "UNKNOWN",
                                          "source": "st_external_market_context",
                                          "freshness_floor_at": baseline_close.isoformat(),
                                          "fresh_count": fresh, "snapshot_count": len(latest),
                                          "rows": list(latest.values())}
        except Exception as exc:
            context["external_market"] = {"status": "UNKNOWN", "reason": type(exc).__name__}
    return context


def attach_review(engine, digest: dict, *, now=None, completion=None, context=None) -> dict:
    from biz.review.market_decision import generate_research

    target = str(digest["review_date"])[:10]
    assert_digest(digest, target)
    previous_date = (digest["quality_json"].get("source_dates") or {}).get("previous_bars")
    previous = load_digest(engine, str(previous_date)[:10], required=False) if previous_date else None
    prior_factors = (previous or {}).get("factor_validation_json") or {}
    previous_research = next((item for item in
                             (prior_factors.get("early_briefing"), prior_factors.get("market_decision"))
                             if isinstance(item, dict) and item.get("status") == "COMPLETE"
                             and item.get("next_trade_date") == target), None)
    research = generate_research(
        digest, report_kind="review", next_trade_date=next_session(engine, target),
        previous_research=previous_research, now=now, completion=completion,
        context=context if context is not None else collect_context(engine, target, now=now),
    )
    research["research_sha256"] = artifact_hash(research)
    digest["factor_validation_json"]["market_decision"] = research
    digest["compact_review"] = research["content"]
    return digest


def persist_briefing(engine, digest: dict, research: dict) -> None:
    """Lock one exact row and update only its morning artifact, guarding lineage."""
    target = digest["review_date"]
    with engine.begin() as conn:
        suffix = " FOR UPDATE" if engine.dialect.name == "mysql" else ""
        row = conn.execute(text("SELECT publish_status, factor_validation_json FROM "
                                "st_quant_review_digest WHERE review_date=:d AND adjust_type=0" + suffix),
                           {"d": target}).mappings().one_or_none()
        if not row or row["publish_status"] != "ready":
            raise RuntimeError("DATA_BLOCKED stage=briefing_write: parent review unavailable")
        factors = row["factor_validation_json"]
        factors = json.loads(factors) if isinstance(factors, str) else dict(factors)
        parent = factors.get("market_decision") or {}
        if parent.get("research_sha256") != research["parent_research_sha256"]:
            raise RuntimeError("DATA_BLOCKED stage=briefing_write: parent review changed")
        existing = factors.get("early_briefing") or {}
        if existing.get("generated_at", "") > research["generated_at"]:
            raise RuntimeError("DATA_BLOCKED stage=briefing_write: newer briefing already exists")
        factors["early_briefing"] = research
        conn.execute(text("UPDATE st_quant_review_digest SET factor_validation_json=:f "
                          "WHERE review_date=:d AND adjust_type=0"),
                     {"f": json.dumps(factors, ensure_ascii=False, default=str), "d": target})


def generate_briefing(engine, review_date: str, *, persist: bool = True,
                      now=None, completion=None, digest=None, context=None) -> dict:
    from biz.review.market_decision import generate_research

    digest = digest if digest is not None else load_digest(engine, review_date)
    assert_digest(digest, review_date)
    parent = digest["factor_validation_json"].get("market_decision") or {}
    target = next_session(engine, review_date)
    if (parent.get("status") != "COMPLETE" or parent.get("review_date") != review_date
            or parent.get("next_trade_date") != target or not parent.get("research_sha256")):
        raise RuntimeError("DATA_BLOCKED stage=previous_review: verified research is missing or stale")
    research = generate_research(
        digest, report_kind="briefing", next_trade_date=target,
        previous_research=parent, now=now, completion=completion,
        context=context if context is not None else collect_context(engine, review_date, now=now, overnight=True),
    )
    research["parent_research_sha256"] = parent["research_sha256"]
    research["parent_research"] = parent
    research["research_sha256"] = artifact_hash(research)
    if persist:
        persist_briefing(engine, digest, research)
        stored = load_digest(engine, review_date)["factor_validation_json"].get("early_briefing") or {}
        if (stored.get("research_sha256") != research["research_sha256"]
                or stored.get("content") != research["content"]):
            raise RuntimeError("FAILED stage=briefing_readback: canonical report differs from generation")
        return stored
    return research
