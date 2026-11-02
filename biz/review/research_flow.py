"""Publication flow for the shared, evidence-bound market research artifact.

The existing digest row is the durable publication unit. A morning update never
recalculates yesterday's facts or replaces its frozen review.
"""
from __future__ import annotations

import hashlib
import json
import re
import time as monotonic_time
from contextlib import nullcontext
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.common.batch_db import routed_read_engine

TZ = ZoneInfo("Asia/Shanghai")
JSON_FIELDS = ("quality_json", "market_structure_json", "industry_rotation_json", "factor_validation_json")
_UNSET = object()


def read_records(statement, engine, *, params=None):
    """Single bounded query, with no batch-helper retry or shared transaction."""
    with routed_read_engine(statement, engine).connect() as connection:
        return [dict(row) for row in connection.execute(statement, params or {}).mappings().all()]


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
    context: dict[str, Any] = {"cutoff_at": cutoff.isoformat(), "source": "ProBigA",
                               "market_data_asof_at": current.isoformat()}
    from biz.review.theme_evidence import collect_theme_evidence
    context["theme_evidence"] = collect_theme_evidence(engine, review_date, asof=current)
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
    # A close bulletin is older than the latest evening headlines. Query its
    # exact date separately so the latest-news window cannot silently omit it.
    try:
        rows = read_records(text(
            "SELECT /*+ MAX_EXECUTION_TIME(15000) */ source, title, content, publish_time, first_seen_at "
            "FROM st_news_flash WHERE publish_time>=:start AND publish_time<=:end "
            "AND first_seen_at<=:cutoff AND (title LIKE '%成交额合计%' OR content LIKE '%成交额合计%') "
            "ORDER BY publish_time DESC LIMIT 5"), engine,
            params={"start": datetime.fromisoformat(review_date),
                    "end": min(cutoff.replace(tzinfo=None), datetime.combine(
                        date.fromisoformat(review_date), time(23, 59, 59))),
                    "cutoff": cutoff.replace(tzinfo=None)})
        context["market_turnover"] = reported_turnover(rows)
    except Exception as exc:
        context["market_turnover"] = {"status": "UNKNOWN", "reason": type(exc).__name__}
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
    from biz.review.research_inputs import enrich_context
    return enrich_context(engine, {"review_date": review_date}, context,
                          report_kind="briefing" if overnight else "review", now=current)


def reported_turnover(rows: list[dict]) -> dict:
    """Keep a media-reported SH/SZ total distinct from computed all-A turnover."""
    for row in rows:
        content = str(row.get("content") or "")
        if "沪深" not in str(row.get("title") or "") + content:
            continue
        parts = []
        for label in ("沪市", "深市"):
            match = re.search(label + r"[^。；;]{0,30}?([\d,]+(?:\.\d+)?)\s*亿元", content)
            if not match:
                break
            parts.append(float(match.group(1).replace(",", "")))
        if len(parts) != 2 or not all(value > 0 for value in parts):
            continue
        result = {"status": "OBSERVED", "scope": "沪深两市，不含北交所",
                  "definition": "新闻报道值，独立于本库日线金额求和；不能视为资金净流入",
                  "reported_total_amount": round(sum(parts) * 100_000_000, 2),
                  "source": row.get("source"), "title": row.get("title"),
                  "publish_time": row.get("publish_time"), "first_seen_at": row.get("first_seen_at"),
                  "content": content[:1800]}
        change = re.search(r"(?:较|比)(?:上一交易日|前一交易日|上一日|昨日)[^。；;]{0,12}?(增加|增量|减少|减量)\s*([\d,]+(?:\.\d+)?)\s*亿元", content)
        if change:
            result["reported_change_amount"] = float(change.group(2).replace(",", "")) * 100_000_000 * (1 if change.group(1) in {"增加", "增量"} else -1)
        return result
    return {"status": "UNKNOWN", "reason": "no_exact_session_sh_sz_turnover_bulletin"}


def prepare_research_facts(digest: dict, context: dict) -> dict:
    """Freeze monetary quarantine before either model sees the fact packet."""
    digest = deepcopy(digest)
    amount_quality = (context.get("theme_evidence") or {}).get("amount_quality")
    if amount_quality is not None:
        from biz.review.amount_integrity import apply_amount_integrity
        market = digest.get("market_structure_json") or {}
        def receipt(key: str, field: str) -> dict:
            sampled = amount_quality.get(key) or {"status": "UNKNOWN"}
            full = market.get(field)
            # A limited clean sample cannot upgrade unknown market coverage,
            # nor should it erase an existing complete calculation. A sampled
            # inconsistency is a direct contradiction and must quarantine it.
            if full and not sampled.get("inconsistent_count"):
                return full
            return sampled
        verified = apply_amount_integrity(
            digest, target_quality=receipt("target", "amount_quality"),
            previous_quality=receipt("previous", "previous_amount_quality"))
        digest = verified
    return digest


def automatic_previous_research(engine, digest: dict) -> dict | None:
    from biz.review.market_decision import verify_automatic_research
    previous_date = (digest["quality_json"].get("source_dates") or {}).get("previous_bars")
    previous = load_digest(engine, str(previous_date)[:10], required=False) if previous_date else None
    factors = (previous or {}).get("factor_validation_json") or {}
    for candidate in (factors.get("early_briefing"), factors.get("market_decision")):
        if isinstance(candidate, dict) and candidate.get("next_trade_date") == digest["review_date"]:
            try:
                verify_automatic_research(candidate)
            except RuntimeError:
                continue
            return candidate
    return None


def attach_review(engine, digest: dict, *, now=None, context=None, next_trade_date=None,
                  previous_research=_UNSET, checkpoint=None, progress=None) -> dict:
    from biz.review.market_decision import generate_research
    if checkpoint and context is None:
        raise RuntimeError("BLOCKED stage=research_input_resume: frozen context is required")
    target = str(digest["review_date"])[:10]
    assert_digest(digest, target)
    context = context if context is not None else collect_context(engine, target, now=now)
    verified = prepare_research_facts(digest, context)
    digest.clear()
    digest.update(verified)
    if previous_research is _UNSET:
        previous_research = automatic_previous_research(engine, digest)
    research = generate_research(
        digest, report_kind="review", next_trade_date=next_trade_date or next_session(engine, target),
        previous_research=previous_research, now=now, context=context, engine=engine,
        checkpoint=checkpoint, progress=progress,
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
                      now=None, digest=None, context=None, next_trade_date=None,
                      checkpoint=None, progress=None) -> dict:
    from biz.review.market_decision import generate_research, verify_automatic_research

    if checkpoint and context is None:
        raise RuntimeError("BLOCKED stage=research_input_resume: frozen context is required")

    digest = digest if digest is not None else load_digest(engine, review_date)
    assert_digest(digest, review_date)
    parent = digest["factor_validation_json"].get("market_decision") or {}
    target = next_trade_date or next_session(engine, review_date)
    if (parent.get("status") != "COMPLETE" or parent.get("review_date") != review_date
            or parent.get("next_trade_date") != target or not parent.get("research_sha256")):
        raise RuntimeError("DATA_BLOCKED stage=previous_review: verified research is missing or stale")
    verify_automatic_research(parent)
    research = generate_research(
        digest, report_kind="briefing", next_trade_date=target,
        previous_research=parent, now=now, engine=engine, checkpoint=checkpoint, progress=progress,
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


def publication_snapshot(engine, review_date: str) -> dict:
    """Optimistic publication guard, captured before expensive model work."""
    from biz.review.market_decision import source_digest_identity
    row = load_digest(engine, review_date, required=False)
    return {"review_date": review_date, "row_sha256": artifact_hash(row) if row else None,
            "source_digest_sha256": source_digest_identity(row) if row else None,
            "captured_at": datetime.now(TZ).isoformat()}


class _PublicationTransaction:
    """Bind existing publishers to the one transaction owning the report pair."""
    def __init__(self, connection):
        self.connection = connection
        self.dialect = connection.dialect
    def begin(self):
        return nullcontext(self.connection)
    def connect(self):
        return nullcontext(self.connection)


def verify_report_pair(digest: dict, briefing: dict) -> None:
    from biz.review.market_decision import source_digest_identity, verify_automatic_research
    target = str(digest["review_date"])[:10]
    assert_digest(digest, target)
    parent = (digest.get("factor_validation_json") or {}).get("market_decision") or {}
    for report, kind in ((parent, "review"), (briefing, "briefing")):
        verify_automatic_research(report)
        if (report.get("report_kind") != kind or report.get("review_date") != target
                or report.get("source_digest_sha256") != source_digest_identity(digest)
                or report.get("research_sha256") != artifact_hash({key: value for key, value in report.items() if key != "research_sha256"})):
            raise RuntimeError("FAILED stage=pair_validation: report identity differs from frozen facts")
    if (digest.get("compact_review") != parent.get("content")
            or briefing.get("next_trade_date") != parent.get("next_trade_date")
            or briefing.get("parent_research_sha256") != parent.get("research_sha256")
            or briefing.get("parent_research") != parent):
        raise RuntimeError("FAILED stage=pair_validation: report lineage or content differs")


def persist_report_pair(engine, digest: dict, briefing: dict, *, expected: dict,
                        backup, progress=None) -> dict:
    """Publish two untouched automatic reports atomically, with no delivery.

    A row snapshot, source identity and scheduler locks prevent a long model
    run from silently replacing a newer publication. Neither fact timestamps
    nor market cutoffs are changed to bypass the digest publisher's guards.
    """
    from biz.review.market_decision import source_digest_identity
    from biz.review.quant_digest import persist_quant_digest
    verify_report_pair(digest, briefing)
    target = digest["review_date"]
    if expected.get("review_date") != target or not callable(backup):
        raise RuntimeError("FAILED stage=publication_input: missing exact snapshot or backup sink")
    deadline = monotonic_time.monotonic() + 60
    body_hashes = {"review": hashlib.sha256(digest["compact_review"].encode("utf-8")).hexdigest(),
                   "briefing": hashlib.sha256(briefing["content"].encode("utf-8")).hexdigest()}
    publication_id = artifact_hash({"expected": expected, "review": digest["factor_validation_json"]["market_decision"]["research_sha256"],
                                    "briefing": briefing["research_sha256"]})
    def tick(stage):
        if monotonic_time.monotonic() >= deadline:
            raise RuntimeError("FAILED stage=" + stage + ": publication_deadline_exceeded")
        if progress:
            progress({"stage": stage, "status": "RUNNING", "publication_id": publication_id,
                      "last_progress_at": datetime.now(TZ).isoformat()})
    def select_row(connection, lock=False):
        suffix = " FOR UPDATE" if lock and engine.dialect.name == "mysql" else ""
        raw = connection.execute(text("SELECT /*+ MAX_EXECUTION_TIME(15000) */ * FROM st_quant_review_digest "
                                      "WHERE review_date=:d AND adjust_type=0" + suffix), {"d": target}).mappings().one_or_none()
        return decode_digest(dict(raw)) if raw else None
    def delivery_count(connection):
        return connection.execute(text("SELECT /*+ MAX_EXECUTION_TIME(15000) */ COUNT(*) FROM sys_wecom_delivery_receipt "
                                        "WHERE content_sha256 IN (:review,:briefing)"), body_hashes).scalar_one()
    document = deepcopy(digest)
    document["factor_validation_json"]["early_briefing"] = briefing
    tick("publication_lock")
    with engine.begin() as connection:
        if engine.dialect.name == "mysql":
            connection.execute(text("SET SESSION innodb_lock_wait_timeout=5"))
        suffix = " FOR UPDATE" if engine.dialect.name == "mysql" else ""
        connection.execute(text("SELECT /*+ MAX_EXECUTION_TIME(15000) */ id FROM st_scheduled_tasks WHERE task_type IN "
                                "('daily_review','early_briefing','evening_review','quant_review_digest') ORDER BY id" + suffix)).all()
        active = connection.execute(text("SELECT /*+ MAX_EXECUTION_TIME(15000) */ run_uid FROM st_scheduled_task_history "
                                          "WHERE task_type IN ('daily_review','early_briefing','evening_review','quant_review_digest') "
                                          "AND run_at>=:d AND LOWER(status) IN ('running','queued','pending') LIMIT 1"), {"d": target}).first()
        jobs = connection.execute(text("SELECT /*+ MAX_EXECUTION_TIME(15000) */ job_id FROM st_job_run_v4 "
                                        "WHERE LOWER(status) IN ('running','queued','pending') AND "
                                        "(job_type LIKE '%review%' OR job_type LIKE '%brief%') LIMIT 1")).first()
        if active or jobs:
            raise RuntimeError("BLOCKED stage=publication_owner: another report producer is active")
        before = select_row(connection, lock=True)
        if ((artifact_hash(before) if before else None) != expected.get("row_sha256")
                or (source_digest_identity(before) if before else None) != expected.get("source_digest_sha256")):
            raise RuntimeError("BLOCKED stage=publication_source: durable source or publication changed")
        backup({"publication_id": publication_id, "review_date": target, "row": before,
                "expected": expected, "captured_at": datetime.now(TZ).isoformat()})
        before_deliveries = delivery_count(connection)
        if before_deliveries:
            raise RuntimeError("BLOCKED stage=no_wecom: this content already has delivery receipts")
        tick("publication_write")
        scope = _PublicationTransaction(connection)
        persist_quant_digest(scope, document)
        # The digest publisher deliberately preserves a previously stored
        # morning report. Replace that snapshot under the same row lock and
        # lineage guard before this transaction can commit either report.
        persist_briefing(scope, document, briefing)
        saved = select_row(connection)
        saved_briefing = saved["factor_validation_json"].get("early_briefing") or {}
        verify_report_pair(saved, saved_briefing)
        if (saved["compact_review"] != digest["compact_review"] or saved_briefing != briefing
                or saved["factor_validation_json"]["market_decision"] != digest["factor_validation_json"]["market_decision"]):
            raise RuntimeError("FAILED stage=publication_write_readback: generated artifacts differ")
        if delivery_count(connection) != before_deliveries:
            raise RuntimeError("FAILED stage=no_wecom: delivery receipt appeared")
        tick("publication_commit")
    tick("publication_independent_readback")
    with engine.connect() as connection:
        saved = select_row(connection)
        saved_briefing = saved["factor_validation_json"].get("early_briefing") or {}
        verify_report_pair(saved, saved_briefing)
        if (saved["compact_review"] != digest["compact_review"] or saved_briefing != briefing
                or delivery_count(connection) != 0):
            raise RuntimeError("FAILED stage=publication_independent_readback: content or notification mismatch")
    tick("publication_database_complete")
    return {"status": "DATABASE_COMPLETE", "publication_id": publication_id, "review_date": target,
            "briefing_date": briefing["next_trade_date"], "review_sha256": digest["factor_validation_json"]["market_decision"]["research_sha256"],
            "briefing_sha256": briefing["research_sha256"], "review_body_sha256": body_hashes["review"],
            "briefing_body_sha256": body_hashes["briefing"], "wecom_delivery_invoked": False,
            "matched_wecom_receipts_before": 0, "matched_wecom_receipts_after": 0,
            "next_gate": "production_api", "completed_at": datetime.now(TZ).isoformat()}
