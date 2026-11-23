from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from integrations.bigqmt.membership_snapshot import _canonical_hash as _membership_hash
from integrations.bigqmt.reference import PROVIDER_ID
from server.engine.strategy_industry_history import (
    IndustrySnapshotIntegrityError,
    IndustrySnapshotNotReady,
    _exact_snapshot_contract,
    select_industry_history_source,
)
from server.common.pit_facts import (
    PIT_AVAILABLE,
    PIT_DATA_BLOCKED,
    load_event_facts,
    load_finance_facts,
    normalize_decision_at,
    resolve_common_fact_cutoff,
    SealedFinanceInputs, HistoricalEventRead, subset_historical_event_read, PITFactBatch, FINANCE_REVISION_TABLE, EVENT_REVISION_TABLE,
)
from server.common.strategy_daily_input_window import (
    V3_DAILY_INPUT_SESSIONS,
    daily_input_catalog_join,
    daily_input_snapshot,
    resolve_daily_input_sessions,
    validate_daily_input_sessions,
)
from server.common.pit_sealed_event_read import event_fact_manifest

from .context import load_asof_context, theme_context_score


from .theme_history import (
    EvidenceStatus,
    MARKET_TIMEZONE,
    ThemeMembershipRecord,
    ThemeMembershipSnapshot,
    resolve_theme_history,
)
from .theme_features import (
    Membership,
    attach_best_theme,
    build_theme_alias_index,
    calculate_theme_statistics,
    diversified_universe_codes,
    infer_name_theme_memberships,
)

from .daily_feature_math import (
    DERIVED_CHANGE_PCT_PROTOCOL, _FINANCE_NUMERIC_FIELDS, _MONITORING_ONLY_NUMERIC_FIELDS,
    _safe_pct,
    _optional_finite_float,
    _missing_finance_numeric_fields,
    _derive_change_pct_from_close_pre_close,
    _history_session_evidence,
    _rolling_close_drawdown_pct,
    _snapshot_feature_value,
    _snapshot_feature_payload,
    _percentile,
    _theme_context_label,
    _qmt_attestation_evidence,
    _daily_source_label,
    normalize_daily_bars,
    _eligible_daily_history_codes,
    _restricted_entry_name,
    _block_entry_candidate_features,
    _market_features,
    _event_features,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DailyFactInputs:
    cutoff: dict[str, Any]
    finance: SealedFinanceInputs
    events: HistoricalEventRead
    finance_facts: PITFactBatch | None = None
    event_facts: PITFactBatch | None = None


def _latest_trade_dates(
    engine: Engine,
    *,
    as_of: date,
    count: int,
    decision_known_at: datetime,
) -> list[date]:
    return [date.fromisoformat(day) for day in resolve_daily_input_sessions(
        engine, target_trade_date=as_of.isoformat(), session_count=count,
        decision_known_at=decision_known_at,
    )]


def _expected_trade_date(
    primary_engine: Engine,
    *,
    as_of: date,
) -> date:
    try:
        with primary_engine.connect() as connection:
            value = connection.execute(
                text(
                    """
                    SELECT MAX(trade_date)
                    FROM si_trade_calendar
                    WHERE trade_status = 1
                      AND trade_date <= :as_of
                    """
                ),
                {"as_of": as_of},
            ).scalar()
    except Exception as exc:
        raise RuntimeError(
            "TRADE_CALENDAR_NOT_READY: 无法确认应到交易日"
        ) from exc
    if not isinstance(value, date):
        raise RuntimeError(
            "TRADE_CALENDAR_NOT_READY: 没有可用交易日"
        )
    return value


def _load_bars(
    engine: Engine,
    *,
    dates: list[date],
    decision_known_at: datetime,
) -> pd.DataFrame:
    if not dates:
        return pd.DataFrame()
    # Do not materialize the result as ``list[RowMapping]``.  A normal
    # production universe is roughly 350k bars here; keeping every SQLAlchemy
    # mapping alive while pandas makes a second copy can push the scheduler
    # above its memory high-water mark.  Build bounded tuple-backed chunks and
    # release each database batch before doing feature work.
    chunks: list[pd.DataFrame] = []
    with daily_input_snapshot(engine) as connection:
        window = validate_daily_input_sessions(
            connection, sessions=[day.isoformat() for day in dates],
            decision_known_at=decision_known_at,
        )
        catalog_join, catalog_params = daily_input_catalog_join(window)
        statement = text(f"""
            SELECT k.stock_code,
                   CASE WHEN k.trade_date = :latest_date
                        THEN k.short_name ELSE '' END AS short_name,
                   k.trade_date, k.open, k.close, k.high, k.low,
                   k.pre_close, k.amount
            FROM sm_stock_kline AS k
            {catalog_join}
            WHERE k.k_type = 1 AND k.adjust_type = 0
              AND k.trade_date IN :dates
              AND k.received_at IS NOT NULL
              AND k.received_at <= :decision_known_at
            ORDER BY k.stock_code, k.trade_date
        """).bindparams(bindparam("dates", expanding=True))
        result = connection.execute(
            statement,
            {"dates": dates, "latest_date": dates[-1],
             "decision_known_at": window["decision_known_at"], **catalog_params},
        )
        columns = list(result.keys())
        while True:
            rows = result.fetchmany(10_000)
            if not rows:
                break
            chunks.append(
                pd.DataFrame.from_records(rows, columns=columns)
            )
    frame = (
        pd.concat(chunks, ignore_index=True)
        if chunks
        else pd.DataFrame(columns=columns)
    )
    chunks.clear()
    frame.attrs["qmt_daily_input_window"] = window
    if frame.empty:
        return frame
    return normalize_daily_bars(frame)


def _latest_research_industry_snapshot_date(
    engine: Engine,
    *,
    as_of: date,
) -> str:
    """Select a verified candidate captured by the historical day end."""

    target = as_of.isoformat()
    historical_day_end = datetime.combine(
        as_of + timedelta(days=1), datetime.min.time()
    )
    with engine.connect() as connection:
        value = connection.execute(
            text(
                """
                SELECT snapshot_date
                FROM qmt_membership_snapshot_run
                WHERE snapshot_date < :target
                  AND source = :source
                  AND quality_status = 'QMT_VALIDATED'
                  AND capture_mode = 'qmt_close_full_refresh'
                  AND captured_at < :historical_day_end
                ORDER BY snapshot_date DESC, captured_at DESC
                LIMIT 1
                """
            ),
            {
                "target": target,
                "source": PROVIDER_ID,
                "historical_day_end": historical_day_end,
            },
        ).scalar()
    return date.fromisoformat(str(value)[:10]).isoformat() if value else ""


def _load_industries(
    engine: Engine,
    codes: list[str],
    *,
    as_of: date | None = None,
    decision_at: datetime | None = None,
    include_evidence: bool = False,
    allow_research_last_known: bool = False,
) -> (
    dict[str, tuple[str, str]]
    | tuple[dict[str, tuple[str, str]], dict[str, Any]]
):
    if not codes:
        empty_evidence = {
            "status": PIT_DATA_BLOCKED,
            "reason": "PIT_INDUSTRY_EMPTY_SCOPE",
            "snapshot_hash": hashlib.sha256(b"[]").hexdigest(),
            "status_by_code": {},
            "reason_by_code": {},
        }
        return ({}, empty_evidence) if include_evidence else {}
    if as_of is None:
        evidence = {
            "status": PIT_DATA_BLOCKED,
            "reason": "PIT_INDUSTRY_EXACT_DATE_REQUIRED",
            "snapshot_hash": hashlib.sha256(
                b"PIT_INDUSTRY_EXACT_DATE_REQUIRED"
            ).hexdigest(),
            "status_by_code": {code: PIT_DATA_BLOCKED for code in codes},
            "reason_by_code": {
                code: "PIT_INDUSTRY_EXACT_DATE_REQUIRED" for code in codes
            },
        }
        return ({}, evidence) if include_evidence else {}
    day_end = datetime.combine(as_of + timedelta(days=1), datetime.min.time())
    cutoff = normalize_decision_at(decision_at) if decision_at is not None else day_end
    result: dict[str, tuple[str, str]] = {}
    reason = ""
    run_payload: dict[str, Any] = {}
    try:
        target = as_of.isoformat()
        fallback_reason = ""
        source_date = ""
        run: dict[str, Any] | None = None
        rows: list[dict[str, Any]] = []
        try:
            try:
                source_date, fallback_reason = select_industry_history_source(
                    engine, trade_date=target, decision_known_at=cutoff,
                )
            except IndustrySnapshotNotReady:
                if allow_research_last_known:
                    source_date = _latest_research_industry_snapshot_date(engine, as_of=as_of)
                    if source_date:
                        fallback_reason = "RETROSPECTIVE_RESEARCH_LAST_KNOWN_QMT_SNAPSHOT"
                if not source_date:
                    raise
            run, rows = _exact_snapshot_contract(engine, trade_date=source_date)
        except IndustrySnapshotNotReady:
            reason = "PIT_INDUSTRY_EXACT_DATE_SNAPSHOT_MISSING"
        except IndustrySnapshotIntegrityError as exc:
            reason = (
                "PIT_INDUSTRY_SNAPSHOT_INCOMPLETE"
                if "数不完整" in str(exc)
                else "PIT_INDUSTRY_SNAPSHOT_PROVENANCE_INVALID"
            )

        if run is not None:
            captured_at = datetime.fromisoformat(str(run["captured_at"]))
            research_snapshot_late = allow_research_last_known and captured_at >= day_end
            if captured_at > cutoff or research_snapshot_late:
                reason = "PIT_INDUSTRY_SNAPSHOT_PROVENANCE_INVALID"
                run = None
                rows = []
        if run is not None:
            expected = int(run["industry_relation_count"])
            actual = len(rows)
            run_payload = {
                "snapshot_date": str(run["trade_date"]),
                "target_snapshot_date": target,
                "source_snapshot_date": str(run["trade_date"]),
                "source": str(run["source"]),
                "capture_mode": str(run["capture_mode"]),
                "fallback_reason": fallback_reason,
                "previous_session_fallback": bool(fallback_reason),
                "retrospective_last_known": bool(
                    fallback_reason
                    == "RETROSPECTIVE_RESEARCH_LAST_KNOWN_QMT_SNAPSHOT"
                ),
                "snapshot_age_days": (
                    as_of - date.fromisoformat(str(run["trade_date"])[:10])
                ).days,
                "captured_at": str(run["captured_at"]),
                "expected_industry_count": int(run["industry_count"]),
                "actual_industry_count": len({
                    str(row.get("industry_code") or "") for row in rows
                }),
                "expected_relation_count": expected,
                "actual_relation_count": actual,
                "industry_hash": str(run["industry_hash"]),
            }
            requested = set(codes)
            for row in rows:
                if str(row.get("industry_type") or "") not in {
                    "L1", "一级行业", "申万一级", "SW2021",
                }:
                    continue
                code = str(row.get("stock_code") or "")[:6]
                if code not in requested:
                    continue
                result.setdefault(
                    code,
                    (
                        str(row.get("industry_code") or ""),
                        str(row.get("industry_name") or ""),
                    ),
                )
    except Exception as exc:
        reason = f"PIT_INDUSTRY_SCHEMA_OR_CHAIN_INVALID:{type(exc).__name__}"
        logger.warning("PIT industry exact-date lookup blocked: %s", exc)
        result = {}
    status_by_code = {
        code: PIT_AVAILABLE if code in result else PIT_DATA_BLOCKED
        for code in codes
    }
    reason_by_code = {
        code: (
            ""
            if code in result
            else (reason or "PIT_INDUSTRY_CODE_NOT_IN_EXACT_SNAPSHOT")
        )
        for code in codes
    }
    snapshot_payload = {
        "schema": "probiga.v3-industry-pit-selection.v1",
        "as_of": as_of.isoformat(),
        "decision_at": cutoff.isoformat(),
        "run": run_payload,
        "memberships": {
            code: list(result[code]) for code in sorted(result)
        },
        "status_by_code": status_by_code,
        "reason_by_code": reason_by_code,
    }
    evidence = {
        "status": (
            PIT_AVAILABLE
            if result and len(result) == len(codes)
            else PIT_DATA_BLOCKED
        ),
        "reason": reason or (
            "" if len(result) == len(codes)
            else "PIT_INDUSTRY_PARTIAL_CODE_COVERAGE"
        ),
        "snapshot_hash": hashlib.sha256(
            json.dumps(
                snapshot_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        "status_by_code": status_by_code,
        "reason_by_code": reason_by_code,
        **run_payload,
    }
    return (result, evidence) if include_evidence else result


def _load_theme_memberships(
    engine: Engine,
    *,
    as_of: date,
    codes: list[str],
    industries: dict[str, tuple[str, str]],
    decision_at: datetime | None = None,
    allow_research_last_known: bool = False,
) -> tuple[dict[str, list[Membership]], date | None]:
    """Read eligible immutable concepts and retain available SW L1 memberships."""

    result: dict[str, list[Membership]] = defaultdict(list)
    for code in codes:
        industry_code, industry_name = industries.get(code, ("", ""))
        if industry_code:
            result[code].append(
                (industry_code, industry_name, "industry")
            )
    as_of_exclusive = datetime.combine(as_of + timedelta(days=1), datetime.min.time())
    cutoff = normalize_decision_at(decision_at) if decision_at is not None else as_of_exclusive
    deadline = time.monotonic() + 120
    stage = 'RUN'
    try:
        with engine.connect() as connection:
            run = connection.execute(
                text(
                    """
                    SELECT /*+ MAX_EXECUTION_TIME(12000) */ snapshot_date, source, captured_at,
                           concept_relation_count, concept_hash
                    FROM qmt_membership_snapshot_run
                    WHERE snapshot_date <= :as_of
                      AND (
                        :allow_research_last_known = 1 OR snapshot_date = :as_of
                        OR snapshot_date = (
                            SELECT MAX(trade_date) FROM si_trade_calendar
                            WHERE trade_status = 1 AND trade_date < :as_of
                        )
                      )
                      AND source = :source
                      AND quality_status = 'QMT_VALIDATED'
                      AND capture_mode = 'qmt_close_full_refresh'
                      AND captured_at <= :decision_cutoff
                      AND captured_at < :as_of_exclusive
                    ORDER BY snapshot_date DESC, captured_at DESC, source
                    LIMIT 1
                    """
                ),
                {
                    "as_of": as_of,
                    "allow_research_last_known": allow_research_last_known,
                    "source": PROVIDER_ID,
                    "decision_cutoff": cutoff,
                    "as_of_exclusive": as_of_exclusive,
                },
            ).mappings().first()
            if not run:
                return dict(result), None
            snapshot_date = run.get("snapshot_date")
            if isinstance(snapshot_date, str):
                snapshot_date = date.fromisoformat(snapshot_date[:10])
            if not isinstance(snapshot_date, date):
                return dict(result), None
            source = str(run.get("source") or "").strip()
            captured_at = run.get("captured_at")
            captured_at = normalize_decision_at(captured_at)
            expected_relations = int(run.get("concept_relation_count") or 0)
            source_close = datetime.combine(snapshot_date, datetime.min.time()).replace(hour=15)
            if captured_at.date() != snapshot_date or not source_close <= captured_at <= cutoff:
                return dict(result), None
            verify_prior = snapshot_date != as_of and not allow_research_last_known
            stage = 'COVERAGE'
            relation_evidence = connection.execute(
                text(
                    """
                    SELECT /*+ MAX_EXECUTION_TIME(12000) */ COUNT(*) AS relation_count,
                           MIN(captured_at) AS first_captured_at,
                           MAX(captured_at) AS last_captured_at
                    FROM qmt_concept_member_snapshot
                    WHERE snapshot_date = :snapshot_date
                      AND source = :source
                      AND quality_status = 'QMT_VALIDATED'
                    """
                ),
                {
                    "snapshot_date": snapshot_date,
                    "source": source,
                },
            ).mappings().one()
            actual_relations = int(
                relation_evidence.get("relation_count") or 0
            )
            first_captured_at = relation_evidence.get("first_captured_at")
            last_captured_at = relation_evidence.get("last_captured_at")
            if (
                expected_relations <= 0
                or actual_relations != expected_relations
                or first_captured_at is None or last_captured_at is None
                or normalize_decision_at(first_captured_at) != normalize_decision_at(captured_at)
                or normalize_decision_at(last_captured_at) != normalize_decision_at(captured_at)
            ):
                return dict(result), None
            stage = 'DIRECTORY'
            if expected_relations > 600000:
                raise ValueError('concept snapshot exceeds 600000 relations')
            directory_index = (' FORCE INDEX (uk_qmt_concept_member_snapshot)'
                               if connection.dialect.name == 'mysql' else '')
            directory = connection.execute(text('''SELECT /*+ MAX_EXECUTION_TIME(12000) */ id,stock_code
                FROM qmt_concept_member_snapshot''' + directory_index + '''
                WHERE snapshot_date=:day AND source=:source
                ORDER BY concept_code,stock_code LIMIT 600001'''),
                {'day':snapshot_date,'source':source}).mappings().all()
            ids = [row['id'] for row in directory]
            if len(ids)>600000 or len(ids)<expected_relations or len(ids)!=len(set(ids)):
                raise ValueError('concept snapshot directory coverage differs')
            requested = set(codes)
            selected = [row['id'] for row in directory if verify_prior or row['stock_code'] in requested]
            directory_codes = {row['id']:row['stock_code'] for row in directory}
            primary = ' FORCE INDEX (PRIMARY)' if connection.dialect.name == 'mysql' else ''
            statement_sql = '''SELECT /*+ MAX_EXECUTION_TIME({timeout_ms}) */ id,snapshot_date,source,
                stock_code,concept_code,concept_name,short_name,quality_status,captured_at
                FROM qmt_concept_member_snapshot''' + primary + ' WHERE id IN :ids'
            rows = []
            stage = 'MEMBERS'
            for offset in range(0,len(selected),256):
                remaining = deadline - time.monotonic()
                if remaining<=0:
                    raise TimeoutError('concept snapshot read exceeded 120 seconds')
                batch=selected[offset:offset+256]
                statement = text(statement_sql.format(timeout_ms=max(1,min(12000,int(remaining*1000))))).bindparams(
                    bindparam('ids',expanding=True))
                records=connection.execute(statement,{'ids':batch}).mappings().all()
                if len(records)!=len(batch) or {row['id'] for row in records}!=set(batch):
                    raise ValueError('concept snapshot member IDs differ')
                for row in records:
                    if str(row['snapshot_date'])[:10]!=snapshot_date.isoformat() or row['source']!=source:
                        raise ValueError('concept snapshot member scope differs')
                    if row['stock_code']!=directory_codes[row['id']]:
                        raise ValueError('concept snapshot member security differs')
                    if row['quality_status']=='QMT_VALIDATED':
                        if normalize_decision_at(row['captured_at'])!=captured_at:
                            raise ValueError('concept snapshot member clock differs')
                        rows.append(row)
            if time.monotonic()>=deadline:
                raise TimeoutError('concept snapshot read exceeded 120 seconds')
            rows.sort(key=lambda row:(row['stock_code'],row['concept_code']))
            if verify_prior:
                identities = [
                    tuple(str(row[column] or "") for column in (
                        "concept_code", "concept_name", "stock_code", "short_name",
                    ))
                    for row in rows
                ]
                if (
                    len(identities) != expected_relations
                    or _membership_hash(identities) != run.get("concept_hash")
                ):
                    return dict(result), None
                requested = set(codes)
                rows = [row for row in rows if row["stock_code"] in requested]
    except Exception as exc:
        raise RuntimeError('QMT_CONCEPT_SNAPSHOT_READ_FAILED:' + stage + ':' + type(exc).__name__) from exc
    recorded_at = captured_at
    if recorded_at.tzinfo is None or recorded_at.utcoffset() is None:
        recorded_at = recorded_at.replace(tzinfo=MARKET_TIMEZONE)
    else:
        recorded_at = recorded_at.astimezone(MARKET_TIMEZONE)
    history_view = resolve_theme_history(
        signal_date=as_of,
        membership_snapshots=(
            ThemeMembershipSnapshot(
                snapshot_id=(
                    f"{source}:{snapshot_date.isoformat()}:"
                    f"{recorded_at.isoformat()}"
                ),
                as_of=snapshot_date,
                recorded_at=recorded_at,
                is_complete=True,
                records=tuple(
                    ThemeMembershipRecord(
                        stock_code=str(row["stock_code"])[:6],
                        theme_code=str(row.get("concept_code") or "").strip(),
                        theme_name=str(
                            row.get("concept_name")
                            or row.get("concept_code")
                            or ""
                        ).strip(),
                        source="concept",
                        effective_from=snapshot_date,
                        effective_to=None,
                    )
                    for row in rows
                ),
            ),
        ),
        news_snapshots=(),
        membership_max_age_days=None,
    )
    if history_view.membership_evidence_status is not EvidenceStatus.AVAILABLE:
        return dict(result), None
    seen: set[tuple[str, str]] = set()
    non_actionable_fragments = (
        "融资融券",
        "转融券",
        "富时罗素",
        "MSCI",
        "标普道琼斯",
        "沪股通",
        "深股通",
        "机构重仓",
        "基金重仓",
        "社保重仓",
        "证金汇金",
        "QFII",
        "低价",
        "高价",
        "预盈预增",
    )
    for code, memberships in history_view.memberships.items():
        for concept_code, concept_name, membership_source in memberships:
            if membership_source != "concept":
                continue
            if not concept_code or (code, concept_code) in seen:
                continue
            if any(
                fragment.lower() in concept_name.lower()
                for fragment in non_actionable_fragments
            ):
                continue
            seen.add((code, concept_code))
            result[code].append(
                (
                    concept_code,
                    concept_name,
                    "concept",
                )
            )
    if time.monotonic()>=deadline:
        raise RuntimeError('QMT_CONCEPT_SNAPSHOT_READ_FAILED:RESOLVE:TimeoutError')
    return dict(result), snapshot_date


def _load_finance(
    engine: Engine,
    *,
    as_of: date,
    codes: list[str],
    decision_at: datetime | None,
    fact_cutoff_at: datetime | str | None = None,
    sealed_finance_inputs: SealedFinanceInputs | None = None,
    prepared_batch: PITFactBatch | None = None,
) -> dict[str, dict[str, Any]]:
    if not codes:
        return {}
    if decision_at is None:
        return {
            code: {
                "finance_pit_status": PIT_DATA_BLOCKED,
                "finance_pit_reason": "PIT_FINANCE_EXACT_DECISION_TIME_REQUIRED",
                "finance_manifest_hash": hashlib.sha256(
                    f"finance:{as_of}:missing-decision-at".encode("utf-8")
                ).hexdigest(),
            }
            for code in codes
        }
    if prepared_batch is not None and (
        sealed_finance_inputs is None or sealed_finance_inputs.engine is not engine
        or sealed_finance_inputs.as_of_date!=as_of or prepared_batch.table_name!=FINANCE_REVISION_TABLE
        or prepared_batch.global_status!=PIT_AVAILABLE
        or normalize_decision_at(prepared_batch.decision_at)!=normalize_decision_at(decision_at)
        or normalize_decision_at(prepared_batch.fact_cutoff_at)!=normalize_decision_at(fact_cutoff_at)
        or not set(codes)<=prepared_batch.status_by_code.keys()
    ):
        raise ValueError('PIT_DAILY_FINANCE_SNAPSHOT_DIFFERS')
    batch = prepared_batch if prepared_batch is not None else load_finance_facts(
        engine,
        codes=codes,
        decision_at=decision_at,
        fact_cutoff_at=fact_cutoff_at,
        as_of_date=as_of,
        **({'sealed_finance_inputs':sealed_finance_inputs} if sealed_finance_inputs is not None else {}),
    )
    result: dict[str, dict[str, Any]] = {}
    for code in codes:
        raw = dict(batch.facts.get(code) or {})
        coverage = dict(batch.coverage_by_code.get(code) or {})
        status = batch.status_for(code)
        item: dict[str, Any] = {
            key: _optional_finite_float(raw.get(key))
            for key in _FINANCE_NUMERIC_FIELDS
        }
        item.update(
            {
                "finance_pit_status": (
                    PIT_AVAILABLE if status == PIT_AVAILABLE else PIT_DATA_BLOCKED
                ),
                "finance_pit_reason": (
                    batch.reason_for(code)
                    or (
                        "" if status == PIT_AVAILABLE
                        else "PIT_FINANCE_COVERAGE_UNPROVEN"
                    )
                ),
                "finance_manifest_hash": batch.manifest_hash,
                "finance_revision_id": raw.get("finance_revision_id"),
                "finance_content_hash": raw.get("finance_content_hash"),
                "finance_published_at": raw.get("finance_published_at"),
                "finance_known_at": raw.get("finance_known_at"),
                "finance_report_date": raw.get("finance_report_date"),
                "finance_fact_cutoff_at": batch.fact_cutoff_at,
                "finance_decision_at": batch.decision_at,
                "finance_authoritative_empty": bool(
                    status == PIT_AVAILABLE and not raw and coverage
                ),
                "finance_coverage_id": coverage.get("coverage_id"),
                "finance_coverage_response_hash": coverage.get(
                    "coverage_response_hash"
                ),
                "finance_coverage_watermark_hash": coverage.get(
                    "coverage_watermark_hash"
                ),
            }
        )
        result[code] = item
    return result


def _load_recent_notices(
    engine: Engine,
    *,
    as_of: date,
    codes: list[str],
    decision_at: datetime | None,
    fact_cutoff_at: datetime | str | None = None,
    include_evidence: bool = False,
    historical_selection: HistoricalEventRead | None = None,
    prepared_batch: PITFactBatch | None = None,
) -> (
    dict[str, list[dict[str, Any]]]
    | tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]
):
    if not codes:
        empty_evidence = {
            "status": PIT_DATA_BLOCKED,
            "reason": "PIT_EVENT_EMPTY_SCOPE",
            "manifest_hash": hashlib.sha256(b"[]").hexdigest(),
            "status_by_code": {},
            "reason_by_code": {},
        }
        return ({}, empty_evidence) if include_evidence else {}
    if decision_at is None:
        evidence = {
            "status": PIT_DATA_BLOCKED,
            "reason": "PIT_EVENT_EXACT_DECISION_TIME_REQUIRED",
            "manifest_hash": hashlib.sha256(
                f"event:{as_of}:missing-decision-at".encode("utf-8")
            ).hexdigest(),
            "status_by_code": {code: PIT_DATA_BLOCKED for code in codes},
            "reason_by_code": {
                code: "PIT_EVENT_EXACT_DECISION_TIME_REQUIRED" for code in codes
            },
        }
        result: dict[str, list[dict[str, Any]]] = {}
        return (result, evidence) if include_evidence else result
    if prepared_batch is not None and (
        historical_selection is None or historical_selection.selection.engine is not engine
        or historical_selection.selection.window_end!=as_of or prepared_batch.table_name!=EVENT_REVISION_TABLE
        or historical_selection.selection.window_start!=as_of-timedelta(days=20)
        or historical_selection.selection.decision_at!=normalize_decision_at(decision_at)
        or historical_selection.fact_cutoff_at!=normalize_decision_at(fact_cutoff_at)
        or prepared_batch.global_status!=PIT_AVAILABLE
        or normalize_decision_at(prepared_batch.decision_at)!=normalize_decision_at(decision_at)
        or normalize_decision_at(prepared_batch.fact_cutoff_at)!=normalize_decision_at(fact_cutoff_at)
        or not set(codes)<=prepared_batch.status_by_code.keys()
        or prepared_batch.manifest_hash!=event_fact_manifest(prepared_batch,historical_selection)
    ):
        raise ValueError('PIT_EVENT_PREPARED_SNAPSHOT_DIFFERS')
    batch = prepared_batch if prepared_batch is not None else load_event_facts(
        engine,
        codes=codes,
        decision_at=decision_at,
        fact_cutoff_at=fact_cutoff_at,
        start_date=as_of - timedelta(days=20),
        end_date=as_of,
        require_qmt_complete_batch=True,
        **({'historical_selection':historical_selection} if historical_selection is not None else {}),
    )
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for code in codes:
        for raw in batch.facts.get(code) or []:
            if len(result[code]) >= 10:
                break
            row = dict(raw)
            published_at = str(row.get("event_published_at") or "")
            try:
                row["notice_date"] = date.fromisoformat(published_at[:10])
            except ValueError:
                row["notice_date"] = None
            result[code].append(row)
    status_by_code = {
        code: (
            PIT_AVAILABLE
            if batch.status_for(code) == PIT_AVAILABLE
            else PIT_DATA_BLOCKED
        )
        for code in codes
    }
    reason_by_code = {
        code: (
            batch.reason_for(code)
            or (
                "" if status_by_code[code] == PIT_AVAILABLE
                else "PIT_EVENT_COVERAGE_UNPROVEN"
            )
        )
        for code in codes
    }
    evidence = {
        "status": (
            PIT_AVAILABLE
            if all(value == PIT_AVAILABLE for value in status_by_code.values())
            else PIT_DATA_BLOCKED
        ),
        "reason": (
            ""
            if all(value == PIT_AVAILABLE for value in status_by_code.values())
            else "PIT_EVENT_PARTIAL_OR_UNVERIFIED_COVERAGE"
        ),
        "manifest_hash": batch.manifest_hash,
        "status_by_code": status_by_code,
        "reason_by_code": reason_by_code,
        "coverage_by_code": {
            code: dict(batch.coverage_by_code.get(code) or {})
            for code in codes
        },
        "fact_cutoff_at": batch.fact_cutoff_at,
        "decision_at": batch.decision_at,
    }
    output = dict(result)
    return (output, evidence) if include_evidence else output


def load_daily_feature_universe(
    primary_engine: Engine,
    kline_engine: Engine,
    *,
    as_of: date,
    context_cutoff_at: datetime | None = None,
    limit: int = 5000,
    required_codes: Iterable[str] = (),
    allow_research_industry_last_known: bool = False,
) -> dict[str, Any]:
    required_code_set = {
        str(code).zfill(6)
        for code in required_codes
        if str(code)
    }
    expected_trade_date = _expected_trade_date(
        primary_engine,
        as_of=as_of,
    )
    daily_cutoff = context_cutoff_at or datetime.now(MARKET_TIMEZONE)
    dates = _latest_trade_dates(
        primary_engine, as_of=expected_trade_date,
        count=V3_DAILY_INPUT_SESSIONS, decision_known_at=daily_cutoff,
    )
    if not dates or dates[-1] != expected_trade_date:
        actual = dates[-1].isoformat() if dates else "无"
        raise RuntimeError(
            "QMT_DAILY_KLINE_NOT_READY: "
            f"应到 {expected_trade_date.isoformat()}，实际仅到 {actual}"
        )
    frame = _load_bars(kline_engine, dates=dates, decision_known_at=daily_cutoff)
    return build_daily_feature_universe(
        primary_engine, as_of=as_of, dates=dates, frame=frame,
        context_cutoff_at=context_cutoff_at, limit=limit,
        required_codes=required_code_set,
        allow_research_industry_last_known=allow_research_industry_last_known,
    )


def prepare_feature_identity_context(primary_engine, *, as_of, dates, frame,
                                     context_cutoff_at=None, required_codes=(),
                                     allow_research_industry_last_known=False, entry_scope_codes=None,
                                     research_keywords=None):
    from .daily_feature_math import validate_feature_input
    validate_feature_input(dates, frame)
    required = {str(code).zfill(6) for code in required_codes if str(code)}
    codes = _eligible_daily_history_codes(frame, expected_trade_date=dates[-1], required_codes=required)
    if entry_scope_codes is not None:
        scope = {str(code).zfill(6) for code in entry_scope_codes}
        codes = [code for code in codes if code in scope or code in required]
    industries, evidence = _load_industries(primary_engine, codes, as_of=as_of,
        decision_at=context_cutoff_at, include_evidence=True,
        allow_research_last_known=allow_research_industry_last_known)
    themes, snapshot = _load_theme_memberships(primary_engine, as_of=as_of, codes=codes,
        industries=industries, decision_at=context_cutoff_at,
        allow_research_last_known=allow_research_industry_last_known)
    raw_themes={code:list(memberships) for code,memberships in themes.items()}
    names = {str(row.stock_code)[:6]:str(row.short_name or '')
        for row in frame.sort_values('trade_date').groupby('stock_code',sort=False,observed=True).tail(1).itertuples(index=False)
        if str(row.stock_code)[:6] in set(codes)}
    for code,inferred in infer_name_theme_memberships(names).items():
        existing=themes.setdefault(code,[]);keys={(item[0],item[2]) for item in existing}
        for membership in inferred:
            if (membership[0],membership[2]) not in keys:existing.append(membership)
    market_context=load_asof_context(primary_engine,as_of=as_of,cutoff_at=context_cutoff_at,
        theme_aliases=build_theme_alias_index(themes),allow_legacy_display=False)
    from .theme_features import _paper_research_theme_keywords
    return {'codes':codes,'industries':industries,'industry_evidence':evidence,
            'research_keywords':(_paper_research_theme_keywords() if research_keywords is None else research_keywords),
            'theme_memberships':raw_themes,'concept_snapshot_date':snapshot,'market_context':market_context}


def build_daily_feature_universe(
    primary_engine: Engine, *, as_of: date, dates: list[date], frame: pd.DataFrame,
    context_cutoff_at: datetime | None = None, limit: int = 5000,
    required_codes: Iterable[str] = (), allow_research_industry_last_known: bool = False,
    entry_scope_codes: Iterable[str] | None = None,
    fact_inputs: DailyFactInputs | None = None,
) -> dict[str, Any]:
    """Prepare live observations, then run the shared pure feature calculations."""
    from .daily_feature_math import compute_price_features, compute_finance_features, finish_feature_universe
    required_codes = tuple(required_codes)
    entry_scope_codes = tuple(entry_scope_codes) if entry_scope_codes is not None else None
    context=prepare_feature_identity_context(primary_engine,as_of=as_of,dates=dates,frame=frame,
        context_cutoff_at=context_cutoff_at,required_codes=required_codes,
        allow_research_industry_last_known=allow_research_industry_last_known,entry_scope_codes=entry_scope_codes)
    state=compute_price_features(as_of=as_of,dates=dates,frame=frame,context=context,
        context_cutoff_at=context_cutoff_at,limit=limit,required_codes=required_codes,entry_scope_codes=entry_scope_codes)
    common_cutoff: dict[str, Any] = {
        "status": PIT_DATA_BLOCKED,
        "reason": "PIT_COMMON_CUTOFF_EXACT_DECISION_TIME_REQUIRED",
        "fact_cutoff_at": "",
        "receipt_root_hash": "",
    }
    if fact_inputs is not None:
        if (fact_inputs.finance.engine is not primary_engine or fact_inputs.finance.as_of_date!=as_of
                or fact_inputs.finance.decision_at!=normalize_decision_at(context_cutoff_at)
                or fact_inputs.cutoff.get('status')!=PIT_AVAILABLE):
            raise ValueError('PIT_DAILY_FEATURE_SNAPSHOT_DIFFERS')
        common_cutoff=dict(fact_inputs.cutoff)
    elif context_cutoff_at is not None:
        finances,events=[],[]
        common_cutoff = resolve_common_fact_cutoff(
            primary_engine,
            codes=list(state['base']),
            decision_at=context_cutoff_at,
            finance_start_date="1900-01-01",
            finance_end_date=as_of,
            event_start_date=as_of - timedelta(days=20),
            event_end_date=as_of,
            require_qmt_event_batch=True,
            capture_finance_selection=finances.append,capture_historical_selection=events.append,
        )
        if common_cutoff.get('status')==PIT_AVAILABLE:
            if len(finances)!=1 or len(events)!=1:raise ValueError('PIT_DAILY_FEATURE_SNAPSHOT_INCOMPLETE')
            fact_inputs=DailyFactInputs(common_cutoff,finances[0],events[0])
    pit_reader_decision_at=context_cutoff_at if common_cutoff.get('status')==PIT_AVAILABLE else None
    source_cutoffs=common_cutoff.get('source_cutoff_at') or {}
    finance_cutoff_at=source_cutoffs.get('finance') or common_cutoff.get('fact_cutoff_at') or None
    event_cutoff_at=source_cutoffs.get('event') or common_cutoff.get('fact_cutoff_at') or None
    finance = _load_finance(
        primary_engine,
        as_of=as_of,
        codes=list(state['base']),
        decision_at=pit_reader_decision_at,
        fact_cutoff_at=finance_cutoff_at,
        **({'sealed_finance_inputs':fact_inputs.finance,'prepared_batch':fact_inputs.finance_facts} if fact_inputs is not None else {}),
    )
    state=compute_finance_features(state,common_cutoff,finance)
    notices, event_evidence = _load_recent_notices(
        primary_engine,
        as_of=as_of,
        codes=state['ranked_codes'],
        decision_at=pit_reader_decision_at,
        fact_cutoff_at=event_cutoff_at,
        include_evidence=True,
        **({'historical_selection':subset_historical_event_read(fact_inputs.events,state['ranked_codes'])} if fact_inputs is not None else {}),
        **({'prepared_batch':fact_inputs.event_facts} if fact_inputs is not None and fact_inputs.event_facts is not None else {}),
    )
    return finish_feature_universe(state,notices,event_evidence)
