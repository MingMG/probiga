# -*- coding: utf-8 -*-
"""Hash-verified QMT industry facts for the target or preceding open session."""
from __future__ import annotations

import hashlib
import json
import logging
import time
import re
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from server.common.bounded_read import read_keyset_pages
from server.common.batch_db import _transient_db_errno

from integrations.bigqmt.reference import PROVIDER_ID


QMT_VALIDATED = "QMT_VALIDATED"
L1_INDUSTRY_TYPES = frozenset({"L1", "一级行业", "申万一级", "SW2021"})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")

QMT_PREVIOUS_OPEN_SESSION_REASON = "QMT_VALIDATED_PREVIOUS_OPEN_SESSION"
# Preserve the sealed identities of previously approved recoveries.
QMT_PREVIOUS_SESSION_FALLBACKS = {
    "2026-08-28": "QMT_HISTORICAL_SECTOR_API_UNAVAILABLE",
    "2026-08-31": "QMT_MEMBERSHIP_CAPTURE_SKIPPED_DURING_RELEASE",
    "2026-09-04": "QMT_MEMBERSHIP_CAPTURE_SKIPPED_DURING_CALENDAR_RECOVERY",
}
# The 2026-08-31 close snapshot was missed while the release was being
# installed, and 2026-08-28 has no immutable snapshot to carry.  Bind this one
# reviewed recovery date to the last actually captured partition.  This is an
# explicit exception, not a general "latest available" lookup.
QMT_EXPLICIT_SESSION_FALLBACK_SOURCE_DATES = {
    "2026-08-31": "2026-08-27",
    "2026-09-04": "2026-09-02",
}
STRATEGY_INDUSTRY_HISTORY_PRODUCTION_CUTOVER_DATE = "2026-08-28"


class IndustrySnapshotNotReady(RuntimeError):
    """The exact-date immutable QMT snapshot has not arrived yet."""


class IndustrySnapshotIntegrityError(RuntimeError):
    """The immutable QMT snapshot or copied ledger differs from its contract."""


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")).hexdigest()


def _canonical_qmt_industry_hash(rows: Iterable[Mapping[str, Any]]) -> str:
    """Reproduce ``membership_snapshot._canonical_hash`` without pandas."""

    values = [
        tuple(
            str(row.get(column) or "")
            for column in (
                "industry_code",
                "industry_name",
                "industry_type",
                "stock_code",
                "short_name",
            )
        )
        for row in rows
    ]
    payload = json.dumps(
        sorted(values), ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _iso_datetime(value: Any, *, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip().replace(" ", "T")
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise IndustrySnapshotIntegrityError(
                f"QMT行业快照{field}不是有效时间"
            ) from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    return parsed.isoformat(timespec="seconds")


def previous_open_session_date(engine, *, trade_date: str) -> str:
    """Resolve exactly one preceding open session from the canonical calendar."""

    target = date.fromisoformat(str(trade_date)[:10]).isoformat()
    try:
        with (engine.connect() if hasattr(engine, "connect") else nullcontext(engine)) as connection:
            previous = connection.execute(text("""
                SELECT MAX(trade_date)
                FROM si_trade_calendar
                WHERE trade_date < :trade_date AND trade_status=1
            """), {"trade_date": target}).scalar()
    except Exception as exc:
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照降级无法验证前一开市日"
        ) from exc
    if not previous:
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照降级缺少前一开市日"
        )
    return date.fromisoformat(str(previous)[:10]).isoformat()


def authorized_industry_fallback(
    engine,
    *,
    trade_date: str,
) -> tuple[str, str]:
    """Return the preceding open session, including existing recovery proofs."""

    target = date.fromisoformat(str(trade_date)[:10]).isoformat()
    reason = QMT_PREVIOUS_SESSION_FALLBACKS.get(
        target, QMT_PREVIOUS_OPEN_SESSION_REASON,
    )
    source_date = QMT_EXPLICIT_SESSION_FALLBACK_SOURCE_DATES.get(target)
    if source_date is None:
        source_date = previous_open_session_date(engine, trade_date=target)
    return date.fromisoformat(source_date).isoformat(), reason


def _visible_industry_run(
    engine, *, trade_date: str, source: str, decision_known_at: str | None,
) -> dict[str, Any] | None:
    with (engine.connect() if hasattr(engine, "connect") else nullcontext(engine)) as connection:
        runs = connection.execute(text("""
            SELECT source, quality_status, capture_mode, captured_at,
                   industry_count, industry_relation_count, industry_hash
            FROM qmt_membership_snapshot_run
            WHERE snapshot_date=:trade_date
        """), {"trade_date": trade_date}).mappings().all()
    earliest = f"{trade_date}T15:00:00"
    midnight = (date.fromisoformat(trade_date) + timedelta(days=1)).isoformat() + "T00:00:00"
    visible = []
    for run in runs:
        captured_at = _iso_datetime(run["captured_at"], field="运行时间")
        if not earliest <= captured_at < midnight:
            raise IndustrySnapshotIntegrityError("QMT行业快照不是目标交易日收盘后的精确日期事实")
        if decision_known_at is None or captured_at <= decision_known_at:
            visible.append(run)
    if not visible:
        return None
    if len(visible) != 1 or visible[0]["source"] != source:
        raise IndustrySnapshotIntegrityError("QMT行业快照来源或运行记录不唯一")
    run = visible[0]
    if run["quality_status"] != QMT_VALIDATED:
        raise IndustrySnapshotIntegrityError(f"{trade_date}的QMT行业快照尚未达到{QMT_VALIDATED}")
    if (
        run["capture_mode"] != "qmt_close_full_refresh"
        or not _SHA256.fullmatch(str(run["industry_hash"] or "").lower())
        or int(run["industry_count"] or 0) <= 0
        or int(run["industry_relation_count"] or 0) <= 0
    ):
        raise IndustrySnapshotIntegrityError("QMT行业快照运行证据不完整")
    return dict(run)


def select_industry_history_source(
    engine, *, trade_date: str, decision_known_at: datetime | str | None = None,
    source: str = PROVIDER_ID,
) -> tuple[str, str]:
    """Select one available immutable partition from its published metadata."""
    target = date.fromisoformat(str(trade_date)[:10]).isoformat()
    cutoff = (
        _iso_datetime(decision_known_at, field="分析决策时间")
        if decision_known_at is not None else None
    )
    if _visible_industry_run(engine, trade_date=target, source=source, decision_known_at=cutoff) is not None:
        return target, ""
    return _select_previous_industry_source(engine, target=target, source=source, cutoff=cutoff)


def _select_previous_industry_source(engine, *, target: str, source: str, cutoff: str | None) -> tuple[str, str]:
    prior, reason = authorized_industry_fallback(engine, trade_date=target)
    if _visible_industry_run(engine, trade_date=prior, source=source, decision_known_at=cutoff) is None:
        raise IndustrySnapshotNotReady(f"{target}及允许沿用的{prior}均没有可用的QMT行业快照")
    return prior, reason


def _exact_snapshot_contract(
    engine, *, trade_date: str, source: str = PROVIDER_ID,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    target = date.fromisoformat(str(trade_date)[:10]).isoformat()
    deadline = time.monotonic() + 120
    with (engine.connect() if hasattr(engine, "connect") else nullcontext(engine)) as connection:
        runs = [dict(row) for row in connection.execute(text("""
            SELECT /*+ MAX_EXECUTION_TIME(4000) */ snapshot_date, source, quality_status, capture_mode,
                   industry_count, industry_relation_count, industry_hash,
                   captured_at
            FROM qmt_membership_snapshot_run
            WHERE snapshot_date=:trade_date AND source=:source
            ORDER BY captured_at, source LIMIT 2
        """), {
            "trade_date": target,
            "source": source,
        }).mappings().all()]
        if not runs:
            raise IndustrySnapshotNotReady(
                f"{target}的QMT精确日期行业快照尚未发布"
            )
        if len(runs) != 1:
            raise IndustrySnapshotIntegrityError(
                f"{target}存在重复QMT行业快照运行记录"
            )
        run = runs[0]
        expected_count = int(run.get('industry_relation_count') or 0)
        def fetch(cursor, size):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('industry snapshot read deadline')
            scope = '' if cursor is None else ''' AND (industry_code>:industry
                OR (industry_code=:industry AND stock_code>:stock))'''
            parameters = {'trade_date':target,'source':source}
            if cursor is not None:
                parameters.update(industry=cursor[0],stock=cursor[1])
            index = ' FORCE INDEX (uk_qmt_industry_member_snapshot)' if connection.dialect.name=='mysql' else ''
            return [dict(row) for row in connection.execute(text(f'''
                SELECT /*+ MAX_EXECUTION_TIME({max(1,min(12000,int(remaining*1000)))}) */
                    snapshot_date,source,industry_code,industry_name,industry_type,stock_code,
                    short_name,quality_status,captured_at FROM qmt_industry_member_snapshot{index}
                    WHERE snapshot_date=:trade_date AND source=:source {scope}
                    ORDER BY industry_code,stock_code LIMIT {size}'''),parameters).mappings()]
        try:
            rows = read_keyset_pages(fetch,key=lambda row:(row['industry_code'],row['stock_code']),
                deadline=deadline, max_rows=max(1,expected_count+1),
                retryable=lambda exc: not getattr(exc,'connection_invalidated',False)
                    and _transient_db_errno(exc) in (1205,1969,3024),
                logger=logging.getLogger(__name__),stage='context-industry-members')
        except ValueError as exc:
            raise IndustrySnapshotIntegrityError(str(exc)) from exc



    if str(run.get("quality_status") or "") != QMT_VALIDATED:
        raise IndustrySnapshotNotReady(
            f"{target}的QMT行业快照尚未达到{QMT_VALIDATED}"
        )
    capture_mode = str(run.get("capture_mode") or "")
    if capture_mode != "qmt_close_full_refresh":
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照不是收盘全量冻结模式"
        )
    published_hash = str(run.get("industry_hash") or "").lower()
    if not _SHA256.fullmatch(published_hash):
        raise IndustrySnapshotIntegrityError("QMT行业快照缺少有效发布哈希")
    expected_count = int(run.get("industry_relation_count") or 0)
    if expected_count <= 0 or len(rows) != expected_count:
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照关系数不完整："
            f"发布{expected_count}，实际{len(rows)}"
        )
    expected_industries = int(run.get("industry_count") or 0)
    actual_industries = len({str(row.get("industry_code") or "") for row in rows})
    if expected_industries <= 0 or actual_industries != expected_industries:
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照行业数不完整："
            f"发布{expected_industries}，实际{actual_industries}"
        )
    run_captured_at = _iso_datetime(run.get("captured_at"), field="运行时间")
    for row in rows:
        if str(row.get("snapshot_date") or "")[:10] != target:
            raise IndustrySnapshotIntegrityError("QMT行业成员混入其他数据日")
        if str(row.get("source") or "") != source:
            raise IndustrySnapshotIntegrityError("QMT行业成员来源与运行记录不一致")
        if str(row.get("quality_status") or "") != QMT_VALIDATED:
            raise IndustrySnapshotIntegrityError("QMT行业成员存在未验证记录")
        if _iso_datetime(row.get("captured_at"), field="成员时间") != run_captured_at:
            raise IndustrySnapshotIntegrityError("QMT行业成员时间与运行记录不一致")
    actual_hash = _canonical_qmt_industry_hash(rows)
    if actual_hash != published_hash:
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照canonical hash校验失败"
        )
    captured_value = datetime.fromisoformat(run_captured_at)
    earliest = datetime.combine(
        date.fromisoformat(target), datetime.min.time()
    ).replace(hour=15)
    cutoff_value = datetime.combine(
        date.fromisoformat(target) + timedelta(days=1), datetime.min.time()
    )
    if not earliest <= captured_value < cutoff_value:
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照不是目标交易日收盘后的精确日期事实"
        )
    return ({
        "trade_date": target,
        "source": source,
        "quality_status": QMT_VALIDATED,
        "capture_mode": capture_mode,
        "industry_count": expected_industries,
        "industry_relation_count": expected_count,
        "industry_hash": published_hash,
        "captured_at": run_captured_at,
    }, rows)


def build_history_rows(
    source_rows: Iterable[Mapping[str, Any]], *, trade_date: str,
    source: str, industry_hash: str, captured_at: Any,
    source_snapshot_date: str | None = None,
    capture_mode: str = "qmt_close_full_refresh",
    fallback_reason: str = "",
    authorized_fallback_date: str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Build the strategy ledger only from one verified QMT snapshot."""

    target = date.fromisoformat(str(trade_date)[:10]).isoformat()
    source_date = date.fromisoformat(
        str(source_snapshot_date or target)[:10]
    ).isoformat()
    normalized_mode = str(capture_mode or "").strip()
    normalized_fallback_reason = str(fallback_reason or "").strip()
    legacy_reason = QMT_PREVIOUS_SESSION_FALLBACKS.get(target)
    expected_fallback_reason = legacy_reason or QMT_PREVIOUS_OPEN_SESSION_REASON
    expected_source_date = QMT_EXPLICIT_SESSION_FALLBACK_SOURCE_DATES.get(
        target, authorized_fallback_date,
    )
    if normalized_mode != "qmt_close_full_refresh":
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照不是收盘全量冻结模式"
        )
    if source_date == target:
        if normalized_fallback_reason:
            raise IndustrySnapshotIntegrityError(
                "QMT精确日期行业快照不得声明降级原因"
            )
    elif (
        normalized_fallback_reason != expected_fallback_reason
        or source_date >= target
        or (not expected_source_date and not legacy_reason)
        or (
            expected_source_date is not None
            and source_date != expected_source_date
        )
    ):
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照降级未获得目标日显式授权"
        )
    published_hash = str(industry_hash or "").lower()
    if not _SHA256.fullmatch(published_hash):
        raise IndustrySnapshotIntegrityError("QMT行业快照发布哈希无效")
    normalized_time = _iso_datetime(captured_at, field="运行时间")
    cutoff = (date.fromisoformat(target) + timedelta(days=1)).isoformat() + "T00:00:00"
    source_cutoff = (
        date.fromisoformat(source_date) + timedelta(days=1)
    ).isoformat() + "T00:00:00"
    captured_value = datetime.fromisoformat(normalized_time)
    earliest = datetime.combine(
        date.fromisoformat(source_date), datetime.min.time()
    ).replace(hour=15)
    if not earliest <= captured_value < datetime.fromisoformat(source_cutoff):
        raise IndustrySnapshotIntegrityError(
            (
                "QMT行业快照不是目标交易日收盘后的精确日期事实"
                if source_date == target else
                "QMT行业快照不是来源交易日收盘后的真实日期事实"
            )
        )
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in source_rows:
        industry_type = str(raw.get("industry_type") or "").strip()
        if industry_type not in L1_INDUSTRY_TYPES:
            continue
        code = str(raw.get("stock_code") or "").strip().zfill(6)
        industry_code = str(raw.get("industry_code") or "").strip()
        name = str(raw.get("industry_name") or "").strip()
        if (
            not re.fullmatch(r"[0-9]{6}", code)
            or code in seen
            or not industry_code
            or not name
            or len(name) > 120
            or len(industry_type) > 40
            or len(str(source)) > 80
        ):
            raise IndustrySnapshotIntegrityError(
                "QMT一级行业快照存在重复或不完整证券事实"
            )
        seen.add(code)
        fact_payload = {
            "trade_date": target,
            "source": source,
            "industry_hash": published_hash,
            "industry_code": industry_code,
            "industry_name": name,
            "industry_type": industry_type,
            "stock_code": code,
        }
        if normalized_fallback_reason:
            fact_payload.update({
                "source_snapshot_date": source_date,
                "capture_mode": normalized_mode,
                "fallback_reason": normalized_fallback_reason,
            })
        fact_digest = _digest(fact_payload)
        normalized.append({
            "stock_code": code,
            "industry_name": name,
            "industry_type": industry_type,
            "source_system": str(source)[:80],
            "source_fact_id": f"qmt:{published_hash}:{fact_digest}",
            "source_effective_at": normalized_time,
            "source_etl_sync_at": normalized_time,
        })
    normalized.sort(key=lambda row: row["stock_code"])
    if not normalized:
        raise IndustrySnapshotIntegrityError("QMT行业快照没有一级行业证券事实")
    snapshot_payload = {
        "schema": "probiga.strategy-industry-qmt-snapshot.v2",
        "trade_date": target,
        "as_of_exclusive": cutoff,
        "qmt_source": source,
        "qmt_industry_hash": published_hash,
        "qmt_captured_at": normalized_time,
        "facts": normalized,
    }
    if normalized_fallback_reason:
        snapshot_payload.update({
            "qmt_source_snapshot_date": source_date,
            "qmt_capture_mode": normalized_mode,
            "qmt_fallback_reason": normalized_fallback_reason,
        })
    snapshot_id = _digest(snapshot_payload)
    rows: list[dict[str, Any]] = []
    for item in normalized:
        payload = {
            "snapshot_id": snapshot_id,
            "trade_date": target,
            "as_of_exclusive": cutoff,
            **item,
        }
        rows.append({**payload, "row_hash": _digest(payload)})
    return snapshot_id, rows


def prepare_industry_history(
    engine, *, trade_date: str, source: str = PROVIDER_ID,
    decision_known_at: datetime | str | None = None,
    expected_proof_sha256: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    target = date.fromisoformat(str(trade_date)[:10]).isoformat()
    if expected_proof_sha256 is not None:
        if decision_known_at is None:
            raise IndustrySnapshotIntegrityError("核对已封存行业证据需要原分析决策时间")
        binding, history = _resolve_membership_and_history(
            engine, target=target, source=source, decision_known_at=decision_known_at,
            expected_proof_sha256=expected_proof_sha256,
        )
        if history is not None:
            return history
        source_date, fallback_reason = binding["source_snapshot_date"], binding["fallback_reason"]
    else:
        source_date, fallback_reason = select_industry_history_source(
            engine, trade_date=target, source=source, decision_known_at=decision_known_at,
        )
    return _prepare_selected_industry_history(
        engine, target=target, source_date=source_date, source=source, fallback_reason=fallback_reason,
    )


def _prepare_selected_industry_history(
    engine, *, target: str, source_date: str, source: str, fallback_reason: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    run, source_rows = _exact_snapshot_contract(engine, trade_date=source_date, source=source)
    snapshot_id, rows = build_history_rows(
        source_rows,
        trade_date=target,
        source=run["source"],
        industry_hash=run["industry_hash"],
        captured_at=run["captured_at"],
        source_snapshot_date=run["trade_date"],
        capture_mode=run["capture_mode"],
        fallback_reason=fallback_reason,
        authorized_fallback_date=source_date if fallback_reason else None,
    )
    report = {
        "status": "VALIDATED",
        "trade_date": target,
        "snapshot_id": snapshot_id,
        "source": run["source"],
        "source_snapshot_date": run["trade_date"],
        "capture_mode": run["capture_mode"],
        "fallback_reason": fallback_reason,
        "previous_session_fallback": bool(fallback_reason),
        "source_snapshot_hash": run["industry_hash"],
        "source_snapshot_captured_at": run["captured_at"],
        "source_relation_count": run["industry_relation_count"],
        "selected_row_count": len(rows),
        "row_count": len(rows),
        "historical_backfill_allowed": bool(fallback_reason),
        "mutable_current_table_backfill_allowed": False,
        "immutable_exact_date_recovery_allowed": True,
        "historical_recovery_source": (
            (
                "IMMUTABLE_QMT_EXPLICIT_PRIOR_SESSION"
                if target in QMT_EXPLICIT_SESSION_FALLBACK_SOURCE_DATES
                else "IMMUTABLE_QMT_PREVIOUS_OPEN_SESSION"
            )
            if fallback_reason
            else "IMMUTABLE_QMT_EXACT_DATE"
        ),
    }
    return report, rows


def resolve_analysis_industry_membership_binding(
    engine,
    *,
    trade_date: str,
    decision_known_at: datetime | str,
    source: str = PROVIDER_ID,
    expected_proof_sha256: str | None = None,
) -> dict[str, Any]:
    """Bind the available validated membership to the analysis decision."""

    target = date.fromisoformat(str(trade_date)[:10]).isoformat()
    binding, _history = _resolve_membership_and_history(
        engine, target=target, source=source, decision_known_at=decision_known_at,
        expected_proof_sha256=expected_proof_sha256,
    )
    return binding


def _resolve_membership_and_history(
    engine, *, target: str, source: str, decision_known_at: datetime | str,
    expected_proof_sha256: str | None,
) -> tuple[dict[str, Any], tuple[dict[str, Any], list[dict[str, Any]]] | None]:
    if expected_proof_sha256 is not None and not _SHA256.fullmatch(expected_proof_sha256):
        raise IndustrySnapshotIntegrityError("已封存分析行业证据哈希无效")
    source_date, fallback_reason = select_industry_history_source(
        engine, trade_date=target, source=source, decision_known_at=decision_known_at,
    )
    if not fallback_reason:
        exact = _verify_exact_membership(
            engine, target=target, source=source, decision_known_at=decision_known_at,
        )
        proof_sha256 = _digest(exact)
        if expected_proof_sha256 is None or proof_sha256 == expected_proof_sha256:
            return {
                "snapshot_date": target,
                "source": source,
                "proof_sha256": proof_sha256,
                "proof_mode": "EXACT_QMT_MEMBERSHIP_SNAPSHOT",
                "source_snapshot_date": target,
                "previous_session_fallback": False,
                "fallback_reason": "",
                "captured_at": _iso_datetime(exact.get("captured_at"), field="运行时间"),
                "industry_relation_count": int(exact.get("industry_relation_count") or 0),
                "concept_relation_count": int(exact.get("concept_relation_count") or 0),
            }, None
        # A same-day capture can reach the database after the analysis read.
        # Replay may use the sealed prior proof only if it still verifies.
        source_date, fallback_reason = _select_previous_industry_source(
            engine, target=target, source=source,
            cutoff=_iso_datetime(decision_known_at, field="分析决策时间"),
        )

    report, rows = _prepare_selected_industry_history(
        engine, target=target, source_date=source_date, source=source,
        fallback_reason=fallback_reason,
    )
    if expected_proof_sha256 is not None and report["snapshot_id"] != expected_proof_sha256:
        raise IndustrySnapshotIntegrityError("QMT行业快照与已封存分析证据不一致")
    return {
        "snapshot_date": target,
        "source": source,
        "proof_sha256": report["snapshot_id"],
        "proof_mode": (
            "EXPLICIT_PRIOR_SESSION_INDUSTRY_CARRY_FORWARD"
            if target in QMT_EXPLICIT_SESSION_FALLBACK_SOURCE_DATES
            else "PREVIOUS_OPEN_SESSION_INDUSTRY_CARRY_FORWARD"
        ),
        "source_snapshot_date": source_date,
        "previous_session_fallback": True,
        "fallback_reason": fallback_reason,
        "captured_at": report["source_snapshot_captured_at"],
        "industry_relation_count": report["source_relation_count"],
        "concept_relation_count": 0,
    }, (report, rows)


def _verify_exact_membership(
    engine, *, target: str, source: str, decision_known_at: datetime | str,
) -> dict[str, Any]:
    from integrations.bigqmt.membership_snapshot import verify_existing_membership_snapshot

    cutoff = datetime.fromisoformat(_iso_datetime(decision_known_at, field="分析决策时间"))
    try:
        exact = verify_existing_membership_snapshot(
            engine, snapshot_date=date.fromisoformat(target), decision_known_at=cutoff, source=source,
        )
    except RuntimeError as exc:
        raise IndustrySnapshotIntegrityError("目标日QMT板块成员完整性校验失败") from exc
    if str(exact.get("source") or "") != source:
        raise IndustrySnapshotIntegrityError("QMT行业快照来源与分析合同不一致")
    return exact


def _verify_existing_rows(
    connection, *, report: Mapping[str, Any], rows: list[dict[str, Any]], pinned: bool,
) -> bool:
    snapshot_scope = " AND snapshot_id=:snapshot_id" if pinned else ""
    existing = [dict(row) for row in connection.execute(text(f"""
        SELECT snapshot_id, stock_code, row_hash
        FROM st_strategy_industry_history
        WHERE trade_date=:trade_date{snapshot_scope}
        ORDER BY stock_code
    """), {"trade_date": report["trade_date"], "snapshot_id": report["snapshot_id"]}).mappings().all()]
    if not existing:
        return False
    expected = [
        {
            "snapshot_id": row["snapshot_id"],
            "stock_code": row["stock_code"],
            "row_hash": row["row_hash"],
        }
        for row in rows
    ]
    if existing != expected:
        raise IndustrySnapshotIntegrityError(
            "当日策略行业历史与QMT精确日期快照不一致，拒绝覆盖"
        )
    return True


def capture_industry_history(
    engine, *, trade_date: str, source: str = PROVIDER_ID,
    decision_known_at: datetime | str | None = None,
    expected_proof_sha256: str | None = None,
) -> dict[str, Any]:
    """Copy the membership bound to one decision into the append-only ledger."""

    report, rows = prepare_industry_history(
        engine, trade_date=trade_date, source=source, decision_known_at=decision_known_at,
        expected_proof_sha256=expected_proof_sha256,
    )
    pinned = expected_proof_sha256 is not None

    def write_or_verify(connection) -> bool:
        if _verify_existing_rows(connection, report=report, rows=rows, pinned=pinned):
            return True
        insert = text("""
            INSERT INTO st_strategy_industry_history
            (snapshot_id, trade_date, as_of_exclusive, stock_code,
             industry_name, industry_type, source_system,
             source_fact_id, source_effective_at, source_etl_sync_at, row_hash)
            VALUES
            (:snapshot_id, :trade_date, :as_of_exclusive, :stock_code,
             :industry_name, :industry_type, :source_system,
             :source_fact_id, :source_effective_at, :source_etl_sync_at, :row_hash)
        """)
        for offset in range(0, len(rows), 500):
            connection.execute(insert, rows[offset:offset + 500])
        return False

    if not hasattr(engine, "connect"):
        if not engine.in_transaction():
            raise IndustrySnapshotIntegrityError("策略行业历史需要调用方事务")
        idempotent = write_or_verify(engine)
    else:
        try:
            with engine.begin() as connection:
                idempotent = write_or_verify(connection)
        except IntegrityError as exc:
            # A concurrent replay is valid only if every immutable row matches.
            with engine.connect() as connection:
                idempotent = _verify_existing_rows(connection, report=report, rows=rows, pinned=pinned)
            if not idempotent:
                raise IndustrySnapshotIntegrityError("策略行业历史并发写入发生不可验证冲突") from exc
    return {**report, "status": "COMPLETED", "idempotent_replay": idempotent}


__all__ = [
    "IndustrySnapshotIntegrityError",
    "IndustrySnapshotNotReady",
    "L1_INDUSTRY_TYPES",
    "QMT_EXPLICIT_SESSION_FALLBACK_SOURCE_DATES",
    "QMT_PREVIOUS_OPEN_SESSION_REASON",
    "QMT_PREVIOUS_SESSION_FALLBACKS",
    "STRATEGY_INDUSTRY_HISTORY_PRODUCTION_CUTOVER_DATE",
    "authorized_industry_fallback",
    "build_history_rows",
    "capture_industry_history",
    "prepare_industry_history",
    "previous_open_session_date",
    "resolve_analysis_industry_membership_binding",
    "select_industry_history_source",
]
