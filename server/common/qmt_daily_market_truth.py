"""Fail-closed consumer binding for catalog-complete, attested QMT daily bars."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from sqlalchemy import text

from server.common.qmt_attestation_contract import (
    ATTESTATION_PROTOCOL_VERSION,
    QMT_V2_BOUND_DAILY_ENTRY_KEYS,
    canonical_digest,
    expected_stock_set_contract,
    validated_universe_manifest,
)
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_trade_calendar import load_trade_calendar_receipt


QMT_DAILY_PROVIDER = "gj_big_qmt_inner"


def _timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat(sep=" ")
    raw = str(value or "").strip()[:19]
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").isoformat(sep=" ")
    except ValueError as exc:
        raise ValueError("QMT daily truth decision_known_at is invalid") from exc


def _date(value: Any) -> str:
    raw = str(value or "")[:10]
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError("QMT daily truth date is invalid") from exc
    if parsed != raw:
        raise ValueError("QMT daily truth date is invalid")
    return raw


@dataclass(frozen=True)
class QmtDailyMarketTruth:
    run_id: str
    run_start_date: str
    run_end_date: str
    run_finished_at: str
    decision_known_at: str
    catalog_batch_id: str
    catalog_manifest_hash: str
    catalog_member_set_hash: str
    calendar_batch_id: str
    calendar_manifest_hash: str
    calendar_session_set_hash: str
    attested_row_count: int
    requested_sessions: tuple[str, ...]
    truth_hash: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": "probiga.qmt-daily-market-consumer-truth.v1",
            "run_id": self.run_id,
            "run_start_date": self.run_start_date,
            "run_end_date": self.run_end_date,
            "run_finished_at": self.run_finished_at,
            "decision_known_at": self.decision_known_at,
            "catalog_batch_id": self.catalog_batch_id,
            "catalog_manifest_hash": self.catalog_manifest_hash,
            "catalog_member_set_hash": self.catalog_member_set_hash,
            "calendar_batch_id": self.calendar_batch_id,
            "calendar_manifest_hash": self.calendar_manifest_hash,
            "calendar_session_set_hash": self.calendar_session_set_hash,
            "attested_row_count": self.attested_row_count,
            "requested_sessions": list(self.requested_sessions),
            "truth_hash": self.truth_hash,
        }


def _validate_bound_daily_entries(
    daily: Mapping[str, Mapping[str, Any]],
    *,
    catalog: Any,
    calendar: Any,
    run_start_date: str,
    run_end_date: str,
) -> int:
    run_sessions = calendar.sessions_between(run_start_date, run_end_date)
    if set(daily) != set(run_sessions):
        raise RuntimeError("QMT attestation manifest/session inventory differs")
    expected_total = 0
    for day in run_sessions:
        entry = daily[day]
        if set(entry) != set(QMT_V2_BOUND_DAILY_ENTRY_KEYS):
            raise RuntimeError("QMT daily attestation is not catalog-bound")
        expected_codes = catalog.eligible_codes(day)
        expected_set = expected_stock_set_contract(day, expected_codes)
        if (
            entry["catalog_batch_id"] != catalog.batch_id
            or entry["catalog_member_count"] != catalog.member_count
            or entry["catalog_member_set_hash"] != catalog.member_set_hash
            or entry["catalog_manifest_hash"] != catalog.manifest_hash
            or entry["calendar_batch_id"] != calendar.batch_id
            or entry["calendar_session_set_hash"] != calendar.session_set_hash
            or entry["calendar_manifest_hash"] != calendar.manifest_hash
            or entry["calendar_known_at"] != calendar.known_at
            or entry["stock_count"] != expected_set["stock_count"]
            or entry["stock_set_hash"] != expected_set["stock_set_hash"]
        ):
            raise RuntimeError("QMT attestation catalog/calendar root differs")
        expected_total += int(entry["stock_count"])
    return expected_total


def load_qmt_daily_market_truth(
    connection: Any,
    *,
    start_date: str,
    end_date: str,
    decision_known_at: Any,
) -> QmtDailyMarketTruth:
    """Verify one completed range run and every currently consumed target row."""

    start = _date(start_date)
    end = _date(end_date)
    if start > end:
        raise ValueError("QMT daily truth range is invalid")
    decision_time = _timestamp(decision_known_at)
    run = connection.execute(text("""
        SELECT run_id, provider, start_date, end_date, status,
               target_rows, qmt_rows, matched_rows, missing_qmt_rows,
               mismatched_rows, already_attested_rows, updated_rows,
               tolerance_json, finished_at
        FROM qmt_kline_attestation_run
        WHERE provider=:provider
          AND status='COMPLETED'
          AND start_date<=:start_date
          AND end_date>=:end_date
          AND finished_at IS NOT NULL
          AND finished_at<=:decision_known_at
        ORDER BY finished_at DESC, run_id DESC
        LIMIT 1
    """), {
        "provider": QMT_DAILY_PROVIDER,
        "start_date": start,
        "end_date": end,
        "decision_known_at": decision_time,
    }).mappings().one_or_none()
    if run is None:
        raise RuntimeError(
            "no completed QMT daily attestation covers the requested range"
        )
    run_row = dict(run)
    run_start = _date(run_row["start_date"])
    run_end = _date(run_row["end_date"])
    run_finished_at = _timestamp(run_row["finished_at"])
    daily = validated_universe_manifest(
        run_row["tolerance_json"],
        start_date=run_start,
        end_date=run_end,
    )
    if not all(set(entry) == set(QMT_V2_BOUND_DAILY_ENTRY_KEYS)
               for entry in daily.values()):
        raise RuntimeError("completed QMT attestation is not catalog-bound")
    catalog_batch_ids = {entry["catalog_batch_id"] for entry in daily.values()}
    calendar_batch_ids = {entry["calendar_batch_id"] for entry in daily.values()}
    if len(catalog_batch_ids) != 1 or len(calendar_batch_ids) != 1:
        raise RuntimeError("QMT attestation changes market roots inside one run")
    catalog = load_stock_catalog(
        connection,
        batch_id=next(iter(catalog_batch_ids)),
        decision_known_at=decision_time,
    )
    calendar = load_trade_calendar_receipt(
        connection,
        batch_id=next(iter(calendar_batch_ids)),
        start_date=run_start,
        end_date=run_end,
        decision_known_at=decision_time,
    )
    expected_total = _validate_bound_daily_entries(
        daily,
        catalog=catalog,
        calendar=calendar,
        run_start_date=run_start,
        run_end_date=run_end,
    )
    if (
        int(run_row.get("target_rows") or 0) != expected_total
        or int(run_row.get("qmt_rows") or 0) != expected_total
        or int(run_row.get("matched_rows") or 0) != expected_total
        or int(run_row.get("missing_qmt_rows") or 0) != 0
        or int(run_row.get("mismatched_rows") or 0) != 0
        # A reused row attestation is immutable evidence for its original
        # run, not proof that this selected run observed that exact target
        # value.  Until there is an append-only run-to-row membership receipt,
        # consumers must require every proof row to belong to this run.
        or int(run_row.get("already_attested_rows") or 0) != 0
        or int(run_row.get("updated_rows") or 0) != expected_total
    ):
        raise RuntimeError("completed QMT attestation counters differ")

    requested_sessions = calendar.sessions_between(start, end)
    requested_expected_count = sum(
        int(daily[day]["stock_count"]) for day in requested_sessions
    )
    proof_rows = connection.execute(text("""
        SELECT k.trade_date,
               COUNT(*) AS attested_row_count,
               COUNT(DISTINCT LEFT(k.stock_code, 6)) AS attested_stock_count
        FROM sm_stock_kline AS k
        JOIN qmt_stock_catalog_member AS member
          ON member.batch_id=:catalog_batch_id
         AND member.stock_code=LEFT(k.stock_code, 6)
         AND member.instrument_type='STOCK'
         AND member.list_date<=k.trade_date
         AND (member.expire_date IS NULL OR member.expire_date>k.trade_date)
        WHERE k.k_type=1 AND k.adjust_type=0
          AND k.trade_date BETWEEN :start_date AND :end_date
          AND EXISTS (
              SELECT 1 FROM qmt_kline_attestation_row AS attestation
              WHERE attestation.target_id=k.id
                AND BINARY attestation.run_id=BINARY :selected_run_id
                AND BINARY attestation.protocol_version=
                    BINARY :protocol_version
                AND attestation.created_at<=:run_finished_at
                AND BINARY attestation.source_data_version=
                    BINARY k.data_version
                AND BINARY attestation.source_pre_close_origin=
                    BINARY 'NATIVE_QMT'
                AND attestation.trade_date=k.trade_date
                AND attestation.stock_code=LEFT(k.stock_code, 6)
                AND attestation.source_pre_close=k.pre_close
                AND attestation.attested_open=k.`open`
                AND attestation.attested_close=k.`close`
                AND attestation.attested_high=k.high
                AND attestation.attested_low=k.low
                AND attestation.attested_volume=k.volume
                AND attestation.attested_amount=k.amount
          )
        GROUP BY k.trade_date
        ORDER BY k.trade_date
    """), {
        "catalog_batch_id": catalog.batch_id,
        "start_date": start,
        "end_date": end,
        "protocol_version": ATTESTATION_PROTOCOL_VERSION,
        "selected_run_id": str(run_row["run_id"]),
        "run_finished_at": run_finished_at,
    }).mappings().all()
    observed = {
        str(row.get("trade_date") or "")[:10]: (
            int(row.get("attested_row_count") or 0),
            int(row.get("attested_stock_count") or 0),
        )
        for row in proof_rows
    }
    expected_by_day = {
        day: int(daily[day]["stock_count"]) for day in requested_sessions
    }
    if (
        set(observed) != set(expected_by_day)
        or any(observed[day] != (count, count)
               for day, count in expected_by_day.items())
    ):
        raise RuntimeError("current QMT target rows/attestations are incomplete")
    payload = {
        "schema": "probiga.qmt-daily-market-consumer-truth.v1",
        "run_id": str(run_row["run_id"]),
        "run_start_date": run_start,
        "run_end_date": run_end,
        "run_finished_at": run_finished_at,
        "decision_known_at": decision_time,
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "calendar_batch_id": calendar.batch_id,
        "calendar_manifest_hash": calendar.manifest_hash,
        "calendar_session_set_hash": calendar.session_set_hash,
        "attested_row_count": requested_expected_count,
        "requested_sessions": requested_sessions,
    }
    return QmtDailyMarketTruth(
        run_id=payload["run_id"],
        run_start_date=run_start,
        run_end_date=run_end,
        run_finished_at=run_finished_at,
        decision_known_at=decision_time,
        catalog_batch_id=catalog.batch_id,
        catalog_manifest_hash=catalog.manifest_hash,
        catalog_member_set_hash=catalog.member_set_hash,
        calendar_batch_id=calendar.batch_id,
        calendar_manifest_hash=calendar.manifest_hash,
        calendar_session_set_hash=calendar.session_set_hash,
        attested_row_count=requested_expected_count,
        requested_sessions=tuple(requested_sessions),
        truth_hash=canonical_digest(payload),
    )


__all__ = [
    "QMT_DAILY_PROVIDER",
    "QmtDailyMarketTruth",
    "load_qmt_daily_market_truth",
]
