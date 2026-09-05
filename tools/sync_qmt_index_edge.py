#!/usr/bin/env python3
"""Build-bound, fail-closed BigQMT publisher for index market data.

This is intentionally separate from the legacy ``run_single_table`` path.
The task identity fixes the provider and host; the publisher fixes the exact
QMT release, reference batch, code/session inventory, and (for minutes) the
native 241-bar grid before it changes a business table.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt import bridge
from integrations.qmt.info import to_qmt_index_symbol
from server.common.batch_db import create_batch_engine, replace_table_rows
from server.common.kline_data import get_kline_engine
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_history_coverage import QMT_MINUTE_GRID_PROFILE, minute_time_grid
from server.common.qmt_trade_calendar import (
    load_trade_calendar_receipt,
    validate_trade_calendar_runtime_schema,
)
from tools.run_qmt_windows_edge_release_bootstrap import (
    validate_bigqmt_strategy_release,
)


RESULT_SCHEMA = "probiga.qmt-index-edge-result.v1"
MANIFEST_SCHEMA = "probiga.qmt-index-edge-manifest.v1"
PROVIDER = "gj_big_qmt_inner"
EDGE_ROLE = "qmt_windows_edge"
TASK_TYPES = {
    "current": "qmt_index_current",
    "kline": "qmt_index_kline",
    "minute": "qmt_index_minute",
}
MIN_FORMAL_INDEX_COUNT = 50
_SHA40 = re.compile(r"[0-9a-f]{40}")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
INDEX_HISTORY_READY_TIME = time(15, 10)


class IndexDataBlocked(RuntimeError):
    """The provider/runtime cannot prove an exact publishable data slice."""


@dataclass(frozen=True)
class IndexCatalogMember:
    index_code: str
    qmt_code: str
    name: str
    list_date: str | None
    expire_date: str | None
    batch_id: str

    def eligible(self, trade_date: str) -> bool:
        # QMT does not supply an inception date for every index. Unknown
        # dates still require a real bar for the requested session; no date
        # is fabricated and missing market rows remain incomplete.
        return (self.list_date is None or self.list_date <= trade_date) and (
            self.expire_date is None or self.expire_date >= trade_date
        )


def _now() -> datetime:
    return datetime.now(_SHANGHAI).replace(tzinfo=None, microsecond=0)


def _iso_date(value: Any, *, field: str) -> str:
    if isinstance(value, datetime):
        raw = value.date().isoformat()
    elif isinstance(value, date):
        raw = value.isoformat()
    else:
        raw = str(value or "")[:10]
    try:
        normalized = date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise IndexDataBlocked(f"DATA_BLOCKED: {field} is not an ISO date") from exc
    if normalized != raw:
        raise IndexDataBlocked(f"DATA_BLOCKED: {field} is not an ISO date")
    return normalized


def _digest(value: Any) -> str:
    return canonical_digest(value)


def _number(value: Any, *, default: float = 0.0) -> float:
    parsed = pd.to_numeric(value, errors="coerce")
    return float(default) if pd.isna(parsed) else float(parsed)


def _storage_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Match the existing index tables' DECIMAL(50,6) before write and hash."""
    def normalize(value: Any) -> float | None:
        if value is None or pd.isna(value):
            return None
        try:
            with localcontext() as context:
                context.prec = 50
                number = Decimal(str(value))
                if not number.is_finite() or abs(number) >= Decimal("1e44"):
                    raise InvalidOperation
                number = number.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
                # MySQL does not retain the sign of a decimal zero.
                return float(number) if number else 0.0
        except (InvalidOperation, ValueError) as exc:
            raise IndexDataBlocked("DATA_BLOCKED: index numeric value exceeds storage contract") from exc

    for column in ("open", "close", "price", "high", "low", "avg_price",
                   "volume", "amount", "change", "change_pct"):
        if column in frame:
            frame[column] = frame[column].map(normalize)
    return frame


def _frame_hash(frame: pd.DataFrame) -> str:
    return _digest(frame.astype(object).where(pd.notna(frame), None).to_dict("records"))


def _chunks(values: Sequence[str], size: int) -> Iterable[list[str]]:
    for offset in range(0, len(values), max(1, int(size))):
        yield list(values[offset : offset + max(1, int(size))])


def _expected_build_sha(explicit: str = "") -> str:
    value = str(
        explicit
        or os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if _SHA40.fullmatch(value) is None or value == "0" * 40:
        raise IndexDataBlocked(
            "DATA_BLOCKED: exact scheduler build SHA is unavailable"
        )
    return value


def _validate_scheduler_identity(dataset: str) -> None:
    role = str(os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") or "").strip()
    if role != EDGE_ROLE:
        raise IndexDataBlocked(
            "DATA_BLOCKED: QMT index publisher is not on qmt_windows_edge"
        )
    observed_type = str(
        os.environ.get("PROBIGA_SCHEDULER_TASK_TYPE") or ""
    ).strip()
    if observed_type and observed_type != TASK_TYPES[dataset]:
        raise IndexDataBlocked(
            "DATA_BLOCKED: scheduler task type differs from the fixed dataset"
        )


def _validate_release(build_sha: str) -> dict[str, Any]:
    try:
        payload = bridge.capabilities(timeout=60)
        return validate_bigqmt_strategy_release(
            payload,
            expected_build_sha=build_sha,
        )
    except Exception as exc:
        raise IndexDataBlocked(
            "DATA_BLOCKED: exact-main frozen BigQMT release proof is unavailable"
        ) from exc


def _load_calendar_receipt(
    engine: Any,
    *,
    start_date: str,
    end_date: str,
    known_at: datetime,
) -> Any:
    try:
        validate_trade_calendar_runtime_schema(engine)
        with engine.connect() as connection:
            return load_trade_calendar_receipt(
                connection,
                start_date=start_date,
                end_date=end_date,
                decision_known_at=known_at,
            )
    except Exception as exc:
        raise IndexDataBlocked(
            "DATA_BLOCKED: immutable QMT calendar receipt does not cover the range"
        ) from exc


def _resolve_sessions(
    engine: Any,
    *,
    dataset: str,
    latest_session: bool,
    start_date: str,
    end_date: str,
    now: datetime,
) -> tuple[Any, list[str]]:
    if dataset not in TASK_TYPES:
        raise IndexDataBlocked("DATA_BLOCKED: unsupported QMT index dataset")
    current = now
    if current.tzinfo is not None:
        current = current.astimezone(_SHANGHAI).replace(tzinfo=None)
    today = current.date().isoformat()
    if latest_session:
        latest_allowed = current.date()
        if (
            dataset in {"kline", "minute"}
            and current.time() < INDEX_HISTORY_READY_TIME
        ):
            latest_allowed -= timedelta(days=1)
        search_start = (latest_allowed - timedelta(days=14)).isoformat()
        latest_end = latest_allowed.isoformat()
        receipt = _load_calendar_receipt(
            engine,
            start_date=search_start,
            end_date=latest_end,
            known_at=current,
        )
        sessions = receipt.sessions_between(search_start, latest_end)
        if not sessions:
            raise IndexDataBlocked(
                "DATA_BLOCKED: QMT calendar has no latest observed session"
            )
        return receipt, [sessions[-1]]
    start = _iso_date(start_date, field="start_date")
    end = _iso_date(end_date, field="end_date")
    if start > end or end > today:
        raise IndexDataBlocked("DATA_BLOCKED: requested index range is invalid")
    receipt = _load_calendar_receipt(
        engine,
        start_date=start,
        end_date=end,
        known_at=now,
    )
    sessions = receipt.sessions_between(start, end)
    if not sessions:
        raise IndexDataBlocked(
            "DATA_BLOCKED: requested index range has no authoritative sessions"
        )
    return receipt, sessions


def _mapping_rows(result: Any) -> list[dict[str, Any]]:
    mappings = getattr(result, "mappings", None)
    if callable(mappings):
        return [dict(row) for row in mappings().all()]
    return [dict(row) for row in result]


def _load_index_catalog(
    engine: Any,
    *,
    expected_batch_id: str | None = None,
) -> list[IndexCatalogMember]:
    with engine.connect() as connection:
        catalog_rows = _mapping_rows(connection.execute(text("""
            SELECT index_code, name, source
            FROM si_all_index_code
            ORDER BY index_code
        """)))
    if len(catalog_rows) < MIN_FORMAL_INDEX_COUNT:
        raise IndexDataBlocked(
            "DATA_BLOCKED: formal QMT index catalog is unexpectedly small"
        )
    normalized_catalog: list[tuple[str, str, str]] = []
    symbols: list[str] = []
    seen: set[str] = set()
    for row in catalog_rows:
        code = str(row.get("index_code") or "").strip().zfill(6)
        # These are SZSE volume-statistics records, not OHLC price indexes.
        if len(code) == 6 and code.isdigit() and code.startswith("395"):
            continue
        symbol = to_qmt_index_symbol(code)
        if (
            len(code) != 6
            or not code.isdigit()
            or symbol is None
            or code in seen
            or str(row.get("source") or "").strip().lower() not in {"qmt", PROVIDER}
        ):
            raise IndexDataBlocked(
                "DATA_BLOCKED: formal QMT index catalog identity is invalid"
            )
        seen.add(code)
        symbols.append(symbol)
        normalized_catalog.append((code, symbol, str(row.get("name") or "").strip()))

    if len(normalized_catalog) < MIN_FORMAL_INDEX_COUNT:
        raise IndexDataBlocked("DATA_BLOCKED: formal QMT price-index catalog is unexpectedly small")

    # Use the canonical expanding bind; a raw tuple is not portable across the
    # SQLAlchemy/PyMySQL and SQLite test boundaries.
    from sqlalchemy import bindparam

    detail_statement = text("""
        SELECT qmt_code, stock_code, short_name, list_date, expire_date,
               batch_id, data_source, permission_status
        FROM qmt_instrument_detail
        WHERE qmt_code IN :qmt_codes
    """).bindparams(bindparam("qmt_codes", expanding=True))
    with engine.connect() as connection:
        detail_rows = _mapping_rows(
            connection.execute(detail_statement, {"qmt_codes": symbols})
        )
    details = {str(row.get("qmt_code") or "").upper(): row for row in detail_rows}
    if set(details) != set(symbols):
        raise IndexDataBlocked(
            "DATA_BLOCKED: QMT instrument details do not exactly cover index catalog"
        )

    batches = {str(row.get("batch_id") or "") for row in detail_rows}
    if len(batches) != 1 or not next(iter(batches), ""):
        raise IndexDataBlocked("DATA_BLOCKED: QMT index instrument batches differ")
    catalog_batch_id = next(iter(batches))
    if expected_batch_id is not None and catalog_batch_id != expected_batch_id:
        raise IndexDataBlocked("DATA_BLOCKED: QMT index catalog batch differs from receipt")
    # Catalog and exchange calendar are independent publications. Bind the
    # catalog to its real, immutable QMT reference batch, not the calendar ID.
    with engine.connect() as connection:
        load_stock_catalog(connection, batch_id=catalog_batch_id,
                           decision_known_at=_now().replace(tzinfo=None))

    members: list[IndexCatalogMember] = []
    for code, symbol, name in normalized_catalog:
        row = details[symbol]
        list_date = (
            None if row.get("list_date") in (None, "")
            else _iso_date(row.get("list_date"), field="index_list_date")
        )
        expire_raw = row.get("expire_date")
        expire_date = (
            None
            if expire_raw in (None, "")
            else _iso_date(expire_raw, field="index_expire_date")
        )
        if (
            str(row.get("stock_code") or "").zfill(6) != code
            or str(row.get("data_source") or "") not in {PROVIDER, "gj_qmt"}
            or str(row.get("permission_status") or "") != "SUPPORTED"
        ):
            raise IndexDataBlocked(
                "DATA_BLOCKED: QMT index instrument source or permission differs"
            )
        members.append(IndexCatalogMember(
            index_code=code,
            qmt_code=symbol,
            name=name or str(row.get("short_name") or "").strip(),
            list_date=list_date,
            expire_date=expire_date,
            batch_id=catalog_batch_id,
        ))
    return members


def expected_codes_by_session(
    catalog: Sequence[IndexCatalogMember],
    sessions: Sequence[str],
) -> dict[str, tuple[str, ...]]:
    expected: dict[str, tuple[str, ...]] = {}
    for session in sessions:
        codes = tuple(sorted(member.index_code for member in catalog if member.eligible(session)))
        if not codes:
            raise IndexDataBlocked(
                f"DATA_BLOCKED: index catalog has no eligible codes for {session}"
            )
        expected[session] = codes
    return expected


def _catalog_by_code(catalog: Sequence[IndexCatalogMember]) -> dict[str, IndexCatalogMember]:
    return {member.index_code: member for member in catalog}


def _capture_receipt(
    response: Mapping[str, Any],
    *,
    requested_codes: Sequence[str],
    row_count: int | None = None,
) -> dict[str, Any]:
    codes = sorted(str(code or "").strip().upper() for code in requested_codes)
    return {
        key: response.get(key)
        for key in (
            "request_id",
            "action",
            "status",
            "source",
            "bridge_version",
            "generated_at",
            "strategy_release_protocol",
            "strategy_identity_protocol",
            "strategy_identity_frozen",
            "strategy_identity_status",
            "strategy_build_sha",
            "strategy_git_blob",
            "strategy_source_sha256",
            "strategy_artifact_sha256",
            "strategy_loaded_identity_sha256",
        )
    } | {
        "requested_code_count": len(codes),
        "requested_code_set_hash": _digest(codes),
        "row_count": (
            len(response.get("rows") or []) if row_count is None else int(row_count)
        ),
    }


def _validate_capture_receipts(
    receipts: Sequence[Mapping[str, Any]],
    *,
    dataset: str,
    build_sha: str,
    release: Mapping[str, Any],
) -> None:
    expected_action = {
        "current": "current",
        "kline": "kline",
        "minute": "minute",
    }[dataset]
    request_ids: set[str] = set()
    for receipt in receipts:
        request_id = str(receipt.get("request_id") or "")
        if (
            not request_id
            or request_id in request_ids
            or receipt.get("action") != expected_action
            or receipt.get("status") != "ok"
            or receipt.get("source") != PROVIDER
            or receipt.get("bridge_version") != "bigqmt_inner_v2"
            or receipt.get("strategy_release_protocol")
            != release.get("strategy_release_protocol")
            or receipt.get("strategy_identity_protocol")
            != release.get("strategy_identity_protocol")
            or receipt.get("strategy_identity_frozen") is not True
            or receipt.get("strategy_identity_status") != "BOUND"
            # The release proof already verifies exact strategy content against
            # this app build. An unchanged loaded strategy keeps its own build.
            or str(release.get("compatible_app_build_sha") or release.get("strategy_build_sha") or "").lower() != build_sha
            or not release.get("strategy_build_sha")
            or receipt.get("strategy_build_sha") != release.get("strategy_build_sha")
            or receipt.get("strategy_git_blob") != release.get("strategy_git_blob")
            or receipt.get("strategy_source_sha256")
            != release.get("strategy_source_sha256")
            or receipt.get("strategy_artifact_sha256")
            != release.get("strategy_artifact_sha256")
            or receipt.get("strategy_loaded_identity_sha256")
            != release.get("strategy_loaded_identity_sha256")
            or int(receipt.get("requested_code_count") or 0) <= 0
            or int(receipt.get("row_count") or 0) < 0
        ):
            raise IndexDataBlocked(
                "DATA_BLOCKED: BigQMT data response is not bound to frozen release"
            )
        request_ids.add(request_id)
    if not request_ids:
        raise IndexDataBlocked(
            "DATA_BLOCKED: BigQMT data response receipts are unavailable"
        )


def _validate_raw_symbol(
    row: Mapping[str, Any],
    *,
    catalog_by_code: Mapping[str, IndexCatalogMember],
) -> str:
    code = str(row.get("stock_code") or "").strip().zfill(6)
    member = catalog_by_code.get(code)
    if member is None or str(row.get("qmt_code") or "").strip().upper() != member.qmt_code:
        raise IndexDataBlocked(
            "DATA_BLOCKED: BigQMT response contains an unexpected index identity"
        )
    return code


def validate_current_frame(
    frame: pd.DataFrame,
    *,
    catalog: Sequence[IndexCatalogMember],
    trade_date: str,
    captured_at: datetime,
) -> pd.DataFrame:
    expected = {member.index_code for member in catalog if member.eligible(trade_date)}
    if frame is None or frame.empty:
        raise IndexDataBlocked("DATA_BLOCKED: BigQMT index current returned no rows")
    by_code = _catalog_by_code(catalog)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in frame.to_dict("records"):
        code = _validate_raw_symbol(raw, catalog_by_code=by_code)
        if code in seen:
            raise IndexDataBlocked("DATA_BLOCKED: duplicate BigQMT index current row")
        seen.add(code)
        snapshot = pd.to_datetime(raw.get("snapshot_at"), errors="coerce")
        if not pd.isna(snapshot) and getattr(snapshot, "tzinfo", None) is not None:
            snapshot = snapshot.tz_convert(_SHANGHAI).tz_localize(None)
        price = pd.to_numeric(raw.get("price"), errors="coerce")
        open_price = pd.to_numeric(raw.get("open"), errors="coerce")
        high = pd.to_numeric(raw.get("high"), errors="coerce")
        low = pd.to_numeric(raw.get("low"), errors="coerce")
        if (
            pd.isna(snapshot)
            or snapshot.date().isoformat() != trade_date
            or pd.isna(price)
            or pd.isna(open_price)
            or pd.isna(high)
            or pd.isna(low)
            or min(float(price), float(open_price), float(high), float(low)) <= 0
            or float(high) < max(float(price), float(open_price), float(low))
            or float(low) > min(float(price), float(open_price), float(high))
        ):
            raise IndexDataBlocked(
                "DATA_BLOCKED: BigQMT index current contains stale/invalid OHLC"
            )
        delta = (captured_at - snapshot.to_pydatetime()).total_seconds()
        if delta < -5:
            raise IndexDataBlocked("DATA_BLOCKED: index current timestamp is in the future")
        in_live_window = (
            time(9, 30) <= captured_at.time() <= time(11, 31)
            or time(13, 0) <= captured_at.time() <= time(15, 1)
        )
        if in_live_window and delta > 180:
            raise IndexDataBlocked("DATA_BLOCKED: index current snapshot is stale")
        rows.append({
            "index_code": code,
            "trade_time": snapshot.to_pydatetime(),
            "trade_date": trade_date,
            "open": float(open_price),
            "price": float(price),
            "high": float(high),
            "low": float(low),
            "volume": max(0.0, _number(raw.get("volume"))),
            "amount": max(0.0, _number(raw.get("amount"))),
            "change": _number(raw.get("change")),
            "change_pct": _number(raw.get("change_pct")),
            "snapshot_at": snapshot.to_pydatetime(),
            "etl_sync_at": captured_at,
        })
    if seen != expected:
        raise IndexDataBlocked(
            "DATA_BLOCKED: BigQMT index current code set is incomplete"
        )
    return _storage_frame(pd.DataFrame(rows).sort_values("index_code").reset_index(drop=True))


def validate_kline_frame(
    frame: pd.DataFrame,
    *,
    catalog: Sequence[IndexCatalogMember],
    expected_by_session: Mapping[str, Sequence[str]],
    captured_at: datetime,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        raise IndexDataBlocked("DATA_BLOCKED: BigQMT index kline returned no rows")
    by_code = _catalog_by_code(catalog)
    expected_keys = {
        (code, session)
        for session, codes in expected_by_session.items()
        for code in codes
    }
    rows: list[dict[str, Any]] = []
    observed: set[tuple[str, str]] = set()
    for raw in frame.to_dict("records"):
        code = _validate_raw_symbol(raw, catalog_by_code=by_code)
        session = _iso_date(raw.get("trade_date"), field="kline_trade_date")
        trade_at = pd.to_datetime(raw.get("trade_time"), errors="coerce")
        if pd.isna(trade_at) or trade_at.date().isoformat() != session:
            raise IndexDataBlocked("DATA_BLOCKED: index kline timestamp is invalid")
        key = (code, session)
        if key not in expected_keys or key in observed:
            raise IndexDataBlocked("DATA_BLOCKED: index kline key inventory differs")
        observed.add(key)
        values = {
            name: pd.to_numeric(raw.get(name), errors="coerce")
            for name in ("open", "close", "high", "low", "volume", "amount")
        }
        if (
            any(pd.isna(values[name]) for name in ("open", "close", "high", "low"))
            or min(float(values[name]) for name in ("open", "close", "high", "low")) <= 0
            or float(values["high"]) < max(float(values[name]) for name in ("open", "close", "low"))
            or float(values["low"]) > min(float(values[name]) for name in ("open", "close", "high"))
            or pd.isna(values["volume"])
            or pd.isna(values["amount"])
            or float(values["volume"]) < 0
            or float(values["amount"]) < 0
        ):
            raise IndexDataBlocked("DATA_BLOCKED: index kline OHLCV is invalid")
        rows.append({
            "index_code": code,
            "trade_time": trade_at.to_pydatetime(),
            "trade_date": session,
            "k_type": 1,
            "open": float(values["open"]),
            "close": float(values["close"]),
            "high": float(values["high"]),
            "low": float(values["low"]),
            "volume": float(values["volume"]),
            "amount": float(values["amount"]),
            "change": _number(raw.get("change")),
            "change_pct": _number(raw.get("change_pct")),
            "etl_sync_at": captured_at,
        })
    if observed != expected_keys:
        raise IndexDataBlocked("DATA_BLOCKED: index kline code/session grid is incomplete")
    return _storage_frame(pd.DataFrame(rows).sort_values(["trade_date", "index_code"]).reset_index(drop=True))


def validate_minute_frame(
    frame: pd.DataFrame,
    *,
    catalog: Sequence[IndexCatalogMember],
    expected_by_session: Mapping[str, Sequence[str]],
    captured_at: datetime,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        raise IndexDataBlocked("DATA_BLOCKED: BigQMT index minute returned no rows")
    by_code = _catalog_by_code(catalog)
    grid = minute_time_grid()
    expected_code_sessions = {
        (code, session)
        for session, codes in expected_by_session.items()
        for code in codes
    }
    expected_keys = {
        (code, session, minute)
        for session, codes in expected_by_session.items()
        for code in codes
        for minute in grid
    }
    rows: list[dict[str, Any]] = []
    observed: set[tuple[str, str, str]] = set()
    raw_keys: set[tuple[str, str, str]] = set()
    for raw in frame.to_dict("records"):
        code = _validate_raw_symbol(raw, catalog_by_code=by_code)
        trade_at = pd.to_datetime(raw.get("trade_time"), errors="coerce")
        if pd.isna(trade_at) or trade_at.second != 0:
            raise IndexDataBlocked("DATA_BLOCKED: index minute timestamp is invalid")
        session = trade_at.date().isoformat()
        raw_session = _iso_date(raw.get("trade_date"), field="minute_trade_date")
        if raw_session != session:
            raise IndexDataBlocked("DATA_BLOCKED: index minute date fields differ")
        minute = trade_at.strftime("%H:%M:%S")
        key = (code, session, minute)
        if (code, session) not in expected_code_sessions or key in raw_keys:
            raise IndexDataBlocked("DATA_BLOCKED: index minute key inventory differs")
        raw_keys.add(key)
        # This published dataset is the A-share 241-point session, not each
        # index's full native day. Cross-market indices can also quote during
        # the lunch break and after 15:00; project those out without inventing
        # any of the required A-share points or hiding duplicate provider keys.
        if key not in expected_keys:
            continue
        observed.add(key)
        price = pd.to_numeric(raw.get("price", raw.get("close")), errors="coerce")
        volume = pd.to_numeric(raw.get("volume"), errors="coerce")
        amount = pd.to_numeric(raw.get("amount"), errors="coerce")
        if (
            pd.isna(price)
            or float(price) <= 0
            or pd.isna(volume)
            or float(volume) < 0
            or pd.isna(amount)
            or float(amount) < 0
        ):
            raise IndexDataBlocked("DATA_BLOCKED: index minute values are invalid")
        avg_price = pd.to_numeric(raw.get("avg_price"), errors="coerce")
        rows.append({
            "index_code": code,
            "trade_time": trade_at.to_pydatetime(),
            "trade_date": session,
            "price": float(price),
            "avg_price": None if pd.isna(avg_price) else float(avg_price),
            "change": _number(raw.get("change")),
            "change_pct": _number(raw.get("change_pct")),
            "volume": float(volume),
            "amount": float(amount),
            "snapshot_at": captured_at,
            "etl_sync_at": captured_at,
        })
    if observed != expected_keys:
        raise IndexDataBlocked("DATA_BLOCKED: index minute 241-bar grid is incomplete")
    return _storage_frame(pd.DataFrame(rows).sort_values(
        ["trade_date", "index_code", "trade_time"]
    ).reset_index(drop=True))


def _code_predicate(codes: Sequence[str], *, prefix: str) -> tuple[str, dict[str, str]]:
    params = {f"{prefix}_{index}": code for index, code in enumerate(sorted(set(codes)))}
    if not params:
        raise IndexDataBlocked("DATA_BLOCKED: replacement code scope is empty")
    placeholders = ",".join(f":{name}" for name in params)
    return f"index_code IN ({placeholders})", params


def _replace_validated(
    frame: pd.DataFrame,
    *,
    dataset: str,
    primary_engine: Any,
    history_engine: Any,
    codes: Sequence[str],
    sessions: Sequence[str],
) -> int:
    predicate, params = _code_predicate(codes, prefix=f"qmt_index_{dataset}")
    if dataset == "current":
        return replace_table_rows(
            frame,
            "sm_index_current",
            primary_engine,
            where_sql=predicate,
            params=params,
            chunksize=1000,
            method="multi",
        )
    params.update({"start_date": min(sessions), "end_date": max(sessions)})
    date_scope = "trade_date BETWEEN :start_date AND :end_date"
    if dataset == "kline":
        params["k_type"] = 1
        date_scope += " AND k_type=:k_type"
    return replace_table_rows(
        frame,
        "sm_index_kline" if dataset == "kline" else "sm_index_minute",
        history_engine,
        where_sql=f"{predicate} AND {date_scope}",
        params=params,
        chunksize=2000,
        method="multi",
    )


def _read_published(
    *,
    dataset: str,
    primary_engine: Any,
    history_engine: Any,
    catalog: Sequence[IndexCatalogMember],
    codes: Sequence[str],
    sessions: Sequence[str],
) -> pd.DataFrame:
    predicate, params = _code_predicate(codes, prefix=f"verify_qmt_index_{dataset}")
    if dataset == "current":
        table_name = "sm_index_current"
        engine = primary_engine
        where_sql = predicate
    else:
        table_name = "sm_index_kline" if dataset == "kline" else "sm_index_minute"
        engine = history_engine
        params.update({"start_date": min(sessions), "end_date": max(sessions)})
        where_sql = f"{predicate} AND trade_date BETWEEN :start_date AND :end_date"
        if dataset == "kline":
            params["k_type"] = 1
            where_sql += " AND k_type=:k_type"
    with engine.connect() as connection:
        rows = _mapping_rows(connection.execute(
            text(f"SELECT * FROM `{table_name}` WHERE {where_sql}"),
            params,
        ))
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    symbol_map = {member.index_code: member.qmt_code for member in catalog}
    frame["stock_code"] = frame["index_code"].astype(str).str.zfill(6)
    frame["qmt_code"] = frame["stock_code"].map(symbol_map)
    return frame


def _fetch_frames(
    *,
    dataset: str,
    catalog: Sequence[IndexCatalogMember],
    expected_by_session: Mapping[str, Sequence[str]],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    by_code = _catalog_by_code(catalog)
    if dataset == "current":
        session = next(iter(expected_by_session))
        symbols = [by_code[code].qmt_code for code in expected_by_session[session]]
        capture = bridge.current_capture(symbols, batch_size=300, timeout=180)
        return (
            pd.DataFrame(capture.get("rows") or []),
            [_capture_receipt(capture, requested_codes=symbols)],
        )
    if dataset == "kline":
        all_codes = sorted({code for codes in expected_by_session.values() for code in codes})
        symbols = [by_code[code].qmt_code for code in all_codes]
        parts: list[pd.DataFrame] = []
        receipts: list[dict[str, Any]] = []
        for batch in _chunks(symbols, 40):
            capture = bridge.kline_capture(
                batch,
                start_date=min(expected_by_session),
                end_date=max(expected_by_session),
                dividend_type="none",
                download_history=True,
                batch_size=40,
                timeout=600,
            )
            part = pd.DataFrame(capture.get("rows") or [])
            if part is not None and not part.empty:
                parts.append(part)
            receipts.append(_capture_receipt(capture, requested_codes=batch))
        return (
            pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(),
            receipts,
        )
    parts = []
    receipts = []
    for session, codes in expected_by_session.items():
        symbols = [by_code[code].qmt_code for code in codes]
        capture = bridge.minute_capture(
            symbols,
            trade_date=session,
            start_date=session,
            end_date=session,
            count=0,
            download_history=True,
            batch_size=40,
            timeout=1800,
        )
        part = pd.DataFrame(capture.get("rows") or [])
        if part is not None and not part.empty:
            parts.append(part)
        for batch_receipt in capture.get("batch_receipts") or []:
            requested = list(batch_receipt.get("requested_codes") or [])
            response = dict(batch_receipt)
            receipts.append(_capture_receipt(
                response,
                requested_codes=requested,
                row_count=int(batch_receipt.get("row_count") or 0),
            ))
    return (
        pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(),
        receipts,
    )


def _manifest(
    *,
    dataset: str,
    build_sha: str,
    release: Mapping[str, Any],
    calendar: Any,
    catalog: Sequence[IndexCatalogMember],
    expected_by_session: Mapping[str, Sequence[str]],
    row_count: int,
    source_frame_hash: str,
    capture_receipts: Sequence[Mapping[str, Any]],
    captured_at: datetime,
    applied: bool,
) -> dict[str, Any]:
    codes = sorted({code for values in expected_by_session.values() for code in values})
    expected_keys = [
        [session, code]
        for session, values in sorted(expected_by_session.items())
        for code in sorted(values)
    ]
    grid = list(minute_time_grid()) if dataset == "minute" else []
    payload = {
        "schema": MANIFEST_SCHEMA,
        "dataset": dataset,
        "provider": PROVIDER,
        "build_sha": build_sha,
        "strategy_git_blob": release["strategy_git_blob"],
        "strategy_source_sha256": release["strategy_source_sha256"],
        "strategy_artifact_sha256": release["strategy_artifact_sha256"],
        "strategy_loaded_identity_sha256": release[
            "strategy_loaded_identity_sha256"
        ],
        "calendar_batch_id": calendar.batch_id,
        "catalog_batch_id": catalog[0].batch_id,
        "catalog_member_hash": _digest([asdict(member) for member in catalog]),
        "calendar_manifest_hash": calendar.manifest_hash,
        "calendar_session_set_hash": calendar.session_set_hash,
        "sessions": sorted(expected_by_session),
        "session_set_hash": _digest(sorted(expected_by_session)),
        "requested_code_count": len(codes),
        "requested_code_set_hash": _digest(codes),
        "expected_code_session_count": len(expected_keys),
        "expected_code_session_hash": _digest(expected_keys),
        "minute_grid_count": len(grid),
        "minute_grid_hash": _digest(grid) if grid else None,
        "minute_scope": QMT_MINUTE_GRID_PROFILE if grid else None,
        "expected_row_count": row_count,
        "source_frame_hash": source_frame_hash,
        "source_response_count": len(capture_receipts),
        "source_response_receipt_hash": _digest(list(capture_receipts)),
        "captured_at": captured_at.isoformat(sep=" ", timespec="seconds"),
        "applied": bool(applied),
    }
    return payload


def build_complete_result(
    *,
    dataset: str,
    manifest: Mapping[str, Any],
    written_rows: int,
    verified_rows: int,
) -> dict[str, Any]:
    expected_rows = int(manifest["expected_row_count"])
    result = {
        "schema": RESULT_SCHEMA,
        "status": "COMPLETE" if manifest.get("applied") else "DRY_RUN",
        "dataset": dataset,
        "provider": PROVIDER,
        "applied": bool(manifest.get("applied")),
        "expected_rows": expected_rows,
        "written_rows": int(written_rows),
        "db_verified_rows": int(verified_rows),
        "requested_code_count": int(manifest["requested_code_count"]),
        "responded_code_count": int(manifest["requested_code_count"]),
        "exact_coverage": True,
        "manifest": dict(manifest),
        "manifest_hash": _digest(dict(manifest)),
    }
    return result


def validate_task_result(payload: Mapping[str, Any], return_code: int) -> str:
    if payload.get("schema") != RESULT_SCHEMA:
        raise ValueError("QMT index result schema differs")
    status = str(payload.get("status") or "")
    if status == "BLOCKED":
        reason = str(payload.get("reason") or "")
        if int(return_code) != 3 or not reason.startswith("DATA_BLOCKED:"):
            raise ValueError("QMT index blocked result differs")
        return "blocked"
    manifest = payload.get("manifest")
    if not isinstance(manifest, Mapping):
        raise ValueError("QMT index result manifest is missing")
    dataset = str(payload.get("dataset") or "")
    expected_rows = int(payload.get("expected_rows") or 0)
    sessions = manifest.get("sessions")
    requested_code_count = int(manifest.get("requested_code_count") or 0)
    expected_code_sessions = int(
        manifest.get("expected_code_session_count") or 0
    )
    hash_fields = (
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256",
        "calendar_manifest_hash",
        "calendar_session_set_hash",
        "session_set_hash",
        "requested_code_set_hash",
        "expected_code_session_hash",
        "source_frame_hash",
        "source_response_receipt_hash",
    )
    if (
        int(return_code) != 0
        or status != "COMPLETE"
        or dataset not in TASK_TYPES
        or payload.get("provider") != PROVIDER
        or payload.get("applied") is not True
        or payload.get("exact_coverage") is not True
        or manifest.get("schema") != MANIFEST_SCHEMA
        or manifest.get("dataset") != dataset
        or manifest.get("provider") != PROVIDER
        or manifest.get("applied") is not True
        or _SHA40.fullmatch(str(manifest.get("build_sha") or "")) is None
        or str(manifest.get("build_sha")) == "0" * 40
        or re.fullmatch(
            r"[0-9a-f]{40}|[0-9a-f]{64}",
            str(manifest.get("strategy_git_blob") or ""),
        ) is None
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(manifest.get(field) or ""))
            is None
            for field in hash_fields
        )
        or not str(manifest.get("calendar_batch_id") or "")
        or not str(manifest.get("catalog_batch_id") or "")
        or re.fullmatch(r"[0-9a-f]{64}", str(manifest.get("catalog_member_hash") or "")) is None
        or not isinstance(sessions, list)
        or not sessions
        or sessions != sorted(set(str(item) for item in sessions))
        or _digest(sessions) != manifest.get("session_set_hash")
        or requested_code_count <= 0
        or expected_code_sessions < requested_code_count
        or _digest(dict(manifest)) != payload.get("manifest_hash")
        or expected_rows <= 0
        or int(manifest.get("expected_row_count") or 0) != expected_rows
        or int(manifest.get("source_response_count") or 0) <= 0
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(manifest.get("source_response_receipt_hash") or ""),
        ) is None
        or int(payload.get("written_rows") or -1) != expected_rows
        or int(payload.get("db_verified_rows") or -1) != expected_rows
        or int(payload.get("requested_code_count") or 0) != requested_code_count
        or int(payload.get("responded_code_count") or -1)
        != int(payload.get("requested_code_count") or 0)
    ):
        raise ValueError("QMT index exact result proof differs")
    if dataset == "minute":
        grid = list(minute_time_grid())
        if (
            int(manifest.get("minute_grid_count") or 0) != len(grid)
            or manifest.get("minute_grid_hash") != _digest(grid)
            or expected_rows != expected_code_sessions * len(grid)
        ):
            raise ValueError("QMT index exact minute proof differs")
    elif (
        int(manifest.get("minute_grid_count") or 0) != 0
        or manifest.get("minute_grid_hash") is not None
        or expected_rows != expected_code_sessions
    ):
        raise ValueError("QMT index exact non-minute proof differs")
    return "complete"


def validate_persisted_result(
    primary_engine: Any,
    payload: Mapping[str, Any],
    *,
    now: datetime | None = None,
    expected_session: str = "",
) -> dict[str, Any]:
    """Re-read the exact latest index partition before scheduler success."""

    if validate_task_result(payload, 0) != "complete":
        raise IndexDataBlocked("DATA_BLOCKED: index task result is invalid")
    manifest = payload["manifest"]
    if not isinstance(manifest, Mapping):
        raise IndexDataBlocked("DATA_BLOCKED: index manifest is unavailable")
    expected_build_sha = _expected_build_sha()
    if str(manifest.get("build_sha") or "").lower() != expected_build_sha:
        raise IndexDataBlocked("DATA_BLOCKED: stale index build receipt replay")

    verified_at = now or _now()
    if verified_at.tzinfo is not None:
        verified_at = verified_at.astimezone(_SHANGHAI).replace(tzinfo=None)
    verified_at = verified_at.replace(microsecond=0)
    expected = str(expected_session or "").strip()
    if expected:
        expected = _iso_date(expected, field="expected_session")
        if (
            payload["dataset"] == "current"
            and expected != verified_at.date().isoformat()
        ):
            raise IndexDataBlocked(
                "DATA_BLOCKED: current index snapshot cannot validate a historical date"
            )
    calendar, latest_sessions = _resolve_sessions(
        primary_engine,
        dataset=str(payload["dataset"]),
        latest_session=not bool(expected),
        start_date=expected,
        end_date=expected,
        now=verified_at,
    )
    sessions = [str(item) for item in manifest.get("sessions") or []]
    if sessions != latest_sessions:
        raise IndexDataBlocked("DATA_BLOCKED: stale index session receipt replay")
    if (
        str(manifest.get("calendar_batch_id") or "") != str(calendar.batch_id)
        or str(manifest.get("calendar_manifest_hash") or "")
        != str(calendar.manifest_hash)
        or str(manifest.get("calendar_session_set_hash") or "")
        != str(calendar.session_set_hash)
    ):
        raise IndexDataBlocked(
            "DATA_BLOCKED: index calendar receipt differs from current authority"
        )

    catalog = _load_index_catalog(
        primary_engine,
        expected_batch_id=str(manifest.get("catalog_batch_id") or ""),
    )
    if _digest([asdict(member) for member in catalog]) != manifest.get("catalog_member_hash"):
        raise IndexDataBlocked("DATA_BLOCKED: index instrument metadata changed since capture")
    expected_by_session = expected_codes_by_session(catalog, sessions)
    codes = sorted(
        {code for values in expected_by_session.values() for code in values}
    )
    expected_keys = [
        [session, code]
        for session, values in sorted(expected_by_session.items())
        for code in sorted(values)
    ]
    if (
        int(manifest.get("requested_code_count") or 0) != len(codes)
        or manifest.get("requested_code_set_hash") != _digest(codes)
        or int(manifest.get("expected_code_session_count") or 0)
        != len(expected_keys)
        or manifest.get("expected_code_session_hash") != _digest(expected_keys)
    ):
        raise IndexDataBlocked(
            "DATA_BLOCKED: index catalog receipt differs from current authority"
        )

    captured_at = pd.to_datetime(manifest.get("captured_at"), errors="coerce")
    if pd.isna(captured_at) or getattr(captured_at, "tzinfo", None) is not None:
        raise IndexDataBlocked("DATA_BLOCKED: index capture timestamp is invalid")
    captured_at = captured_at.to_pydatetime().replace(microsecond=0)
    history_engine = get_kline_engine()
    published = _read_published(
        dataset=str(payload["dataset"]),
        primary_engine=primary_engine,
        history_engine=history_engine,
        catalog=catalog,
        codes=codes,
        sessions=sessions,
    )
    if payload["dataset"] == "current":
        verified = validate_current_frame(
            published,
            catalog=catalog,
            trade_date=sessions[0],
            captured_at=captured_at,
        )
    elif payload["dataset"] == "kline":
        verified = validate_kline_frame(
            published,
            catalog=catalog,
            expected_by_session=expected_by_session,
            captured_at=captured_at,
        )
    else:
        verified = validate_minute_frame(
            published,
            catalog=catalog,
            expected_by_session=expected_by_session,
            captured_at=captured_at,
        )
    verified_hash = _frame_hash(verified)
    if (
        len(verified) != int(payload.get("db_verified_rows") or 0)
        or len(verified) != int(manifest.get("expected_row_count") or 0)
        or verified_hash != manifest.get("source_frame_hash")
    ):
        raise IndexDataBlocked(
            "DATA_BLOCKED: persisted index partition differs from manifest"
        )
    return {
        "dataset": payload["dataset"],
        "sessions": sessions,
        "row_count": len(verified),
        "row_hash": verified_hash,
    }


def run(
    *,
    dataset: str,
    latest_session: bool,
    start_date: str,
    end_date: str,
    apply: bool,
    expected_build_sha: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    if dataset not in TASK_TYPES:
        raise IndexDataBlocked("DATA_BLOCKED: unsupported QMT index dataset")
    captured_at = (now or _now()).replace(tzinfo=None, microsecond=0)
    _validate_scheduler_identity(dataset)
    build_sha = _expected_build_sha(expected_build_sha)
    release = _validate_release(build_sha)
    primary_engine = create_batch_engine()
    calendar, sessions = _resolve_sessions(
        primary_engine,
        dataset=dataset,
        latest_session=latest_session,
        start_date=start_date,
        end_date=end_date,
        now=captured_at,
    )
    if dataset == "current":
        if len(sessions) != 1 or sessions[0] != captured_at.date().isoformat():
            raise IndexDataBlocked(
                "DATA_BLOCKED: current index snapshot cannot reconstruct a historical date"
            )
    elif sessions[-1] == captured_at.date().isoformat() and captured_at.time() < time(15, 10):
        raise IndexDataBlocked(
            "DATA_BLOCKED: closed-session index history is not final before 15:10"
        )
    catalog = _load_index_catalog(primary_engine)
    expected = expected_codes_by_session(catalog, sessions)
    raw, capture_receipts = _fetch_frames(
        dataset=dataset,
        catalog=catalog,
        expected_by_session=expected,
    )
    _validate_capture_receipts(
        capture_receipts,
        dataset=dataset,
        build_sha=build_sha,
        release=release,
    )
    release_after_capture = _validate_release(build_sha)
    if release_after_capture != release:
        raise IndexDataBlocked(
            "DATA_BLOCKED: frozen BigQMT release identity changed during capture"
        )
    if dataset == "current":
        validated = validate_current_frame(
            raw,
            catalog=catalog,
            trade_date=sessions[0],
            captured_at=captured_at,
        )
    elif dataset == "kline":
        validated = validate_kline_frame(
            raw,
            catalog=catalog,
            expected_by_session=expected,
            captured_at=captured_at,
        )
    else:
        validated = validate_minute_frame(
            raw,
            catalog=catalog,
            expected_by_session=expected,
            captured_at=captured_at,
        )
    # The formal task owns every QMT-catalog index in the target partition.
    # Delete/verify that full scope so a newly listed or expired code cannot
    # leave stale rows from an older partial publisher behind.
    codes = sorted(member.index_code for member in catalog)
    history_engine = get_kline_engine()
    written = 0
    verified_rows = len(validated)
    if apply:
        written = _replace_validated(
            validated,
            dataset=dataset,
            primary_engine=primary_engine,
            history_engine=history_engine,
            codes=codes,
            sessions=sessions,
        )
        # The atomic writer returned exactly the already validated frame.  A
        # different count proves a schema/trigger/runtime boundary changed.
        if written != len(validated):
            raise IndexDataBlocked(
                "DATA_BLOCKED: index database write count differs from manifest"
            )
        published = _read_published(
            dataset=dataset,
            primary_engine=primary_engine,
            history_engine=history_engine,
            catalog=catalog,
            codes=codes,
            sessions=sessions,
        )
        if dataset == "current":
            verified = validate_current_frame(
                published,
                catalog=catalog,
                trade_date=sessions[0],
                captured_at=captured_at,
            )
        elif dataset == "kline":
            verified = validate_kline_frame(
                published,
                catalog=catalog,
                expected_by_session=expected,
                captured_at=captured_at,
            )
        else:
            verified = validate_minute_frame(
                published,
                catalog=catalog,
                expected_by_session=expected,
                captured_at=captured_at,
            )
        verified_rows = len(verified)
        if _frame_hash(verified) != _frame_hash(validated):
            raise IndexDataBlocked("DATA_BLOCKED: persisted index partition differs from source")
    manifest = _manifest(
        dataset=dataset,
        build_sha=build_sha,
        release=release,
        calendar=calendar,
        catalog=catalog,
        expected_by_session=expected,
        row_count=len(validated),
        source_frame_hash=_frame_hash(validated),
        capture_receipts=capture_receipts,
        captured_at=captured_at,
        applied=apply,
    )
    return build_complete_result(
        dataset=dataset,
        manifest=manifest,
        written_rows=written,
        verified_rows=verified_rows,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=sorted(TASK_TYPES), required=True)
    range_group = parser.add_mutually_exclusive_group(required=True)
    range_group.add_argument("--latest-session", action="store_true")
    range_group.add_argument("--start-date")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.start_date and not args.end_date:
        parser.error("--end-date is required with --start-date")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        payload = run(
            dataset=args.dataset,
            latest_session=bool(args.latest_session),
            start_date=str(args.start_date or ""),
            end_date=str(args.end_date or ""),
            apply=bool(args.apply),
            expected_build_sha=str(args.expected_build_sha or ""),
        )
        return_code = 0
    except IndexDataBlocked as exc:
        reason = str(exc)
        if not reason.startswith("DATA_BLOCKED:"):
            reason = "DATA_BLOCKED: " + reason
        payload = {
            "schema": RESULT_SCHEMA,
            "status": "BLOCKED",
            "dataset": args.dataset,
            "provider": PROVIDER,
            "reason": reason,
        }
        return_code = 3
    except Exception as exc:  # pragma: no cover - top-level defensive guard
        payload = {
            "schema": RESULT_SCHEMA,
            "status": "FAILED",
            "dataset": args.dataset,
            "provider": PROVIDER,
            "reason": f"{type(exc).__name__}: {exc}",
        }
        return_code = 2
    rendered = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    print(rendered if args.json else rendered)
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
