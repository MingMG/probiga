#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Fast, fail-closed intraday A-share capital-flow snapshot collector.

The collector reads the authoritative active-stock universe from the latest
unadjusted daily K-line, fetches Eastmoney's all-market cumulative flow ranking
with deterministic pagination, and writes one complete current-minute snapshot
to the dedicated minute/flow database.

It deliberately does not reuse the legacy realtime or per-stock crawlers.  A
snapshot is committed only on a configured trading day, inside the continuous
auction sessions, and after the requested universe reaches the coverage gate.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from datetime import date, datetime, time as wall_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import requests
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine  # noqa: E402
from server.common.minute_data import get_minute_engine  # noqa: E402
from server.common.mysql_lock import mysql_named_lock  # noqa: E402


EASTMONEY_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
EASTMONEY_MARKETS = (
    "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,"
    "m:0+t:81+s:2048"
)
EASTMONEY_FIELDS = "f12,f62,f66,f72,f78,f84,f124"
MAX_SOURCE_AGE_SECONDS = 180
EASTMONEY_TOKEN = "bd1d9ddb04089700cf9c27f6f7426281"
# Share the canonical flow-writer lock with the per-stock/stage publisher.
LOCK_NAME = "probiga:capital_flow_minute"
SHANGHAI_TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")
FLOW_FIELDS = (
    "main_net_inflow",
    "max_net_inflow",
    "lg_net_inflow",
    "mid_net_inflow",
    "sm_net_inflow",
)
EASTMONEY_FLOW_FIELDS = {
    "main_net_inflow": "f62",
    "max_net_inflow": "f66",
    "lg_net_inflow": "f72",
    "mid_net_inflow": "f78",
    "sm_net_inflow": "f84",
}
_CODE_RE = re.compile(r"^[0-9]{6}$")


class CoverageError(RuntimeError):
    """Raised before any write when the active-universe coverage gate fails."""

    def __init__(self, result: dict[str, Any]):
        self.result = result
        super().__init__(
            "capital-flow coverage "
            f"{result['coverage']:.4%} is below required {result['min_coverage']:.4%}"
        )


def _shanghai_now() -> datetime:
    return datetime.now(SHANGHAI_TZ).replace(tzinfo=None)


def _as_shanghai_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=None)
    return value.astimezone(SHANGHAI_TZ).replace(tzinfo=None)


def normalize_stock_code(value: object) -> str:
    """Normalize common A-share code spellings and reject ambiguous values."""
    code = str(value or "").strip().upper()
    for suffix in (".SH", ".SZ", ".BJ"):
        if code.endswith(suffix):
            code = code[: -len(suffix)]
            break
    for prefix in ("SH", "SZ", "BJ"):
        if code.startswith(prefix) and len(code) == 8:
            code = code[2:]
            break
    if not _CODE_RE.fullmatch(code) or code == "000000":
        raise ValueError(f"invalid A-share stock code: {value!r}")
    return code


def parse_extra_codes(values: Iterable[str]) -> set[str]:
    codes: set[str] = set()
    for raw in values:
        for item in str(raw or "").split(","):
            if item.strip():
                codes.add(normalize_stock_code(item))
    return codes


def is_continuous_auction_time(now: datetime) -> bool:
    """Return whether *now* is inside an A-share continuous auction session."""
    current = _as_shanghai_naive(now).time()
    return (
        wall_time(9, 30) <= current <= wall_time(11, 30)
        or wall_time(13, 0) <= current <= wall_time(15, 0)
    )


def is_trade_day(kline_engine: Any, target_date: date) -> bool:
    """Resolve the exchange calendar from the K-line database, fail closed."""
    with kline_engine.connect() as conn:
        status = conn.execute(
            text(
                "SELECT trade_status FROM si_trade_calendar "
                "WHERE trade_date = :trade_date LIMIT 1"
            ),
            {"trade_date": target_date.isoformat()},
        ).scalar()
    try:
        return int(status) == 1
    except (TypeError, ValueError):
        return False


def load_latest_active_codes(kline_engine: Any) -> tuple[str, set[str]]:
    """Load stocks represented in the latest unadjusted daily K-line."""
    latest_sql = text(
        "SELECT MAX(trade_date) FROM sm_stock_kline "
        "WHERE k_type = 1 AND adjust_type = 0"
    )
    codes_sql = text(
        "SELECT DISTINCT stock_code FROM sm_stock_kline "
        "WHERE trade_date = :trade_date AND k_type = 1 AND adjust_type = 0 "
        "ORDER BY stock_code"
    )
    with kline_engine.connect() as conn:
        latest = conn.execute(latest_sql).scalar()
        if latest is None:
            raise RuntimeError("latest unadjusted daily K-line universe is empty")
        latest_date = str(latest)[:10]
        raw_codes = conn.execute(codes_sql, {"trade_date": latest_date}).scalars().all()

    codes: set[str] = set()
    for raw in raw_codes:
        try:
            codes.add(normalize_stock_code(raw))
        except ValueError:
            continue
    if not codes:
        raise RuntimeError(f"active K-line universe is empty for {latest_date}")
    return latest_date, codes


def _finite_float(value: object) -> float | None:
    if value in (None, "", "-", "--"):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _parse_flow_item(item: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        code = normalize_stock_code(item.get("f12"))
    except ValueError:
        return None
    row: dict[str, Any] = {"stock_code": code}
    source_epoch = _finite_float(item.get("f124"))
    if source_epoch is None or source_epoch <= 0:
        return None
    try:
        row["source_time"] = datetime.fromtimestamp(source_epoch, SHANGHAI_TZ).replace(tzinfo=None)
    except (ValueError, OverflowError, OSError):
        return None
    for output_name, source_name in EASTMONEY_FLOW_FIELDS.items():
        number = _finite_float(item.get(source_name))
        if number is None:
            return None
        row[output_name] = number
    return row


def _new_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 ProBigA-Intraday-Flow/1.0",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://data.eastmoney.com/",
        }
    )
    return session


def _request_page(
    session: Any,
    *,
    page: int,
    page_size: int,
    timeout: float,
    attempts: int,
    retry_delay: float,
    sleep: Callable[[float], None],
) -> Mapping[str, Any]:
    params = {
        "pn": page,
        "pz": page_size,
        "po": 0,
        "np": 1,
        "ut": EASTMONEY_TOKEN,
        "fltt": 2,
        "invt": 2,
        # Code order is stable across pages while cumulative flow values move.
        "fid": "f12",
        "fs": EASTMONEY_MARKETS,
        "fields": EASTMONEY_FIELDS,
    }
    last_error: BaseException | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            response = session.get(EASTMONEY_URL, params=params, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, Mapping):
                raise ValueError("Eastmoney returned a non-object payload")
            return payload
        except (requests.RequestException, TypeError, ValueError) as exc:
            last_error = exc
            if attempt < max(1, attempts):
                sleep(max(0.0, retry_delay) * (2 ** (attempt - 1)))
    raise RuntimeError(
        f"Eastmoney page {page} failed after {max(1, attempts)} attempts: {last_error}"
    ) from last_error


def _page_items(payload: Mapping[str, Any]) -> tuple[int, list[Mapping[str, Any]]]:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise ValueError("Eastmoney payload has no data object")
    raw_items = data.get("diff")
    if isinstance(raw_items, Mapping):
        raw_items = list(raw_items.values())
    if raw_items is None:
        raw_items = []
    if not isinstance(raw_items, list):
        raise ValueError("Eastmoney data.diff is not a list")
    items = [item for item in raw_items if isinstance(item, Mapping)]
    try:
        total = int(data.get("total") or 0)
    except (TypeError, ValueError):
        total = 0
    return max(0, total), items


def fetch_eastmoney_capital_flow(
    *,
    session: Any | None = None,
    page_size: int = 100,
    timeout: float = 15.0,
    attempts: int = 3,
    retry_delay: float = 0.5,
    max_pages: int = 100,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    """Fetch a deterministic, complete all-market Eastmoney flow snapshot."""
    if page_size < 1 or max_pages < 1:
        raise ValueError("page_size and max_pages must be positive")
    owned_session = session is None
    session = session or _new_session()
    expected_total: int | None = None
    seen_codes: set[str] = set()
    valid_rows: dict[str, dict[str, Any]] = {}
    pages = 0
    try:
        for page in range(1, max_pages + 1):
            payload = _request_page(
                session,
                page=page,
                page_size=page_size,
                timeout=timeout,
                attempts=attempts,
                retry_delay=retry_delay,
                sleep=sleep,
            )
            total, items = _page_items(payload)
            pages = page
            if expected_total is None:
                if total <= 0:
                    raise RuntimeError("Eastmoney reported an empty market universe")
                expected_total = total
            elif total != expected_total:
                raise RuntimeError(
                    f"Eastmoney universe changed during pagination: {expected_total} -> {total}"
                )
            if not items:
                raise RuntimeError(
                    f"Eastmoney pagination ended at {len(seen_codes)}/{expected_total} codes"
                )

            before = len(seen_codes)
            # Local ordering protects deterministic deduplication even if a
            # provider node serializes data.diff as an object.
            items.sort(key=lambda item: str(item.get("f12") or ""))
            for item in items:
                try:
                    code = normalize_stock_code(item.get("f12"))
                except ValueError:
                    continue
                seen_codes.add(code)
                parsed = _parse_flow_item(item)
                if parsed is not None:
                    valid_rows[code] = parsed
            if len(seen_codes) == before:
                raise RuntimeError(f"Eastmoney pagination stalled on page {page}")
            if len(seen_codes) >= expected_total:
                break
        else:
            raise RuntimeError(
                f"Eastmoney pagination exceeded {max_pages} pages at "
                f"{len(seen_codes)}/{expected_total or 0} codes"
            )
    finally:
        if owned_session:
            session.close()

    if expected_total is None or len(seen_codes) < expected_total:
        raise RuntimeError(
            f"incomplete Eastmoney market snapshot: {len(seen_codes)}/{expected_total or 0}"
        )
    ordered = {code: valid_rows[code] for code in sorted(valid_rows)}
    return ordered, {
        "provider_total": expected_total,
        "provider_seen": len(seen_codes),
        "provider_valid": len(ordered),
        "pages": pages,
    }


def write_current_minute_snapshot(
    minute_engine: Any,
    *,
    trade_time: datetime,
    rows: Sequence[Mapping[str, Any]],
    snapshot_at: datetime,
    chunk_size: int = 1000,
) -> int:
    """Atomically replace exactly one minute, leaving all other minutes intact."""
    if not rows:
        raise ValueError("refusing to write an empty capital-flow snapshot")
    minute_start = trade_time.replace(second=0, microsecond=0)
    minute_end = minute_start + timedelta(minutes=1)
    synced_at = snapshot_at.replace(microsecond=0)
    params = [
        {
            "stock_code": normalize_stock_code(row["stock_code"]),
            "trade_time": minute_start,
            "main_net_inflow": row["main_net_inflow"],
            "max_net_inflow": row["max_net_inflow"],
            "lg_net_inflow": row["lg_net_inflow"],
            "mid_net_inflow": row["mid_net_inflow"],
            "sm_net_inflow": row["sm_net_inflow"],
            "snapshot_at": synced_at,
            "etl_sync_at": synced_at,
            "source_time": row["source_time"],
            "received_at": synced_at,
            "data_source": "east_push2delay",
        }
        for row in rows
    ]
    insert_sql = text(
        "INSERT INTO sm_stock_capital_flow_min "
        "(stock_code, trade_time, main_net_inflow, max_net_inflow, "
        "lg_net_inflow, mid_net_inflow, sm_net_inflow, snapshot_at, etl_sync_at, "
        "source_time, received_at, data_source) "
        "VALUES (:stock_code, :trade_time, :main_net_inflow, :max_net_inflow, "
        ":lg_net_inflow, :mid_net_inflow, :sm_net_inflow, :snapshot_at, :etl_sync_at, "
        ":source_time, :received_at, :data_source)"
    )
    inserted = 0
    # Delete and all insert chunks share one transaction and therefore one
    # commit/rollback boundary.
    with minute_engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM sm_stock_capital_flow_min "
                "WHERE trade_time >= :minute_start AND trade_time < :minute_end"
            ),
            {"minute_start": minute_start, "minute_end": minute_end},
        )
        for offset in range(0, len(params), max(1, chunk_size)):
            chunk = params[offset : offset + max(1, chunk_size)]
            conn.execute(insert_sql, chunk)
            inserted += len(chunk)
    return inserted


def run_sync(
    *,
    min_coverage: float = 0.98,
    extra_codes: Iterable[str] = (),
    now: datetime | None = None,
    kline_engine: Any | None = None,
    minute_engine: Any | None = None,
    session: Any | None = None,
    page_size: int = 100,
    timeout: float = 15.0,
    attempts: int = 3,
    retry_delay: float = 0.5,
    max_pages: int = 100,
    lock_timeout: int = 0,
    dry_run: bool = False,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Run one guarded current-minute snapshot collection."""
    if not 0.0 < float(min_coverage) <= 1.0:
        raise ValueError("min_coverage must be in (0, 1]")
    run_at = _as_shanghai_naive(now or _shanghai_now())
    if not is_continuous_auction_time(run_at):
        return {
            "status": "skipped",
            "reason": "outside_continuous_auction",
            "now": run_at.isoformat(sep=" ", timespec="seconds"),
        }

    kline_engine = kline_engine or get_kline_engine()
    if not is_trade_day(kline_engine, run_at.date()):
        return {
            "status": "skipped",
            "reason": "not_trade_day",
            "now": run_at.isoformat(sep=" ", timespec="seconds"),
        }

    latest_kline_date, active_codes = load_latest_active_codes(kline_engine)
    extras = parse_extra_codes(extra_codes)
    target_codes = active_codes | extras
    if not target_codes:
        raise RuntimeError("target capital-flow universe is empty")

    fetched, provider = fetch_eastmoney_capital_flow(
        session=session,
        page_size=page_size,
        timeout=timeout,
        attempts=attempts,
        retry_delay=retry_delay,
        max_pages=max_pages,
        sleep=sleep,
    )
    # Pagination may take time: validate against completion, never the request's
    # start time. Explicit `now` keeps deterministic read-only/test runs possible.
    observed_at = _as_shanghai_naive(now or _shanghai_now())
    fresh = {
        code: row for code, row in fetched.items()
        if isinstance(row.get("source_time"), datetime)
        and row["source_time"].date() == observed_at.date()
        and 0 <= (observed_at - row["source_time"]).total_seconds() <= MAX_SOURCE_AGE_SECONDS
    }
    selected = [fresh[code] for code in sorted(target_codes) if code in fresh]
    missing_codes = sorted(target_codes - fresh.keys())
    coverage = len(selected) / len(target_codes)
    trade_time = run_at.replace(second=0, microsecond=0)
    result: dict[str, Any] = {
        "status": "dry_run" if dry_run else "ready",
        "trade_time": trade_time.isoformat(sep=" ", timespec="minutes"),
        "latest_kline_date": latest_kline_date,
        "active_codes": len(active_codes),
        "extra_codes": len(extras),
        "expected_codes": len(target_codes),
        "selected_codes": len(selected),
        "missing_codes": missing_codes,
        "coverage": coverage,
        "min_coverage": float(min_coverage),
        "source_stale_or_missing": len(fetched) - len(fresh),
        **provider,
    }
    if coverage < float(min_coverage):
        result["status"] = "coverage_failed"
        raise CoverageError(result)
    if dry_run:
        result["written_rows"] = 0
        return result

    minute_engine = minute_engine or get_minute_engine()
    # Network pagination and coverage validation deliberately happen before
    # acquiring the canonical writer lock.  The lock is held only across the
    # final atomic replace so it cannot starve the full-history publisher.
    with mysql_named_lock(
        minute_engine,
        LOCK_NAME,
        timeout_seconds=max(0, int(lock_timeout)),
    ):
        result["written_rows"] = write_current_minute_snapshot(
            minute_engine,
            trade_time=trade_time,
            rows=selected,
            snapshot_at=observed_at,
        )
        result["status"] = "written"
        return result


def _emit(payload: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
        return
    for key, value in payload.items():
        print(f"{key}: {value}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch and atomically persist one intraday all-market capital-flow snapshot."
    )
    parser.add_argument("--min-coverage", type=float, default=0.98)
    parser.add_argument(
        "--extra-code",
        action="append",
        default=[],
        help="Additional code(s), repeatable or comma-separated.",
    )
    # The live endpoint currently caps data.diff at 100 rows per page even
    # when pz is larger; keep the explicit request aligned with that contract.
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--retry-delay", type=float, default=0.5)
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--lock-timeout", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = run_sync(
            min_coverage=args.min_coverage,
            extra_codes=args.extra_code,
            page_size=args.page_size,
            timeout=args.timeout,
            attempts=args.attempts,
            retry_delay=args.retry_delay,
            max_pages=args.max_pages,
            lock_timeout=args.lock_timeout,
            dry_run=args.dry_run,
        )
    except CoverageError as exc:
        result = dict(exc.result)
        result.update({"error_type": type(exc).__name__, "error": str(exc)})
        _emit(result, as_json=args.json)
        return 2
    except Exception as exc:
        result = {
            "status": "error",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        _emit(result, as_json=args.json)
        return 1
    _emit(result, as_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
