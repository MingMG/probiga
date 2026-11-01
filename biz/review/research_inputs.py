"""Read-only, bounded technical and PIT-finance evidence for shared research.

This consumer never collects or repairs source data.  Price history describes
the version acquired now; it does not certify that version was known intraday.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, time as day_time
from decimal import Decimal
import hashlib
import json
import math
import time
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, event, text

from biz.review.technical_facts import compute_technical_facts
from server.common.config import get_kline_mysql_url
from server.common.engine_factory import create_pooled_engine
from server.common.pit_facts import load_finance_history_facts

TZ = ZoneInfo("Asia/Shanghai")
MAX_STOCKS, MAX_INDICES, HISTORY_SESSIONS = 12, 6, 121
TOTAL_SECONDS, FINANCE_SECONDS, QUERY_MS = 90, 30, 15000
INTRADAY_TOTAL_SECONDS = 30
FINANCE_ROW_CAP, FINANCE_QUERY_CAP, FINANCE_BYTE_CAP = 12000, 160, 48 * 1024 * 1024
FINANCE_FIELDS = (
    "basic_eps", "diluted_eps", "non_gaap_eps", "net_asset_ps", "cap_reserve_ps", "undist_profit_ps",
    "oper_cf_ps", "total_rev", "gross_profit", "net_profit_attr_sh", "non_gaap_net_profit",
    "total_rev_yoy_gr", "net_profit_yoy_gr", "non_gaap_net_profit_yoy_gr", "total_rev_qoq_gr", "net_profit_qoq_gr",
    "roe_wtd", "roe_non_gaap_wtd", "roa_wtd", "gross_margin", "net_margin",
    "asset_liab_ratio", "curr_ratio", "quick_ratio", "cash_flow_ratio",
)
FINANCE_METADATA = (
    "finance_report_date", "finance_published_at", "finance_known_at",
    "finance_publication_precision", "finance_publication_time_verified",
    "finance_publication_reason", "finance_observed_available_at",
    "finance_fact_cutoff_at", "finance_decision_at", "finance_revision_id",
    "finance_content_hash", "finance_source", "finance_pit_verified",
)


class EvidenceReadError(RuntimeError):
    pass


def _stamp(value) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=TZ) if parsed.tzinfo is None else parsed.astimezone(TZ)


def _day(value) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)[:10]).isoformat()


def _number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _safe(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return _number(value)
    if isinstance(value, dict):
        return {str(key): _safe(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _hash(value):
    return hashlib.sha256(json.dumps(_safe(value), ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _reason(exc):
    # Never expose a DBAPI exception string: it can contain a connection URL.
    return str(exc) if isinstance(exc, EvidenceReadError) else type(exc).__name__


class _Reader:
    def __init__(self, engine, deadline):
        self.engine, self.deadline = engine, deadline
        self.finance_deadline = deadline
        self.finance_queries = self.finance_rows = self.finance_bytes = 0
        self.in_finance = False
        self.failure = None
        self.progress = None

    def check(self, stage, *, reserve=0):
        if time.monotonic() + reserve >= min(self.deadline, self.finance_deadline if self.in_finance else self.deadline):
            self.failure = f"{stage}_deadline_exceeded"
            raise EvidenceReadError(self.failure)

    def query(self, stage, sql, params=None, expanding=(), cap=1):
        # Never start a 15-second statement with less than its budget left.
        self.check(stage, reserve=QUERY_MS / 1000)
        statement = text(sql)
        if expanding:
            statement = statement.bindparams(*(bindparam(key, expanding=True) for key in expanding))
        with self.engine.connect() as connection:
            records = [dict(row) for row in connection.execute(statement, params or {}).mappings().fetchmany(cap + 1)]
        self.check(stage)
        if len(records) > cap:
            raise EvidenceReadError(stage + "_row_cap_exceeded")
        if self.progress is not None:
            self.progress({"stage": stage, "status": "RUNNING", "rows": len(records)})
        return records

    def finance(self, codes, asof, target):
        self.in_finance = True
        self.finance_deadline = min(self.deadline, time.monotonic() + FINANCE_SECONDS)
        try:
            self.check("finance")
            batch = load_finance_history_facts(self.engine, codes=codes, decision_at=asof,
                                              fact_cutoff_at=asof, as_of_date=target, limit_per_code=8)
            self.check("finance")
            if self.failure:
                raise EvidenceReadError(self.failure)
            return batch
        finally:
            self.in_finance = False


def _dedicated_reader(url, deadline):
    engine = create_pooled_engine(url, connect_args={"connect_timeout": 8, "read_timeout": 15, "write_timeout": 15},
                                  pool_size=1, max_overflow=0, pool_timeout=8, pool_pre_ping=False)
    reader = _Reader(engine, deadline)

    @event.listens_for(engine, "connect")
    def configure(dbapi, _record):
        cursor = dbapi.cursor()
        try:
            cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={QUERY_MS}")
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
        finally:
            cursor.close()

    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def bound(_connection, _cursor, statement, parameters, _context, _many):
        reader.check("finance" if reader.in_finance else "source_read", reserve=QUERY_MS / 1000)
        command = statement.lstrip().split(None, 1)[0].upper()
        if command not in {"SELECT", "SHOW", "WITH", "EXPLAIN", "DESCRIBE"}:
            raise EvidenceReadError("non_read_statement_rejected")
        if reader.in_finance:
            reader.finance_queries += 1
            if reader.finance_queries > FINANCE_QUERY_CAP:
                reader.failure = "finance_query_cap_exceeded"
                raise EvidenceReadError(reader.failure)
        return statement, parameters

    @event.listens_for(engine, "after_cursor_execute")
    def result_bound(_connection, cursor, _statement, _parameters, _context, _many):
        if not reader.in_finance:
            return
        reader.check("finance")
        reader.finance_rows += max(0, cursor.rowcount)
        # PyMySQL's buffered cursor makes bytes/row bounds visible before the
        # PIT decoder materializes revision payloads.  Values are never logged.
        for row in getattr(cursor, "_rows", ()) or ():
            reader.finance_bytes += sum(len(item.encode("utf-8")) if isinstance(item, str)
                                        else len(item) if isinstance(item, bytes) else 0 for item in row)
        if reader.finance_rows > FINANCE_ROW_CAP or reader.finance_bytes > FINANCE_BYTE_CAP:
            reader.failure = "finance_result_budget_exceeded"
            raise EvidenceReadError(reader.failure)
        if reader.progress is not None:
            reader.progress({"stage": "finance_history_read", "status": "RUNNING", "completed_queries": reader.finance_queries})

    return reader


@contextmanager
def _sources(engine, deadline):
    # The supplied engine owns the primary route; K-line has its own configured
    # route.  Both pools are private so socket/session bounds cannot alter jobs.
    primary = kline = None
    try:
        primary = _dedicated_reader(engine.url, deadline)
        kline = _dedicated_reader(get_kline_mysql_url(), deadline)
        yield primary, kline
    finally:
        for reader in (primary, kline):
            if reader is not None:
                reader.engine.dispose()


def _scope(context, report_kind):
    observation = context.get("intraday_observation") or {}
    stocks = observation.get("key_stocks", []) if report_kind == "intraday" else (context.get("theme_evidence") or {}).get("stocks", [])
    unique = {}
    for row in stocks:
        code = str(row.get("stock_code") or "")
        if len(code) == 6 and code.isdigit() and code not in unique:
            unique[code] = row
    stock_scope = list(unique.values())[:MAX_STOCKS]
    if report_kind == "intraday":
        indices = (observation.get("market") or {}).get("indices") or observation.get("benchmarks") or []
    else:
        indices = (context.get("indices") or {}).get("rows") or []
    if isinstance(indices, dict):
        indices = [dict(row, index_code=code) if isinstance(row, dict) else {"index_code": code} for code, row in indices.items()]
    index_scope, seen = [], set()
    for row in indices:
        code = str(row.get("index_code") or "")
        if len(code) == 6 and code.isdigit() and code not in seen:
            index_scope.append({**row, "index_code": code})
            seen.add(code)
    index_scope = index_scope[:MAX_INDICES]
    return stock_scope, index_scope


def _calendar(reader, target, intraday):
    operator = "<" if intraday else "<="
    rows = reader.query("technical_calendar", f"SELECT /*+ MAX_EXECUTION_TIME({QUERY_MS}) */ trade_date "
                        f"FROM si_trade_calendar WHERE trade_status=1 AND trade_date{operator}:target "
                        f"ORDER BY trade_date DESC LIMIT {HISTORY_SESSIONS}", {"target": target}, cap=HISTORY_SESSIONS)
    days = [_day(row["trade_date"]) for row in rows]
    if not days or len(set(days)) != len(days) or days != sorted(days, reverse=True):
        raise EvidenceReadError("technical_calendar_invalid")
    if (intraday and days[0] >= target) or (not intraday and days[0] != target):
        raise EvidenceReadError("technical_calendar_target_missing")
    return list(reversed(days))


def _bars(reader, codes, dates, *, index=False):
    if not codes:
        return [], {"state": "COMPLETE", "total_units": 0, "completed_units": 0}
    table, key = ("sm_index_kline", "index_code") if index else ("sm_stock_kline", "stock_code")
    columns = reader.query("technical_schema", "SELECT /*+ MAX_EXECUTION_TIME(15000) */ COLUMN_NAME "
                           "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table",
                           {"table": table}, cap=80)
    available = {str(row["COLUMN_NAME"]) for row in columns}
    required = {key, "trade_date", "open", "close", "high", "low", "k_type"}
    if not required <= available:
        raise EvidenceReadError(table + "_required_columns_missing")
    if not {"adjust_type", "pre_close"} <= available:
        return [], {"state": "COMPLETE", "total_units": 0, "completed_units": 0, "rows": 0,
                    "reason": "unadjusted_or_previous_close_not_proven", "available_columns": sorted(available)}
    names = [key, "short_name", "trade_date", "adjust_type", "open", "high", "low", "close", "pre_close",
             "volume", "amount", "quality_status", "data_source", "received_at", "etl_sync_at"]
    projection = ",".join("`" + field + "`" for field in names if field in available)
    # Live EXPLAIN/read receipts proved five-date batches stay bounded, whereas
    # all-stock 121-date reads timed out.  Reuse that measured partition shape.
    chunks = [dates] if index else [dates[max(0, len(dates) - offset - 5):len(dates) - offset]
                                   for offset in range(0, len(dates), 5)]
    output, completed = [], 0
    receipt = {"state": "RUNNING", "total_units": len(chunks), "completed_units": 0, "table": table,
               "available_columns": sorted(available), "rows": 0, "retry_count": 0}
    for batch in chunks:
        cap = len(codes) * len(batch)
        hint = "" if index else " FORCE INDEX (uk_sm_stock_kline_business)"
        sql = (f"SELECT /*+ MAX_EXECUTION_TIME({QUERY_MS}) */ {projection} FROM {table}{hint} "
               f"WHERE k_type=1" + (" AND adjust_type=0" if "adjust_type" in available else "") +
               f" AND {key} IN :codes AND trade_date IN :dates ORDER BY {key},trade_date LIMIT {cap + 1}")
        try:
            parameters = {"codes": codes, "dates": batch}
            if completed == 0:
                # Preserve the measured access path, and fail this source card
                # if its schema no longer supports it; do not guess another
                # index or retry an expensive query after a timeout.
                reader.query("technical_query_plan", "EXPLAIN " + sql, parameters, ("codes", "dates"), cap=10)
            rows = reader.query("technical_" + table, sql, parameters, ("codes", "dates"), cap=cap)
        except Exception as exc:
            receipt.update(state="FAILED", reason=_reason(exc), failed_dates=batch,
                           completed_units=completed, rows=len(output))
            # Keep completed date partitions for independent MA availability;
            # the calendar proves which samples are missing.  Never retry.
            return output, receipt
        if any(str(row.get(key)) not in codes or _day(row["trade_date"]) not in batch for row in rows):
            raise EvidenceReadError(table + "_out_of_scope_rows")
        output.extend(rows)
        completed += 1
        receipt.update(completed_units=completed, rows=len(output), last_progress_at=datetime.now(TZ).isoformat())
    receipt["state"] = "COMPLETE"
    return output, receipt


def _price_match(rows, frozen, target):
    if _day(frozen.get("trade_date") or target) != target:
        return None
    baseline = frozen.get("ohlc") or frozen
    matching = [row for row in rows if _day(row["trade_date"]) == target]
    if len(matching) != 1:
        return None
    compared = 0
    for field in ("open", "high", "low", "close", "pre_close"):
        old, new = _number(baseline.get(field)), _number(matching[0].get(field))
        if old is not None and new is not None:
            compared += 1
            if not math.isclose(old, new, rel_tol=1e-8, abs_tol=1e-8):
                return False
    return True if compared else None


def _technical_group(rows, scope, key, days, asof, *, compare_frozen):
    result = []
    for entry in scope:
        code = entry if isinstance(entry, str) else str(entry[key])
        selected = sorted([row for row in rows if str(row.get(key)) == code], key=lambda row: _day(row["trade_date"]))
        price_match = _price_match(selected, entry, days[-1]) if compare_frozen and isinstance(entry, dict) else None
        conflict = price_match is False
        # An observed provider label is not a proof of volume units or of the
        # entire window's amount/volume price consistency.  No receipt is forged.
        values = compute_technical_facts([] if conflict else selected, trade_date=days[-1], expected_dates=days)
        if conflict:
            values["issues"].append({"code": "frozen_target_price_revision_conflict", "severity": "ERROR"})
        result.append({key: code, "short_name": entry.get("short_name") if isinstance(entry, dict) else None,
                       "source": "sm_stock_kline" if key == "stock_code" else "sm_index_kline",
                       "source_snapshot_sha256": _hash(selected), "as_of_at": asof.isoformat(),
                       "price_revision_matches_frozen": price_match,
                       "price_revision_comparison": "CONFLICT" if conflict else "MATCH" if price_match is True else "NOT_COMPARED",
                       **values})
    return result


def _finance_projection(batch, codes, asof):
    output = []
    for code in codes:
        source_status, reason = batch.status_for(code), batch.reason_for(code)
        supplied = batch.facts.get(code) or []
        if not isinstance(supplied, list):
            supplied = []
            reason = "finance_history_shape_invalid"
        history, invalid = [], False
        for row in supplied[:8]:
            try:
                known = _stamp(row.get("finance_known_at"))
                if not row.get("finance_pit_verified") or known > asof:
                    raise ValueError("not_known")
                published = row.get("finance_published_at")
                if row.get("finance_publication_time_verified"):
                    if _stamp(published) > asof:
                        raise ValueError("not_published")
                elif not published or _day(published) > asof.date().isoformat():
                    raise ValueError("date_only_not_published")
                if _day(row["finance_report_date"]) > asof.date().isoformat():
                    raise ValueError("future_report")
            except (ValueError, TypeError, KeyError):
                invalid = True
                break
            history.append({**{key: _safe(row.get(key)) for key in FINANCE_METADATA},
                            "metrics": {key: number for key in FINANCE_FIELDS if (number := _number(row.get(key))) is not None}})
        if invalid:
            history, reason = [], "finance_publication_or_knowledge_after_asof_or_unverified"
        valid = source_status == "AVAILABLE" and bool(history) and not invalid
        if not valid:
            history = []
        output.append({"stock_code": code, "status": "OBSERVED" if valid else "UNKNOWN", "source_status": source_status,
                       "reason": reason or ("" if valid else "finance_no_usable_reports"),
                       "latest": history[0] if history else None, "history": history,
                       "coverage": _safe(batch.coverage_by_code.get(code) or {})})
    return output


def enrich_context(engine, digest, context, *, report_kind, now=None, progress=None):
    """Enrich an immutable report snapshot from existing public stores only.

    Source errors degrade only their evidence cards.  The report engine remains
    responsible for refusing claims which require those unavailable measurements.
    """
    if report_kind not in {"review", "briefing", "intraday"}:
        raise ValueError("invalid_report_kind")
    result = deepcopy(context)
    acquired = _stamp(now or datetime.now(TZ))
    target = _day(digest["review_date"])
    # No fallback to acquisition time: a missing as-of must never authorize
    # future finance disclosures or a current-session daily closing bar.
    asof = _stamp(context["cutoff_at"])
    stocks, indices = _scope(context, report_kind)
    codes = [str(row["stock_code"]) for row in stocks]
    technical = {"status": "UNKNOWN", "baseline_trade_date": None, "as_of_at": asof.isoformat(),
                 "acquired_at": acquired.isoformat(), "price_basis": "unadjusted_closed_daily_bars",
                 "availability_note": "本次读取的已闭市日线版本；不证明这些历史值在当时盘中已知，不能作为当时技术信号已经触发的证据",
                 "stocks": [], "indices": [], "coverage": {"stocks_requested": len(codes), "indices_requested": len(indices)}, "issues": []}
    fundamental = {"status": "UNKNOWN", "as_of_at": asof.isoformat(), "acquired_at": acquired.isoformat(),
                   "source": "st_pit_finance_revision", "manifest_hash": "", "stocks": [],
                   "coverage": {"expected_count": len(codes), "observed_count": 0}, "issues": []}
    result["technical_facts"], result["fundamental_facts"] = technical, fundamental
    deadline = time.monotonic() + (INTRADAY_TOTAL_SECONDS if report_kind == "intraday" else TOTAL_SECONDS)
    try:
        with _sources(engine, deadline) as (primary, kline):
            primary.progress = kline.progress = progress
            try:
                if report_kind != "intraday" and asof < datetime.combine(date.fromisoformat(target), day_time(15), TZ):
                    raise EvidenceReadError("target_session_not_closed_at_asof")
                days = _calendar(primary, target, report_kind == "intraday")
                technical["baseline_trade_date"] = days[-1]
                technical["coverage"]["expected_sessions"] = len(days)
                for scope, key, index in ((stocks, "stock_code", False), (indices, "index_code", True)):
                    try:
                        rows, receipt = _bars(kline, [str(row[key]) if isinstance(row, dict) else row for row in scope], days, index=index)
                        field = "indices" if index else "stocks"
                        technical[field] = _technical_group(rows, scope, key, days, asof,
                                                            compare_frozen=report_kind != "intraday")
                        technical["coverage"][field + "_read"] = receipt
                    except Exception as exc:
                        technical["issues"].append({"stage": "index_history" if index else "stock_history", "reason": _reason(exc)})
                technical["status"] = "OBSERVED" if any(row["status"] == "OBSERVED" for row in technical["stocks"] + technical["indices"]) else "UNKNOWN"
            except Exception as exc:
                technical["issues"].append({"stage": "technical_calendar", "reason": _reason(exc)})
            if codes:
                try:
                    batch = primary.finance(codes, asof, target)
                    fundamental["manifest_hash"] = batch.manifest_hash
                    fundamental["stocks"] = _finance_projection(batch, codes, asof)
                    count = sum(row["status"] == "OBSERVED" for row in fundamental["stocks"])
                    fundamental["coverage"]["observed_count"] = count
                    fundamental["status"] = "OBSERVED" if count else "UNKNOWN"
                except Exception as exc:
                    fundamental["issues"].append({"stage": "finance_history", "reason": _reason(exc)})
                    fundamental["stocks"] = [{"stock_code": code, "status": "UNKNOWN", "reason": "finance_read_unproven", "latest": None, "history": [], "coverage": {}} for code in codes]
    except Exception as exc:
        technical["issues"].append({"stage": "source_connection", "reason": _reason(exc)})
        fundamental["issues"].append({"stage": "source_connection", "reason": _reason(exc)})
    return result
