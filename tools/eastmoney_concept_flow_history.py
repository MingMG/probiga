"""Dated Eastmoney board fund flows for an already published daily K-line roster."""

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import sqlite3
import re
from time import monotonic

from sqlalchemy import text

from server.common.kline_data import get_kline_engine
from server.common.qmt_attestation_contract import canonical_digest
from server.common.sector_heatmap_store import read_concept_names
from tools.sync_eastmoney_concept_market import (
    DataBlocked,
    EASTMONEY_TOKEN,
    EastmoneyConceptProvider,
    _code_set_hash,
    daily_content_hash,
)


PROVIDER = "eastmoney.push2his"
SOURCE_URL = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
ROSTER_SOURCE = "sm_concept_east_kline"
HISTORY_WINDOW = 100
MAX_ROSTER_COUNT = 1_500
MIN_ROSTER_COUNT = 100
HISTORY_DEADLINE_SECONDS = 180
MAX_MISSING_DETAILS = 64
MISSING_REASONS = frozenset({
    "HISTORY_DEADLINE_EXCEEDED", "HISTORY_REQUEST_FAILED", "HISTORY_IDENTITY_UNAVAILABLE",
    "HISTORY_ROWS_INVALID", "EXACT_FLOW_ROW_MISSING", "HISTORY_WINDOW_UNPROVEN",
    "EXACT_FLOW_ROW_INVALID",
})
_WORKERS = 4
_GROUP_SIZE = 32
_CODE = re.compile(r"BK[0-9]{4,8}")
_FIELDS = (
    "main_net_inflow", "sm_net_inflow", "mid_net_inflow", "lg_net_inflow",
    "max_net_inflow", "main_net_inflow_rate", "sm_net_inflow_rate",
    "mid_net_inflow_rate", "lg_net_inflow_rate", "max_net_inflow_rate",
    "close", "change_pct",
)
FLOW_COLUMNS = tuple(field for field in _FIELDS if field != "close")
_RECEIPT_SEARCH_LIMIT = 32


class _HistoryProvider(EastmoneyConceptProvider):
    def fetch(self, code):
        return self._request_json(SOURCE_URL, {
            "secid": f"90.{code}",
            "lmt": str(HISTORY_WINDOW),
            "klt": "101",
            "fields1": "f1,f2,f3,f7",
            "fields2": ",".join(f"f{i}" for i in range(51, 66)),
            "ut": EASTMONEY_TOKEN,
        })


def _market_decimal(value):
    number = Decimal(str(value))
    if not number.is_finite() or abs(number) >= Decimal(10**18):
        raise ValueError("invalid Eastmoney history number")
    return number


def _roster_receipt(engine, source_date, now, codes, content_digest, *, history_id=None):
    from server.common.scheduler_validation import (
        _eastmoney_concept_market_output_status,
        _eastmoney_concept_market_payload,
    )

    end = date.fromisoformat(source_date) + timedelta(days=4)
    history_filter = "AND id=:history_id" if history_id is not None else ""
    with engine.connect() as connection:
        results = connection.execute(text(f"""
            SELECT /*+ MAX_EXECUTION_TIME(3000) */ id,run_uid,output
            FROM st_scheduled_task_history
            WHERE task_type='eastmoney_concept_kline' AND status='success'
              AND exit_code=0 AND run_at>=:target AND run_at<:end AND finished_at<=:observed
              {history_filter}
            ORDER BY run_at DESC,id DESC LIMIT :row_limit
        """), {"target": source_date, "end": end.isoformat(), "observed": now,
                "history_id": history_id, "row_limit": _RECEIPT_SEARCH_LIMIT}).mappings().all()
    code_digest = _code_set_hash(codes)
    for row in results:
        output = row["output"]
        receipt = _eastmoney_concept_market_payload(output)
        if (receipt is None or receipt.get("target_trade_date") != source_date
                or _eastmoney_concept_market_output_status(
                    "eastmoney_concept_kline", output, return_code=0
                ) != "success"):
            continue
        dataset = receipt["dataset_results"]["kline"]
        if (dataset.get("code_set_sha256") == code_digest
                and dataset.get("content_sha256") == content_digest
                and dataset.get("row_count") == len(codes)
                and re.fullmatch(r"[0-9a-f]{32}", str(row["run_uid"] or ""))):
            return {"history_id": row["id"], "run_uid": row["run_uid"],
                    "receipt_id": receipt["receipt_id"], "result_hash": receipt["result_sha256"]}
    raise RuntimeError(f"DATA_BLOCKED: no exact Eastmoney K-line roster receipt matches {source_date}")


def published_roster(primary, source_date, now, *, engine=None, history_id=None):
    history = engine if engine is not None else get_kline_engine()
    with history.connect() as connection:
        rows = connection.execute(text("""
            SELECT /*+ MAX_EXECUTION_TIME(3000) */
                index_code,trade_date,trade_time,etl_sync_at,
                open,close,high,low,volume,amount,`change`,change_pct,k_type
            FROM sm_concept_east_kline
            WHERE trade_date=:target AND k_type=1 AND etl_sync_at<=:observed
            ORDER BY index_code,trade_time LIMIT :row_limit
        """), {"target": source_date, "observed": now,
                "row_limit": MAX_ROSTER_COUNT + 1}).mappings().all()
    codes = [str(row["index_code"] or "") for row in rows]
    try:
        midnight = datetime.combine(date.fromisoformat(source_date), time.min)
        valid_dates = all(str(row["trade_date"])[:10] == source_date
                          and datetime.fromisoformat(str(row["trade_time"])) == midnight for row in rows)
    except ValueError:
        valid_dates = False
    if (not valid_dates or not MIN_ROSTER_COUNT <= len(codes) <= MAX_ROSTER_COUNT
            or len(set(codes)) != len(codes) or any(not _CODE.fullmatch(code) for code in codes)):
        raise RuntimeError(
            f"DATA_BLOCKED: historical Eastmoney K-line roster is unavailable for {source_date}"
        )
    content_digest = daily_content_hash(rows)
    receipt = _roster_receipt(primary, source_date, now, codes, content_digest, history_id=history_id)
    return codes, content_digest, receipt


def _exact_names(source_date, now, codes):
    try:
        rows = read_concept_names(source_date, now=now)
    except (OSError, sqlite3.Error, ValueError):
        return {}
    names = {row["index_code"]: row["index_name"] for row in rows}
    return names if set(names) == set(codes) else {}


def _capture_board(provider, code, source_date, exact_names, deadline):
    if monotonic() >= deadline:
        return None, {"code": code, "reason": "HISTORY_DEADLINE_EXCEEDED"}
    try:
        payload = provider.fetch(code)
    except DataBlocked:
        return None, {"code": code, "reason": "HISTORY_REQUEST_FAILED"}

    commitment = {"code": code, "response_hash": canonical_digest(payload)}
    data = payload.get("data")
    if (payload.get("rc") != 0 or not isinstance(data, dict)
            or data.get("code") != code or str(data.get("market")) != "90"):
        return None, {**commitment, "reason": "HISTORY_IDENTITY_UNAVAILABLE"}
    lines = data.get("klines")
    if (not isinstance(lines, list) or len(lines) > HISTORY_WINDOW
            or any(not isinstance(line, str) or not 0 < len(line) <= 512 for line in lines)):
        return None, {**commitment, "reason": "HISTORY_ROWS_INVALID"}
    parts = [line.split(",") for line in lines]
    try:
        days = [date.fromisoformat(row[0]).isoformat() for row in parts]
        if (any(len(row) != 15 or row[0] != day for row, day in zip(parts, days))
                or days != sorted(set(days))):
            raise ValueError("invalid Eastmoney history dates")
    except (TypeError, ValueError):
        return None, {**commitment, "reason": "HISTORY_ROWS_INVALID"}
    if source_date not in days:
        reason = "EXACT_FLOW_ROW_MISSING" if days and days[0] <= source_date <= days[-1] else "HISTORY_WINDOW_UNPROVEN"
        return None, {**commitment, "reason": reason}

    native = parts[days.index(source_date)]
    name = exact_names.get(code) or str(data.get("name") or "").strip()
    try:
        values = {field: _market_decimal(value) for field, value in zip(_FIELDS, native[1:])}
        if not 0 < len(name) <= 128 or not name.isprintable() or values["close"] <= 0:
            raise ValueError("invalid board name or close")
    except (InvalidOperation, ValueError):
        return None, {**commitment, "reason": "EXACT_FLOW_ROW_INVALID"}

    # The historical chart stores rounded native amounts. The existing database
    # contract derives main flow from the published large and super-large values.
    values["main_net_inflow"] = values["lg_net_inflow"] + values["max_net_inflow"]
    values["main_net_inflow_rate"] = values["lg_net_inflow_rate"] + values["max_net_inflow_rate"]
    row = {"index_code": code, "index_name": name, "stock_name": "", "stock_code": "",
           **{field: float(values[field]) for field in FLOW_COLUMNS}}
    return row, {**commitment, "row_hash": canonical_digest(native)}


def capture_history(primary, source_date, now):
    codes, roster_content_hash, receipt = published_roster(primary, source_date, now)
    names = _exact_names(source_date, now, codes)
    provider = _HistoryProvider(retries=1, timeout=6)
    started = monotonic()
    deadline = started + HISTORY_DEADLINE_SECONDS
    rows, evidence = [], []
    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        for offset in range(0, len(codes), _GROUP_SIZE):
            group = codes[offset:offset + _GROUP_SIZE]
            for row, proof in pool.map(
                lambda code: _capture_board(provider, code, source_date, names, deadline), group
            ):
                evidence.append(proof)
                if row is not None:
                    rows.append(row)
            print(f"Eastmoney history {source_date} {len(evidence)}/{len(codes)} "
                  f"boards checked, {len(rows)} verified, elapsed={monotonic()-started:.1f}s", flush=True)

    missing = [item for item in evidence if "reason" in item]
    native = {
        "source_url": SOURCE_URL,
        "roster_source": ROSTER_SOURCE,
        "roster_count": len(codes),
        "roster_code_set_hash": canonical_digest(codes),
        "roster_content_hash": roster_content_hash,
        "roster_receipt": receipt,
        "available_count": len(rows),
        "missing_count": len(missing),
        "missing": missing[:MAX_MISSING_DETAILS],
        "missing_hash": canonical_digest(missing),
        "source_rows_hash": canonical_digest(evidence),
        "name_source": "exact_date_concept_names" if names else "eastmoney_history_response",
        "top_stock_available": False,
    }
    return rows, native


def content_hash(rows):
    def stored_number(value):
        rounded = _market_decimal(value).quantize(Decimal("0.01"))
        # MySQL DECIMAL stores one zero value, regardless of the provider sign.
        return format(rounded.copy_abs() if rounded.is_zero() else rounded, "f")

    return canonical_digest([{
        "index_code": row["index_code"], "index_name": row["index_name"],
        **{field: stored_number(row[field]) for field in FLOW_COLUMNS},
    } for row in rows])
