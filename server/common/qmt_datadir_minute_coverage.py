"""Exact native minute matrices bound to the immutable daily trade scope."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from hashlib import sha256
import json
import socket
import threading
import time
import zlib
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_daily_coverage import load_daily_manifest
from server.common.qmt_index_minute_grid import CORE_GRID
from server.common.qmt_datadir_progress import MinuteImportProgress
from server.common.auxiliary_runtime_schema import QMT_DATADIR_STAGE_INDEX_NAME

GRID_HASH = canonical_digest(list(CORE_GRID))
SCHEMA = "probiga.qmt-datadir-minute-coverage.v1"
_MATRIX_FIELDS = ("stock_code", "trade_time", "trade_date", "price", "change", "change_pct",
    "volume", "amount", "qmt_code", "data_source", "source_time", "batch_id", "data_version",
    "quality_status", "permission_status")
_NUMERIC_FIELDS = ("price", "change", "change_pct", "volume", "amount")
_MAX_GROUP_JSON_BYTES = 1024 * 1024


def trade_scope(engine, day: str, *, manifest_hash: str = "", decision_known_at=None) -> dict:
    cutoff = decision_known_at or datetime.now(ZoneInfo("Asia/Shanghai"))
    proof = load_daily_manifest(engine, day, decision_known_at=cutoff,
        manifest_hash=manifest_hash)
    if proof is None:
        raise ValueError("DATA_BLOCKED: exact daily manifest is required before minute publication")
    codes = tuple(item["stock_code"] for item in proof["entities"]
        if item["classification"] == "TRADED"
        and (Decimal(item["evidence"]["volume"]) > 0 or Decimal(item["evidence"]["amount"]) > 0))
    if not codes:
        raise ValueError("DATA_BLOCKED: exact daily trade scope is empty")
    return dict(full_market_manifest_hash=proof["manifest_hash"], codes=codes,
        daily_close_by_code={item["stock_code"]: item["evidence"]["close"]
            for item in proof["entities"] if item["stock_code"] in codes},
        expected_traded_code_set_sha256=canonical_digest(list(codes)),
        eligible_code_count=len(codes))


def validate_code_scope(entry: dict, scope: dict) -> None:
    expected, actual = set(scope["codes"]), set(entry["codes"])
    if actual != expected or len(entry["codes"]) != len(expected):
        raise ValueError("exact minute trade scope differs: missing="
            + ",".join(sorted(expected - actual)[:20]) + "; unexpected="
            + ",".join(sorted(actual - expected)[:20]))
    if (set(entry["code_rows"]) != expected
            or any(int(count) != len(CORE_GRID) for count in entry["code_rows"].values())
            or int(entry["rows"]) != len(expected) * len(CORE_GRID)):
        raise ValueError("exact native minute grid is incomplete")


def matrix_consume_budget(scope: dict) -> float:
    # The bounded production sample sustained 5,000 rows/s including TLS,
    # decompression and both hashes. Allow 2,000 rows/s plus startup slack.
    return min(900.0, max(180.0, 60.0 + len(scope["codes"]) * len(CORE_GRID) / 2000.0))


def validate_matrix(rows, *, day: str, scope: dict, deadline_seconds: float | None = None,
                    progress=None, query_started=None, include_business_fingerprint: bool = False) -> dict:
    """Consume ordered rows in bounded memory; hash matches canonical_rows JSON."""
    from server.common.qmt_datadir_result import _normalize

    expected, grid = set(scope["codes"]), set(CORE_GRID)
    target_day = date.fromisoformat(day)
    grid_seconds = {int(moment[:2]) * 3600 + int(moment[3:5]) * 60 + int(moment[6:]): moment
                    for moment in CORE_GRID}
    symbols = {code: code + (".SH" if code.startswith("6") else ".BJ"
               if code.startswith(("4", "8", "9")) else ".SZ") for code in expected}
    close_anchors = {code: Decimal(str(scope["daily_close_by_code"][code])) for code in expected}
    encode = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode
    cached_normalize = lru_cache(maxsize=4096, typed=True)(_normalize)

    def normalize(value):
        # Native timestamps, dates and repeated six-decimal values recur across
        # securities. Keep normalization bounded, without changing its bytes.
        # Equal aware datetimes may have different textual offsets, so bypass
        # the equality-based cache for those values.
        if isinstance(value, Decimal):
            # Decimal equality equates signed zeros; canonical JSON does not.
            if value.is_zero() and value.is_signed():
                return _normalize(value)
            return cached_normalize(value)
        if isinstance(value, datetime):
            return _normalize(value) if value.tzinfo is not None else cached_normalize(value)
        if isinstance(value, date):
            return cached_normalize(value)
        return value

    seen_codes, current_times = set(), set()
    current_code, count = None, 0
    digest = sha256(b"[")
    business_digest = sha256(b"[") if include_business_fingerprint else None
    business_fields = ("stock_code", "trade_time", "trade_date", "price", "change",
                       "change_pct", "volume", "amount", "qmt_code")
    consume_started = time.monotonic()
    deadline_seconds = matrix_consume_budget(scope) if deadline_seconds is None else deadline_seconds
    deadline = consume_started + deadline_seconds
    for source in rows:
        if progress is not None and count == 0:
            progress.emit("execute_firstrow", status="COMPLETE",
                phase_seconds=round(time.monotonic() - (query_started or consume_started), 6))
            progress.emit("consume", budget_seconds=deadline_seconds)
        if time.monotonic() > deadline:
            raise TimeoutError("minute_matrix_readback: total deadline exceeded")
        row = dict(source)
        code = str(row["stock_code"])
        stamp = row["trade_time"]
        stamp = stamp if isinstance(stamp, datetime) else datetime.fromisoformat(str(stamp))
        source_time = row.get("source_time")
        source_time = source_time if isinstance(source_time, datetime) else datetime.fromisoformat(str(source_time))
        trade_day = row["trade_date"]
        trade_day = trade_day.date() if isinstance(trade_day, datetime) else trade_day
        trade_day = trade_day if isinstance(trade_day, date) else date.fromisoformat(str(trade_day)[:10])
        moment = grid_seconds.get(stamp.hour * 3600 + stamp.minute * 60 + stamp.second)
        if (code not in expected or trade_day != target_day or stamp.date() != target_day or moment is None
                or stamp.microsecond or row["data_source"] != "qmt_datadir"
                or row.get("qmt_code") != symbols.get(code)
                or source_time != stamp
                or row.get("quality_status") != "VERIFIED"
                or row.get("permission_status") != "SUPPORTED"):
            raise ValueError("minute matrix security/date/source differs")
        if code != current_code:
            if current_code is not None and current_times != grid:
                raise ValueError("minute matrix native grid is incomplete")
            if code in seen_codes:
                raise ValueError("minute matrix security ordering differs")
            seen_codes.add(code)
            current_code, current_times = code, set()
        if moment in current_times:
            raise ValueError("minute matrix has a duplicate native timestamp")
        current_times.add(moment)
        numbers = {field: value if isinstance(value, Decimal) else Decimal(str(value))
                   for field in _NUMERIC_FIELDS for value in (row[field],)}
        for field in ("price", "volume", "amount"):
            number = numbers[field]
            if not number.is_finite() or number < 0 or (field == "price" and number == 0):
                raise ValueError("minute matrix native numeric field differs")
        if any(not numbers[field].is_finite() for field in ("change", "change_pct")):
            raise ValueError("minute matrix native change field differs")
        if moment == "15:00:00" and numbers["price"] != close_anchors[code]:
            raise ValueError("minute matrix closing price differs from exact daily anchor")
        if count:
            digest.update(b",")
            if business_digest is not None:
                business_digest.update(b",")
        normalized = {str(key): normalize(value) for key, value in row.items()}
        digest.update(encode(normalized).encode("utf-8"))
        if business_digest is not None:
            business = {key: normalized[key] if key not in numbers or isinstance(row[key], Decimal)
                        else normalize(numbers[key])
                for key in business_fields}
            business_digest.update(encode(business).encode("utf-8"))
        count += 1
        if progress is not None and count % 10000 == 0:
            progress.emit("consume", completed_units=count, current_unit=code,
                phase_seconds=round(time.monotonic() - consume_started, 6),
                rows_per_second=round(count / max(time.monotonic() - consume_started, 0.000001), 2),
                budget_seconds=deadline_seconds,
                budget_remaining_seconds=round(max(0.0, deadline-time.monotonic()), 6))
    if seen_codes != expected or current_times != grid or count != len(expected) * len(grid):
        raise ValueError("minute matrix exact security/time coverage is incomplete")
    digest.update(b"]")
    if progress is not None:
        progress.emit("consume", status="COMPLETE", completed_units=count,
            current_unit=current_code, phase_seconds=round(time.monotonic() - consume_started, 6),
            budget_seconds=deadline_seconds)
    fingerprint = dict(row_count=count, code_count=len(seen_codes), row_hash=digest.hexdigest(),
        full_market_manifest_hash=scope["full_market_manifest_hash"],
        expected_traded_code_set_sha256=scope["expected_traded_code_set_sha256"],
        eligible_code_count=len(expected), coverage=1.0,
        minute_grid_count=len(CORE_GRID), minute_grid_sha256=GRID_HASH, bad_grid_codes=0)
    if business_digest is not None:
        business_digest.update(b"]")
        fingerprint["business_row_hash"] = business_digest.hexdigest()
    return fingerprint


def _decode_matrix_group(group, *, expected_codes):
    """Decode the existing bounded MySQL COMPRESS wire format without loss."""
    code, count, packed = group.get("stock_code"), group.get("row_count"), group.get("packed_rows")
    if (code not in expected_codes or type(count) is not int or count != len(CORE_GRID)
            or not isinstance(packed, (bytes, bytearray, memoryview))):
        raise RuntimeError("minute_matrix_readback: compressed group scope/count/type differs")
    packed = bytes(packed)
    if not 4 < len(packed) <= _MAX_GROUP_JSON_BYTES + 65536:
        raise RuntimeError("minute_matrix_readback: compressed package size exceeds budget")
    declared = int.from_bytes(packed[:4], "little")
    if not 0 < declared <= _MAX_GROUP_JSON_BYTES:
        raise RuntimeError("minute_matrix_readback: declared JSON size exceeds budget")
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(packed[4:], _MAX_GROUP_JSON_BYTES + 1)
    except zlib.error as exc:
        raise RuntimeError("minute_matrix_readback: invalid compressed package") from exc
    if (len(raw) > _MAX_GROUP_JSON_BYTES or decoder.unconsumed_tail or not decoder.eof
            or len(raw) != declared or decoder.unused_data not in (b"", b".")
            or (decoder.unused_data == b"." and packed[-2] != 0x20)
            or (not decoder.unused_data and packed[-1] == 0x20)):
        raise RuntimeError("minute_matrix_readback: compressed length/stream boundary differs")

    def object_pairs(pairs):
        row = {}
        for key, value in pairs:
            if key in row:
                raise ValueError("duplicate JSON key")
            row[key] = value
        return row

    def invalid_constant(_value):
        raise ValueError("non-finite JSON constant")

    try:
        rows = json.loads(raw.decode("utf-8"), object_pairs_hook=object_pairs,
                          parse_constant=invalid_constant)
        if not isinstance(rows, list) or len(rows) != count:
            raise ValueError("JSON row count differs")
        for row in rows:
            if (not isinstance(row, dict) or set(row) != set(_MATRIX_FIELDS)
                    or row["stock_code"] != code
                    or any(value is not None and not isinstance(value, str) for value in row.values())):
                raise ValueError("JSON fields/security/types differ")
            for field in _NUMERIC_FIELDS:
                row[field] = Decimal(row[field]) if row[field] is not None else None
            for field in ("trade_time", "source_time"):
                row[field] = datetime.fromisoformat(row[field]) if row[field] is not None else None
            row["trade_date"] = date.fromisoformat(row["trade_date"]) if row["trade_date"] is not None else None
    except (ValueError, ArithmeticError, UnicodeError, RecursionError) as exc:
        raise RuntimeError("minute_matrix_readback: malformed compressed JSON") from exc
    return sorted(rows, key=lambda row: (row["stock_code"], row["trade_time"]))


def _read_matrix_query(connection, sql, params, *, deadline):
    """Bound execute and the complete reply by the remaining consume budget."""
    started = time.monotonic()
    remaining = deadline - started
    if remaining <= 0:
        raise TimeoutError("minute_matrix_readback: total deadline exceeded")
    call_limit = min(20.0, remaining)
    raw = getattr(connection, "connection", None)
    dbapi = getattr(raw, "dbapi_connection", None)
    original = {name: getattr(dbapi, name) for name in ("_read_timeout", "_write_timeout")
                if hasattr(dbapi, name)}
    call_limit = min(call_limit, remaining, *(value for value in original.values() if value is not None))
    for name in original:
        setattr(dbapi, name, call_limit)
    call_deadline = started + call_limit
    expired = threading.Event()
    sock = getattr(dbapi, "_sock", None)

    def abort_reply():
        expired.set()
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    watchdog = threading.Timer(max(0.0, call_deadline - time.monotonic()), abort_reply) if sock is not None else None
    if watchdog is not None:
        watchdog.daemon = True
        watchdog.start()
    try:
        # This is the same socket watchdog used by reference publication, but
        # it also covers fetch: a fragmented TLS reply must not reset the cap.
        sql = sql.replace("MAX_EXECUTION_TIME(20000)", f"MAX_EXECUTION_TIME({max(1, int(call_limit * 1000))})")
        rows = connection.execute(text(sql), params).mappings().all()
    except Exception as exc:
        if expired.is_set() or time.monotonic() >= call_deadline:
            raise TimeoutError("minute_matrix_readback: query/reply deadline exceeded") from exc
        if isinstance(exc, ValueError):
            raise RuntimeError("minute_matrix_readback: invalid transport response") from exc
        raise
    finally:
        if watchdog is not None:
            watchdog.cancel()
            watchdog.join()  # An old timer must never affect the next query.
        for name, value in original.items():
            setattr(dbapi, name, value)
    if expired.is_set() or time.monotonic() >= call_deadline:
        raise TimeoutError("minute_matrix_readback: query/reply deadline exceeded")
    return rows


def _mysql_matrix_rows(connection, day, *, scope, deadline, run_id=None):
    """Read each stock once in compressed batches over the existing indexes."""
    staged = run_id is not None
    table = "qmt_datadir_minute_stage" if staged else "sm_stock_minute"
    index = QMT_DATADIR_STAGE_INDEX_NAME if staged else "idx_smm_date_source_code_time"
    predicate = "trade_date=:day AND " + ("run_id=:run" if staged else "data_source='qmt_datadir'")
    params = {"day": day, "run": run_id}
    expected = set(scope["codes"])
    directory = _read_matrix_query(connection,
        f"SELECT /*+ MAX_EXECUTION_TIME(20000) */ stock_code,COUNT(*) AS row_count "
        f"FROM {table} FORCE INDEX (`{index}`) WHERE {predicate} "
        f"GROUP BY stock_code ORDER BY stock_code LIMIT {len(expected) + 1}",
        params, deadline=deadline)
    if (len(directory) != len(expected) or {row["stock_code"] for row in directory} != expected
            or any(row["row_count"] != len(CORE_GRID) for row in directory)):
        raise ValueError("minute_matrix_readback: exact security/count directory differs")
    ordered = sorted(expected)
    for offset in range(0, len(ordered), 50):
        if time.monotonic() >= deadline:
            raise TimeoutError("minute_matrix_readback: total deadline exceeded")
        batch = ordered[offset:offset + 50]
        batch_params = {**params, **{f"code_{i}": code for i, code in enumerate(batch)}}
        placeholders = ",".join(f":code_{i}" for i in range(len(batch)))
        projections = {field: f"`{field}`" for field in _MATRIX_FIELDS}
        if staged:
            projections.update(data_source="'qmt_datadir'", source_time="trade_time", batch_id=":run",
                data_version="'qmt_datadir_v1'", quality_status="'VERIFIED'", permission_status="'SUPPORTED'")
        fields = ",".join(f"'{field}'," + (f"CAST({projections[field]} AS CHAR)" if field in _NUMERIC_FIELDS
                         else projections[field]) for field in _MATRIX_FIELDS)
        groups = _read_matrix_query(connection,
            "SELECT /*+ MAX_EXECUTION_TIME(20000) */ stock_code,COUNT(*) AS row_count,"
            f"COMPRESS(CAST(JSON_ARRAYAGG(JSON_OBJECT({fields})) AS CHAR CHARACTER SET utf8mb4)) AS packed_rows "
            f"FROM {table} FORCE INDEX (`{index}`) WHERE {predicate} AND stock_code IN ({placeholders}) "
            "GROUP BY stock_code ORDER BY stock_code",
            batch_params, deadline=deadline)
        if (len(groups) != len(batch) or {group["stock_code"] for group in groups} != set(batch)):
            raise RuntimeError("minute_matrix_readback: compressed batch security groups differ")
        if time.monotonic() >= deadline:
            raise TimeoutError("minute_matrix_readback: total deadline exceeded")
        for group in sorted(groups, key=lambda group: group["stock_code"]):
            yield from _decode_matrix_group(group, expected_codes=set(batch))


def persisted_matrix(engine, day: str, *, scope: dict, run_id: str = "",
                     operation: str = "matrix_readback", include_business_fingerprint: bool = False) -> dict:
    progress = MinuteImportProgress(run_id=run_id,
        total_units=len(scope["codes"])*len(CORE_GRID), operation=operation, days=[day])
    consume_budget = matrix_consume_budget(scope)
    progress.emit("provider_probe")
    connection_scope = engine.connect() if callable(getattr(engine, "connect", None)) else nullcontext(engine)
    with connection_scope as connection:
        foreign = connection.execute(text(
            "SELECT /*+ MAX_EXECUTION_TIME(20000) */ data_source FROM sm_stock_minute "
            "WHERE trade_date=:day AND (data_source IS NULL OR data_source<>'qmt_datadir') LIMIT 1"
        ), {"day": day}).first()
        if foreign is not None:
            progress.emit("provider_probe", status="FAILED", error_type="UnexpectedProvider")
            raise ValueError("minute matrix contains an unexpected provider")
        query_started = time.monotonic()
        mysql = connection.dialect.name == "mysql"
        progress.emit("execute_firstrow", query_budget_seconds=20 if mysql else 120,
            consume_budget_seconds=consume_budget)
        try:
            rows = (_mysql_matrix_rows(connection, day, scope=scope, deadline=query_started + consume_budget)
                    if mysql else connection.execution_options(stream_results=True, max_row_buffer=2000).execute(text(
                "SELECT /*+ MAX_EXECUTION_TIME(120000) */ stock_code,trade_time,trade_date,price,"
                "`change`,change_pct,volume,amount,qmt_code,data_source,source_time,batch_id,"
                "data_version,quality_status,permission_status FROM sm_stock_minute "
                "WHERE trade_date=:day AND data_source='qmt_datadir' ORDER BY stock_code,trade_time"
            ), {"day": day}).mappings())
        except Exception as exc:
            progress.emit("execute_firstrow", status="FAILED", error_type=type(exc).__name__,
                phase_seconds=round(time.monotonic()-query_started, 6))
            raise
        try:
            return validate_matrix(rows, day=day, scope=scope, progress=progress,
                query_started=query_started, include_business_fingerprint=include_business_fingerprint,
                deadline_seconds=consume_budget)
        except Exception as exc:
            progress.emit("consume", status="FAILED", error_type=type(exc).__name__,
                budget_seconds=consume_budget)
            raise
        finally:
            close_started = time.monotonic()
            progress.emit("close")
            try:
                rows.close()
            except Exception as exc:
                progress.emit("close", status="FAILED", error_type=type(exc).__name__,
                    phase_seconds=round(time.monotonic()-close_started, 6))
                raise
            else:
                progress.emit("close", status="COMPLETE",
                    phase_seconds=round(time.monotonic()-close_started, 6))


def staged_matrix(connection, day: str, *, scope: dict, run_id: str) -> dict:
    """Recheck a retained source landing area before resuming publication."""
    progress = MinuteImportProgress(run_id=run_id,
        total_units=len(scope["codes"]) * len(CORE_GRID), operation="stage_readback", days=[day])
    rows = (_mysql_matrix_rows(connection, day, scope=scope,
                deadline=time.monotonic() + matrix_consume_budget(scope), run_id=run_id)
            if connection.dialect.name == "mysql" else connection.execution_options(stream_results=True, max_row_buffer=2000).execute(text(
        "SELECT /*+ MAX_EXECUTION_TIME(120000) */ stock_code,trade_time,trade_date,price,"
        "`change`,change_pct,volume,amount,qmt_code,'qmt_datadir' AS data_source,"
        "trade_time AS source_time,:run AS batch_id,'qmt_datadir_v1' AS data_version,"
        "'VERIFIED' AS quality_status,'SUPPORTED' AS permission_status "
        "FROM qmt_datadir_minute_stage WHERE run_id=:run AND trade_date=:day "
        "ORDER BY stock_code,trade_time"
    ), {"run": run_id, "day": day}).mappings())
    try:
        return validate_matrix(rows, day=day, scope=scope, progress=progress)
    finally:
        rows.close()


def load_verified_minute_publication(engine, day: str, *, decision_known_at=None) -> dict | None:
    from server.common.qmt_datadir_result import load_receipt

    receipt = load_receipt(engine, dataset="stock_minute", trade_date=day)
    if receipt is None:
        return None
    cutoff = decision_known_at or datetime.now(ZoneInfo("Asia/Shanghai"))
    cutoff = datetime.fromisoformat(str(cutoff)) if not isinstance(cutoff, datetime) else cutoff
    cutoff = cutoff.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None) if cutoff.tzinfo else cutoff
    finished = receipt.get("finished_at")
    finished = datetime.fromisoformat(str(finished)) if finished is not None else None
    if receipt["status"] != "COMPLETE" or finished is None or finished > cutoff:
        raise ValueError("exact minute completion is unavailable at the decision cutoff")
    detail = receipt.get("detail") or {}
    daily_hash = str(detail.get("full_market_manifest_hash") or "")
    if len(daily_hash) != 64 or detail.get("coverage") != 1.0:
        raise ValueError("exact minute receipt lacks its daily scope proof")
    scope = trade_scope(engine, day, manifest_hash=daily_hash, decision_known_at=cutoff)
    actual = persisted_matrix(engine, day, scope=scope, run_id=str(receipt["run_id"]),
        operation="receipt_readback")
    if (int(receipt["row_count"]) != actual["row_count"]
            or int(receipt["code_count"]) != actual["code_count"]
            or detail.get("readback_rows") != actual["row_count"]
            or detail.get("readback_codes") != actual["code_count"]
            or any(detail.get(key) != actual[key] for key in (
                "row_hash", "coverage", "eligible_code_count", "full_market_manifest_hash",
                "expected_traded_code_set_sha256", "minute_grid_count", "minute_grid_sha256", "bad_grid_codes"))):
        raise ValueError("exact minute receipt and persisted matrix differ")
    core = dict(schema=SCHEMA, provider="qmt_datadir", dataset="stock_minute", trade_date=day,
        status="EXACT", strategy_eligible=True, run_id=receipt["run_id"],
        captured_at=finished.isoformat(sep=" "), **actual)
    return {**core, "manifest_hash": canonical_digest(core)}
