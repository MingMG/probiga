"""Post-close replay of minute price and amount anomalies on one sealed QMT session."""
from __future__ import annotations

from collections import deque
from contextlib import closing
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
from heapq import heappush, heapreplace
from os import fstat
from pathlib import Path
from statistics import median
from time import monotonic
from zoneinfo import ZoneInfo

import numpy as np
from sqlalchemy import text

from integrations.qmt.datadir import (
    CST, DAT_HEADER, DAT_RECORD_BYTES, DAT_WORD_COUNT, MINUTE_PERIOD, WORD_TIME,
    DatFile, dat_path, iter_minute_rows,
)
from integrations.qmt.datadir_import import new_batch_id
from server.common.qmt_datadir_minute_coverage import (
    GRID_HASH,
    _MATRIX_FIELDS,
    _NUMERIC_FIELDS,
    _decode_matrix_group,
    _read_matrix_query,
    matrix_consume_budget,
    trade_scope,
    validate_matrix,
)
from server.common.qmt_datadir_result import SHA64, load_receipt
from server.common.qmt_index_minute_grid import CORE_GRID


SCHEMA = "probiga.intraday-minute-replay.v1"
RULE_VERSION = "minute-anomaly-v1"
TZ = ZoneInfo("Asia/Shanghai")
PRICE_LOOKBACK = 3
PRICE_THRESHOLD_PCT = Decimal("2")
AMOUNT_LOOKBACK = 10
AMOUNT_RATIO = Decimal("4")
AMOUNT_FLOOR = Decimal("5000000")
MAX_CODES = 7_000
_BATCH_CODES = 32
TOP_PER_KIND = 10
KINDS = ("RAPID_RISE", "RAPID_DROP", "AMOUNT_SURGE")
_ONE_MINUTE = timedelta(minutes=1)
_PRECISION = Decimal("0.0001")


class MinuteReplayBlocked(RuntimeError):
    pass


def _clock(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return parsed.astimezone(TZ).replace(tzinfo=None) if parsed.tzinfo else parsed


def _stamp(value: datetime) -> str:
    return value.replace(tzinfo=TZ).isoformat(timespec="seconds")


class _MinuteAnomalies:
    def __init__(self):
        self._code = ""
        self._bars = deque(maxlen=AMOUNT_LOOKBACK)
        self._active: set[str] = set()
        self._top: dict[str, list] = {kind: [] for kind in KINDS}
        self.counts = dict.fromkeys(KINDS, 0)

    def _retain(self, kind, code, stamp, severity, evidence):
        self.counts[kind] += 1
        ranking = self._top[kind]
        key = (severity, stamp, code)
        if len(ranking) == TOP_PER_KIND and key <= ranking[0][0]:
            return
        item = (key, {"kind": kind, "stock_code": code, "bar_at": _stamp(stamp), **evidence})
        if len(ranking) < TOP_PER_KIND:
            heappush(ranking, item)
        else:
            heapreplace(ranking, item)

    def observe(self, row):
        code, stamp, price, amount = (row[key] for key in ("stock_code", "trade_time", "price", "amount"))
        if code != self._code or (self._bars and stamp - self._bars[-1][0] != _ONE_MINUTE):
            self._code = code
            self._bars.clear()
            self._active.clear()
        # Ordered QMT rows contain exact decimal yuan amounts per bar.
        active = set()
        if len(self._bars) >= PRICE_LOOKBACK:
            reference_at, reference_price, _ = self._bars[-PRICE_LOOKBACK]
            delta = (price - reference_price) * 100
            if abs(delta) >= reference_price * PRICE_THRESHOLD_PCT:
                kind = "RAPID_RISE" if delta > 0 else "RAPID_DROP"
                active.add(kind)
                if kind not in self._active:
                    change = delta / reference_price
                    self._retain(kind, code, stamp, abs(change), {
                        "window_start_at": _stamp(reference_at), "change_pct": float(change.quantize(_PRECISION)),
                        "reference_price": float(reference_price), "price": float(price),
                    })
        if len(self._bars) == AMOUNT_LOOKBACK and amount >= AMOUNT_FLOOR:
            typical = median(bar[2] for bar in self._bars)
            if typical > 0 and amount >= typical * AMOUNT_RATIO:
                active.add("AMOUNT_SURGE")
                if "AMOUNT_SURGE" not in self._active:
                    ratio = amount / typical
                    self._retain("AMOUNT_SURGE", code, stamp, ratio, {
                        "window_start_at": _stamp(self._bars[0][0]), "amount_yuan": float(amount),
                        "median_amount_yuan": float(typical), "amount_ratio": float(ratio.quantize(_PRECISION)),
                    })
        self._active = active
        self._bars.append((stamp, price, amount))

    def results(self):
        return {kind: [item for _, item in sorted(ranking, reverse=True)] for kind, ranking in self._top.items()}


def _sealed_source(connection, day: str, cutoff: datetime):
    receipt = load_receipt(connection, dataset="stock_minute", trade_date=day)
    if receipt is None or receipt.get("status") != "COMPLETE" or not receipt.get("finished_at"):
        raise MinuteReplayBlocked("source_receipt: no completed exact-date QMT minute receipt")
    available_at = _clock(receipt["finished_at"])
    if not datetime.combine(date.fromisoformat(day), time(15)) <= available_at <= cutoff:
        raise MinuteReplayBlocked("source_receipt: minute data was not yet available at the requested cutoff")
    detail = receipt["detail"]
    codes, rows = receipt["code_count"], receipt["row_count"]
    daily_hash = str(detail.get("full_market_manifest_hash") or "")
    if (str(receipt["trade_date"]) != day or not receipt.get("run_id")
            or not 0 < codes <= MAX_CODES or rows != codes * len(CORE_GRID)
            or detail.get("eligible_code_count") != codes or detail.get("coverage") != 1.0
            or detail.get("readback_rows") != rows or detail.get("readback_codes") != codes
            or detail.get("minute_grid_count") != len(CORE_GRID) or detail.get("minute_grid_sha256") != GRID_HASH
            or detail.get("bad_grid_codes") != 0
            or any(SHA64.fullmatch(str(value or "")) is None for value in (
                daily_hash, detail.get("row_hash"), detail.get("expected_traded_code_set_sha256"), receipt.get("content_sha256"),
            ))):
        raise MinuteReplayBlocked("source_receipt: QMT minute scope or digest differs")
    return receipt, available_at


def _minute_rows(connection, day: str, *, scope: dict, deadline: float):
    """Cover the entire date/source index with small, nonoverlapping code ranges."""
    ordered = sorted(scope["codes"])
    fields = ",".join(f"'{field}'," + (f"CAST(`{field}` AS CHAR)" if field in _NUMERIC_FIELDS else f"`{field}`")
                      for field in _MATRIX_FIELDS)
    lower = ""
    for offset in range(0, len(ordered), _BATCH_CODES):
        batch = ordered[offset:offset + _BATCH_CODES]
        # The final range is unbounded above so an unexpected extra security
        # cannot hide outside the independently verified trade scope.
        bounded = offset + _BATCH_CODES < len(ordered)
        lower_clause = "AND stock_code>:lower" if offset else ""
        upper_clause = "AND stock_code<=:upper" if bounded else ""
        try:
            groups = _read_matrix_query(connection, f"""
                SELECT /*+ MAX_EXECUTION_TIME(20000) */ stock_code, COUNT(*) AS row_count,
                    COMPRESS(CAST(JSON_ARRAYAGG(JSON_OBJECT({fields})) AS CHAR CHARACTER SET utf8mb4)) AS packed_rows
                FROM sm_stock_minute FORCE INDEX (`idx_smm_date_source_code_time`)
                WHERE trade_date=:day AND data_source='qmt_datadir' {lower_clause} {upper_clause}
                GROUP BY stock_code ORDER BY stock_code LIMIT {_BATCH_CODES + 1}
            """, {"day": day, "lower": lower, "upper": batch[-1]}, deadline=deadline)
        except TimeoutError as exc:
            raise TimeoutError(f"minute_batch_readback: timed out for {batch[0]} through {batch[-1]}") from exc
        if [group.get("stock_code") for group in groups] != batch:
            raise MinuteReplayBlocked(f"minute_batch_readback: security coverage differs from {batch[0]} through {batch[-1]}")
        expected = set(batch)
        for group in groups:
            yield from _decode_matrix_group(group, expected_codes=expected)
        lower = batch[-1]


def replay_minute_anomalies(engine, target: date, *, cutoff: datetime | None = None, progress=None) -> dict:
    """Read the canonical matrix once and release results only after exact hash readback."""
    day = target.isoformat()
    now = datetime.now(TZ).replace(tzinfo=None)
    cutoff = min(now, _clock(cutoff)) if cutoff is not None else now
    detector = _MinuteAnomalies()
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
        if connection.dialect.name != "mysql":
            raise MinuteReplayBlocked("source_connection: expected the canonical MySQL minute database")
        connection.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
        receipt, available_at = _sealed_source(connection, day, cutoff)
        detail = receipt["detail"]
        scope = trade_scope(connection, day, manifest_hash=detail["full_market_manifest_hash"],
                            decision_known_at=available_at, verify_rows=True)
        if len(scope["codes"]) != receipt["code_count"]:
            raise MinuteReplayBlocked("daily_scope: expected traded-security count differs")
        foreign = connection.execute(text("""
            SELECT /*+ MAX_EXECUTION_TIME(20000) */ 1 FROM sm_stock_minute
            WHERE trade_date=:day AND (data_source IS NULL OR data_source<>'qmt_datadir') LIMIT 1
        """), {"day": day}).first()
        if foreign is not None:
            raise MinuteReplayBlocked("minute_matrix: unexpected provider in exact session")
        budget = matrix_consume_budget(scope)
        started = monotonic()
        if progress is not None:
            progress.emit("receipt", status="COMPLETE", total_units=receipt["row_count"],
                          receipt_run_id=receipt["run_id"], consume_budget_seconds=budget)
        with closing(_minute_rows(connection, day, scope=scope, deadline=started + budget)) as rows:
            def observed_rows():
                for row in rows:
                    detector.observe(row)
                    yield row

            verified = validate_matrix(observed_rows(), day=day, scope=scope, deadline_seconds=budget,
                                       query_started=started, progress=progress)
        if (verified["row_count"] != receipt["row_count"] or verified["code_count"] != receipt["code_count"]
                or any(verified[key] != detail[key] for key in (
                    "row_hash", "full_market_manifest_hash", "expected_traded_code_set_sha256",
                    "eligible_code_count", "coverage", "minute_grid_count", "minute_grid_sha256", "bad_grid_codes",
                ))):
            raise MinuteReplayBlocked("minute_readback: current matrix differs from the original completion receipt")
    return _report(day, receipt, available_at, verified, detector, source="qmt_datadir")


def _report(day, receipt, available_at, verified, detector, *, source):
    return {
        "schema": SCHEMA, "status": "COMPLETE", "mode": "POST_CLOSE_MINUTE_REPLAY",
        "trade_date": day, "source": source, "rule_version": RULE_VERSION,
        "source_receipt_run_id": receipt["run_id"], "evidence_available_at": _stamp(available_at),
        "generated_at": _stamp(datetime.now(TZ).replace(tzinfo=None)),
        "scope": "verified_traded_securities", "bar_time_basis": "qmt_native_bar_start",
        "event_selection": "first_bar_of_each_contiguous_threshold_crossing",
        "source_proof": verified,
        "rules": {
            "price_window_minutes": PRICE_LOOKBACK, "abs_price_change_pct": float(PRICE_THRESHOLD_PCT),
            "amount_median_prior_bars": AMOUNT_LOOKBACK, "amount_ratio": float(AMOUNT_RATIO),
            "amount_floor_yuan": int(AMOUNT_FLOOR), "per_kind_result_limit": TOP_PER_KIND,
        },
        "candidate_counts": detector.counts, "strongest_events": detector.results(),
    }


def _native_session_records(path: Path, target: date):
    """Read the native day slice by seeking fixed-size records, without scanning the file."""
    start_epoch = int(datetime.combine(target, time.min, tzinfo=CST).timestamp())
    header_bytes = len(DAT_HEADER)
    with path.open("rb") as stream:
        initial = fstat(stream.fileno())
        body = initial.st_size - header_bytes
        if stream.read(header_bytes) != DAT_HEADER or body < 0 or body % DAT_RECORD_BYTES:
            raise MinuteReplayBlocked(f"local_dat: invalid file format for {path.name}")
        record_count = body // DAT_RECORD_BYTES

        def position(epoch):
            low, high = 0, record_count
            while low < high:
                middle = (low + high) // 2
                stream.seek(header_bytes + middle * DAT_RECORD_BYTES + WORD_TIME * 4)
                sample = stream.read(4)
                if len(sample) != 4:
                    raise MinuteReplayBlocked(f"local_dat: short timestamp read for {path.name}")
                if int.from_bytes(sample, "little") < epoch:
                    low = middle + 1
                else:
                    high = middle
            return low

        first, last = position(start_epoch), position(start_epoch + 86400)
        if not len(CORE_GRID) <= last - first <= 512:
            raise MinuteReplayBlocked(f"local_dat: native day slice length differs for {path.name}")
        stream.seek(header_bytes + first * DAT_RECORD_BYTES)
        raw = stream.read((last - first) * DAT_RECORD_BYTES)
        final = fstat(stream.fileno())
        if (len(raw) != (last - first) * DAT_RECORD_BYTES
                or (initial.st_size, initial.st_mtime_ns) != (final.st_size, final.st_mtime_ns)):
            raise MinuteReplayBlocked(f"local_dat: source changed during read for {path.name}")
    rows = np.frombuffer(raw, dtype="<u4").reshape(-1, DAT_WORD_COUNT)
    clocks = rows[:, WORD_TIME]
    if clocks[0] < start_epoch or clocks[-1] >= start_epoch + 86400 or np.any(clocks[1:] <= clocks[:-1]):
        raise MinuteReplayBlocked(f"local_dat: exact-date ordered time slice differs for {path.name}")
    return rows


def _local_rows(root, target, *, scope, receipt, digest, detector):
    columns = ("stock_code", "trade_time", "trade_date", "price", "change", "change_pct", "volume", "amount", "qmt_code")
    batch_id = new_batch_id(target, receipt["run_id"])
    for code in sorted(scope["codes"]):
        market = "SH" if code.startswith("6") else "BJ" if code.startswith(("4", "8", "9")) else "SZ"
        path = dat_path(root, market, MINUTE_PERIOD, code)
        records = _native_session_records(path, target)
        dat = DatFile(market, code, MINUTE_PERIOD, path, records)
        digest.update(dat.qmt_code.encode())
        digest.update(sha256(records.tobytes()).digest())
        for _, native in iter_minute_rows(dat, records, CORE_GRID):
            row = dict(zip(columns, native))
            pct = row["change_pct"].quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
            row["change_pct"] = pct if pct else pct.copy_abs()
            detector.observe(row)
            yield {**row, "source_time": row["trade_time"], "data_source": "qmt_datadir",
                   "batch_id": batch_id, "data_version": "qmt_datadir_v1", "quality_status": "VERIFIED",
                   "permission_status": "SUPPORTED"}


def replay_local_minute_anomalies(engine, target: date, root: Path, *, cutoff: datetime | None = None, progress=None) -> dict:
    """Replay verified Windows cache slices with immutable publication comparisons."""
    day = target.isoformat()
    started_at = datetime.now(TZ).replace(tzinfo=None)
    known_at = min(started_at, _clock(cutoff)) if cutoff is not None else started_at
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
        if connection.dialect.name != "mysql":
            raise MinuteReplayBlocked("source_connection: expected the canonical MySQL receipt database")
        connection.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
        receipt, available_at = _sealed_source(connection, day, known_at)
        scope = trade_scope(connection, day, manifest_hash=receipt["detail"]["full_market_manifest_hash"],
                            decision_known_at=available_at, verify_rows=True)
    if len(scope["codes"]) != receipt["code_count"]:
        raise MinuteReplayBlocked("daily_scope: expected traded-security count differs")
    detector = _MinuteAnomalies()
    native_digest = sha256()
    if progress is not None:
        progress.emit("receipt", status="COMPLETE", total_units=receipt["row_count"], receipt_run_id=receipt["run_id"])
    proof = validate_matrix(_local_rows(root, target, scope=scope, receipt=receipt, digest=native_digest, detector=detector),
                            day=day, scope=scope, progress=progress)
    raw_sha = native_digest.hexdigest()
    raw_match = raw_sha == receipt["content_sha256"]
    matrix_match = proof["row_hash"] == receipt["detail"]["row_hash"]
    if not (raw_match or matrix_match):
        available_at = datetime.now(TZ).replace(tzinfo=None)
        if cutoff is not None and available_at > _clock(cutoff):
            raise MinuteReplayBlocked("local_dat: unmatched cache was first verified after the requested cutoff")
    report = _report(day, receipt, available_at, proof, detector, source="qmt_local_datadir")
    report["receipt_comparison"] = {
        "native_slice_sha256": raw_sha, "receipt_native_slice_sha256": receipt["content_sha256"],
        "native_slice_digest_matches": raw_match, "reconstructed_matrix_digest_matches": matrix_match,
        "receipt_matrix_sha256": receipt["detail"]["row_hash"],
        "availability_basis": "publication_receipt" if raw_match or matrix_match else "local_validation_completion",
    }
    return report
