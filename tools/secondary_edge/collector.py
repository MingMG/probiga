from __future__ import annotations

"""Collect real QMT evidence in a private candidate-host database.

This entry point deliberately does not import env_config, the production
scheduler, or a service supervisor. The installer owns schema creation. The
worker can only read and write its own acceptance records and never promotes
a host, consumes production claims, creates grants, or opens an SSH tunnel.
"""

import argparse
import hashlib
import json
import math
import os
import re
import signal
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


CHINA = timezone(timedelta(hours=8))
ENDPOINT = ("127.0.0.1", 33085, "probiga_secondary")
SECTORS = ("上证A股", "深证A股", "京市A股")
SYMBOL = re.compile(r"^\d{6}\.(?:SH|SZ|BJ)$")
SHA = re.compile(r"^[0-9a-f]{40}$")
REQUEST_TIMEOUT = 30
MAX_AGE = 35

SCHEMA_SQL = (
    """CREATE TABLE IF NOT EXISTS secondary_edge_latest_quote (
        qmt_code VARCHAR(16) PRIMARY KEY, source_time DATETIME(6) NOT NULL,
        observed_at DATETIME(6) NOT NULL, received_at DATETIME(6) NOT NULL,
        price DECIMAL(30,8) NOT NULL, volume DOUBLE NOT NULL, amount DOUBLE NOT NULL,
        payload LONGTEXT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS secondary_edge_history (
        kind VARCHAR(8) NOT NULL, qmt_code VARCHAR(16) NOT NULL,
        trade_time DATETIME(6) NOT NULL, payload LONGTEXT NOT NULL,
        captured_at DATETIME(6) NOT NULL,
        PRIMARY KEY (kind, qmt_code, trade_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS secondary_edge_checkpoint (
        job_key VARCHAR(128) PRIMARY KEY, kind VARCHAR(8) NOT NULL,
        trade_date DATE NOT NULL, codes_json LONGTEXT NOT NULL,
        status VARCHAR(16) NOT NULL, attempts INT NOT NULL DEFAULT 0,
        row_count INT NOT NULL DEFAULT 0, last_error VARCHAR(512) NULL,
        updated_at DATETIME(6) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS secondary_edge_summary (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        captured_at DATETIME(6) NOT NULL, status VARCHAR(16) NOT NULL,
        payload LONGTEXT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
)


def _json(value: Any) -> str:
    def encode(item: Any):
        if isinstance(item, datetime):
            return item.isoformat(timespec="microseconds")
        if hasattr(item, "item"):
            return item.item()
        raise TypeError(type(item).__name__)
    return json.dumps(value, ensure_ascii=False, default=encode, allow_nan=False)


def initialize_schema(connection) -> None:
    """Installer-only bootstrap: verify the private DB before the first DDL."""
    assert_connection(connection)
    with connection.cursor() as cursor:
        for statement in SCHEMA_SQL:
            cursor.execute(statement)
    connection.commit()


def assert_connection(connection, database: str = ENDPOINT[2]) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT @@port AS port, DATABASE() AS database_name")
        row = cursor.fetchone()
    if isinstance(row, dict):
        observed = (int(row["port"]), row["database_name"])
    else:
        observed = (int(row[0]), row[1])
    if observed != (ENDPOINT[1], database):
        raise RuntimeError("SECONDARY_DATABASE_IDENTITY_MISMATCH")


def validate_config(config: dict[str, Any], root: Path) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise ValueError("SECONDARY_CONFIG_INVALID")
    db = config.get("mysql") or {}
    if (db.get("host"), db.get("port"), db.get("database")) != ENDPOINT:
        raise ValueError("SECONDARY_ENDPOINT_MUST_BE_127_0_0_1_33085_probiga_secondary")
    _validate_account(db)
    clone = config.get("source_db")
    if clone:
        if ((clone.get("host"), clone.get("port")) != ENDPOINT[:2]
                or clone.get("database") not in {"probiga", "probiga_qmt_history"}):
            raise ValueError("SECONDARY_SOURCE_MUST_BE_A_LOCAL_PRIVATE_CLONE")
        _validate_account(clone)
    build = str(config.get("expected_build_sha") or "")
    if not SHA.fullmatch(build) or build == "0" * 40:
        raise ValueError("SECONDARY_BUILD_SHA_INVALID")
    for name, low, high, default in (
        ("poll_seconds", 30, 3600, 60), ("sample_size", 0, 20000, 50),
        ("history_days", 1, 31, 5),
    ):
        value = config.get(name, default)
        if type(value) is not int or not low <= value <= high:
            raise ValueError("SECONDARY_" + name.upper() + "_INVALID")
        config[name] = value
    qmt_home = Path(str(config.get("qmt_home") or ""))
    if not qmt_home.is_absolute() or qmt_home.is_symlink():
        raise ValueError("SECONDARY_QMT_HOME_INVALID")
    if not root.is_absolute() or root.is_symlink():
        raise ValueError("SECONDARY_ROOT_INVALID")
    return config


def _validate_account(db: dict[str, Any]) -> None:
    if (not isinstance(db, dict) or not db.get("user") or not db.get("password")
            or str(db["user"]).casefold() in {"root", "probiga_runtime", "probiga_migrator"}
            or not Path(str(db.get("ssl_ca") or "")).is_absolute()):
        raise ValueError("SECONDARY_PRIVATE_ACCOUNT_AND_ABSOLUTE_CA_REQUIRED")


def connect_database(db: dict[str, Any]):
    import pymysql
    connection = pymysql.connect(
        host=db["host"], port=db["port"], database=db["database"],
        user=db["user"], password=db["password"], charset="utf8mb4",
        connect_timeout=10, read_timeout=30, write_timeout=30, autocommit=False,
        ssl_ca=db["ssl_ca"], ssl_verify_cert=True, ssl_verify_identity=False,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        assert_connection(connection, db["database"])
    except BaseException:
        connection.close()
        raise
    return connection


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if result.tzinfo is not None:
        result = result.astimezone(CHINA).replace(tzinfo=None)
    return result


def _number(value: Any, *, positive: bool = False) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(result) or result < 0 or (positive and result <= 0):
        return None
    return result


def normalize_quotes(rows: list[dict[str, Any]], *, now: datetime) -> list[dict[str, Any]]:
    """Reject absent native times and invalid prices; never synthesize a row."""
    result = {}
    for row in rows:
        code = str(row.get("qmt_code") or "").upper()
        source = _timestamp(row.get("source_time"))
        observed = _timestamp(row.get("received_at"))
        price = _number(row.get("price"), positive=True)
        volume, amount = _number(row.get("volume")), _number(row.get("amount"))
        if (not SYMBOL.fullmatch(code) or source is None or observed is None
                or price is None or volume is None or amount is None
                or source > observed + timedelta(seconds=2)
                or observed > now + timedelta(seconds=2)):
            continue
        normalized = {**row, "qmt_code": code, "source_time": source,
                      "received_at": observed, "price": price, "volume": volume, "amount": amount}
        previous = result.get(code)
        if previous is None or source >= previous["source_time"]:
            result[code] = normalized
    return list(result.values())


def quote_metrics(rows: list[dict[str, Any]], universe: list[str], *, now: datetime,
                  market_open: bool | None, heartbeat: dict[str, Any],
                  generated_ts: Any) -> dict[str, Any]:
    expected = set(universe)
    covered = {row["qmt_code"] for row in rows} & expected
    fresh = {row["qmt_code"] for row in rows if row["qmt_code"] in expected
             and -2 <= (now - row["source_time"]).total_seconds() <= MAX_AGE}
    clock = now.replace(tzinfo=CHINA).timestamp()
    def age(value):
        number = _number(value, positive=True)
        return None if number is None else clock - number
    heartbeat_age = age(heartbeat.get("updated_ts"))
    snapshot_age = age(generated_ts)
    transport_ready = (heartbeat.get("status") in {"running", "busy"}
                       and heartbeat_age is not None and -2 <= heartbeat_age <= MAX_AGE
                       and snapshot_age is not None and -2 <= snapshot_age <= MAX_AGE)
    coverage = len(covered) / len(expected) if expected else 0.0
    fresh_ratio = len(fresh) / len(expected) if expected else 0.0
    if market_open is None:
        freshness = "UNKNOWN_TRADING_CALENDAR"
    elif not market_open:
        freshness = "NOT_EVALUATED_MARKET_CLOSED"
    else:
        freshness = "PASS" if transport_ready and coverage == 1 and fresh_ratio == 1 else "FAIL"
    latest = max((row["source_time"] for row in rows), default=None)
    return {"expected_codes": len(expected), "covered_codes": len(covered),
            "coverage_ratio": coverage, "fresh_codes": len(fresh), "fresh_ratio": fresh_ratio,
            "missing_code_sample": sorted(expected - covered)[:20],
            "latest_source_time": latest.isoformat() if latest else None,
            "heartbeat_age_seconds": heartbeat_age, "snapshot_age_seconds": snapshot_age,
            "transport_ready": transport_ready, "market_open": market_open,
            "market_data_fresh": freshness}


def sample_codes(universe: list[str], limit: int) -> list[str]:
    if limit == 0 or len(universe) <= limit:
        return list(universe)
    return [universe[index * len(universe) // limit] for index in range(limit)]


class Store:
    def __init__(self, connection):
        self.connection = connection

    def quotes(self, rows, now):
        with self.connection.cursor() as cursor:
            cursor.executemany("""INSERT INTO secondary_edge_latest_quote
                (qmt_code,source_time,observed_at,received_at,price,volume,amount,payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                observed_at=IF(VALUES(source_time)>=source_time,VALUES(observed_at),observed_at),
                received_at=IF(VALUES(source_time)>=source_time,VALUES(received_at),received_at),
                price=IF(VALUES(source_time)>=source_time,VALUES(price),price),
                volume=IF(VALUES(source_time)>=source_time,VALUES(volume),volume),
                amount=IF(VALUES(source_time)>=source_time,VALUES(amount),amount),
                payload=IF(VALUES(source_time)>=source_time,VALUES(payload),payload),
                source_time=GREATEST(source_time,VALUES(source_time))""",
                [(row["qmt_code"], row["source_time"], row["received_at"], now,
                  row["price"], row["volume"], row["amount"], _json(row)) for row in rows])
        self.connection.commit()

    def plan(self, sessions, codes, now):
        for day in sessions:
            for kind in ("daily", "minute"):
                for offset in range(0, len(codes), 20):
                    batch = codes[offset:offset + 20]
                    digest = hashlib.sha256(_json(batch).encode()).hexdigest()[:24]
                    key = f"{kind}:{day}:{digest}"
                    with self.connection.cursor() as cursor:
                        cursor.execute("""INSERT INTO secondary_edge_checkpoint
                            (job_key,kind,trade_date,codes_json,status,updated_at)
                            VALUES (%s,%s,%s,%s,'PENDING',%s)
                            ON DUPLICATE KEY UPDATE job_key=VALUES(job_key)""",
                            (key, kind, day, _json(batch), now))
        self.connection.commit()

    def pending(self):
        with self.connection.cursor() as cursor:
            cursor.execute("""SELECT job_key,kind,trade_date,codes_json FROM secondary_edge_checkpoint
                WHERE status<>'SUCCESS' ORDER BY updated_at,job_key LIMIT 1""")
            return cursor.fetchone()

    def progress(self):
        with self.connection.cursor() as cursor:
            cursor.execute("""SELECT COUNT(*) AS total,
                COALESCE(SUM(status='SUCCESS'),0) AS succeeded,
                COALESCE(SUM(status='FAILED'),0) AS failed
                FROM secondary_edge_checkpoint""")
            row = cursor.fetchone()
        return {"total_batches": int(row["total"]), "succeeded_batches": int(row["succeeded"]),
                "failed_batches": int(row["failed"]),
                "remaining_batches": int(row["total"]) - int(row["succeeded"])}

    def history_success(self, job, rows, now):
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany("""INSERT INTO secondary_edge_history
                    (kind,qmt_code,trade_time,payload,captured_at) VALUES (%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE payload=VALUES(payload),captured_at=VALUES(captured_at)""",
                    [(job["kind"], row["qmt_code"], row["trade_time"], _json(row), now) for row in rows])
                cursor.execute("""UPDATE secondary_edge_checkpoint SET status='SUCCESS',
                    attempts=attempts+1,row_count=%s,last_error=NULL,updated_at=%s WHERE job_key=%s""",
                    (len(rows), now, job["job_key"]))
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def history_failed(self, job, code, now):
        self.connection.rollback()
        with self.connection.cursor() as cursor:
            cursor.execute("""UPDATE secondary_edge_checkpoint SET status='FAILED',
                attempts=attempts+1,last_error=%s,updated_at=%s WHERE job_key=%s""",
                (code[:512], now, job["job_key"]))
        self.connection.commit()

    def summary(self, status, now):
        with self.connection.cursor() as cursor:
            cursor.execute("INSERT INTO secondary_edge_summary (captured_at,status,payload) VALUES (%s,%s,%s)",
                           (now, status["status"], _json(status)))
        self.connection.commit()


class QmtReader:
    def __init__(self, root: Path, config):
        code = root / "code"
        if not code.is_dir() or code.is_symlink():
            raise RuntimeError("SECONDARY_CODE_DIRECTORY_INVALID")
        sys.path.insert(0, str(code))
        # Explicitly discard ambient production configuration for this worker.
        for name in list(os.environ):
            if name.startswith(("PROBIGA_", "SSH_", "REMOTE_")) or name in {
                "MYSQL_URL", "QMT_HISTORY_MYSQL_URL", "MINUTE_MYSQL_URL", "DATABASE_URL"
            }:
                os.environ.pop(name, None)
        os.environ["BIG_QMT_HOME"] = str(config["qmt_home"])
        os.environ["PROBIGA_DEPLOYMENT_MODE"] = "development"
        os.environ["BIG_QMT_REMOTE_PORTFOLIO_ENABLED"] = "0"
        from integrations.bigqmt import bridge
        from integrations.bigqmt import spool
        from integrations.bigqmt.release_identity import validate_strategy_release_payload
        self.bridge, self.spool = bridge, spool
        self.validate_payload = validate_strategy_release_payload
        self.config, self.code = config, code
        self.identity = None

    def capabilities(self):
        result = self.bridge.capabilities(timeout=REQUEST_TIMEOUT)
        self.validate_payload(result, expected_build_sha=self.config["expected_build_sha"],
                              root=self.code, source_path=self.code / "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py")
        self.identity = result
        return result

    def _assert_capture_identity(self, capture):
        fields = ("model_instance_id", "strategy_identity_frozen", "strategy_identity_status",
                  "strategy_build_sha", "strategy_git_blob", "strategy_source_sha256",
                  "strategy_loaded_identity_sha256")
        if self.identity is None or any(capture.get(field) != self.identity.get(field) for field in fields):
            raise RuntimeError("SECONDARY_REQUEST_MODEL_IDENTITY_CHANGED")

    def control_probe(self, codes):
        capture = self.bridge.current_capture(codes[:3], batch_size=3, timeout=REQUEST_TIMEOUT)
        self._assert_capture_identity(capture)
        # current's snapshot_at can fall back to request time in the existing
        # QMT API. It proves transport only; native full snapshots prove freshness.
        return {"status": "PASS", "rows": len(capture.get("rows") or []),
                "market_freshness_evidence": False}

    def universe(self):
        codes = set()
        for sector in SECTORS:
            frame = self.bridge.sector_members(sector, timeout=REQUEST_TIMEOUT)
            sector_codes = set()
            for row in frame.to_dict("records"):
                symbol = str(row.get("qmt_code") or "").upper()
                if SYMBOL.fullmatch(symbol):
                    sector_codes.add(symbol)
            if not sector_codes:
                raise RuntimeError("SECONDARY_NATIVE_SECTOR_EMPTY")
            codes.update(sector_codes)
        if not codes:
            raise RuntimeError("SECONDARY_NATIVE_UNIVERSE_EMPTY")
        return sorted(codes)

    def watchlist(self, codes):
        self.spool.write_watchlist(all_codes=codes, tracked_codes=sample_codes(codes, 50),
                                  qmt_home=self.config["qmt_home"], full_refresh_seconds=30)

    def calendar(self, now):
        capture = self.bridge.trading_calendar_capture(
            "SH", start_date=(now - timedelta(days=90)).date().isoformat(),
            end_date=now.date().isoformat(), timeout=REQUEST_TIMEOUT)
        self._assert_capture_identity(capture)
        if capture.get("source_method") != "ContextInfo.get_trading_dates":
            raise RuntimeError("SECONDARY_NATIVE_CALENDAR_PROOF_INVALID")
        return sorted({str(row.get("trade_date") or "")[:10] for row in capture.get("rows", [])
                       if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(row.get("trade_date") or "")[:10])})

    def quotes(self):
        paths = self.spool.bridge_paths(self.config["qmt_home"])
        heartbeat = self.spool.read_json(paths["heartbeat"])
        payload = self.spool.read_json(paths["full"])
        rows = self.bridge.native_snapshot_frame(payload).to_dict("records")
        return rows, heartbeat, payload.get("generated_ts")

    def history(self, job, codes):
        day = str(job["trade_date"])
        if job["kind"] == "daily":
            capture = self.bridge.kline_capture(codes, start_date=day, end_date=day,
                                               batch_size=20, timeout=REQUEST_TIMEOUT)
        else:
            capture = self.bridge.minute_capture(codes, trade_date=day, batch_size=20,
                                                timeout=REQUEST_TIMEOUT)
        receipts = capture.get("batch_receipts")
        if not isinstance(receipts, list) or not receipts:
            raise RuntimeError("SECONDARY_HISTORY_BATCH_RECEIPTS_MISSING")
        for receipt in receipts:
            self._assert_capture_identity(receipt)
        return capture.get("rows") or []


def normalize_history(rows, job, codes):
    expected = set(codes)
    normalized = []
    day = str(job["trade_date"])
    for raw in rows:
        code = str(raw.get("qmt_code") or "").upper()
        stamp = _timestamp(raw.get("trade_time"))
        if code not in expected or stamp is None or stamp.date().isoformat() != day:
            raise RuntimeError("SECONDARY_HISTORY_ROW_OUTSIDE_REQUEST")
        for field in ("open", "close", "high", "low", "volume", "amount"):
            if _number(raw.get(field)) is None:
                raise RuntimeError("SECONDARY_HISTORY_NATIVE_VALUE_INVALID")
        if _number(raw.get("close"), positive=True) is None:
            raise RuntimeError("SECONDARY_HISTORY_NATIVE_CLOSE_INVALID")
        normalized.append({**raw, "qmt_code": code, "trade_time": stamp})
    if {row["qmt_code"] for row in normalized} != expected:
        raise RuntimeError("SECONDARY_HISTORY_CODE_COVERAGE_INCOMPLETE")
    return normalized


def collect_history(store, qmt, now):
    job = store.pending()
    if not job:
        return {"status": "IDLE"}
    codes = json.loads(job["codes_json"])
    started = time.monotonic()
    try:
        rows = normalize_history(qmt.history(job, codes), job, codes)
        store.history_success(job, rows, now)
        return {"status": "SUCCESS", "job_key": job["job_key"], "rows": len(rows),
                "duration_seconds": round(time.monotonic() - started, 3)}
    except Exception as exc:
        # Never publish an exception string: network/DB errors can contain credentials.
        error = safe_error(exc)
        store.history_failed(job, error, now)
        return {"status": "FAILED", "job_key": job["job_key"], "error": error,
                "duration_seconds": round(time.monotonic() - started, 3)}


def safe_error(exc):
    if str(exc).startswith("SECONDARY_") and re.fullmatch(r"[A-Z0-9_]+", str(exc)):
        return str(exc)
    return type(exc).__name__


def clone_availability(connection):
    """Only fixed SELECT queries against an independent local clone."""
    results = {}
    with connection.cursor() as cursor:
        cursor.execute("SELECT DATABASE() AS database_name")
        database = cursor.fetchone()["database_name"]
        tables = (("sm_stock_current", "stock_code", "snapshot_at", "stock_code"),
                  ("qmt_stock_catalog_batch", "batch_id", "captured_at", "batch_id")) if database == "probiga" else (
                      ("qmt_local_stock_kline", "stock_code", "trade_date", "id"),)
        for table, key, stamp, order in tables:
            # A large HDD clone must not be scanned with COUNT(*) on every
            # health cycle. The estimate is explicitly labelled, and a real
            # primary-key lookup independently proves accessible rows.
            cursor.execute("SELECT TABLE_ROWS AS estimated_rows FROM information_schema.TABLES "
                           "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (database, table))
            estimate = cursor.fetchone()
            cursor.execute(f"SELECT `{key}` AS record_key,`{stamp}` AS record_time FROM `{table}` "
                           f"ORDER BY `{order}` DESC LIMIT 1")
            row = cursor.fetchone()
            results[table] = {"estimated_rows": int(estimate["estimated_rows"] or 0) if estimate else None,
                              "sample_record_present": row is not None,
                              "sample_record_time": str(row["record_time"]) if row and row["record_time"] else None}
    connection.rollback()
    return {"status": "AVAILABLE" if all(row["sample_record_present"] for row in results.values()) else "EMPTY",
            "database": database, "tables": results}


def atomic_status(root: Path, payload) -> None:
    temporary = root / f".status-{os.getpid()}.tmp"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(_json(payload))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, root / "status.json")


@contextmanager
def singleton(root: Path):
    path = root / "collector.lock"
    if path.is_symlink():
        raise RuntimeError("SECONDARY_LOCK_PATH_UNSAFE")
    handle = path.open("a+b")
    try:
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            if path.stat().st_size == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise RuntimeError("SECONDARY_COLLECTOR_ALREADY_RUNNING") from exc
        else:
            import fcntl
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise RuntimeError("SECONDARY_COLLECTOR_ALREADY_RUNNING") from exc
        yield
    finally:
        handle.close()


def run_cycle(store, qmt, config, *, now: datetime | None, universe, sessions):
    started = time.monotonic()
    capabilities = qmt.capabilities()
    rows, heartbeat, generated_ts = qmt.quotes()
    # Network discovery/control requests can take seconds. Evaluate source
    # clocks against the time of the actual read, not the cycle start time.
    now = now or datetime.now(CHINA).replace(tzinfo=None)
    if heartbeat.get("model_instance_id") != capabilities.get("model_instance_id"):
        raise RuntimeError("SECONDARY_HEARTBEAT_MODEL_IDENTITY_MISMATCH")
    rows = normalize_quotes(rows, now=now)
    market_open = (None if sessions is None else now.date().isoformat() in sessions
                   and ((9, 30) <= (now.hour, now.minute) <= (11, 30)
                        or (13, 0) <= (now.hour, now.minute) < (15, 0)))
    metrics = quote_metrics(rows, universe, now=now, market_open=market_open,
                            heartbeat=heartbeat, generated_ts=generated_ts)
    expected = set(universe)
    store.quotes([row for row in rows if row["qmt_code"] in expected], now)
    status = {"schema": "probiga.secondary-edge-acceptance.v1", "candidate_only": True,
              "production_promoted": False, "captured_at": now.isoformat(),
              "status": "READY" if metrics["transport_ready"] and metrics["coverage_ratio"] == 1
                        and metrics["market_data_fresh"] != "FAIL" else "DEGRADED",
              "quotes": metrics, "strategy_model_instance_id": capabilities["model_instance_id"],
              "history": collect_history(store, qmt, now)}
    status["duration_seconds"] = round(time.monotonic() - started, 3)
    if status["history"]["status"] == "FAILED":
        status["status"] = "DEGRADED"
    status["history_progress"] = store.progress()
    status["acceptance_status"] = (
        "AWAITING_LIVE_MARKET" if metrics["market_data_fresh"] != "PASS"
        else "HISTORY_IN_PROGRESS" if status["history_progress"]["remaining_batches"]
        else "OBSERVING"
    )
    return status


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    root = args.root.absolute()
    config = validate_config(json.loads((root / "config.json").read_text(encoding="utf-8-sig")), root)
    stop = False
    def request_stop(*_):
        nonlocal stop
        stop = True
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, request_stop)
    failures, completed, started = 0, 0, time.monotonic()
    with singleton(root):
        qmt = QmtReader(root, config)
        universe, sessions, discovery_date = [], None, None
        while not stop:
            cycle_start = time.monotonic()
            now = datetime.now(CHINA).replace(tzinfo=None)
            connection = None
            try:
                connection = connect_database(config["mysql"])
                store = Store(connection)
                if discovery_date != now.date():
                    qmt.capabilities()
                    universe = qmt.universe()
                    qmt.watchlist(universe)
                    qmt.control_probe(universe)
                    sessions = qmt.calendar(now)
                    closed = [day for day in sessions if day < now.date().isoformat()
                              or (now.hour, now.minute) >= (15, 10)]
                    if not closed:
                        raise RuntimeError("SECONDARY_CLOSED_TRADING_SESSIONS_EMPTY")
                    store.plan(closed[-config["history_days"]:], sample_codes(universe, config["sample_size"]), now)
                    discovery_date = now.date()
                status = run_cycle(store, qmt, config, now=None, universe=universe, sessions=sessions)
                now = datetime.fromisoformat(status["captured_at"])
                status["source_clone"] = {"status": "NOT_CONFIGURED"}
                if config.get("source_db"):
                    clone = connect_database(config["source_db"])
                    try:
                        status["source_clone"] = clone_availability(clone)
                    finally:
                        clone.close()
                    if status["source_clone"]["status"] != "AVAILABLE":
                        status["status"] = "DEGRADED"
                completed += 1
                failures = 0 if status["status"] == "READY" else failures + 1
                status.update({"completed_cycles": completed, "consecutive_failures": failures,
                               "uptime_seconds": round(time.monotonic() - started, 1)})
                store.summary(status, now)
            except Exception as exc:
                if connection is not None:
                    connection.rollback()
                failures += 1
                status = {"schema": "probiga.secondary-edge-acceptance.v1", "candidate_only": True,
                          "production_promoted": False, "status": "BLOCKED", "captured_at": now.isoformat(),
                          "error": safe_error(exc), "consecutive_failures": failures,
                          "completed_cycles": completed, "market_data_fresh": "NOT_PASSED"}
            finally:
                if connection is not None:
                    connection.close()
            atomic_status(root, status)
            if args.once:
                return 0 if status["status"] == "READY" else 2
            deadline = cycle_start + config["poll_seconds"]
            while not stop and time.monotonic() < deadline:
                time.sleep(min(1, max(0, deadline - time.monotonic())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
