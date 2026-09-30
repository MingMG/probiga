from datetime import datetime, timedelta
import json

import pytest

from tools.secondary_edge import collector as c


BUILD = "a" * 40
NOW = datetime(2026, 9, 30, 10, 0, 0)


def config(tmp_path):
    return {
        "mysql": {"host": "127.0.0.1", "port": 33085, "database": "probiga_secondary",
                  "user": "secondary_collector", "password": "private-secret",
                  "ssl_ca": str(tmp_path / "ca.pem")},
        "qmt_home": str(tmp_path / "qmt"), "expected_build_sha": BUILD,
    }


@pytest.mark.parametrize("key,value", [
    ("host", "localhost"), ("host", "remote.example"), ("port", 3306),
    ("port", 13306), ("database", "probiga"), ("database", "probiga_qmt_history"),
    ("user", "root"), ("user", "probiga_runtime"), ("password", ""),
])
def test_rejects_production_or_nonprivate_config(tmp_path, key, value):
    value_config = config(tmp_path)
    value_config["mysql"][key] = value
    with pytest.raises(ValueError):
        c.validate_config(value_config, tmp_path)


def test_default_config_and_clone_are_only_local(tmp_path):
    value_config = c.validate_config(config(tmp_path), tmp_path)
    assert value_config["sample_size"] == 50
    assert value_config["history_days"] == 5
    assert value_config["poll_seconds"] == 60
    value_config["source_db"] = {**value_config["mysql"], "database": "probiga"}
    c.validate_config(value_config, tmp_path)
    value_config["source_db"]["port"] = 3306
    with pytest.raises(ValueError, match="PRIVATE_CLONE"):
        c.validate_config(value_config, tmp_path)


def quote(code="000001.SZ", **changes):
    return {"qmt_code": code, "source_time": NOW - timedelta(seconds=4),
            "received_at": NOW - timedelta(seconds=2), "price": 10.3,
            "volume": 100, "amount": 1000, **changes}


@pytest.mark.parametrize("changes", [
    {"source_time": None}, {"received_at": None}, {"price": 0}, {"price": True},
    {"price": float("nan")}, {"volume": -1}, {"amount": float("inf")},
    {"source_time": NOW + timedelta(seconds=30)},
])
def test_invalid_native_quotes_are_not_fabricated(changes):
    assert c.normalize_quotes([quote(**changes)], now=NOW) == []


def test_quote_normalization_preserves_native_source_and_deduplicates():
    rows = c.normalize_quotes([
        quote(source_time=NOW - timedelta(minutes=5)),
        quote(source_time=(NOW - timedelta(seconds=4)).isoformat()),
    ], now=NOW)
    assert len(rows) == 1
    assert rows[0]["source_time"] == NOW - timedelta(seconds=4)
    assert rows[0]["received_at"] != NOW


def metrics(rows, market_open=True, heartbeat_seconds=2):
    stamp = NOW.replace(tzinfo=c.CHINA).timestamp()
    return c.quote_metrics(rows, ["000001.SZ", "600000.SH"], now=NOW,
                           market_open=market_open,
                           heartbeat={"status": "running", "updated_ts": stamp - heartbeat_seconds},
                           generated_ts=stamp - 2)


def test_newly_published_old_quotes_fail_freshness_and_report_source_time():
    old = NOW - timedelta(days=1)
    rows = c.normalize_quotes([quote(source_time=old), quote("600000.SH", source_time=old)], now=NOW)
    result = metrics(rows)
    assert result["coverage_ratio"] == 1
    assert result["transport_ready"]
    assert result["market_data_fresh"] == "FAIL"
    assert result["fresh_ratio"] == 0
    assert result["latest_source_time"] == old.isoformat()


def test_full_fresh_coverage_passes_and_partial_does_not():
    rows = c.normalize_quotes([quote(), quote("600000.SH")], now=NOW)
    assert metrics(rows)["market_data_fresh"] == "PASS"
    partial = metrics(rows[:1])
    assert partial["market_data_fresh"] == "FAIL"
    assert partial["missing_code_sample"] == ["600000.SH"]
    assert metrics(rows, heartbeat_seconds=36)["market_data_fresh"] == "FAIL"


def test_closed_market_and_unknown_calendar_never_pass_freshness():
    assert metrics([], market_open=False)["market_data_fresh"] == "NOT_EVALUATED_MARKET_CLOSED"
    assert metrics([], market_open=None)["market_data_fresh"] == "UNKNOWN_TRADING_CALENDAR"


def test_sampling_spans_universe_and_zero_means_full():
    universe = [f"{i:06d}.SH" for i in range(100)]
    assert c.sample_codes(universe, 4) == [universe[i] for i in (0, 25, 50, 75)]
    assert c.sample_codes(universe, 0) == universe


class Cursor:
    def __init__(self, connection):
        self.connection = connection
    def __enter__(self):
        return self
    def __exit__(self, *_):
        return False
    def execute(self, sql, params=None):
        self.connection.calls.append((sql, params))
        if self.connection.fail_update and "status='SUCCESS'" in sql:
            raise RuntimeError("DB failure including private-secret")
    def executemany(self, sql, params):
        self.connection.calls.append((sql, list(params)))
    def fetchone(self):
        return self.connection.row


class Connection:
    def __init__(self, *, port=33085, database="probiga_secondary", fail_update=False):
        self.row = {"port": port, "database_name": database}
        self.calls = []
        self.commits = self.rollbacks = 0
        self.fail_update = fail_update
    def cursor(self):
        return Cursor(self)
    def commit(self):
        self.commits += 1
    def rollback(self):
        self.rollbacks += 1


def test_bootstrap_refuses_other_instance_before_any_ddl():
    connection = Connection(port=3306)
    with pytest.raises(RuntimeError, match="IDENTITY_MISMATCH"):
        c.initialize_schema(connection)
    assert all("CREATE" not in sql for sql, _ in connection.calls)


def test_schema_is_private_and_runtime_only_dml():
    connection = Connection()
    c.initialize_schema(connection)
    assert len([sql for sql, _ in connection.calls if sql.startswith("CREATE")]) == 4
    connection.calls.clear()
    store = c.Store(connection)
    rows = c.normalize_quotes([quote()], now=NOW)
    store.quotes(rows, NOW)
    store.plan(["2026-09-29"], ["000001.SZ"], NOW)
    store.summary({"status": "READY", "candidate_only": True}, NOW)
    for sql, _ in connection.calls:
        assert "secondary_edge_" in sql
        assert not any(word in sql for word in ("CREATE", "ALTER", "DROP", "st_scheduled", "GRANT"))


def history_row(**changes):
    return {"qmt_code": "000001.SZ", "trade_time": "2026-09-29 15:00:00",
            "open": 10, "close": 10.5, "high": 11, "low": 9,
            "volume": 100, "amount": 1000, **changes}


JOB = {"job_key": "daily:2026-09-29:hash", "kind": "daily", "trade_date": "2026-09-29",
       "codes_json": '["000001.SZ"]'}


def test_history_rejects_wrong_date_and_missing_codes():
    with pytest.raises(RuntimeError, match="OUTSIDE_REQUEST"):
        c.normalize_history([history_row(trade_time="2026-09-28 15:00:00")], JOB, ["000001.SZ"])
    with pytest.raises(RuntimeError, match="COVERAGE_INCOMPLETE"):
        c.normalize_history([], JOB, ["000001.SZ"])


def test_history_checkpoint_advances_only_with_committed_rows():
    connection = Connection(fail_update=True)
    store = c.Store(connection)
    rows = c.normalize_history([history_row()], JOB, ["000001.SZ"])
    with pytest.raises(RuntimeError):
        store.history_success(JOB, rows, NOW)
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert "INSERT INTO secondary_edge_history" in connection.calls[0][0]


def test_history_failures_remain_retryable_and_do_not_leak_credentials():
    class MemoryStore:
        def __init__(self):
            self.done, self.errors = False, []
        def pending(self):
            return None if self.done else JOB
        def history_failed(self, job, error, now):
            self.errors.append(error)
        def history_success(self, job, rows, now):
            self.done = True
    class Reader:
        def __init__(self):
            self.attempt = 0
        def history(self, job, codes):
            self.attempt += 1
            if self.attempt == 1:
                raise RuntimeError("private-secret: connection failed")
            return [history_row()]
    store, reader = MemoryStore(), Reader()
    assert c.collect_history(store, reader, NOW)["status"] == "FAILED"
    assert not store.done
    assert store.errors == ["RuntimeError"]
    assert c.collect_history(store, reader, NOW)["status"] == "SUCCESS"
    assert store.done
    assert c.collect_history(store, reader, NOW)["status"] == "IDLE"


def test_run_cycle_closed_market_ready_does_not_mean_freshness_passed():
    class Reader:
        def capabilities(self):
            return {"model_instance_id": "candidate-model"}
        def quotes(self):
            stamp = NOW.replace(tzinfo=c.CHINA).timestamp()
            return [quote(), quote("600000.SH")], {
                "model_instance_id": "candidate-model", "status": "running", "updated_ts": stamp - 2}, stamp - 2
    class MemoryStore:
        def quotes(self, rows, now):
            assert len(rows) == 2
        def pending(self):
            return None
        def progress(self):
            return {"remaining_batches": 0}
    status = c.run_cycle(MemoryStore(), Reader(), {}, now=NOW,
                         universe=["000001.SZ", "600000.SH"], sessions=["2026-09-29"])
    assert status["status"] == "READY"
    assert status["quotes"]["market_data_fresh"] == "NOT_EVALUATED_MARKET_CLOSED"
    assert status["production_promoted"] is False
    assert status["acceptance_status"] == "AWAITING_LIVE_MARKET"


def test_duplicate_collector_fails_and_status_file_has_no_secrets(tmp_path):
    with c.singleton(tmp_path):
        with pytest.raises(RuntimeError, match="ALREADY_RUNNING"):
            with c.singleton(tmp_path):
                pass
    payload = {"status": "BLOCKED", "error": c.safe_error(RuntimeError("private-secret"))}
    c.atomic_status(tmp_path, payload)
    raw = (tmp_path / "status.json").read_text(encoding="utf-8")
    assert "private-secret" not in raw
    assert json.loads(raw) == payload


def test_minute_capture_checks_each_real_batch_envelope_not_synthetic_top_level():
    reader = object.__new__(c.QmtReader)
    reader.identity = {"model_instance_id": "real-model", "strategy_identity_frozen": True,
                       "strategy_identity_status": "BOUND", "strategy_build_sha": BUILD,
                       "strategy_git_blob": "b" * 40, "strategy_source_sha256": "c" * 64,
                       "strategy_loaded_identity_sha256": "d" * 64}
    class Bridge:
        def minute_capture(self, *args, **kwargs):
            return {"status": "ok", "rows": [history_row()],
                    "batch_receipts": [dict(reader.identity)]}
    reader.bridge = Bridge()
    minute = {**JOB, "kind": "minute"}
    assert reader.history(minute, ["000001.SZ"]) == [history_row()]
    reader.identity["model_instance_id"] = "changed-model"
    old = dict(reader.identity, model_instance_id="real-model")
    class ChangedBridge:
        def minute_capture(self, *args, **kwargs):
            return {"rows": [history_row()], "batch_receipts": [old]}
    reader.bridge = ChangedBridge()
    with pytest.raises(RuntimeError, match="IDENTITY_CHANGED"):
        reader.history(minute, ["000001.SZ"])
