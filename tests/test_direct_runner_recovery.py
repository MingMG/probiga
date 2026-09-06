"""Three runner/QMT boundaries using real temporary transport, never production."""
from datetime import datetime, timedelta
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from acquisition.models import WorkUnit
from acquisition.datasets import get_spec
from acquisition.qmt_model import SHANGHAI, MAX_REQUEST_BYTES, MAX_RESULT_BYTES, publish_json
from acquisition.qmt_transport import QmtTransport
from acquisition.runner import Runner, make_request


NOW = datetime(2026, 9, 4, 16, 5, tzinfo=SHANGHAI)
CODE = "000001.SZ"
UNIT = WorkUnit("stock_daily", "guojin_qmt", "2026-09-04", CODE, "1d", "none")


class FakeStore:
    """Only the transaction outcome contract; DB behavior is tested elsewhere."""
    def __init__(self):
        self.units = {}
        self.new_writes = 0
        self.begin_calls = 0

    def catalog(self, asset):
        return {CODE: {"qmt_code": CODE, "short_name": "fixture", "asset_class": asset}}

    def validate_spec(self, spec):
        return

    def retrying_sources(self, now):
        return []

    def begin_request(self, units, request_id, now):
        self.begin_calls += 1
        for unit in units:
            previous = self.units.get(unit.partition_key)
            if previous and previous["request_id"] == request_id:
                continue
            self.units[unit.partition_key] = {"request_id": request_id, "status": "running"}

    def commit(self, spec, batch):
        counts = {"complete": 0, "no_data": 0, "error": 0, "replayed": 0}
        for result in batch.units:
            current = self.units[result.unit.partition_key]
            assert current["request_id"] == batch.request_id
            if current["status"] == "complete":
                counts["replayed"] += 1
                continue
            assert result.status == "complete", (result.error_code, result.error)
            self.new_writes += len(result.rows)
            current["status"] = "complete"
            counts["complete"] += 1
        return counts


def runner_at(tmp_path, store):
    config = SimpleNamespace(
        state_dir=tmp_path,
        normalization=lambda catalog: {
            "catalog": catalog,
            "volume_factors": {("ContextInfo.get_market_data_ex", "1d", "stock"):
                               {"volume": "1", "amount": "1"}},
        },
    )
    runner = Runner(config, clock=lambda: NOW)
    runner._stores = {"primary": store, "history": store}
    return runner


def activated(tmp_path, store):
    transport = QmtTransport(str(tmp_path / "qmt"))
    plan = make_request([UNIT], NOW - timedelta(minutes=10), timeout=180)
    transport.prepare(plan)
    store.begin_request([UNIT], plan["request_id"], NOW - timedelta(minutes=10))
    transport.activate(plan["request_id"])
    return transport, plan


def result_for(plan):
    return {"request": plan, "received_at": NOW.isoformat(),
            "source_method": "ContextInfo.get_market_data_ex",
            "outcomes": {CODE: {"status": "data", "rows": [{
                "qmt_code": CODE, "time": "20260904150000", "open": 11,
                "high": 13, "low": 10, "close": 12, "preClose": 11,
                "volume": 100, "amount": 1200,
            }]}}}


def write_ready(transport, result):
    path = Path(transport.root) / (result["request"]["request_id"] + ".ready.json")
    publish_json(str(path), result, MAX_RESULT_BYTES, immutable=True)


def test_stale_stopped_model_is_rejected_before_running_state_or_active_file(tmp_path):
    store = FakeStore()
    runner = runner_at(tmp_path, store)
    transport = runner._qmt_transport()
    publish_json(str(Path(transport.root) / "heartbeat.json"), {
        "status": "stopped", "updated_at": (NOW - timedelta(hours=2)).isoformat(),
        "pid": 1234, "instance_id": "old-model", "active_request_id": None,
        "error_code": None,
    }, MAX_REQUEST_BYTES)
    # Budget zero makes an accidental native wait instantaneous in this test.
    with pytest.raises(RuntimeError, match="QMT_MODEL_UNAVAILABLE"):
        runner.acquire([UNIT], budget_remaining=0)
    assert store.begin_calls == 0
    assert transport.recover()["active"] is None
    assert transport.recover()["prepared"] == []


def test_latest_alist_target_uses_trade_calendar_on_weekend(tmp_path):
    store = FakeStore()
    saturday = NOW + timedelta(days=1, hours=3)
    calendar = {(saturday.date() - timedelta(days=offset)).isoformat(): 0
                for offset in range(31)}
    calendar["2026-09-04"] = 1
    store.calendar = lambda *args: calendar
    runner = runner_at(tmp_path, store)
    runner.clock = lambda: saturday
    assert runner._target(get_spec("alist_daily")) == "2026-09-04"
    assert runner._target(get_spec("alist_detail")) == "2026-09-04"


def test_commit_then_partial_archive_recovers_without_duplicate_business_write(tmp_path, monkeypatch):
    store = FakeStore()
    transport, plan = activated(tmp_path, store)
    result = result_for(plan)
    write_ready(transport, result)
    runner = runner_at(tmp_path, store)
    runner._consume(result)
    assert store.new_writes == 1
    original = os.unlink
    def fail_final_release(path, *args, **kwargs):
        if os.fspath(path) == str(Path(transport.root) / "active.json"):
            raise OSError("simulated exit immediately before active removal")
        return original(path, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(os, "unlink", fail_final_release)
        with pytest.raises(OSError):
            transport.archive(plan["request_id"])
    resumed = runner_at(tmp_path, store)
    assert resumed.recover_qmt() is True
    assert store.new_writes == 1
    assert resumed._qmt_transport().recover()["active"] is None


def test_wait_timeout_keeps_original_request_and_late_ready_resumes_on_next_start(tmp_path):
    store = FakeStore()
    transport, plan = activated(tmp_path, store)
    with pytest.raises(TimeoutError):
        transport.wait_result(plan["request_id"], timeout=0)
    resumed = runner_at(tmp_path, store)
    assert resumed.recover_qmt() is False
    assert store.begin_calls == 1 and store.new_writes == 0
    assert transport.recover()["active"] == plan
    write_ready(transport, result_for(plan))
    assert runner_at(tmp_path, store).recover_qmt() is True
    assert store.new_writes == 1
    assert store.units[UNIT.partition_key]["request_id"] == plan["request_id"]
    assert transport.recover()["active"] is None


def test_live_commit_failure_can_resume_after_native_snapshot_is_replaced(tmp_path, monkeypatch):
    """A replaced snapshot must not strand the prior DB-owned tick forever."""
    from acquisition.store import StaleRequest

    class StrictStore(FakeStore):
        def begin_request(self, units, request_id, now):
            for unit in units:
                previous = self.units.get(unit.partition_key)
                if previous and previous["status"] == "running" and previous["request_id"] != request_id:
                    raise StaleRequest("unfinished request must be recovered before a replacement")
            super().begin_request(units, request_id, now)

        def calendar(self, start, end):
            return {"2026-09-04": 1}

    live_now = NOW.replace(hour=10, minute=0)
    store = StrictStore()
    runner = runner_at(tmp_path, store)
    runner.clock = lambda: live_now
    runner.config.require_writes = lambda: None
    runner.config.data = {"datasets": ["stock_current"]}
    transport = runner._qmt_transport()
    tick = WorkUnit("stock_current", "guojin_qmt", "2026-09-04", CODE, "tick", "none")
    plan = make_request([tick], live_now)
    first = result_for(plan)
    first["received_at"] = live_now.isoformat()
    path = str(Path(transport.root) / "stock_current.snapshot.json")
    publish_json(path, first, MAX_RESULT_BYTES)

    def failed_consume(raw):
        raise OSError("simulated transient DB commit failure")

    monkeypatch.setattr(runner, "_consume", failed_consume)
    with pytest.raises(OSError):
        runner.live_once()
    newer = result_for(make_request([tick], live_now + timedelta(seconds=15)))
    newer["received_at"] = (live_now + timedelta(seconds=15)).isoformat()
    publish_json(path, newer, MAX_RESULT_BYTES)
    runner.clock = lambda: live_now + timedelta(seconds=15)
    consumed = []

    def successful_consume(raw):
        consumed.append(raw["request"]["request_id"])
        store.units[tick.partition_key]["status"] = "complete"
        return {"complete": 1}

    monkeypatch.setattr(runner, "_consume", successful_consume)
    runner.live_once()
    assert consumed, "the live partition must resume without manual DB edits"


def test_source_failure_stops_other_products_of_same_source_this_run(tmp_path, monkeypatch):
    store = FakeStore()
    store.states = lambda *args: []
    store.calendar = lambda *args: {"2026-09-04": 1}
    runner = runner_at(tmp_path, store)
    runner.config.require_writes = lambda: None
    runner.config.data = {"start_date": "2026-09-04"}
    monkeypatch.setattr(runner, "recover_http", lambda: None)
    monkeypatch.setattr(runner, "recover_qmt", lambda: True)
    monkeypatch.setattr(runner, "_target", lambda *args: "2026-09-04")
    monkeypatch.setattr(runner, "status", lambda *args: {"status": "partial"})
    called = []

    def unavailable(units, remaining):
        called.append(units[0].dataset)
        return {"error": len(units), "error_codes": ["SOURCE_UNAVAILABLE"]}

    monkeypatch.setattr(runner, "acquire", unavailable)
    runner.run(["stock_daily", "index_daily"])
    assert called == ["stock_daily"], "one failed source must not be retried through the next product"


def test_retired_source_cooldown_does_not_block_current_qmt_source(tmp_path, monkeypatch):
    store = FakeStore()
    store.states = lambda *args: []
    store.calendar = lambda *args: {"2026-09-04": 1}
    store.retrying_sources = lambda _now: [{
        "dataset": "stock_daily", "source": "eastmoney",
        "next_retry_at": NOW + timedelta(minutes=5),
    }]
    runner = runner_at(tmp_path, store)
    runner.config.require_writes = lambda: None
    runner.config.data = {"start_date": "2026-09-04"}
    monkeypatch.setattr(runner, "recover_http", lambda: None)
    monkeypatch.setattr(runner, "recover_qmt", lambda: True)
    monkeypatch.setattr(runner, "_target", lambda *args: "2026-09-04")
    monkeypatch.setattr(runner, "status", lambda *args: {"status": "partial"})
    called = []
    monkeypatch.setattr(runner, "acquire", lambda units, _remaining: (
        called.append(units[0].dataset) or
        {"complete": len(units), "error": 0, "no_data": 0, "error_codes": []}
    ))
    runner.run(["stock_daily"])
    assert called == ["stock_daily"]


def test_one_bad_security_does_not_withhold_later_batches(tmp_path, monkeypatch):
    store = FakeStore()
    store.states = lambda *args: []
    store.calendar = lambda *args: {"2026-09-04": 1}
    store.catalog = lambda _asset: {
        f"{number:06d}.SZ": {"qmt_code": f"{number:06d}.SZ", "list_date": "2020-01-01"}
        for number in range(1, 42)
    }
    runner = runner_at(tmp_path, store)
    runner.config.require_writes = lambda: None
    runner.config.data = {"start_date": "2026-09-04"}
    monkeypatch.setattr(runner, "recover_http", lambda: None)
    monkeypatch.setattr(runner, "recover_qmt", lambda: True)
    monkeypatch.setattr(runner, "_target", lambda *args: "2026-09-04")
    monkeypatch.setattr(runner, "status", lambda *args: {"status": "partial"})
    batch_sizes = []

    def one_malformed(units, remaining):
        batch_sizes.append(len(units))
        return ({"complete": 39, "error": 1, "error_codes": ["INVALID_RESPONSE"]}
                if len(batch_sizes) == 1 else {"complete": 1, "error": 0, "error_codes": []})

    monkeypatch.setattr(runner, "acquire", one_malformed)
    runner.run(["stock_daily"])
    assert batch_sizes == [40, 1]
