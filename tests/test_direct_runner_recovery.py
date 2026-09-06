"""Three runner/QMT boundaries using real temporary transport, never production."""
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from acquisition.config import DirectEtfWriterDisabled
from acquisition.models import WorkUnit, key_fingerprint
from acquisition.datasets import get_spec
from acquisition.qmt_model import SHANGHAI, MAX_REQUEST_BYTES, MAX_RESULT_BYTES, publish_json
from acquisition.qmt_transport import QmtTransport
from acquisition.runner import Runner, make_request


NOW = datetime(2026, 9, 4, 16, 5, tzinfo=SHANGHAI)
CODE = "000001.SZ"
UNIT = WorkUnit("stock_daily", "guojin_qmt", "2026-09-04", CODE, "1d", "none")
ETF_UNIT = WorkUnit("etf_daily", "guojin_qmt", "2026-09-04", "510300.SH", "1d", "none")


class FakeStore:
    """Only the transaction outcome contract; DB behavior is tested elsewhere."""
    def __init__(self):
        self.units = {}
        self.new_writes = 0
        self.begin_calls = 0
        self.failed_requests = []

    def catalog(self, asset):
        return {CODE: {"qmt_code": CODE, "short_name": "fixture", "asset_class": asset}}

    def validate_spec(self, spec):
        return

    def retrying_sources(self, now):
        return []

    def prune_stale_partition_states(self, _spec, _target_date, _expected_keys):
        return 0

    def begin_request(self, units, request_id, now):
        self.begin_calls += 1
        for unit in units:
            previous = self.units.get(unit.partition_key)
            if previous and previous["request_id"] == request_id:
                continue
            self.units[unit.partition_key] = {"request_id": request_id, "status": "running"}

    def fail_request(self, units, request_id, code, now, retry_seconds=900):
        self.failed_requests.append((request_id, code))
        for unit in units:
            current = self.units.get(unit.partition_key)
            if current and current["request_id"] == request_id and current["status"] == "running":
                current["status"] = "error"

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


class DueStore:
    """Mutable progress/catalog evidence for multi-run due-planner tests."""
    def __init__(self, catalog):
        self.catalog_rows = catalog
        self.progress = []
        self.formal = {}
        self.prune_calls = []

    def catalog(self, _asset):
        return self.catalog_rows

    def calendar(self, *_args):
        return {"2026-09-03": 1, "2026-09-04": 1}

    def retrying_sources(self, _now):
        return []

    @staticmethod
    def _supported(state):
        symbol = state["partition_key"].split(":", 1)[0]
        return symbol.endswith((".SH", ".SZ")) and symbol[:2] in {"00", "30", "60", "68"}

    def _selected(self, dataset, source, start=None, end=None, flow_supported_only=False):
        return [state for state in self.progress
                if state["dataset"] == dataset and state["source"] == source
                and (start is None or state["target_date"] >= start)
                and (end is None or state["target_date"] <= end)
                and (not flow_supported_only or self._supported(state))]

    def state_counts(self, dataset, source, start=None, end=None, *, flow_supported_only=False):
        grouped = {}
        for state in self._selected(dataset, source, start, end, flow_supported_only):
            key = state["target_date"], state["status"]
            grouped[key] = grouped.get(key, 0) + 1
        return [{"source": source, "target_date": day, "status": status, "unit_count": count}
                for (day, status), count in grouped.items()]

    def terminal_fingerprints(self, dataset, source, start, end, *, bare_code=False,
                              statuses=("complete", "no_data"), traded_only=False,
                              flow_supported_only=False):
        grouped = {}
        for state in self._selected(dataset, source, start, end, flow_supported_only):
            if state["status"] not in statuses:
                continue
            if traded_only and json.loads(state.get("detail_json") or "{}").get("traded") is not True:
                continue
            key = state["partition_key"][:6] if bare_code else state["partition_key"]
            grouped.setdefault(state["target_date"], []).append(key)
        return {day: key_fingerprint(keys) for day, keys in grouped.items()}

    def states(self, dataset, target=None):
        return [dict(state) for state in self.progress
                if state["dataset"] == dataset and (target is None or state["target_date"] == target)]

    def prune_stale_partition_states(self, spec, target_date, expected_keys):
        expected = set(expected_keys)
        before = len(self.progress)
        self.progress = [state for state in self.progress if not (
            state["dataset"] == spec.name and state["source"] == spec.source
            and state["target_date"] == target_date and state["status"] != "running"
            and state["partition_key"] not in expected
        )]
        removed = before - len(self.progress)
        self.prune_calls.append((target_date, removed))
        return removed

    def capital_flow_partition_fingerprints(self, spec, start, end):
        empty = key_fingerprint(())
        result = {}
        for day, rows in self.formal.items():
            if not start <= day <= end:
                continue
            result[day] = {
                "all": key_fingerprint(code for code, _source in rows),
                "source": key_fingerprint(code for code, source in rows
                                            if source == spec.persisted_source),
            }
        return result


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


def test_direct_acquire_rejects_disabled_etf_before_any_writer_state(tmp_path):
    store = FakeStore()
    runner = runner_at(tmp_path, store)

    with pytest.raises(DirectEtfWriterDisabled):
        runner.acquire([ETF_UNIT], budget_remaining=0)

    assert store.begin_calls == 0 and store.new_writes == 0
    inventory = runner._qmt_transport().recover()
    assert inventory["active"] is None and inventory["prepared"] == []


def test_direct_consume_rejects_disabled_etf_before_normalize_or_commit(tmp_path):
    store = FakeStore()
    runner = runner_at(tmp_path, store)
    forbidden = make_request([ETF_UNIT], NOW)

    with pytest.raises(DirectEtfWriterDisabled):
        runner._consume({"request": forbidden})

    assert store.begin_calls == 0 and store.new_writes == 0


def test_recovery_archives_disabled_active_etf_and_continues_legal_request(tmp_path):
    store = FakeStore()
    runner = runner_at(tmp_path, store)
    transport = runner._qmt_transport()
    forbidden = make_request([ETF_UNIT], NOW)
    forbidden["request_id"] = "a_disabled_etf"
    transport.prepare(forbidden)
    store.begin_request([ETF_UNIT], forbidden["request_id"], NOW)
    transport.activate(forbidden["request_id"])
    legal = make_request([UNIT], NOW)
    legal["request_id"] = "z_legal_stock"
    transport.prepare(legal)
    write_ready(transport, result_for(legal))

    assert runner.recover_qmt() is True

    assert store.new_writes == 1
    assert store.units[ETF_UNIT.partition_key]["status"] == "error"
    assert store.units[UNIT.partition_key]["status"] == "complete"
    inventory = transport.recover()
    assert inventory["active"] is None and inventory["prepared"] == []
    assert inventory["processed"] == ["a_disabled_etf", "z_legal_stock"]


def test_recovery_archives_disabled_prepared_etf_without_claiming_or_writing_it(tmp_path):
    store = FakeStore()
    runner = runner_at(tmp_path, store)
    transport = runner._qmt_transport()
    forbidden = make_request([ETF_UNIT], NOW)
    forbidden["request_id"] = "a_disabled_etf"
    transport.prepare(forbidden)
    legal = make_request([UNIT], NOW)
    legal["request_id"] = "z_legal_stock"
    transport.prepare(legal)
    write_ready(transport, result_for(legal))

    assert runner.recover_qmt() is True

    assert store.begin_calls == 1 and store.new_writes == 1
    assert ETF_UNIT.partition_key not in store.units
    assert store.units[UNIT.partition_key]["status"] == "complete"
    inventory = transport.recover()
    assert inventory["active"] is None and inventory["prepared"] == []
    assert inventory["processed"] == ["a_disabled_etf", "z_legal_stock"]


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


def test_due_daily_skips_completed_history_and_plans_with_target_day_states(tmp_path, monkeypatch):
    store = FakeStore()
    store.calendar = lambda *_args: {"2026-09-03": 1, "2026-09-04": 1}
    store.state_counts = lambda *_args: [{
        "source": "guojin_qmt", "target_date": "2026-09-03",
        "status": "complete", "unit_count": 1,
    }]
    store.terminal_fingerprints = lambda *_args, **_kwargs: {
        "2026-09-03": key_fingerprint({CODE + ":1d:none"}),
    }
    state_calls = []
    store.states = lambda dataset, target=None: (
        state_calls.append((dataset, target)) or []
    )
    runner = runner_at(tmp_path, store)
    runner.config.require_writes = lambda: None
    runner.config.data = {
        "start_date": "2026-09-03", "datasets": ["stock_daily"],
    }
    monkeypatch.setattr(runner, "recover_http", lambda: None)
    monkeypatch.setattr(runner, "recover_qmt", lambda: True)
    monkeypatch.setattr(runner, "_target", lambda *_args: "2026-09-04")
    monkeypatch.setattr(runner, "status", lambda *_args: {"status": "partial"})
    acquired = []
    monkeypatch.setattr(runner, "acquire", lambda units, _remaining: (
        acquired.extend(unit.target_date for unit in units)
        or {"complete": len(units), "error": 0, "no_data": 0,
            "error_codes": []}
    ))

    runner.run(["stock_daily"], due=True)

    assert acquired == ["2026-09-04"]
    assert state_calls == [("stock_daily", "2026-09-04")]


def test_old_daily_gap_is_not_mistaken_for_overlap_refresh(tmp_path, monkeypatch):
    store = FakeStore()
    days = [
        "2026-08-31", "2026-09-01", "2026-09-02",
        "2026-09-03", "2026-09-04",
    ]
    store.calendar = lambda *_args: {day: 1 for day in days}
    store.state_counts = lambda *_args: [
        {"source": "guojin_qmt", "target_date": day,
         "status": "complete", "unit_count": 1}
        for day in days[1:]
    ]
    store.terminal_fingerprints = lambda *_args, **_kwargs: {
        day: key_fingerprint({CODE + ":1d:none"}) for day in days[1:]
    }
    store.states = lambda *_args: []
    runner = runner_at(tmp_path, store)
    runner.config.require_writes = lambda: None
    runner.config.data = {
        "start_date": days[0], "datasets": ["stock_daily"],
    }
    monkeypatch.setattr(runner, "recover_http", lambda: None)
    monkeypatch.setattr(runner, "recover_qmt", lambda: True)
    monkeypatch.setattr(runner, "_target", lambda *_args: days[-1])
    monkeypatch.setattr(runner, "status", lambda *_args: {"status": "partial"})
    planned = []

    def capture_plan(_spec, target, _catalog, _states, **kwargs):
        planned.append((target, kwargs["refresh"]))
        return []

    monkeypatch.setattr("acquisition.runner.plan_units", capture_plan)
    runner.run(["stock_daily"], due=True)

    assert planned == [("2026-09-04", True), ("2026-08-31", False)]


def test_due_catalog_replacement_prunes_stale_key_and_converges(tmp_path, monkeypatch):
    catalog = {
        "000001.SZ": {"list_date": "2020-01-01"},
        "600000.SH": {"list_date": "2020-01-01"},
    }
    store = DueStore(catalog)
    for day, codes in (("2026-09-03", ("000001.SZ", "300001.SZ")),
                       ("2026-09-04", tuple(catalog))):
        for code in codes:
            store.progress.append({
                "dataset": "stock_daily", "source": "guojin_qmt", "target_date": day,
                "partition_key": code + ":1d:none", "status": "complete",
                "last_success_at": "2026-09-04 16:00:00", "next_retry_at": None,
                "detail_json": '{"traded":true}',
            })
    runner = runner_at(tmp_path, store)
    runner.config.require_writes = lambda: None
    runner.config.data = {"start_date": "2026-09-03", "datasets": ["stock_daily"]}
    monkeypatch.setattr(runner, "recover_http", lambda: None)
    monkeypatch.setattr(runner, "recover_qmt", lambda: True)
    monkeypatch.setattr(runner, "_target", lambda *_args: "2026-09-04")
    monkeypatch.setattr(runner, "status", lambda *_args: {"status": "partial"})
    acquired_old = []

    def acquire(units, _remaining):
        for unit in units:
            if unit.target_date == "2026-09-03":
                acquired_old.append(unit.code)
            store.progress.append({
                "dataset": unit.dataset, "source": unit.source, "target_date": unit.target_date,
                "partition_key": unit.partition_key, "status": "complete",
                "last_success_at": "2026-09-04 16:05:00", "next_retry_at": None,
                "detail_json": '{"traded":true}',
            })
        return {"complete": len(units), "error": 0, "no_data": 0, "error_codes": []}

    monkeypatch.setattr(runner, "acquire", acquire)
    runner.run(["stock_daily"], due=True)
    runner.run(["stock_daily"], due=True)

    assert acquired_old == ["600000.SH"]
    old_keys = {state["partition_key"] for state in store.progress
                if state["target_date"] == "2026-09-03"}
    assert old_keys == {"000001.SZ:1d:none", "600000.SH:1d:none"}
    assert [call for call in store.prune_calls if call[0] == "2026-09-03"] == [
        ("2026-09-03", 1),
    ]


def test_due_historical_flow_drift_marks_then_restages_and_converges(tmp_path, monkeypatch):
    catalog = {"000001.SZ": {"list_date": "2020-01-01"}}
    primary, history = DueStore(catalog), DueStore(catalog)
    for day in ("2026-09-03", "2026-09-04"):
        history.progress.append({
            "dataset": "stock_daily", "source": "guojin_qmt", "target_date": day,
            "partition_key": "000001.SZ:1d:none", "status": "complete",
            "last_success_at": "2026-09-04 16:00:00", "next_retry_at": None,
            "detail_json": '{"traded":true}',
        })
        primary.progress.append({
            "dataset": "capital_flow_daily", "source": "guojin_qmt", "target_date": day,
            "partition_key": "000001.SZ:transactioncount1d:none", "status": "complete",
            "last_success_at": "2026-09-04 16:00:00", "next_retry_at": None,
            "detail_json": '{"published":true}',
        })
    primary.formal = {
        "2026-09-03": [("000001", "wrong_source")],
        "2026-09-04": [("000001", "gj_big_qmt_inner")],
    }
    runner = runner_at(tmp_path, primary)
    runner._stores = {"primary": primary, "history": history, "minute": primary}
    runner.config.require_writes = lambda: None
    runner.config.data = {"start_date": "2026-09-03", "datasets": ["capital_flow_daily"]}
    monkeypatch.setattr(runner, "recover_http", lambda: None)
    monkeypatch.setattr(runner, "recover_qmt", lambda: True)
    monkeypatch.setattr(runner, "_target", lambda *_args: "2026-09-04")
    monkeypatch.setattr(runner, "status", lambda *_args: {"status": "partial"})
    old_publish_states = []

    def acquire(units, _remaining):
        for unit in units:
            state = next(item for item in primary.progress
                         if item["target_date"] == unit.target_date
                         and item["partition_key"] == unit.partition_key)
            state["status"] = "staged"
        return {"complete": len(units), "error": 0, "no_data": 0, "error_codes": []}

    def publish(_spec, day, _expected, _now):
        state = next(item for item in primary.progress
                     if item["target_date"] == day)
        if day == "2026-09-03":
            old_publish_states.append(state["status"])
            if state["status"] == "complete":
                state["status"] = "error"
            elif state["status"] == "staged":
                state["status"] = "complete"
                primary.formal[day] = [("000001", "gj_big_qmt_inner")]
        return {"published": state["status"] == "complete", "missing": 0}

    monkeypatch.setattr(runner, "acquire", acquire)
    primary.publish_capital_flow_day = publish
    runner.run(["capital_flow_daily"], due=True)
    runner.run(["capital_flow_daily"], due=True)
    runner.run(["capital_flow_daily"], due=True)

    assert old_publish_states == ["complete", "staged"]
    assert primary.formal["2026-09-03"] == [("000001", "gj_big_qmt_inner")]


def test_explicit_backfill_still_scans_requested_range(tmp_path, monkeypatch):
    store = FakeStore()
    store.calendar = lambda *_args: {"2026-09-03": 1, "2026-09-04": 1}
    store.states = lambda *_args: []
    runner = runner_at(tmp_path, store)
    runner.config.require_writes = lambda: None
    runner.config.data = {"start_date": "2026-09-03"}
    monkeypatch.setattr(runner, "recover_http", lambda: None)
    monkeypatch.setattr(runner, "recover_qmt", lambda: True)
    monkeypatch.setattr(runner, "status", lambda *_args: {"status": "partial"})
    acquired = []
    monkeypatch.setattr(runner, "acquire", lambda units, _remaining: (
        acquired.extend(unit.target_date for unit in units)
        or {"complete": len(units), "error": 0, "no_data": 0,
            "error_codes": []}
    ))

    runner.run(
        ["stock_daily"], start="2026-09-03", end="2026-09-04",
    )

    assert acquired == ["2026-09-04", "2026-09-03"]


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
