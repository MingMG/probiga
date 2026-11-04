from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
import json

import pytest

from tools import crawl_minute_kline as collector
from tests.test_intraday_capital_flow import _context, _replay_database

DAY = "2026-09-11"
CODES = [("000001", 0), ("600000", 1)]


def native():
    return [f"{DAY} {clock},-1,0,3,4,5" for clock in collector.MINUTE_GRID]


def harness(monkeypatch, tmp_path):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    state = {"now": datetime(2026, 9, 11, 16, 0, 5), "interrupted": True,
             "fetched": [], "staged": [], "published": 0}
    monkeypatch.setattr(collector, "_now", lambda: state["now"])
    monkeypatch.setattr(collector, "_create_flow_stage", lambda *args: ("temp", object()))
    def append(connection, stage, rows):
        state["staged"].extend(deepcopy(rows))
        return len(rows)
    def publish(*args):
        state["published"] += 1
        return len(state["staged"])
    monkeypatch.setattr(collector, "_append_flow_stage", append)
    monkeypatch.setattr(collector, "_publish_flow_stage", publish)
    monkeypatch.setattr(collector, "_drop_flow_stage", lambda *args: None)
    @contextmanager
    def fetch(codes, fetcher):
        state["fetched"].append(list(codes))
        def responses():
            for index, (code, market) in enumerate(codes):
                yield code, native(), state["now"]
                if state["interrupted"] and index == 0:
                    raise RuntimeError("observed transport interruption after one complete security")
        yield responses()
    monkeypatch.setattr(collector, "_minute_fetch_workers", fetch)
    return state


def context(clock="16:00:00"):
    return {**_context(clock=clock), "catalog_member_set_hash": "d" * 64}


def collect(ctx):
    return collector._collect_minutes(object(), CODES, kind="flow", table=collector.FLOW_TABLE,
        trade_date=DAY, min_coverage=1.0, context=ctx, no_trade_codes=set())


def recovered(monkeypatch, tmp_path):
    state = harness(monkeypatch, tmp_path)
    with pytest.raises(RuntimeError, match="transport interruption"):
        collect(context())
    assert state["published"] == 0
    store = collector._checkpoint_store("flow", DAY, context(), [code for code, _ in CODES])
    assert store.load("000001")["captured_at"] == DAY + " 16:00:05"
    assert store.load("600000") is None
    assert store.load("__progress__")["completed_units"] == 1
    state.update(now=datetime(2026, 9, 11, 16, 2, 5), interrupted=False, staged=[])
    result = collect(context("16:02:00"))
    return state, result, store


def test_retry_restores_real_closed_source_without_refetch_or_retimestamp(monkeypatch, tmp_path):
    state, result, store = recovered(monkeypatch, tmp_path)
    assert state["fetched"] == [CODES, [CODES[1]]]
    assert result["schema"] == "probiga.public-minute-collection.v2"
    assert result["checkpoint_reused_count"] == 1
    assert result["source_capture_min_at"] == DAY + " 16:00:05"
    assert result["source_capture_max_at"] == DAY + " 16:02:05"
    assert {row["received_at"] for row in state["staged"] if row["stock_code"] == "000001"} == {datetime(2026, 9, 11, 16, 0, 5)}
    assert collector.validate_result(result) == "success"
    assert store.load("__progress__")["total_units"] == 2
    assert store.load("__progress__")["completed_units"] == 2


def test_checkpoint_and_real_database_must_replay_the_same_old_source_capture(monkeypatch, tmp_path):
    state, result, store = recovered(monkeypatch, tmp_path)
    primary, target, load = _replay_database(monkeypatch, result, state["staged"])
    load.return_value[0].member_set_hash = "d" * 64
    try:
        assert collector.replay_result(result, primary, started_at=datetime(2026, 9, 11, 16, 2),
                                       now=datetime(2026, 9, 11, 16, 3)) == "success"
        path = store._path("000001")
        altered = json.loads(path.read_text())
        altered["payload"]["native"][0] = f"{DAY} 09:31,123,0,3,4,5"
        path.write_text(json.dumps(altered))
        with pytest.raises(ValueError, match="checkpoint and database"):
            collector.replay_result(result, primary, started_at=datetime(2026, 9, 11, 16, 2),
                                    now=datetime(2026, 9, 11, 16, 3))
    finally:
        primary.dispose()
        target.dispose()


def test_old_capture_cannot_be_accepted_without_its_durable_scope(monkeypatch, tmp_path):
    _, result, _ = recovered(monkeypatch, tmp_path)
    changed = dict(result, checkpoint_scope=None, source_checkpoints_sha256=None,
                   checkpoint_reused_count=0)
    with pytest.raises(ValueError, match="predates attempt"):
        collector.validate_result(collector._seal_result(changed))


def test_mutable_intraday_prefix_never_creates_a_closed_source_checkpoint(monkeypatch, tmp_path):
    state = harness(monkeypatch, tmp_path)
    state["now"] = datetime(2026, 9, 11, 14, 0)
    assert collector._checkpoint_store("flow", DAY, context("14:00:00"), ["000001"]) is None
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("collection_minutes,publication_hours", [(46, 0), (1, 7)])
def test_completed_flow_is_published_after_slow_collection_or_database_work(
    monkeypatch, tmp_path, collection_minutes, publication_hours,
):
    state = harness(monkeypatch, tmp_path)
    state["interrupted"] = False
    original_fetch = collector._minute_fetch_workers
    original_publish = collector._publish_flow_stage
    @contextmanager
    def slow_fetch(*args):
        with original_fetch(*args) as rows:
            yield rows
        state["now"] += timedelta(minutes=collection_minutes)
    def slow_publish(*args):
        count = original_publish(*args)
        state["now"] += timedelta(hours=publication_hours)
        return count
    monkeypatch.setattr(collector, "_minute_fetch_workers", slow_fetch)
    monkeypatch.setattr(collector, "_publish_flow_stage", slow_publish)
    result = collect(context())
    assert state["published"] == 1
    assert result["written_rows"] == 480
    assert collector.validate_result(result) == "success"
    invalid = dict(result, collection_finished_at=DAY + " 15:59:00")
    with pytest.raises(ValueError, match="execution time"):
        collector.validate_result(collector._seal_result(invalid))
