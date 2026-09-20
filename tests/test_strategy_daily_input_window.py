from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from server.common import qmt_daily_market_truth, qmt_trade_calendar
from server.common import strategy_daily_input_window as window
from server.common.qmt_daily_market_truth import QmtDailyMarketTruth
from server.common.qmt_trade_calendar import QmtTradeCalendarReceipt


def _sessions(count=75):
    days = [datetime(2026, 9, 18) - timedelta(days=offset) for offset in reversed(range(150))]
    return [day.date().isoformat() for day in days if day.weekday() < 5][-count:]


def _calendar(days, batch="calendar"):
    return QmtTradeCalendarReceipt(
        batch_id=batch, source_batch_id="source", known_at="2026-09-18 16:00:00",
        start_date=days[0], end_date=days[-1], session_count=len(days),
        session_set_hash="a" * 64, manifest_hash="b" * 64, sessions=tuple(days),
    )


def _truth(day, cutoff, rows=5550):
    return QmtDailyMarketTruth(
        run_id=f"daily-{day}", run_start_date=day, run_end_date=day,
        run_finished_at=f"{day} 17:00:00", decision_known_at=str(cutoff),
        catalog_batch_id=f"catalog-{day}", catalog_manifest_hash="a" * 64,
        catalog_member_set_hash="b" * 64, calendar_batch_id="calendar",
        calendar_manifest_hash="c" * 64, calendar_session_set_hash="d" * 64,
        attested_row_count=rows, requested_sessions=(day,), truth_hash="e" * 64,
    )


def test_recent_narrow_calendar_does_not_hide_known_complete_window(monkeypatch):
    days = _sessions()
    connection = MagicMock()
    del connection.connect
    connection.execute.return_value.mappings.return_value.all.return_value = [
        {"batch_id": "narrow", "start_date": days[-5]},
        {"batch_id": "complete", "start_date": days[0]},
    ]
    calls = []

    def load(_connection, *, batch_id, **kwargs):
        calls.append((batch_id, kwargs["decision_known_at"]))
        return _calendar(days[-5:] if batch_id == "narrow" else days, batch_id)

    monkeypatch.setattr(qmt_trade_calendar, "load_trade_calendar_receipt", load)
    assert window.resolve_daily_input_sessions(
        connection, target_trade_date=days[-1], session_count=70,
        decision_known_at=datetime(2026, 9, 18, 18),
    ) == days[-70:]
    assert [batch for batch, _cutoff in calls] == ["narrow", "complete"]
    assert all(cutoff == "2026-09-18 18:00:00" for _batch, cutoff in calls)


def test_job_validates_each_calendar_partition_once_and_retains_listing_scope(monkeypatch):
    days = _sessions(70)
    connection = MagicMock()
    # Treat this object as an already-open SQL connection, not a fake Engine.
    del connection.connect
    monkeypatch.setattr(qmt_trade_calendar, "load_trade_calendar_window_receipt", lambda *_a, **_k: _calendar(days))
    calls = []

    def load_truth(_connection, *, start_date, end_date, decision_known_at):
        assert start_date == end_date
        calls.append(start_date)
        return _truth(start_date, decision_known_at, rows=5000 if start_date == days[0] else 5550)

    monkeypatch.setattr(qmt_daily_market_truth, "load_qmt_daily_market_truth", load_truth)
    proof = window.load_daily_input_window(
        connection, target_trade_date=days[-1], session_count=70,
        decision_known_at=datetime(2026, 9, 18, 18),
    )
    assert calls == days
    assert proof["session_count"] == 70
    assert proof["daily_attested_row_count"] == 5000 + 69 * 5550
    assert proof["catalog_batches_by_session"][days[0]] == f"catalog-{days[0]}"
    assert len(proof["daily_partition_roots"]) == 70


def test_missing_old_consumed_day_cannot_be_replaced_with_older_observation(monkeypatch):
    days = _sessions(70)
    missing = days[8]
    calls = []

    def load_truth(_connection, *, start_date, decision_known_at, **_kwargs):
        calls.append(start_date)
        if start_date == missing:
            raise RuntimeError("no completed QMT daily attestation")
        return _truth(start_date, decision_known_at)

    monkeypatch.setattr(qmt_daily_market_truth, "load_qmt_daily_market_truth", load_truth)
    with pytest.raises(RuntimeError, match=missing):
        window.validate_daily_input_sessions(
            object(), sessions=days, decision_known_at=datetime(2026, 9, 18, 18),
        )
    assert calls == days[:9]


def test_partition_roots_survive_clock_change_but_not_changed_native_run(monkeypatch):
    days = _sessions(2)
    altered = [False]

    def load_truth(_connection, *, start_date, decision_known_at, **_kwargs):
        truth = _truth(start_date, decision_known_at)
        if altered[0] and start_date == days[0]:
            return QmtDailyMarketTruth(**{**truth.__dict__, "run_id": "repaired-run"})
        return truth

    monkeypatch.setattr(qmt_daily_market_truth, "load_qmt_daily_market_truth", load_truth)
    first = window.validate_daily_input_sessions(object(), sessions=days, decision_known_at=datetime(2026, 9, 18, 18))
    second = window.validate_daily_input_sessions(object(), sessions=days, decision_known_at=datetime(2026, 9, 18, 19))
    assert first["daily_partition_roots"] == second["daily_partition_roots"]
    altered[0] = True
    repaired = window.validate_daily_input_sessions(object(), sessions=days, decision_known_at=datetime(2026, 9, 18, 19))
    assert first["daily_partition_roots"][days[0]] != repaired["daily_partition_roots"][days[0]]
    assert first["daily_partition_roots"][days[1]] == repaired["daily_partition_roots"][days[1]]


def test_daily_truth_after_knowledge_cutoff_blocks_the_job(monkeypatch):
    def blocked(_connection, **_kwargs):
        raise qmt_daily_market_truth.QmtDailySourceAfterCutoff("source capture crossed attestation cutoff")

    monkeypatch.setattr(qmt_daily_market_truth, "load_qmt_daily_market_truth", blocked)
    with pytest.raises(RuntimeError, match="2026-09-18.*cutoff"):
        window.validate_daily_input_sessions(
            object(), sessions=["2026-09-18"], decision_known_at=datetime(2026, 9, 18, 18),
        )


def test_analysis_chase_dates_come_from_the_authoritative_calendar(monkeypatch):
    from biz.analysis import sync_analysis_fast as analysis
    days = _sessions(21)
    monkeypatch.setattr(qmt_trade_calendar, "load_trade_calendar_window_receipt", lambda *_a, **_k: _calendar(days))
    assert analysis._recent_dates(
        object(), "sm_stock_kline", "trade_date", days[-1], 21,
        decision_known_at=datetime(2026, 9, 18, 18),
    ) == list(reversed(days))


def test_preliminary_snapshot_reuse_does_not_skip_current_window_validation(monkeypatch):
    from biz.analysis import sync_analysis_fast as analysis

    def blocked(*_args, **_kwargs):
        raise RuntimeError("missing canonical daily partition")

    monkeypatch.setattr(analysis, "load_daily_input_window", blocked)
    monkeypatch.setattr(analysis, "load_latest_preliminary_analysis_receipt", lambda *_a, **_k: pytest.fail("invalid input window reached snapshot reuse"))
    with pytest.raises(RuntimeError, match="missing canonical daily partition"):
        analysis._prepare_batch_outputs(
            object(), "2026-09-18", 62, 80, reuse_preliminary_snapshot=True,
            publisher_build_sha="a" * 40, news_cutoff_time="2026-09-18T22:00:00",
        )
