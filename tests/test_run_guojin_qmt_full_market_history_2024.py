from __future__ import annotations

from types import SimpleNamespace

import pytest

from tools import run_guojin_qmt_full_market_history_2024 as history_job


class _Engine:
    url = SimpleNamespace(database="test")


def _prepare_job(monkeypatch, *, expected: set[str], local_snapshots: list[set[str]]):
    source_engine = _Engine()
    local_engine = _Engine()
    snapshots = iter(local_snapshots)

    monkeypatch.setattr(history_job, "_source_engine", lambda: source_engine)
    monkeypatch.setattr(history_job, "get_local_history_engine", lambda: local_engine)
    monkeypatch.setattr(history_job, "ensure_local_history_tables", lambda _engine: None)
    monkeypatch.setattr(history_job, "load_stock_codes", lambda _engine: sorted(expected))
    monkeypatch.setattr(
        history_job,
        "load_trade_dates",
        lambda _engine, *, start_date, end_date: ["2026-08-21"],
    )

    def daily_stock_set(engine, *, table, trade_date, require_native_qmt):
        assert trade_date == "2026-08-21"
        if engine is source_engine:
            assert table == history_job.PRODUCTION_KLINE_TABLE
            assert require_native_qmt is False
            return set(expected)
        assert engine is local_engine
        assert table == history_job.LOCAL_KLINE_TABLE
        assert require_native_qmt is True
        return set(next(snapshots))

    monkeypatch.setattr(history_job, "_daily_stock_set", daily_stock_set)
    return source_engine, local_engine


def _run(tmp_path, *, resume: bool = True):
    return history_job.run_full_history(
        start_date="2026-08-21",
        end_date="2026-08-21",
        modes={"daily"},
        daily_batch_size=120,
        minute_batch_size=80,
        sleep_seconds=0,
        resume=resume,
        log_path=tmp_path / "history.jsonl",
    )


def test_daily_resume_skips_only_when_native_qmt_stock_set_is_exact(monkeypatch, tmp_path):
    expected = {"000001", "300001", "600000"}
    _prepare_job(monkeypatch, expected=expected, local_snapshots=[expected])

    def unexpected_backfill(**_kwargs):
        raise AssertionError("exact daily coverage must skip backfill")

    monkeypatch.setattr(history_job, "backfill_daily_kline_local", unexpected_backfill)

    result = _run(tmp_path)

    assert result["daily_trade_days_done"] == 0
    assert result["errors"] == 0
    assert '"coverage": "exact_stock_set"' in (tmp_path / "history.jsonl").read_text(encoding="utf-8")


def test_daily_resume_does_not_skip_at_eighty_percent_and_rechecks_exact_set(
    monkeypatch,
    tmp_path,
):
    expected = {f"6000{index:02d}" for index in range(10)}
    eighty_percent = set(sorted(expected)[:8])
    _prepare_job(
        monkeypatch,
        expected=expected,
        local_snapshots=[eighty_percent, expected],
    )
    calls = []

    def backfill(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(fetched_rows=10, written_rows=10, batch_count=1)

    monkeypatch.setattr(history_job, "backfill_daily_kline_local", backfill)

    result = _run(tmp_path)

    assert len(calls) == 1
    assert result["daily_trade_days_done"] == 1
    assert result["errors"] == 0
    assert '"verified_rows": 10' in (tmp_path / "history.jsonl").read_text(encoding="utf-8")


def test_daily_backfill_hard_fails_when_exact_stock_set_is_not_restored(monkeypatch, tmp_path):
    expected = {"000001", "300001", "600000"}
    incomplete = {"000001", "600000"}
    _prepare_job(
        monkeypatch,
        expected=expected,
        local_snapshots=[incomplete, incomplete],
    )
    monkeypatch.setattr(
        history_job,
        "backfill_daily_kline_local",
        lambda **_kwargs: SimpleNamespace(fetched_rows=2, written_rows=2, batch_count=1),
    )

    with pytest.raises(RuntimeError, match="coverage mismatch after backfill") as exc_info:
        _run(tmp_path)

    assert "missing=1" in str(exc_info.value)
    assert "300001" in str(exc_info.value)


def test_daily_run_hard_fails_when_production_target_set_is_empty(monkeypatch, tmp_path):
    _prepare_job(monkeypatch, expected=set(), local_snapshots=[])
    monkeypatch.setattr(
        history_job,
        "backfill_daily_kline_local",
        lambda **_kwargs: pytest.fail("empty production target cannot be backfilled safely"),
    )

    with pytest.raises(RuntimeError, match="production daily target set is empty"):
        _run(tmp_path)


def test_pid_alive_checks_current_process_without_signalling_it():
    assert history_job._pid_alive(history_job.os.getpid()) is True
    assert history_job._pid_alive(-1) is False


def test_history_job_lock_is_atomic_and_owned(tmp_path):
    lock_path = tmp_path / "history.lock"

    acquired, owner = history_job._acquire_lock(lock_path)
    second_acquired, second_owner = history_job._acquire_lock(lock_path)

    assert acquired is True
    assert owner == ""
    assert second_acquired is False
    assert second_owner.startswith(str(history_job.os.getpid()))
    history_job._release_lock(lock_path)
    assert not lock_path.exists()


def test_history_job_lock_never_unlinks_a_fresh_initializing_owner(tmp_path):
    lock_path = tmp_path / "history.lock"
    lock_path.write_bytes(b"")

    acquired, owner = history_job._acquire_lock(lock_path)

    assert acquired is False
    assert owner == "lock_initializing"
    assert lock_path.exists()
