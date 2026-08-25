from __future__ import annotations

from types import SimpleNamespace
import json
import os
import stat

import pytest

from tools import run_guojin_qmt_full_market_history_2024 as history_job
from tools.qmt_operations_task_contract import (
    QMT_FULL_HISTORY_LOCK_PATH,
    QMT_FULL_HISTORY_LOG_PATH,
    QMT_FULL_HISTORY_STATE_ROOT,
    TASKS as QMT_OPERATIONS_TASKS,
)


class _Engine:
    url = SimpleNamespace(database="test")

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _prepare_job(monkeypatch, *, expected: set[str], local_snapshots: list[set[str]]):
    source_engine = _Engine()
    local_engine = _Engine()
    snapshots = iter(local_snapshots)

    monkeypatch.setattr(history_job, "_source_engine", lambda: source_engine)
    monkeypatch.setattr(history_job, "get_local_history_engine", lambda: local_engine)
    monkeypatch.setattr(history_job, "validate_local_history_tables", lambda _engine: None)
    catalog = SimpleNamespace(
        batch_id="catalog-batch",
        manifest_hash="a" * 64,
        eligible_codes=lambda _trade_date: sorted(expected),
    )
    calendar = SimpleNamespace(
        batch_id="calendar-batch",
        manifest_hash="b" * 64,
        sessions_between=lambda _start, _end: ["2026-08-21"],
    )
    monkeypatch.setattr(
        history_job, "load_stock_catalog", lambda _connection, **_kwargs: catalog
    )
    monkeypatch.setattr(
        history_job,
        "load_trade_calendar_receipt",
        lambda _connection, **_kwargs: calendar,
    )

    def local_daily_rows(engine, *, trade_date, source_batch_id=""):
        assert trade_date == "2026-08-21"
        assert engine is local_engine
        assert len(source_batch_id) == 64
        return [
            {
                "stock_code": code,
                "trade_date": trade_date,
                "period": "1d",
                "k_type": 1,
                "adjust_type": 0,
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 100,
                "amount": 1000,
                "pre_close": 10,
                "pre_close_origin": "NATIVE_QMT",
                "provider": history_job.BIGQMT_PROVIDER_ID,
                "batch_id": source_batch_id,
            }
            for code in sorted(next(snapshots))
        ]

    monkeypatch.setattr(history_job, "_local_daily_rows", local_daily_rows)
    monkeypatch.setattr(
        history_job,
        "_insert_coverage",
        lambda _engine, bundle: {
            "status": "inserted",
            "manifest_hash": bundle["manifest"]["manifest_hash"],
        },
    )
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
    assert '"coverage": "certified_exact"' in (tmp_path / "history.jsonl").read_text(encoding="utf-8")


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


def test_daily_run_hard_fails_when_catalog_target_set_is_empty(monkeypatch, tmp_path):
    _prepare_job(monkeypatch, expected=set(), local_snapshots=[])
    monkeypatch.setattr(
        history_job,
        "backfill_daily_kline_local",
        lambda **_kwargs: pytest.fail("empty catalog target cannot be backfilled safely"),
    )

    with pytest.raises(RuntimeError, match="historical target universe is empty"):
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


def test_frozen_history_task_uses_only_explicit_persistent_runtime_paths():
    task = next(
        item
        for item in QMT_OPERATIONS_TASKS
        if item["task_type"] == "qmt_local_history_2024"
    )

    assert f"--state-root {QMT_FULL_HISTORY_STATE_ROOT}" in task["script_args"]
    assert f"--lock-path {QMT_FULL_HISTORY_LOCK_PATH}" in task["script_args"]
    assert f"--log-path {QMT_FULL_HISTORY_LOG_PATH}" in task["script_args"]
    assert "data/logs" not in task["script_args"]
    assert "data/runtime" not in task["script_args"]


def test_sealed_code_tree_uses_service_owned_state_root(
    monkeypatch,
    tmp_path,
):
    code_root = tmp_path / "sealed-code"
    code_root.mkdir(mode=0o700)
    state_root = tmp_path / "service-state"
    state_root.mkdir(mode=0o700)
    code_root.chmod(0o555)
    monkeypatch.setattr(history_job, "ROOT", code_root)
    lock_path = state_root / "history.lock"
    log_path = state_root / "history.jsonl"

    root, lock, log = history_job._validated_runtime_paths(
        state_root=str(state_root),
        lock_path=str(lock_path),
        log_path=str(log_path),
    )
    history_job._log(log, {"event": "sealed-code-tree-proof"})
    acquired, owner = history_job._acquire_lock(lock)

    assert (root, lock, log) == (state_root, lock_path, log_path)
    assert acquired is True
    assert owner == ""
    if os.name != "nt":
        assert stat.S_IMODE(log.stat().st_mode) == 0o600
        assert stat.S_IMODE(lock.stat().st_mode) == 0o600
    history_job._release_lock(lock)
    code_root.chmod(0o700)


def test_runtime_paths_reject_state_root_inside_sealed_code_tree(
    monkeypatch,
    tmp_path,
):
    code_root = tmp_path / "code"
    code_root.mkdir(mode=0o700)
    inside = code_root / "runtime"
    inside.mkdir(mode=0o700)
    monkeypatch.setattr(history_job, "ROOT", code_root)

    with pytest.raises(RuntimeError, match="inside the code tree"):
        history_job._validated_runtime_paths(
            state_root=str(inside),
            lock_path=str(inside / "history.lock"),
            log_path=str(inside / "history.jsonl"),
        )


@pytest.mark.skipif(os.name == "nt", reason="POSIX ownership/mode contract")
def test_runtime_paths_reject_code_tree_escape_symlink_and_wrong_mode(
    monkeypatch,
    tmp_path,
):
    code_root = tmp_path / "code"
    code_root.mkdir(mode=0o700)
    monkeypatch.setattr(history_job, "ROOT", code_root)
    inside = code_root / "runtime"
    inside.mkdir(mode=0o700)
    with pytest.raises(RuntimeError, match="inside the code tree"):
        history_job._validated_runtime_paths(
            state_root=str(inside),
            lock_path=str(inside / "history.lock"),
            log_path=str(inside / "history.jsonl"),
        )

    outside = tmp_path / "outside"
    outside.mkdir(mode=0o755)
    with pytest.raises(RuntimeError, match="mode 0700"):
        history_job._validated_runtime_paths(
            state_root=str(outside),
            lock_path=str(outside / "history.lock"),
            log_path=str(outside / "history.jsonl"),
        )

    outside.chmod(0o700)
    target = tmp_path / "target"
    target.write_text("outside", encoding="utf-8")
    link = outside / "history.jsonl"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    with pytest.raises(RuntimeError, match="contains a symlink"):
        history_job._validated_runtime_paths(
            state_root=str(outside),
            lock_path=str(outside / "history.lock"),
            log_path=str(link),
        )


def test_windows_mapping_is_fixed_under_absolute_programdata():
    root, lock, log = history_job._windows_state_mapping(
        r"C:\ProgramData"
    )

    assert root == r"C:\ProgramData\ProBigA\qmt-full-market-history"
    assert lock == root + r"\qmt-full-market-history.lock"
    assert log == root + r"\qmt-full-market-history-2024.jsonl"
    with pytest.raises(RuntimeError, match="absolute drive path"):
        history_job._windows_state_mapping(r"relative\ProgramData")


@pytest.mark.parametrize(
    ("owner", "expected_status", "expected_exit"),
    [
        ("4321 2026-08-25T00:00:00", "already_running", 0),
        ("lock_initializing", "already_running", 0),
        ("lock_error:PermissionError", "lock_error", 2),
        ("lock_error:symlink", "lock_error", 2),
        ("stale_lock_could_not_be_replaced", "lock_error", 2),
    ],
)
def test_main_distinguishes_active_lock_from_lock_io_failure(
    monkeypatch,
    capsys,
    tmp_path,
    owner,
    expected_status,
    expected_exit,
):
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    monkeypatch.setattr(history_job, "_source_engine", lambda: object())
    monkeypatch.setattr(
        history_job,
        "_acquire_lock",
        lambda _path: (False, owner),
    )

    exit_code = history_job.main(
        [
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-08-25",
            "--state-root",
            str(state_root),
            "--lock-path",
            str(state_root / "history.lock"),
            "--log-path",
            str(state_root / "history.jsonl"),
            "--json",
        ]
    )

    assert exit_code == expected_exit
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == expected_status
    assert payload["owner"] == owner
