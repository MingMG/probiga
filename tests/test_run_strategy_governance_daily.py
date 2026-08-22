from __future__ import annotations

import json
import inspect
import sys
from datetime import datetime

import pytest

from tools import run_strategy_governance_daily as daily
from server.engine import strategy_center as strategy_center_engine
from server.engine import strategy_governance as governance_engine


def test_daily_entrypoint_delegates_build_and_persistence_to_locked_governance():
    source = inspect.getsource(daily.main)
    assert "build_strategy_center_snapshot" not in source
    assert "persist_strategy_center_snapshot" not in source
    assert "strategy_snapshot=" not in source
    assert "strategy_limit=" in source


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _CalendarConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params):
        self.engine.sql = str(statement)
        self.engine.params = dict(params)
        if self.engine.error is not None:
            raise self.engine.error
        return _ScalarResult(self.engine.value)


class _CalendarEngine:
    def __init__(self, value=None, error=None):
        self.value = value
        self.error = error
        self.sql = ""
        self.params = {}
        self.disposed = False

    def connect(self):
        return _CalendarConnection(self)

    def dispose(self):
        self.disposed = True


@pytest.mark.parametrize(
    ("now", "calendar_value", "expected_operator", "expected"),
    [
        (
            datetime(2026, 8, 21, 17, 59, 59),
            "2026-08-20",
            "trade_date < :today",
            "2026-08-20",
        ),
        (
            datetime(2026, 8, 21, 18, 0, 0),
            "2026-08-21",
            "trade_date <= :today",
            "2026-08-21",
        ),
        (
            # Saturday: the calendar itself selects the preceding Friday.
            datetime(2026, 8, 22, 12, 0, 0),
            "2026-08-21",
            "trade_date < :today",
            "2026-08-21",
        ),
        (
            # National Day holiday: the last open date remains authoritative.
            datetime(2026, 10, 1, 22, 35, 0),
            "2026-09-30",
            "trade_date <= :today",
            "2026-09-30",
        ),
    ],
)
def test_authoritative_trade_date_obeys_close_weekend_and_holiday(
    now, calendar_value, expected_operator, expected
):
    engine = _CalendarEngine(calendar_value)
    assert daily.authoritative_closed_trade_date(engine, now=now) == expected
    assert expected_operator in engine.sql
    assert engine.params == {"today": now.date().isoformat()}


def test_every_persist_path_requires_the_authoritative_closed_trade_date(
    monkeypatch,
):
    marker_engine = object()
    monkeypatch.setattr(governance_engine, "get_engine", lambda: marker_engine)
    monkeypatch.setattr(
        governance_engine,
        "authoritative_closed_trade_date",
        lambda engine: "2026-08-21" if engine is marker_engine else "",
    )

    assert (
        governance_engine._require_authoritative_closed_trade_date(
            "2026-08-21"
        )
        == "2026-08-21"
    )
    with pytest.raises(
        governance_engine.GovernanceEvidenceNotReady,
        match="不是权威已收盘交易日",
    ):
        governance_engine._require_authoritative_closed_trade_date(
            "2026-08-22"
        )


def test_persist_guard_blocks_when_authoritative_calendar_is_unavailable(
    monkeypatch,
):
    monkeypatch.setattr(governance_engine, "get_engine", lambda: object())
    monkeypatch.setattr(
        governance_engine,
        "authoritative_closed_trade_date",
        lambda _engine: "",
    )
    with pytest.raises(
        governance_engine.GovernanceEvidenceNotReady,
        match="没有已收盘交易日",
    ):
        governance_engine._require_authoritative_closed_trade_date(
            "2026-08-21"
        )


def test_missing_authoritative_calendar_is_structured_blocked(
    monkeypatch, capsys
):
    engine = _CalendarEngine(None)
    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    assert daily.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["target_trade_date"] == ""
    assert payload["input_trade_date"] == ""
    assert payload["automatic_real_order_submission"] is False
    assert engine.disposed is True


def test_calendar_read_error_is_structured_blocked(monkeypatch, capsys):
    engine = _CalendarEngine(error=RuntimeError("calendar unavailable"))
    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    assert daily.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["target_trade_date"] == ""
    assert payload["input_trade_date"] == ""
    assert "RuntimeError" in payload["reason"]
    assert payload["automatic_real_order_submission"] is False
    assert engine.disposed is True


def test_explicit_trade_date_cannot_bypass_authoritative_day(
    monkeypatch, capsys
):
    engine = _CalendarEngine("2026-08-21")
    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_strategy_governance_daily.py", "--trade-date", "2026-08-20"],
    )

    assert daily.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "status": "blocked",
        "reason": (
            "指定交易日不是权威已收盘交易日"
            "（要求2026-08-21，指定2026-08-20）；"
            "未写入治理状态，模拟资金保持现金"
        ),
        "target_trade_date": "2026-08-21",
        "input_trade_date": "",
        "automatic_real_order_submission": False,
    }
    assert engine.disposed is True


def test_old_self_consistent_snapshot_is_not_accepted_for_target_date():
    snapshot = {
        "trade_date": "2026-08-20",
        "data_date": "2026-08-20",
        "source_status": "fresh",
    }
    reason = daily._input_block_reason(
        snapshot,
        "2026-08-21",
        True,
        "底层票池数据新鲜且日期一致",
    )
    assert "要求2026-08-21" in reason
    assert "实际交易日2026-08-20" in reason


def test_target_date_snapshot_has_no_additional_block_reason():
    snapshot = {
        "trade_date": "2026-08-21",
        "data_date": "2026-08-21",
        "source_status": "fresh",
    }
    assert daily._input_block_reason(
        snapshot,
        "2026-08-21",
        True,
        "底层票池数据新鲜且日期一致",
    ) == ""


def test_governance_window_gap_is_structured_blocked_and_keeps_cash(
    monkeypatch, capsys
):
    engine = _CalendarEngine("2026-08-21")
    base = {
        "trade_date": "2026-08-21",
        "data_date": "2026-08-21",
        "source_status": "fresh",
    }
    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        strategy_center_engine,
        "build_strategy_center_snapshot",
        lambda *_args, **_kwargs: base,
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "persist_strategy_center_snapshot",
        lambda snapshot: snapshot,
    )
    monkeypatch.setattr(
        governance_engine,
        "governance_input_ready",
        lambda _snapshot: (True, "ready"),
    )
    monkeypatch.setattr(
        governance_engine,
        "governance_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(
            governance_engine.GovernanceEvidenceNotReady(
                "QMT权威会话缺少11个交易日"
            )
        ),
    )
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    assert daily.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["target_trade_date"] == "2026-08-21"
    assert payload["input_trade_date"] == "2026-08-21"
    assert "QMT权威会话缺少11个交易日" in payload["reason"]
    assert payload["automatic_real_order_submission"] is False


def test_unexpected_governance_failure_is_not_waived_as_input_not_ready(
    monkeypatch
):
    engine = _CalendarEngine("2026-08-21")
    base = {
        "trade_date": "2026-08-21",
        "data_date": "2026-08-21",
        "source_status": "fresh",
    }
    monkeypatch.setattr(daily, "_load_project_env", lambda: None)
    monkeypatch.setattr(daily, "_create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        strategy_center_engine,
        "build_strategy_center_snapshot",
        lambda *_args, **_kwargs: base,
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "persist_strategy_center_snapshot",
        lambda snapshot: snapshot,
    )
    monkeypatch.setattr(
        governance_engine,
        "governance_input_ready",
        lambda _snapshot: (True, "ready"),
    )
    monkeypatch.setattr(
        governance_engine,
        "governance_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("schema drift must fail the release")
        ),
    )
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    with pytest.raises(RuntimeError, match="schema drift"):
        daily.main()
