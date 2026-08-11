from __future__ import annotations

from datetime import datetime

from server.trading_v2 import repository as repository_module
from server.trading_v2.repository import TradingV2ReadRepository


class _Clock(datetime):
    current = datetime(2026, 7, 30, 10, 30)

    @classmethod
    def now(cls, tz=None):
        return cls.current


class _Repository(TradingV2ReadRepository):
    def __init__(self, state, historical=None):
        self.state = dict(state)
        self.historical = historical

    def _one(self, sql, params=None):
        if "WHERE observed_at <" in sql:
            return self.historical
        return dict(self.state)

    def _all(self, sql, params=None):
        return []

    def _enrich_security_rows(self, rows):
        return rows


def _state(observed_at):
    return {
        "state_id": "state-1",
        "observed_at": observed_at,
        "created_at": observed_at,
        "state": "OBSERVING",
        "quality_status": "PASS",
        "actionable": 1,
        "source_provider": "GJ_BIG_QMT_INNER",
        "coverage": 0.95,
        "evidence": [],
    }


def test_stale_snapshot_is_only_historical_and_never_realtime(
    monkeypatch,
):
    monkeypatch.setattr(repository_module, "datetime", _Clock)
    repo = _Repository(_state(datetime(2026, 7, 30, 10, 27)))

    result = repo.intraday_summary(account_id="paper")

    assert result["status"] == "blocked"
    assert result["current_realtime_state"]["status"] == "STALE"
    assert result["current_realtime_state"]["snapshot"] is None
    assert result["latest_historical_snapshot"]["state_id"] == "state-1"
    assert result["market_state"]["actionable"] is False


def test_live_and_previous_historical_snapshots_are_separate(
    monkeypatch,
):
    monkeypatch.setattr(repository_module, "datetime", _Clock)
    previous = _state(datetime(2026, 7, 30, 10, 20))
    previous["state_id"] = "state-previous"
    repo = _Repository(
        _state(datetime(2026, 7, 30, 10, 29, 30)),
        historical=previous,
    )

    result = repo.intraday_summary(account_id="paper")

    assert result["current_realtime_state"]["status"] == "LIVE"
    assert (
        result["current_realtime_state"]["snapshot"]["state_id"]
        == "state-1"
    )
    assert (
        result["latest_historical_snapshot"]["state_id"]
        == "state-previous"
    )
