import json
from datetime import datetime
from time import monotonic

import pytest

from server.common import market_live_ladder as ladder

DAY = "2026-11-23"
NOW = datetime(2026, 11, 23, 10, 30)


@pytest.mark.parametrize("failed", [None, "limit_down", "limit_up"])
def test_live_pools_bind_exact_date_and_keep_partial_results(monkeypatch, failed):
    monkeypatch.setattr(ladder, "clock", lambda value=None: value or NOW)
    def fetch(day, *, pool_kind, **kwargs):
        if pool_kind == failed:
            raise TimeoutError("source unavailable")
        rows = [{"c": "600001", "n": "样本", "p": 10000, "lbc": 2}] if pool_kind == "limit_up" else []
        return None, json.dumps({"rc": 0, "data": {"qdate": "20261123", "tc": len(rows), "pool": rows}})
    monkeypatch.setattr(ladder, "_fetch_pool", fetch)
    result = ladder.capture_live_ladder(DAY, now=NOW, deadline=monotonic()+9)
    assert result["limit_up_count"] == (None if failed == "limit_up" else 1)
    assert result["max_streak"] == (None if failed == "limit_up" else 2)
    assert result["limit_down_count"] == (None if failed == "limit_down" else 0)
    assert result["opened_limit_up_count"] == 0
    assert result["source_at"] is None
    assert set(result["source_errors"]) == ({failed} if failed else set())
    assert all(receipt["source_at"] is None for receipt in result["pool_receipts"].values())


def test_latest_vendor_date_cannot_be_relabelled_as_selected_session(monkeypatch):
    monkeypatch.setattr(ladder, "clock", lambda value=None: value or NOW)
    monkeypatch.setattr(ladder, "_fetch_pool", lambda *a, **kw: (None, json.dumps(
        {"rc": 0, "data": {"qdate": "20261120", "tc": 0, "pool": []}})))
    with pytest.raises(RuntimeError, match="all live limit pool sources"):
        ladder.capture_live_ladder(DAY, now=NOW, deadline=monotonic()+9)
