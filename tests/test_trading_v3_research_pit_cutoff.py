from datetime import date, datetime

import pytest
from sqlalchemy import create_engine

from server.common.pit_facts import PIT_DATA_BLOCKED, resolve_common_fact_cutoff
from server.trading_v3 import decision_worker


def test_research_surfaces_real_resolver_failure_before_any_decision_write(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    known_at = datetime(2026, 9, 7, 7, 30)
    resolved = resolve_common_fact_cutoff(
        engine,
        codes=["000001"],
        decision_at=known_at,
        finance_start_date="1900-01-01",
        finance_end_date="2026-09-04",
        event_start_date="2026-08-15",
        event_end_date="2026-09-04",
        require_qmt_event_batch=True,
    )
    assert resolved["status"] == PIT_DATA_BLOCKED
    assert resolved["reason"] == "PIT_FINANCE_ATOMIC_BATCH_INVALID:OperationalError"

    def load_features(*_args, **_kwargs):
        return {"market_features": {
            "pit_fact_cutoff_at": resolved["fact_cutoff_at"],
            "pit_decision_at": resolved["decision_at"],
            "pit_common_cutoff_reason": resolved["reason"],
        }}

    monkeypatch.setattr(decision_worker, "load_daily_feature_universe", load_features)
    monkeypatch.setattr(decision_worker, "load_v3_config", lambda: {})

    def fail_write(*_args, **_kwargs):
        pytest.fail("blocked research must not write a decision")

    monkeypatch.setattr(decision_worker.TradingV3Repository, "save_decision", fail_write)
    with pytest.raises(RuntimeError) as caught:
        decision_worker.run_retrospective_research_v3(
            engine,
            as_of=date(2026, 9, 4),
            decision_at=datetime(2026, 9, 4, 23, 59, 59, 999999),
            research_known_at=known_at,
            mode="close",
            kline_engine=engine,
        )
    assert str(caught.value) == (
        "RETROSPECTIVE_RESEARCH_PIT_CLOCK_UNAVAILABLE:"
        "pit_common_cutoff_reason=PIT_FINANCE_ATOMIC_BATCH_INVALID:OperationalError"
    )
    engine.dispose()
