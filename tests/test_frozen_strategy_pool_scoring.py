from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

from biz.analysis import sync_analysis_fast as analysis
from server.common.analysis_pool_receipt import decode_score_snapshot
from server.common.daily_delivery_control import FrozenPremarketAnnouncement
from tests.score_snapshot_helpers import scored_publication


def test_two_frozen_scores_keep_the_initial_input_boundary_after_a_late_event(monkeypatch):
    target = "2026-11-10"
    universe = [{"stock_code": "600001"}, {"stock_code": "600002"}]
    fixture = decode_score_snapshot(
        scored_publication(universe, trade_date=target)["score_snapshot"]
    )
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)")
        conn.exec_driver_sql("INSERT INTO si_trade_calendar VALUES ('2026-11-10',1),('2026-11-11',1)")
        conn.exec_driver_sql("CREATE TABLE notice_fixture (stock_code TEXT, available_at TEXT)")
        conn.exec_driver_sql("INSERT INTO notice_fixture VALUES ('600002','2026-11-10T22:21:00')")
        conn.exec_driver_sql("CREATE TABLE st_recommended_stocks (stock_code TEXT)")
        conn.exec_driver_sql("INSERT INTO st_recommended_stocks VALUES ('600001')")

    def score_at_cutoff(*, engine, trade_date, news_cutoff_time, **_):
        with engine.connect() as conn:
            late = conn.execute(text(
                "SELECT stock_code FROM notice_fixture WHERE available_at<=:cutoff"
            ), {"cutoff": news_cutoff_time}).scalar()
        return (
            fixture["analysis_rows"],
            [{"stock_code": late or "600001"}],
            66,
            trade_date,
            trade_date,
            pd.DataFrame([
                {**row, "flow_input_decision_at": news_cutoff_time}
                for row in fixture["scored_rows"]
            ]),
        )

    monkeypatch.setattr(analysis, "_prepare_batch_outputs", score_at_cutoff)
    monkeypatch.setattr(analysis, "_now_shanghai_naive", lambda: datetime(2026, 11, 11, 8, 38))
    common = {"engine": engine, "data_trade_date": target, "build_sha": "2" * 40}
    initial = analysis.prepare_frozen_strategy_pool_scoring(
        **common, phase="INITIAL", run_uid="1" * 32,
    )
    revised = analysis.prepare_frozen_strategy_pool_scoring(
        **common, phase="PREMARKET_REVISION", run_uid="3" * 32,
        required_premarket_announcement=FrozenPremarketAnnouncement({}),
    )
    assert initial["cutoff_at"] == "2026-11-10T22:20:00+08:00"
    assert revised["cutoff_at"] == "2026-11-11T08:30:00+08:00"
    assert initial["evaluated_at"] == revised["evaluated_at"] == "2026-11-11T08:38:00+08:00"
    assert initial["recommendation_rows"] == [{"stock_code": "600001"}]
    assert revised["recommendation_rows"] == [{"stock_code": "600002"}]
    assert decode_score_snapshot(initial["score_snapshot"])["input_decision_at"] == "2026-11-10T22:20:00"
    assert decode_score_snapshot(revised["score_snapshot"])["input_decision_at"] == "2026-11-11T08:30:00"
    with engine.connect() as conn:
        assert conn.execute(text("SELECT stock_code FROM st_recommended_stocks")).scalar_one() == "600001"
