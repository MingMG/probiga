from datetime import date, datetime

from server.trading_v3.decision_worker import _premarket_candidate_pool
from server.trading_v3.premarket_gate import assess_premarket_candidates


SESSION = date(2026, 8, 31)
CUTOFF = datetime(2026, 8, 31, 9, 25, 59)


def _events(
    code: str,
    *,
    price: float,
    pre_close: float = 10.0,
    upper_limit: float = 11.0,
) -> list[dict]:
    return [
        {
            "quote_event_id": f"{code}-1",
            "stock_code": code,
            "quote_at": datetime(2026, 8, 31, 9, 24, 30),
            "bid1": price - 0.01,
            "bid1_volume": 1000,
            "ask1": price,
            "ask1_volume": 900,
            "last_price": price,
            "pre_close": pre_close,
            "upper_limit": upper_limit,
            "suspended": 0,
        },
        {
            "quote_event_id": f"{code}-2",
            "stock_code": code,
            "quote_at": datetime(2026, 8, 31, 9, 25, 30),
            "bid1": price - 0.01,
            "bid1_volume": 1300,
            "ask1": price,
            "ask1_volume": 800,
            "last_price": price,
            "pre_close": pre_close,
            "upper_limit": upper_limit,
            "suspended": 0,
        },
    ]


def test_auction_reranks_all_candidates_without_mechanical_substitution() -> None:
    pool = {
        "run_uid": "run-auction",
        "trade_date": "2026-08-28",
        "pool_readable": True,
        "items": [
            {
                "stock_code": "000001",
                "stock_name": "龙头甲",
                "rank_no": 1,
                "raw_score": 0.92,
                "primary_theme": "算力",
                "dynamic_role": "LEADER",
                "theme_rank": 1,
                "is_strategy_candidate": True,
                "actionability": "BUY_ZONE",
            },
            {
                "stock_code": "000002",
                "stock_name": "候选乙",
                "rank_no": 2,
                "raw_score": 0.82,
                "primary_theme": "算力",
                "dynamic_role": "CORE_ALTERNATIVE",
                "theme_rank": 2,
                "is_strategy_candidate": True,
                "actionability": "BUY_ZONE",
            },
        ],
    }
    result = assess_premarket_candidates(
        pool,
        [*_events("000001", price=11.0), *_events("000002", price=10.2)],
        session_date=SESSION,
        cutoff_at=CUTOFF,
    )
    by_code = {row["stock_code"]: row for row in result["assessments"]}

    assert result["status"] == "COMPLETED"
    assert result["stage"] == "FINAL_0925"
    assert result["automatic_substitution"] is False
    assert result["order_authority"] is False
    assert by_code["000001"]["advisory_action"] == "UNBUYABLE"
    assert by_code["000001"]["decision_rank"] is None
    assert by_code["000002"]["advisory_action"] == "BUY_CANDIDATE"
    assert by_code["000002"]["decision_rank"] == 1
    assert by_code["000001"]["alternative_set"][0]["stock_code"] == "000002"


def test_auction_never_upgrades_research_only_permission() -> None:
    pool = {
        "run_uid": "run-research",
        "pool_readable": True,
        "items": [{
            "stock_code": "000003",
            "stock_name": "研究丙",
            "rank_no": 1,
            "raw_score": 0.95,
            "primary_theme": "机器人",
            "is_strategy_candidate": True,
            "actionability": "RESEARCH_ONLY",
        }],
    }
    result = assess_premarket_candidates(
        pool,
        _events("000003", price=10.1),
        session_date=SESSION,
        cutoff_at=CUTOFF,
    )

    row = result["assessments"][0]
    assert row["gate_status"] == "CONFIRMED"
    assert row["advisory_action"] == "RESEARCH_ONLY"
    assert row["order_authority"] is False


def test_auction_preserves_unavailable_and_valid_empty_truth() -> None:
    unavailable = assess_premarket_candidates(
        {"pool_readable": False, "items": []},
        [],
        session_date=SESSION,
        cutoff_at=CUTOFF,
    )
    valid_empty = assess_premarket_candidates(
        {"pool_readable": True, "items": []},
        [],
        session_date=SESSION,
        cutoff_at=CUTOFF,
    )

    assert unavailable["status"] == "UPSTREAM_UNAVAILABLE"
    assert valid_empty["status"] == "VALID_EMPTY"


def test_worker_pool_keeps_alternatives_but_only_targets_start_as_buy_zone() -> None:
    class Forecast:
        def __init__(self, code: str, score: float):
            self.code = code
            self.score = score

        def as_dict(self):
            return {
                "stock_code": self.code,
                "stock_name": self.code,
                "strategy_key": "right_side_trend",
                "status": "VALIDATED_POSITIVE",
                "raw_score": self.score,
                "theme_code": "算力",
                "features": {},
            }

    pool = _premarket_candidate_pool(
        run_uid="worker-run",
        forecasts=[Forecast("000001", 0.9), Forecast("000002", 0.8)],
        portfolio={
            "targets": [{"stock_code": "000001", "reason": "validated"}],
            "rejected": [],
        },
    )
    by_code = {row["stock_code"]: row for row in pool["items"]}

    assert by_code["000001"]["actionability"] == "BUY_ZONE"
    assert by_code["000002"]["actionability"] == "RESEARCH_ONLY"
    assert by_code["000001"]["related_candidates"][0]["stock_code"] == (
        "000002"
    )
