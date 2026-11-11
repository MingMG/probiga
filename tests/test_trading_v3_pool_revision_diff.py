from copy import deepcopy

import pytest

from server.trading_v3.decision_truth import canonical_hash
from server.trading_v3.pool_revision_diff import build_pool_revision_diff


def _identity(*, later=False, stock_count=0):
    return {
        "run_uid": ("2" if later else "1") * 32,
        "result_hash": ("b" if later else "a") * 64,
        "data_trade_date": "2026-11-10",
        "cutoff": "2026-11-11T08:30:00+08:00" if later else "2026-11-10T18:20:00",
        "complete": True,
        "stock_count": stock_count,
    }


def test_pool_revision_diff_is_stock_unique_and_independent_of_row_order():
    prior_rows = [
        {"stock_code": "600900", "strategies": ["trend"], "reason": "前轮策略"},
        {"stock_code": "000001", "strategy_key": "value", "reasons": ["底仓"]},
        {"stock_code": "000001", "strategy_key": "trend", "reasons": ["底仓", "公告观察"]},
    ]
    next_rows = [
        {"stock_code": "600410", "strategy_keys": ["trend"], "reasons": ["新入选"]},
        {"stock_code": "000001", "strategies": ["value"], "reason": "保留"},
        {"stock_code": "600410", "strategy_key": "event", "reason": "业绩披露"},
    ]
    prior_identity = _identity(stock_count=2)
    next_identity = _identity(later=True, stock_count=2)
    original_rows = deepcopy((prior_rows, next_rows))
    diff = build_pool_revision_diff(
        prior_identity=prior_identity, next_identity=next_identity,
        prior_rows=prior_rows, next_rows=next_rows,
    )
    assert diff["counts"] == {"added": 1, "removed": 1, "retained": 1}
    assert diff["prior"]["run_uid"] == prior_identity["run_uid"]
    assert diff["next"]["result_hash"] == next_identity["result_hash"]
    assert diff["added"] == [{
        "stock_code": "600410", "strategy_keys": ["event", "trend"],
        "reasons": ["业绩披露", "新入选"], "blocking_reasons": [],
    }]
    assert [row["stock_code"] for row in diff["removed"]] == ["600900"]
    assert diff["retained"][0]["stock_code"] == "000001"
    assert diff["retained"][0]["prior"]["strategy_keys"] == ["trend", "value"]
    assert diff["retained"][0]["prior"]["reasons"] == ["公告观察", "底仓"]
    assert diff["retained"][0]["next"]["reasons"] == ["保留"]
    assert diff["diff_sha256"] == canonical_hash({key: value for key, value in diff.items() if key != "diff_sha256"})
    assert (prior_rows, next_rows) == original_rows

    reordered = build_pool_revision_diff(
        prior_identity={**prior_identity, "cutoff": "2026-11-10T10:20:00Z"},
        next_identity=next_identity,
        prior_rows=reversed(prior_rows), next_rows=reversed(next_rows),
    )
    assert reordered == diff


def test_pool_revision_diff_accepts_only_verified_empty_membership():
    prior_identity = _identity(stock_count=1)
    next_identity = _identity(later=True)
    diff = build_pool_revision_diff(
        prior_identity=prior_identity, next_identity=next_identity,
        prior_rows=[{"stock_code": "000001"}], next_rows=[],
    )
    assert diff["next"]["complete"] is True
    assert diff["next"]["stock_count"] == 0
    assert diff["counts"] == {"added": 0, "removed": 1, "retained": 0}

    with pytest.raises(ValueError, match="verified complete"):
        build_pool_revision_diff(
            prior_identity=prior_identity, next_identity={**next_identity, "complete": False},
            prior_rows=[{"stock_code": "000001"}], next_rows=[],
        )
    with pytest.raises(ValueError, match="verified unique stock count"):
        build_pool_revision_diff(
            prior_identity=prior_identity, next_identity={**next_identity, "stock_count": 1},
            prior_rows=[{"stock_code": "000001"}], next_rows=[],
        )


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ({"data_trade_date": "2026-11-11"}, "same data trade date"),
        ({"cutoff": "2026-11-10T10:20:00Z"}, "increasing cutoff"),
        ({"cutoff": "2026-11-10"}, "date or cutoff"),
        ({"cutoff": "2026-11-09T18:20:00"}, "precedes"),
        ({"run_uid": "1" * 32}, "distinct runs"),
        ({"run_uid": "0" * 32}, "run UID"),
        ({"result_hash": "0" * 64}, "result hash"),
        ({"stock_count": False}, "stock count"),
    ],
)
def test_pool_revision_diff_rejects_unverifiable_identity(change, match):
    with pytest.raises(ValueError, match=match):
        build_pool_revision_diff(
            prior_identity=_identity(), next_identity={**_identity(later=True), **change},
            prior_rows=[], next_rows=[],
        )


@pytest.mark.parametrize("stock_code", [1, "1", "000001.SZ", "０００００１"])
def test_pool_revision_diff_rejects_ambiguous_security_code(stock_code):
    with pytest.raises(ValueError, match="six-digit stock code"):
        build_pool_revision_diff(
            prior_identity=_identity(), next_identity=_identity(later=True, stock_count=1),
            prior_rows=[], next_rows=[{"stock_code": stock_code}],
        )
