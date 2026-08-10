from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from server.trading_v6.pit_finance import (
    PitFinanceContractError,
    build_pit_finance_features,
)


SIGNAL = "2026-01-10T15:30:00+08:00"


def _market_rows():
    return [
        {
            "sample_id": f"sample-{code}",
            "instrument_id": code,
            "signal_at": SIGNAL,
            "feature_available_at": "2026-01-10T15:00:00+08:00",
            "raw_close": close,
            "eligible_liquid": True,
        }
        for code, close in (("000001", 10.0), ("000002", 20.0), ("000003", 30.0))
    ]


def _statement(statement_id, code, *, notice, knowledge, net_asset, quality=10.0):
    return {
        "statement_id": statement_id,
        "instrument_id": code,
        "report_date": "2025-09-30",
        "notice_at": notice,
        "knowledge_at": knowledge,
        "net_asset_ps": net_asset,
        "oper_cf_ps": quality / 10,
        "net_profit_yoy_gr": quality,
        "roe_wtd": quality,
        "gross_margin": quality,
        "net_margin": quality,
        "cash_flow_ratio": quality,
        "asset_liab_ratio": 40.0,
    }


def _finance_rows():
    return [
        _statement(
            "a-old", "000001",
            notice="2025-10-30T18:00:00+08:00",
            knowledge="2025-10-30T18:01:00+08:00",
            net_asset=5.0,
            quality=10.0,
        ),
        _statement(
            "b", "000002",
            notice="2025-10-30T18:00:00+08:00",
            knowledge="2025-10-30T18:01:00+08:00",
            net_asset=5.0,
            quality=20.0,
        ),
        _statement(
            "c", "000003",
            notice="2025-10-30T18:00:00+08:00",
            knowledge="2025-10-30T18:01:00+08:00",
            net_asset=5.0,
            quality=20.0,
        ),
        _statement(
            "a-future-revision", "000001",
            notice="2026-01-10T15:29:00+08:00",
            knowledge="2026-01-10T15:30:00.000001+08:00",
            net_asset=50.0,
            quality=99.0,
        ),
    ]


def test_future_revision_is_excluded_and_peer_provenance_is_bound() -> None:
    features = build_pit_finance_features(_market_rows(), _finance_rows())
    without_future = build_pit_finance_features(
        _market_rows(), _finance_rows()[:-1]
    )
    assert [item.as_dict() for item in features] == [
        item.as_dict() for item in without_future
    ]
    assert [item.sample_id for item in features] == [
        "sample-000001", "sample-000002", "sample-000003"
    ]
    first = features[0]
    assert first.statement_id == "a-old"
    assert first.finance_peer_count == 3
    assert first.status == "PIT_RESEARCH_FEATURE_READY"
    assert first.as_dict()["source_certification_status"] == "UNVERIFIED_EXPLICIT_INPUT"
    assert first.as_dict()["production_eligible"] is False
    assert first.activation_eligible is False
    assert first.quality_percentile == pytest.approx(1 / 3)
    assert features[1].quality_percentile == pytest.approx(5 / 6)
    assert features[2].quality_percentile == pytest.approx(5 / 6)
    assert features[0].valuation_percentile == 1.0
    assert features[2].valuation_percentile == pytest.approx(1 / 3)


def test_exact_knowledge_time_is_allowed_but_later_time_is_not() -> None:
    rows = _finance_rows()
    rows.append(
        _statement(
            "a-exact", "000001",
            notice="2026-01-10T15:29:00+08:00",
            knowledge=SIGNAL,
            net_asset=8.0,
            quality=50.0,
        )
    )
    features = build_pit_finance_features(_market_rows(), rows)
    assert features[0].statement_id == "a-exact"


def test_input_order_is_irrelevant_and_peer_change_alters_hash() -> None:
    forward = build_pit_finance_features(_market_rows(), _finance_rows())
    reverse = build_pit_finance_features(
        list(reversed(_market_rows())), list(reversed(_finance_rows()))
    )
    assert [item.as_dict() for item in forward] == [item.as_dict() for item in reverse]

    changed_market = deepcopy(_market_rows())
    changed_market[1]["raw_close"] = 21.0
    changed = build_pit_finance_features(changed_market, _finance_rows())
    assert changed[0].finance_peer_manifest_sha256 != forward[0].finance_peer_manifest_sha256
    assert changed[0].feature_snapshot_sha256 != forward[0].feature_snapshot_sha256


def test_date_only_intraday_ambiguity_and_duplicates_fail_closed() -> None:
    rows = _finance_rows()
    rows[0]["notice_at"] = "2025-10-30"
    with pytest.raises(PitFinanceContractError, match="timezone-aware"):
        build_pit_finance_features(_market_rows(), rows)

    duplicate = _finance_rows()
    duplicate.append(deepcopy(duplicate[0]))
    with pytest.raises(PitFinanceContractError, match="statement_id"):
        build_pit_finance_features(_market_rows(), duplicate)

    markets = _market_rows()
    markets[1]["instrument_id"] = markets[0]["instrument_id"]
    with pytest.raises(PitFinanceContractError, match="duplicate instruments"):
        build_pit_finance_features(markets, _finance_rows())


def test_missing_statement_is_explicitly_data_blocked() -> None:
    features = build_pit_finance_features(_market_rows(), _finance_rows()[:2])
    missing = next(item for item in features if item.instrument_id == "000003")
    assert all(item.finance_peer_count == 2 for item in features)
    assert missing.statement_id is None
    assert missing.status == "DATA_BLOCKED"
    assert missing.as_dict()["actionable_output_allowed"] is False


def test_peer_count_only_includes_rows_with_usable_finance() -> None:
    markets = _market_rows()[:2]
    features = build_pit_finance_features(markets, _finance_rows()[:1])

    assert all(item.finance_peer_count == 1 for item in features)
    assert features[0].status == "PIT_RESEARCH_FEATURE_READY"
    assert features[1].status == "DATA_BLOCKED"


def test_conflicting_statements_at_same_effective_time_fail_closed() -> None:
    rows = _finance_rows()[:-1]
    conflict = deepcopy(rows[0])
    conflict["statement_id"] = "a-conflict"
    conflict["net_profit_yoy_gr"] = 99.0
    rows.append(conflict)

    with pytest.raises(PitFinanceContractError, match="conflicting finance"):
        build_pit_finance_features(_market_rows(), rows)


def test_subprotocol_precision_is_normalized_before_conflict_and_ranking() -> None:
    rows = deepcopy(_finance_rows()[:-1])
    for row in rows[:2]:
        row.update({
            "net_profit_yoy_gr": 1.0,
            "gross_margin": 1.0,
            "net_margin": 1.0,
            "cash_flow_ratio": 1.0,
        })
    rows[0]["roe_wtd"] = 1.0000000000001
    rows[1]["roe_wtd"] = 1.00000000000015
    same_economic_record = deepcopy(rows[0])
    same_economic_record["statement_id"] = "a-same-economic"
    same_economic_record["roe_wtd"] = 1.0000000000002
    rows.append(same_economic_record)

    forward = build_pit_finance_features(_market_rows(), rows)
    swapped = deepcopy(rows)
    swapped[0]["roe_wtd"], swapped[-1]["roe_wtd"] = (
        swapped[-1]["roe_wtd"],
        swapped[0]["roe_wtd"],
    )
    reverse_values = build_pit_finance_features(_market_rows(), swapped)

    assert [item.as_dict() for item in forward] == [
        item.as_dict() for item in reverse_values
    ]


def test_detached_or_mutated_pit_feature_fails_closed() -> None:
    feature = build_pit_finance_features(_market_rows(), _finance_rows())[0]
    detached = replace(feature)
    with pytest.raises(PitFinanceContractError, match="builder attestation"):
        detached.as_dict()

    object.__setattr__(feature, "quality_percentile", 999999.0)
    with pytest.raises(PitFinanceContractError, match="within"):
        feature.as_dict()


def test_direct_future_knowledge_claim_is_rejected() -> None:
    feature = build_pit_finance_features(_market_rows(), _finance_rows())[0]
    with pytest.raises(PitFinanceContractError, match="exceeds signal_at"):
        replace(
            feature,
            notice_at="2026-01-10T15:31:00+08:00",
            knowledge_at="2026-01-10T15:32:00+08:00",
        )


def test_microseconds_and_market_availability_are_snapshot_bound() -> None:
    first_finance = deepcopy(_finance_rows())
    second_finance = deepcopy(_finance_rows())
    first_finance[0]["knowledge_at"] = "2025-10-30T18:01:00.000001+08:00"
    second_finance[0]["knowledge_at"] = "2025-10-30T18:01:00.000002+08:00"
    first = build_pit_finance_features(_market_rows(), first_finance)[0]
    second = build_pit_finance_features(_market_rows(), second_finance)[0]
    assert first.finance_source_manifest_sha256 != second.finance_source_manifest_sha256
    assert first.feature_snapshot_sha256 != second.feature_snapshot_sha256
    assert first.knowledge_at != second.knowledge_at

    first_market = deepcopy(_market_rows())
    second_market = deepcopy(_market_rows())
    first_market[0]["feature_available_at"] = "2026-01-10T15:00:00.000001+08:00"
    second_market[0]["feature_available_at"] = "2026-01-10T15:00:00.000002+08:00"
    first = build_pit_finance_features(first_market, _finance_rows())[0]
    second = build_pit_finance_features(second_market, _finance_rows())[0]
    assert first.finance_peer_manifest_sha256 != second.finance_peer_manifest_sha256
    assert first.feature_snapshot_sha256 != second.feature_snapshot_sha256
    assert first.market_feature_available_at != second.market_feature_available_at
