from copy import deepcopy

import pytest

from server.common import qmt_attestation_contract as contract
from server.engine import strategy_governance
from tools import attest_qmt_daily_kline as attester
from tools import prepare_strategy_governance_qmt_history as preparation


def _manifest():
    daily = {
        "2026-08-21": {
            "stock_count": 2,
            "stock_set_hash": "a" * 64,
        }
    }
    return contract.build_qmt_v2_manifest(daily)


def _validate(payload):
    return contract.validated_universe_manifest(
        payload,
        start_date="2026-08-21",
        end_date="2026-08-21",
    )


def test_all_qmt_v2_consumers_share_one_exact_validator():
    assert attester.validated_universe_manifest is (
        contract.validated_universe_manifest
    )
    assert preparation.validated_universe_manifest is (
        contract.validated_universe_manifest
    )
    assert strategy_governance.validated_universe_manifest is (
        contract.validated_universe_manifest
    )
    assert set(_manifest()) == contract.QMT_V2_MANIFEST_KEYS
    assert _validate(_manifest())["2026-08-21"]["stock_count"] == 2


@pytest.mark.parametrize("missing_key", sorted(contract.QMT_V2_MANIFEST_KEYS))
def test_qmt_v2_manifest_rejects_every_missing_top_level_key(missing_key):
    payload = _manifest()
    payload.pop(missing_key)
    with pytest.raises(ValueError):
        _validate(payload)


def test_qmt_v2_manifest_rejects_extra_top_level_key():
    payload = {**_manifest(), "future_extension": True}
    with pytest.raises(ValueError, match="top-level fields differ"):
        _validate(payload)


@pytest.mark.parametrize("key", sorted(contract.QMT_V2_TOLERANCE_VALUES))
@pytest.mark.parametrize("replacement", ("0.0001", True))
def test_qmt_v2_manifest_rejects_tolerance_string_or_boolean(key, replacement):
    payload = _manifest()
    payload[key] = replacement
    with pytest.raises(ValueError, match="tolerance differs"):
        _validate(payload)


@pytest.mark.parametrize("key", sorted(contract.QMT_V2_TOLERANCE_VALUES))
def test_qmt_v2_manifest_rejects_each_tolerance_value_drift(key):
    payload = _manifest()
    payload[key] += 0.000001
    with pytest.raises(ValueError, match=key):
        _validate(payload)


@pytest.mark.parametrize(
    ("key", "value"),
    (
        ("attestation_protocol", "QMT_DAILY_UNADJUSTED_PRECLOSE_V3"),
        ("universe_manifest_schema", "probiga.qmt-daily-universe.v2"),
    ),
)
def test_qmt_v2_manifest_rejects_wrong_protocol_or_schema(key, value):
    payload = _manifest()
    payload[key] = value
    with pytest.raises(ValueError):
        _validate(payload)


@pytest.mark.parametrize(
    "entry",
    (
        {"stock_count": 2},
        {"stock_count": 2, "stock_set_hash": "a" * 64, "extra": 1},
        {"stock_count": "2", "stock_set_hash": "a" * 64},
        {"stock_count": True, "stock_set_hash": "a" * 64},
        {"stock_count": 2, "stock_set_hash": "A" * 64},
        {"stock_count": 2, "stock_set_hash": "a" * 63},
    ),
)
def test_qmt_v2_manifest_rejects_daily_entry_field_type_or_hash_drift(entry):
    payload = deepcopy(_manifest())
    payload["daily_universe"]["2026-08-21"] = entry
    with pytest.raises(ValueError):
        _validate(payload)
