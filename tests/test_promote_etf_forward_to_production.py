from __future__ import annotations

from copy import deepcopy
from datetime import date

import pytest

from tools.promote_etf_forward_to_production import (
    SCHEMA,
    _content_hash,
    validate_bundle,
)


def _bundle():
    payload = {
        "schema": SCHEMA,
        "generated_at": "2026-07-28 15:30:00",
        "source": "windows_qmt_host",
        "strategies": [{
            "strategy_version": "etf-v1",
            "config_hash": "a" * 64,
            "frozen_at": "2026-07-25 00:00:00",
            "forward_start_date": "2026-07-27",
            "mode": "research_paper_read_only",
            "status": "registered",
            "config_json": (
                '{"forward_protocol":{"automatic_order_submission":false,'
                '"backfill":"prohibited"}}'
            ),
            "registered_at": "2026-07-25 10:00:00",
        }],
        "observations": [{
            "strategy_version": "etf-v1",
            "config_hash": "a" * 64,
            "data_date": min(date.today(), date(2026, 7, 28)).isoformat(),
            "observed_at": "2026-07-28 15:30:00",
            "data_source": "gj_big_qmt_inner",
            "input_hash": "b" * 64,
            "signal_type": "carry",
            "execution_date": None,
            "target_json": '{"511880":1.0}',
            "context_json": '{"automatic_order_submission":false}',
            "created_at": "2026-07-28 15:30:00",
        }],
    }
    payload["content_sha256"] = _content_hash(payload)
    return payload


def test_etf_forward_bundle_accepts_immutable_qmt_observation():
    validate_bundle(_bundle())


def test_etf_forward_bundle_rejects_hash_tampering():
    payload = _bundle()
    payload["observations"][0]["signal_type"] = "monthly_rebalance"
    with pytest.raises(ValueError, match="hash mismatch"):
        validate_bundle(payload)


def test_etf_forward_bundle_rejects_backfilled_observation():
    payload = deepcopy(_bundle())
    payload["observations"][0]["data_date"] = "2026-07-26"
    payload["content_sha256"] = _content_hash(payload)
    with pytest.raises(ValueError, match="retrospective"):
        validate_bundle(payload)
