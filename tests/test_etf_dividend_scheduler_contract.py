from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
import hashlib
import json
from types import SimpleNamespace

import pytest

from server.common import scheduler_validation as validation
from tools import sync_etf_bigqmt_daily as etf


BUILD_SHA = "a" * 40
ADATA_SHA = "b" * 40
ADATA_TREE = "c" * 64
CODE_HASH = "d" * 64


def _sign(payload: dict) -> dict:
    result = deepcopy(payload)
    canonical = json.dumps(
        result,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    result["receipt_id"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return result


def _etf_receipt() -> dict:
    groups = {
        name: {
            "adjust_type": adjust_type,
            "request_id": f"request-{name}",
            "requested_code_count": 14,
            "requested_code_set_hash": CODE_HASH,
            "responded_code_count": 14,
            "responded_code_set_hash": CODE_HASH,
            "source_row_hash": ("1" if name == "none" else "2") * 64,
        }
        for name, adjust_type in (("none", 0), ("front", 1))
    }
    return _sign(
        {
            "schema": "probiga.etf-forward-daily-receipt.v1",
            "status": "PASS",
            "trade_date": "2026-08-26",
            "provider": "gj_big_qmt_inner",
            "executor_owner": "qmt_windows_edge",
            "market_data": {
                "status": "PASS",
                "trade_date": "2026-08-26",
                "groups": groups,
                "database": {
                    "row_count": 28,
                    "row_hash": "3" * 64,
                    "group_hashes": {"none": "4" * 64, "front": "5" * 64},
                },
                "universe": {"count": 14, "code_set_hash": CODE_HASH},
                "source_identity": {
                    "strategy_build_sha": BUILD_SHA,
                    "strategy_identity_frozen": True,
                },
            },
            "forward_ledger": {
                "status": "PASS",
                "write_status": "CREATED",
                "data_date": "2026-08-26",
                "strategy_version": "etf-v1",
                "config_hash": "6" * 64,
                "input_hash": "7" * 64,
                "signal_type": "carry",
            },
            "automatic_order_submission": False,
        }
    )





def _nested(receipt: dict) -> str:
    return json.dumps(
        {
            "executor": "windows_big_qmt_bridge",
            "machine_receipt": receipt,
            "daily_stdout_tail": "diagnostic tail",
        },
        ensure_ascii=False,
    )


@pytest.fixture(autouse=True)
def _release_environment(monkeypatch):
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)
    monkeypatch.setenv("PROBIGA_EXPECTED_ADATA_SHA", ADATA_SHA)
    monkeypatch.setenv("PROBIGA_EXPECTED_ADATA_TREE_SHA256", ADATA_TREE)


def test_etf_machine_gate_accepts_nested_receipt_and_rejects_all_drift() -> None:
    task = {"task_type": "etf_forward_daily"}
    receipt = _etf_receipt()
    assert validation.scheduler_output_status(
        task, _nested(receipt), return_code=0
    ) == "success"

    mutations = (
        lambda item: item["market_data"]["database"].update(row_count=27),
        lambda item: item["market_data"]["groups"]["front"].update(
            responded_code_set_hash="0" * 64
        ),
        lambda item: item["market_data"]["source_identity"].update(
            strategy_build_sha="f" * 40
        ),
        lambda item: item.update(trade_date="2026-08-25"),
    )
    for mutate in mutations:
        changed = deepcopy(receipt)
        changed.pop("receipt_id")
        mutate(changed)
        changed = _sign(changed)
        assert validation.scheduler_output_status(
            task, _nested(changed), return_code=0
        ) == "failed"
    tampered = deepcopy(receipt)
    tampered["receipt_id"] = "0" * 64
    assert validation.scheduler_output_status(
        task, _nested(tampered), return_code=0
    ) == "failed"
    assert validation.scheduler_output_status(
        task, _nested(receipt), return_code=1
    ) == "failed"


def test_etf_unchanged_strategy_carries_verified_app_compatibility():
    receipt = _etf_receipt()
    receipt.pop("receipt_id")
    identity = receipt["market_data"]["source_identity"]
    identity.update(
        strategy_build_sha="f" * 40, compatible_app_build_sha=BUILD_SHA,
        strategy_compatibility_status="CONTENT_COMPATIBLE",
        strategy_git_blob="b" * 40, strategy_source_sha256="c" * 64,
        strategy_artifact_sha256="d" * 64, strategy_loaded_identity_sha256="e" * 64,
    )
    assert etf._release_summary(identity)["compatible_app_build_sha"] == BUILD_SHA
    task = {"task_type": "etf_forward_daily"}
    assert validation.scheduler_output_status(task, _nested(_sign(receipt)), return_code=0) == "success"
    for field in ("compatible_app_build_sha", "strategy_compatibility_status", "strategy_source_sha256"):
        changed = deepcopy(receipt)
        changed["market_data"]["source_identity"][field] = "wrong"
        assert validation.scheduler_output_status(task, _nested(_sign(changed)), return_code=0) == "failed"





def test_data_blocked_etf_run_remains_same_day_retryable():
    assert validation.scheduler_output_status({"task_type":"etf_forward_daily"},
        json.dumps({"schema":"probiga.etf-forward-daily-receipt.v1","status":"DATA_BLOCKED"}), return_code=2) == "failed"



class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self._rows)

    def all(self):
        return self._rows


class _Connection:
    def __init__(self, results):
        self._results = iter(results)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, *_args, **_kwargs):
        return _Result(next(self._results))


class _Engine:
    def __init__(self, connections):
        self._connections = iter(connections)

    def connect(self):
        return _Connection(next(self._connections))


def test_etf_db_validator_binds_partition_and_ledger_and_blocks_replay(
    monkeypatch,
) -> None:
    receipt = _etf_receipt()
    expected = receipt["market_data"]["database"]
    monkeypatch.setattr(etf, "validate_partition_rows", lambda *_args, **_kwargs: expected)
    monkeypatch.setattr(
        validation,
        "authoritative_closed_trade_date",
        lambda _engine, *, now, close_ready_time: now.date().isoformat(),
    )
    observation = [{
        "strategy_version": "etf-v1",
        "data_date": "2026-08-26",
        "config_hash": "6" * 64,
        "input_hash": "7" * 64,
        "signal_type": "carry",
    }]
    engine = _Engine((([{}] * 28,), (observation,)))
    ok, _message = validation._validate_etf_forward_receipt(
        engine,
        output=_nested(receipt),
        now=datetime(2026, 8, 26, 16, 0),
    )
    assert ok is True

    replay_ok, replay_message = validation._validate_etf_forward_receipt(
        object(),
        output=_nested(receipt),
        now=datetime(2026, 8, 27, 16, 0),
    )
    assert replay_ok is False
    assert "authoritative" in replay_message

    monkeypatch.setattr(
        etf,
        "validate_partition_rows",
        lambda *_args, **_kwargs: {**expected, "row_hash": "0" * 64},
    )
    mismatch_engine = _Engine((([{}] * 28,),))
    mismatch_ok, mismatch_message = validation._validate_etf_forward_receipt(
        mismatch_engine,
        output=_nested(receipt),
        now=datetime(2026, 8, 26, 16, 0),
    )
    assert mismatch_ok is False
    assert "differs" in mismatch_message


def test_etf_release_validator_rejects_receipt_from_another_partition() -> None:
    ok, message = validation._validate_etf_forward_receipt(
        object(),
        output=_nested(_etf_receipt()),
        now=datetime(2026, 8, 26, 16, 0),
        release_target_date=date(2026, 8, 25),
    )

    assert ok is False
    assert "release target" in message


def test_etf_validator_accepts_prior_closed_partition_without_backdating_forward(
    monkeypatch,
) -> None:
    receipt = _etf_receipt()
    receipt.pop("receipt_id")
    receipt["forward_ledger"] = {
        "status": "NOT_RUN_HISTORICAL_BACKFILL_PROHIBITED",
        "data_date": "2026-08-26",
    }
    receipt = _sign(receipt)
    expected = receipt["market_data"]["database"]
    monkeypatch.setattr(etf, "validate_partition_rows", lambda *_args, **_kwargs: expected)
    monkeypatch.setattr(
        validation,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "2026-08-26",
    )

    assert validation.scheduler_output_status(
        {"task_type": "etf_forward_daily"},
        _nested(receipt),
        return_code=0,
    ) == "success"
    ok, message = validation._validate_etf_forward_receipt(
        _Engine((([{}] * 28,),)),
        output=_nested(receipt),
        now=datetime(2026, 8, 27, 2, 0),
    )
    assert ok is True
    assert "prior closed partition" in message

    current_ok, current_message = validation._validate_etf_forward_receipt(
        _Engine((([{}] * 28,),)),
        output=_nested(receipt),
        now=datetime(2026, 8, 26, 16, 0),
    )
    assert current_ok is False
    assert "lacks its forward observation" in current_message
