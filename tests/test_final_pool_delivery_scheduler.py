import json

from server.common.scheduler_validation import scheduler_output_status
from tools.ensure_quality_gate import TASKS
from tools.final_pool_delivery_task_contract import TASK
from tools.send_final_pool_wecom import (
    FINAL_POOL_DELIVERY_SCHEMA,
    _canonical_hash,
    _valid_reconstruction_identity,
    validate_cli_result,
)


def _payload(status: str = "SUCCEEDED"):
    deliveries = []
    for day, token in (("2026-09-01", "a"), ("2026-09-02", "b")):
        deliveries.append({
            "trade_date": day,
            "status": "SUCCEEDED",
            "governance_run_uid": token * 32,
            "analysis_run_uid": ("d" if token == "a" else "e") * 32,
            "build_sha": "c" * 40,
            "governance_result_sha256": "f" * 64,
            "canonical_pool_sha256": "9" * 64,
            "gate_hash": "8" * 64,
            "content_sha256": "7" * 64,
            "evidence_as_of": f"{day} 22:35:00",
            "knowledge_cutoff_at": f"{day} 22:20:00",
            "retrospective_reconstruction": False,
            "reconstruction_mode": "NONE",
            "reconstruction_sha256": "",
            "reconstructed_at": "",
            "reconstruction_provenance": {},
            "delivery_id": f"delivery-{token}",
            "segment_count": 1,
            "delivered_count": 1,
            "automatic_substitution": False,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        })
    return {
        "schema": FINAL_POOL_DELIVERY_SCHEMA,
        "status": status,
        "target_trade_date": "2026-09-02",
        "covered_trade_dates": ["2026-09-01", "2026-09-02"],
        "delivery_count": 2,
        "deliveries": deliveries,
        "automatic_substitution": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def test_final_pool_delivery_task_is_installed_as_automatic_user_delivery():
    assert TASK["task_type"] == "final_pool_wecom_delivery"
    assert TASK["script_path"] == "tools/send_final_pool_wecom.py"
    assert TASK["script_args"] == "--json"
    assert TASK["enabled"] == 1
    matches = [row for row in TASKS if row["task_type"] == TASK["task_type"]]
    assert matches == [TASK]


def test_final_pool_delivery_cli_and_scheduler_output_fail_closed():
    completed = _payload()
    assert validate_cli_result(completed, 0) == "completed"
    assert scheduler_output_status(
        TASK,
        json.dumps(completed),
        return_code=0,
    ) == "success"

    blocked = {
        **_payload("DATA_BLOCKED"),
        "retryable": True,
        "delivery_count": 0,
        "deliveries": [],
    }
    assert validate_cli_result(blocked, 2) == "not_ready"
    assert scheduler_output_status(
        TASK,
        json.dumps(blocked),
        return_code=2,
    ) == "blocked"
    assert scheduler_output_status(
        TASK,
        json.dumps({**completed, "real_order_authority": True}),
        return_code=0,
    ) == "failed"
    assert validate_cli_result({**blocked}, 0) == "failed"
    malformed = {**completed, "deliveries": ["not-an-object", {}]}
    assert validate_cli_result(malformed, 0) == "failed"
    assert scheduler_output_status(
        TASK,
        json.dumps(malformed),
        return_code=0,
    ) == "failed"


def test_final_pool_cli_revalidates_reconstruction_receipt_hash():
    item = _payload()["deliveries"][0]
    reconstructed_at = "2026-09-03T13:00:20.000000"
    core = {
        "schema": "probiga.qmt-announcement-historical-reconstruction.v2",
        "mode": "HISTORICAL_RECONSTRUCTION",
        "target_trade_date": item["trade_date"],
        "source_query_cutoff_at": f"{item['trade_date']}T23:59:59.999999",
        "reconstructed_at": reconstructed_at,
        "known_at": reconstructed_at,
        "provider": "cninfo.announcement",
        "source": "cninfo.announcement",
        "scheduler_run_uid": "1" * 32,
        "build_sha": "2" * 40,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    provenance = {**core, "reconstruction_sha256": _canonical_hash(core)}
    item.update({
        "retrospective_reconstruction": True,
        "reconstruction_mode": "HISTORICAL_RECONSTRUCTION",
        "reconstruction_sha256": provenance["reconstruction_sha256"],
        "reconstructed_at": reconstructed_at,
        "reconstruction_provenance": provenance,
        "evidence_as_of": "2026-09-03T13:01:00.000000",
        "knowledge_cutoff_at": "2026-09-03T13:01:00.000000",
    })

    assert _valid_reconstruction_identity(item) is True
    item["evidence_as_of"] = "2026-09-03T13:00:19.999999"
    assert _valid_reconstruction_identity(item) is False
    item["evidence_as_of"] = "2026-09-03T13:01:00.000000"
    item["knowledge_cutoff_at"] = "2026-09-03T05:01:00+00:00"
    assert _valid_reconstruction_identity(item) is False
    item["knowledge_cutoff_at"] = "2026-09-03T13:01:00.000000"
    item["reconstruction_provenance"]["provider"] = "other"
    assert _valid_reconstruction_identity(item) is False
