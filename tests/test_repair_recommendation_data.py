from unittest.mock import patch

from tools import repair_recommendation_data


def _report(*, flow_ready: bool) -> dict:
    return {
        "expected_stocks": 2,
        "sources": [
            {"key": "kline", "ready": True, "coverage": 1.0, "required": True},
            {
                "key": "capital_flow",
                "ready": flow_ready,
                "coverage": 1.0 if flow_ready else 0.5,
                "required": True,
            },
            {"key": "snapshot", "ready": True, "coverage": 1.0, "required": True},
        ],
    }


def test_repair_targets_only_missing_capital_flow_codes():
    before = _report(flow_ready=False)
    after = _report(flow_ready=True)

    with (
        patch("tools.repair_recommendation_data.create_batch_engine", return_value=object()),
        patch(
            "tools.repair_recommendation_data.coverage_report",
            side_effect=[before, before, after, after, after],
        ),
        patch(
            "tools.repair_recommendation_data._missing_source_codes",
            return_value=["000001", "000002"],
        ),
        patch(
            "tools.repair_recommendation_data._run_command",
            return_value={"stage": "repair_historical_capital_flow", "returncode": 0},
        ) as run_command,
    ):
        result = repair_recommendation_data.repair_target_data("2026-07-17")

    command = run_command.call_args.args[0]
    assert command[command.index("--codes") + 1] == "000001,000002"
    assert command[command.index("--sleep") + 1] == "0.05"
    assert result["ready_for_recommendation"] is True
