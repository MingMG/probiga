from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from tools import sync_qmt_primary


def test_formal_bigqmt_minute_route_requires_full_catalog_response_coverage():
    completed = SimpleNamespace(returncode=0)
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={"BIG_QMT_BRIDGE_ENABLED": "true"},
    ), patch(
        "tools.sync_qmt_primary._bigqmt_runtime_available",
        return_value=True,
    ), patch(
        "tools.sync_qmt_primary.subprocess.run",
        return_value=completed,
    ) as run:
        result = sync_qmt_primary.run_dataset(
            "minute_price",
            date_str="2026-08-26",
        )

    assert result["status"] == "success"
    assert result["source_policy"] == "bigqmt_primary"
    environment = run.call_args.kwargs["env"]
    assert environment["DATA_SOURCE_MINUTE"] == "bigqmt"
    assert environment["QMT_PRIMARY_ALLOW_EXTERNAL_FALLBACK"] == "0"
    assert environment["QMT_MINUTE_MIN_COVERAGE"] == "1.0"
    assert environment["MYQUANT_MINUTE_DATE"] == "2026-08-26"


def test_strict_bigqmt_route_never_falls_back_when_runtime_is_unavailable():
    with patch(
        "tools.sync_qmt_primary.build_child_env",
        return_value={"BIG_QMT_BRIDGE_ENABLED": "true"},
    ), patch(
        "tools.sync_qmt_primary._bigqmt_runtime_available",
        return_value=False,
    ), patch("tools.sync_qmt_primary.subprocess.run") as run:
        result = sync_qmt_primary.run_dataset(
            "minute_price",
            date_str="2026-08-26",
            require_bigqmt=True,
        )

    assert result["status"] == "DATA_BLOCKED"
    assert result["source_policy"] == "bigqmt_required_unavailable"
    run.assert_not_called()
