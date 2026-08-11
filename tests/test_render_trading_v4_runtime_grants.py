from __future__ import annotations

import json

import pytest

from server.integrations.v4_database_roles import (
    V4RuntimeDatabaseRole,
    V4RuntimeRoleContractError,
)
from tools.render_trading_v4_runtime_grants import (
    load_principals,
    render_report,
)


def _principal_file(tmp_path):
    path = tmp_path / "principals.json"
    path.write_text(
        json.dumps(
            {
                role.value: {
                    "user": f"ci_{role.value}",
                    "host": "127.0.0.1",
                }
                for role in V4RuntimeDatabaseRole
            }
        ),
        encoding="utf-8",
    )
    return path


def test_render_is_password_free_test_only_and_non_executing(tmp_path):
    report = render_report(
        database="pb_v4_test_runtime_roles",
        principal_path=_principal_file(tmp_path),
    )
    assert report["status"] == "DBA_REVIEW_REQUIRED"
    assert report["statements"]
    assert report["contains_passwords"] is False
    assert report["clears_mysql57_proxy_privileges"] is True
    assert report["requires_post_grant_runtime_audit"] is True
    assert report["executed"] is False
    assert report["production_activation_allowed"] is False
    assert report["actionable_output_allowed"] is False
    with pytest.raises(V4RuntimeRoleContractError, match="v4_test"):
        render_report(
            database="probiga_production",
            principal_path=_principal_file(tmp_path),
        )


def test_principal_input_is_exact_and_duplicate_safe(tmp_path):
    valid = _principal_file(tmp_path)
    assert set(load_principals(valid)) == set(V4RuntimeDatabaseRole)
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text(
        '{"v4_predictor":{},"v4_predictor":{}}',
        encoding="utf-8",
    )
    with pytest.raises(V4RuntimeRoleContractError, match="duplicate"):
        load_principals(duplicate)
