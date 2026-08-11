from __future__ import annotations

import subprocess
import sys
from unittest.mock import MagicMock, patch

from tools import probiga_ops


def test_probiga_ops_lists_commands(capsys):
    assert probiga_ops.main(["--list"]) == 0

    output = capsys.readouterr().out
    assert "check-db" in output
    assert "quality-gate" in output
    assert "security-check" in output


def test_probiga_ops_builds_known_command():
    cmd = probiga_ops.build_command("check-db")

    assert cmd[0] == sys.executable
    assert cmd[1].replace("\\", "/") == "deploy/check_db.py"


def test_probiga_ops_runs_with_child_env_and_timeout():
    completed = MagicMock(returncode=0)

    with patch("tools.probiga_ops.build_child_env", return_value={"MYSQL_URL": "mysql://example"}) as child_env, patch(
        "tools.probiga_ops.subprocess.run",
        return_value=completed,
    ) as run:
        assert probiga_ops.main(["quality-gate", "--timeout-seconds", "12"]) == 0

    child_env.assert_called_once_with(probiga_ops.ROOT)
    assert run.call_args.args[0][1].replace("\\", "/") == "tools/ensure_quality_gate.py"
    assert run.call_args.kwargs["cwd"] == str(probiga_ops.ROOT)
    assert run.call_args.kwargs["env"] == {"MYSQL_URL": "mysql://example"}
    assert run.call_args.kwargs["timeout"] == 12


def test_probiga_ops_timeout_returns_124():
    with patch(
        "tools.probiga_ops.subprocess.run",
        side_effect=subprocess.TimeoutExpired(["python"], 1),
    ):
        assert probiga_ops.main(["check-db", "--timeout-seconds", "1"]) == 124
