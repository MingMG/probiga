import json
import os
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("pending_recovery", [False, True])
@pytest.mark.parametrize("qmt_calls", [False, True])
def test_login_absence_is_data_only_when_no_model_transaction_or_actions(tmp_path, pending_recovery, qmt_calls):
    system_root = os.environ.get("SystemRoot")
    if not system_root:
        pytest.skip("Windows PowerShell 5.1 required")
    powershell = Path(system_root) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    updater = (ROOT / "tools/update_qmt_windows_edge.ps1").read_text(encoding="utf8")
    helper = updater[updater.index("function Invoke-ReadOnlyStrategyPreflight"):updater.index("function Invoke-Git")]
    sha = "a" * 40
    payload = {"schema": "probiga.bigqmt-ui-release-reload.v1", "mode": "PREFLIGHT_ONLY",
               "status": "NEEDS_USER_ACTION", "data_status": "DATA_BLOCKED", "expected_build_sha": sha,
               "reason_code": "QMT_LOGIN_REQUIRED", "qmt_calls": qmt_calls, "database_writes": False,
               "ui_actions_attempted": False, "authentication_attempted": False,
               "automatic_order_submission": False, "direct_python_strategy_execution": False}
    stub = tmp_path / "preflight.cmd"
    stub.write_text("@echo off\necho " + json.dumps(payload, separators=(",", ":")) + "\nexit /b 3\n", encoding="ascii")
    marker = tmp_path / "ProBigA/qmt-model-reload/cold-start-recovery.json"
    if pending_recovery:
        marker.parent.mkdir(parents=True)
        marker.write_text("original model transaction", encoding="ascii")
    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"
    program = f'''
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$env:ProgramData = {literal(tmp_path)}
$PowerShellExe = {literal(stub)}
$StrategyReloader = 'unused'
$ExpectedRoot = {literal(tmp_path)}
function Write-UpdateLog([string]$Message) {{}}
{helper}
$Failed = $false
$Result = ''
try {{ $Result = Invoke-ReadOnlyStrategyPreflight '{sha}' }} catch {{ $Failed = $true }}
[ordered]@{{failed=$Failed;result=$Result}} | ConvertTo-Json -Compress
'''
    script = tmp_path / "probe.ps1"
    script.write_text(program, encoding="utf8")
    completed = subprocess.run([str(powershell), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
                               capture_output=True, text=True, timeout=15, check=False)
    if qmt_calls:
        assert completed.returncode == 3
    else:
        assert completed.returncode == 0, completed.stderr
        result = json.loads(completed.stdout)
        assert result == {"failed": pending_recovery, "result": "" if pending_recovery else "DATA_UNAVAILABLE"}
    if pending_recovery:
        assert marker.read_text(encoding="ascii") == "original model transaction"


def test_runtime_start_is_after_schema_and_before_data_probe():
    updater = (ROOT / "tools/update_qmt_windows_edge.ps1").read_text(encoding="utf8")
    schema = updater.index("$SchemaValidationOutput = &")
    heartbeat = updater.index("$RuntimeScheduler = Start-EdgeScheduler $CurrentSha")
    data_probe = updater.index("$StrategyPreflightStatus = Invoke-ReadOnlyStrategyPreflight $CurrentSha")
    blocked = updater[data_probe:updater.index("$StrategyColdStartRequired", data_probe)]
    assert schema < heartbeat < data_probe
    assert "exit 3" in blocked and "Stop-EdgeScheduler" not in blocked
