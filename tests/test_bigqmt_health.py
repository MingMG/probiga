from __future__ import annotations

import json
import subprocess
import sys
import shutil
import time
from pathlib import Path

import pytest

from integrations.bigqmt.health import evaluate_spool_health, file_token
from integrations.bigqmt.spool import bridge_paths


ROOT = Path(__file__).resolve().parents[1]


def _home(tmp_path: Path) -> Path:
    home = tmp_path / "QMT"
    (home / "bin.x64").mkdir(parents=True)
    (home / "userdata").mkdir()
    (home / "bin.x64" / "XtItClient.exe").write_bytes(b"")
    bridge_paths(home)["root"].mkdir(parents=True)
    return home


def _write(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )


def _healthy_files(tmp_path: Path, now_ts: float = 1_000.0) -> Path:
    home = _home(tmp_path)
    paths = bridge_paths(home)
    _write(
        paths["heartbeat"],
        {
            "status": "running",
            "updated_ts": now_ts - 2,
        },
    )
    _write(
        paths["full"],
        {
            "batch_id": "full-1000",
            "generated_ts": now_ts - 5,
            "quote_count": 5_500,
        },
    )
    token = file_token(paths["full"])
    _write(
        paths["consumer_status"],
        {
            "status": "idle",
            "generated_ts": now_ts - 1,
            "full_sync_receipt": {
                "source_batch_id": "full-1000",
                "source_full_file_token": token,
                "quality_status": "PASS",
            },
        },
    )
    return home


def test_end_to_end_health_requires_all_three_links(tmp_path):
    home = _healthy_files(tmp_path)

    result = evaluate_spool_health(home, now_ts=1_000)

    assert result["healthy"] is True
    assert result["checks"] == {
        "strategy_heartbeat": True,
        "full_market_snapshot": True,
        "sync_receipt": True,
        "level1_callback": True,
        "model_instance": True,
        "request_queue": True,
    }
    assert result["recovery_owner"] == "NONE"
    assert all(layer["healthy"] for layer in result["layers"].values())


def test_stale_heartbeat_blocks_even_with_fresh_files(tmp_path):
    home = _healthy_files(tmp_path)
    paths = bridge_paths(home)
    heartbeat = json.loads(paths["heartbeat"].read_text(encoding="utf-8"))
    heartbeat["updated_ts"] = 960
    _write(paths["heartbeat"], heartbeat)

    result = evaluate_spool_health(home, now_ts=1_000)

    assert result["healthy"] is False
    assert result["failed_checks"] == ["strategy_heartbeat"]


def test_receipt_for_an_older_file_cannot_attest_current_snapshot(tmp_path):
    home = _healthy_files(tmp_path)
    paths = bridge_paths(home)
    consumer = json.loads(
        paths["consumer_status"].read_text(encoding="utf-8")
    )
    consumer["full_sync_receipt"][
        "source_full_file_token"
    ] = "old-file-token"
    _write(paths["consumer_status"], consumer)

    result = evaluate_spool_health(home, now_ts=1_000)

    assert result["healthy"] is False
    assert result["failed_checks"] == ["sync_receipt"]


def test_active_session_requires_a_fresh_genuine_level1_callback(tmp_path):
    home = _healthy_files(tmp_path)

    result = evaluate_spool_health(
        home,
        now_ts=1_000,
        require_level1_callback=True,
    )

    assert result["healthy"] is False
    assert result["failed_checks"] == ["level1_callback"]
    assert result["level1_required"] is True


def test_fresh_level1_callback_completes_the_health_chain(tmp_path):
    home = _healthy_files(tmp_path)
    paths = bridge_paths(home)
    heartbeat = json.loads(
        paths["heartbeat"].read_text(encoding="utf-8")
    )
    heartbeat.update({
        "subscription_id": 7,
        "last_callback_ts": 995,
    })
    _write(paths["heartbeat"], heartbeat)
    _write(
        paths["tracked"],
        {
            "generated_ts": 996,
            "last_callback_ts": 995,
            "quotes": {},
        },
    )

    result = evaluate_spool_health(
        home,
        now_ts=1_000,
        require_level1_callback=True,
    )

    assert result["healthy"] is True
    assert result["checks"]["level1_callback"] is True
    assert result["level1_callback_age_seconds"] == 5


def test_health_cli_imports_real_runtime_dependencies():
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "check_big_qmt_end_to_end_health.py"),
            "--help",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--heartbeat-max-age" in result.stdout


def _idle_consumer(home, now_ts):
    payload = {
        "status": "idle_market_closed",
        "market_session": "off_session",
        "freshness_required": False,
        "generated_ts": now_ts - 1,
        "full_rows": 0,
        "tracked_rows": 0,
    }
    _write(bridge_paths(home)["consumer_status"], payload)
    return payload


def test_closed_market_liveness_does_not_attest_an_ingestion(tmp_path):
    home = _healthy_files(tmp_path)
    _idle_consumer(home, 1_000)

    result = evaluate_spool_health(home, now_ts=1_000)

    assert result["healthy"] is True
    assert result["status"] == "IDLE_MARKET_CLOSED"
    assert result["recovery_owner"] == "NONE"
    assert result["checks"]["sync_receipt"] is False
    assert result["sync_receipt_required"] is False
    assert result["ingestion_attested"] is False
    assert result["receipt"] == {}
    assert result["failed_checks"] == []
    assert result["layers"]["pipeline"]["checks"] == {"consumer_heartbeat": True}


@pytest.mark.parametrize("change", [
    {"generated_ts": 924},
    {"generated_ts": 1_001},
    {"generated_ts": None},
    {"freshness_required": True},
    {"freshness_required": "false"},
    {"market_session": "active"},
    {"status": "error"},
    {"full_rows": 1},
    {"tracked_rows": 1},
    {"full_rows": False},
    {"tracked_rows": None},
])
def test_idle_claim_requires_fresh_explicit_zero_write_evidence(tmp_path, change):
    home = _healthy_files(tmp_path)
    consumer = _idle_consumer(home, 1_000)
    consumer.update(change)
    _write(bridge_paths(home)["consumer_status"], consumer)

    result = evaluate_spool_health(home, now_ts=1_000)

    assert result["healthy"] is False
    assert result["sync_receipt_required"] is True
    assert "sync_receipt" in result["failed_checks"]


def test_session_reopen_requires_an_actual_new_ingestion(tmp_path):
    home = _healthy_files(tmp_path)
    consumer = _idle_consumer(home, 1_000)
    assert evaluate_spool_health(home, now_ts=1_000)["healthy"] is True
    consumer.update(status="idle", market_session="active", freshness_required=True)
    _write(bridge_paths(home)["consumer_status"], consumer)
    result = evaluate_spool_health(home, now_ts=1_000)
    assert result["healthy"] is False
    assert result["recovery_owner"] == "CONSUMER"
    consumer["full_sync_receipt"] = {
        "source_full_file_token": file_token(bridge_paths(home)["full"]),
        "quality_status": "PASS",
    }
    _write(bridge_paths(home)["consumer_status"], consumer)
    result = evaluate_spool_health(home, now_ts=1_000)
    assert result["healthy"] is True
    assert result["ingestion_attested"] is True


def test_closed_market_still_requires_live_matching_model(tmp_path):
    home = _healthy_files(tmp_path)
    _idle_consumer(home, 1_000)
    paths = bridge_paths(home)
    heartbeat = json.loads(paths["heartbeat"].read_text(encoding="utf-8"))
    heartbeat.update(pid=77, updated_ts=960)
    _write(paths["heartbeat"], heartbeat)
    result = evaluate_spool_health(home, now_ts=1_000, expected_client_pid=78)
    assert result["healthy"] is False
    assert set(result["failed_checks"]) == {"strategy_heartbeat", "model_instance"}
    assert result["recovery_owner"] == "QMT_MODEL"


def test_explicit_active_probe_cannot_use_closed_market_exemption(tmp_path):
    home = _healthy_files(tmp_path)
    _idle_consumer(home, 1_000)
    result = evaluate_spool_health(home, now_ts=1_000, require_level1_callback=True)
    assert result["healthy"] is False
    assert result["sync_receipt_required"] is True
    assert result["level1_required"] is True


@pytest.mark.parametrize("expected_pid,change,failed_check", [
    (77, {}, None),
    (78, {}, "model_instance"),
    (77, {"model_instance_id": ""}, "model_instance"),
    (77, {"heartbeat_seq": 0}, "model_instance"),
    (77, {"oldest_pending_request_age_seconds": 61}, "request_queue"),
    (77, {"oldest_inflight_request_age_seconds": 61}, "request_queue"),
])
@pytest.mark.parametrize("console_code_page", [936, 65001])
def test_powershell_recovery_resolves_collector_home_without_process_path(tmp_path, expected_pid, change, failed_check, console_code_page):
    powershell = shutil.which("powershell.exe")
    if not powershell:
        pytest.skip("Windows PowerShell is required")
    now_ts = time.time()
    home = _healthy_files(tmp_path / "中文 交易", now_ts)
    _idle_consumer(home, now_ts)
    heartbeat_path = bridge_paths(home)["heartbeat"]
    heartbeat = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    heartbeat.update(pid=77, schema_version=3, model_instance_id="test-model", heartbeat_seq=1)
    heartbeat.update(change)
    _write(heartbeat_path, heartbeat)
    script = tmp_path / "health_probe.ps1"
    script.write_text(r"""
param($Source, $Root, $TestPython, $QmtHome, [int]$ExpectedPid, [int]$ConsoleCodePage)
$ErrorActionPreference = 'Stop'
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $Source, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'Recovery script syntax invalid' }
foreach ($name in @('Get-Heartbeat', 'Get-EndToEndHealth')) {
    $function = $ast.Find({ param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq $name
    }, $true)
    if (!$function) { throw 'Health function missing' }
    $body = $function.Extent.Text.Replace(
        '$Python = Join-Path $Root ''.venv\Scripts\python.exe''',
        '$Python = $TestPython')
    . ([scriptblock]::Create($body))
}
$HeartbeatMaxAgeSeconds = 30
$FullSnapshotMaxAgeSeconds = 75
$SyncReceiptMaxAgeSeconds = 75
$Level1CallbackMaxAgeSeconds = 15
$env:BIG_QMT_HOME = $QmtHome
$client = [pscustomobject]@{ Id = $ExpectedPid; Path = $null }
[Console]::OutputEncoding = [Text.Encoding]::GetEncoding($ConsoleCodePage)
$result = Get-EndToEndHealth $client | ConvertTo-Json -Depth 5 -Compress
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
[Console]::WriteLine($result)
""", encoding="utf-8-sig")
    result = subprocess.run([
        powershell, "-NoProfile", "-NonInteractive", "-File", str(script),
        "-Source", str(ROOT / "tools/ensure_big_qmt_strategy_running.ps1"),
        "-Root", str(ROOT), "-TestPython", sys.executable,
        "-QmtHome", str(home),
        "-ExpectedPid", str(expected_pid),
        "-ConsoleCodePage", str(console_code_page),
    ], capture_output=True, encoding="utf-8", timeout=30, check=False)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert Path(payload["BridgeRoot"]) == bridge_paths(home)["root"]
    assert payload["Heartbeat"]["pid"] == 77
    assert payload["Healthy"] is (failed_check is None)
    assert payload["SyncReceiptHealthy"] is False
    assert payload["ModelInstanceHealthy"] is (failed_check != "model_instance")
    if failed_check:
        assert failed_check in payload["FailedChecks"]
