from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "complete_mysql84_upgrade.ps1"


def test_completion_script_parses_as_powershell() -> None:
    escaped = str(SCRIPT).replace("'", "''")
    command = (
        "$tokens=$null; $errors=$null; "
        "[System.Management.Automation.Language.Parser]::ParseFile("
        f"'{escaped}', [ref]$tokens, [ref]$errors) | Out-Null; "
        "if($errors.Count){exit 2}"
    )
    completed = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
        check=False,
    )
    assert completed.returncode == 0


def test_completion_orders_freeze_acceptance_data_and_service_cutover() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    stages = [
        '"freeze-writers"',
        '"freeze-source"',
        '"final-acceptance"',
        '"provision-runtime"',
        '"stop-target"',
        '"stop-source"',
        '"data-layout"',
        '"service-cutover"',
    ]
    offsets = [text.index(stage) for stage in stages]
    assert offsets == sorted(offsets)
    assert "business_writers_remain_frozen = $true" in text
    assert "production_trading_activation_changed = $false" in text


def test_completion_preannounces_source_stop_to_lock_guardian() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    preannounce = text.index('WriteAllText($freezeStop, "MYSQL55_SERVICE_STOPPED')
    stop_service = text.index('Stop-Service -Name "MySQL"')
    assert preannounce < stop_service
    assert '"--defaults-file=$sourceOptions" --protocol=tcp --host=127.0.0.1 --port=3306 shutdown' in text
    assert "Legacy MySQL service stop failed through both SCM and mysqladmin" in text
    assert 'WriteAllText($freezeStop, "ABORT' in text
    refresh = text.index("$guardian.Refresh()")
    final_report = text.index("$freezeFinalReport = Get-Content")
    final_validation = text.index('$freezeFinalReport.status -ne "service_stopped"')
    unsafe_exit = text.index('throw "Freeze guardian reported an unsafe lock loss"')
    assert refresh < final_report < final_validation < unsafe_exit


def test_completion_automatically_restores_legacy_after_post_stop_failure() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert '"automatic-rollback"' in text
    assert "-Mode PrepareRollback" in text
    assert "I_CONFIRM_WRITES_FROZEN_AND_MYSQL55_PHYSICAL_DATA_RESTORED" in text
    assert "legacy-reactivation-with-unchanged-env" in text
    assert "legacy-layout-already-intact" in text
    assert '"rolled-back"' in text
    assert '"data-layout.stdout.log"' in text
    assert '"data-layout.stderr.log"' in text
    assert "Confirm-StageMySQL84LayoutEvidence" in text
    assert '"data-layout-verified"' in text
    assert "full_file_manifest_verified -ne $true" in text
    assert "source_ibdata_removed_after_verified_copy -ne $true" in text
    assert "Never discard that" in text
    assert "Cold data-layout transition failed: invocation=$layoutInvocationFailure" in text
    assert "A transient F: failure must never prevent recovery" in text
    status_before_service = text.index('Set-CompletionStatus "running" "automatic-rollback"')
    service_restart = text.index('Start-Service -Name "MySQL"', status_before_service)
    evidence_after_service = text.index("Write-AtomicJson -Path $automaticRollbackService", service_restart)
    assert status_before_service < service_restart < evidence_after_service


def test_completion_can_seed_a_snapshot_bound_binlog_checkpoint() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "[string]$SeedBinlogCheckpoint" in text
    assert 'format -ne "probiga.mysql55_to_mysql84.binlog_checkpoint"' in text
    assert '"binlog-catchup.checkpoint.json"' in text
    assert "Seed binlog checkpoint copy verification failed" in text


def test_completion_can_resume_only_a_sealed_unchanged_acceptance() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "[string]$AcceptedWorkRoot" in text
    assert '"resume-accepted"' in text
    assert "Accepted output hash changed" in text
    assert "Source changed after the accepted frozen snapshot" in text
    assert '"resume-source-restart-tail"' in text
    assert "verify_mysql55_restart_only_binlog_tail.py" in text
    assert "business_or_unknown_event_count" in text
    assert "Live target identity or TLS differs" in text
    assert "--batch --raw --skip-column-names" in text
    assert "resume-business-smoke.json" in text
    assert "Previously provisioned runtime artifacts are not reusable" in text
    assert "Final MySQL service registration requires an elevated Administrator process" in text


def test_completion_requires_post_cutover_production_tls_business_smoke() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    service_cutover = text.index('"service-cutover"')
    production_smoke = text.index('"production-business-smoke"')
    final_pass = text.index('Set-CompletionStatus "passed" "complete"')
    assert service_cutover < production_smoke < final_pass
    assert "--expected-server-port 3306" in text
    assert "I_CONFIRM_READ_ONLY_MYSQL84_PRODUCTION_SMOKE" in text
    assert "production_business_smoke = $productionBusinessSmoke" in text
