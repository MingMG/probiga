from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_qmt_autostart_never_recycles_a_running_client_from_title_only():
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    assert "Test-QmtAutoStartWindow" in source
    assert "DayOfWeek]::Saturday" in source
    assert "Get-QmtRetryDelaySeconds" in source
    assert "QMT_CLIENT_MIN_BACKOFF_SECONDS" in source
    assert "QMT_CLIENT_MAX_BACKOFF_SECONDS" in source
    assert "no daily attempt limit" in source
    assert "QMT_CLIENT_MAX_RESTART_ATTEMPTS" not in source
    assert "XtItClient.exe" in source
    assert "Test-QmtClientLoggedIn" in source
    ensure_source = source.split("function Ensure-QmtClient", 1)[1].split(
        "$python = Resolve-PythonPath", 1
    )[0]
    assert 'status = "login_unverified"' in ensure_source
    assert "Leaving the client untouched" in ensure_source
    assert "Stop-Process" not in ensure_source
    assert '$running.Count -eq 0 -and [string]$state.status -eq "login_unverified"' in ensure_source
    assert "$failures = 0" in ensure_source


def test_big_qmt_strategy_recovery_uses_end_to_end_persistent_backoff():
    source = (
        ROOT / "tools" / "ensure_big_qmt_strategy_running.ps1"
    ).read_text(encoding="utf-8")
    assert "Test-RecoveryWindow" in source
    assert "DayOfWeek]::Saturday" in source
    assert "Get-EndToEndHealth" in source
    assert "FullSnapshotMaxAgeSeconds" in source
    assert "SyncReceiptMaxAgeSeconds" in source
    assert "MinimumBackoffSeconds" in source
    assert "MaximumBackoffSeconds" in source
    assert "MaxAttemptsPerDay" not in source
    assert "PROBIGA_BIGQMT_BRIDGE" in source
    assert "Test-HeartbeatHealthy" in source
    assert '$status -notin @("running", "busy")' in source
    assert "WaitOne(0)" in source
    assert "AbandonedMutexException" in source
    assert "StaleTakeover" in source
    assert "TotalSeconds -lt 120" in source
    assert "client is not logged in" in source
    assert "client_started_at" in source
    assert "no daily attempt limit" in source
    assert "QMT 2.1.19" in source
    assert "0.107 0.077" in source
    assert "0.056/0.039 is the account badge" in source
    assert "0.470 0.015" in source
    assert "FindStrategyPaneLeft" in source
    assert "CreateDIBSection" in source
    assert "BitBlt" in source
    assert "$fullWidthList" in source
    assert "$embeddedList" in source
    assert "$paneLeft + 70" in source
    assert "$paneLeft + 322" in source
    assert "SearchX = 0.325" not in source
    assert "EditX = 0.458" not in source
    assert "0.339 0.151" in source
    assert "last_price" not in source


def test_local_supervisor_invokes_big_qmt_strategy_recovery():
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    assert "BIG_QMT_STRATEGY_AUTO_RECOVER" in source
    assert "ensure_big_qmt_strategy_running.ps1" in source
    strategy_flag = source.split("function Test-BigQmtStrategyAutoRecover", 1)[1].split(
        "function Resolve-QmtClientPath", 1
    )[0]
    assert "return $false" in strategy_flag
    assert "return Test-QmtClientAutoRestart" not in strategy_flag


def test_big_qmt_consumer_gets_cold_start_grace_before_receipt_restart():
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    assert "BIG_QMT_CONSUMER_STARTUP_GRACE_SECONDS" in source
    assert "$consumerStartupGraceSeconds = 300" in source
    assert "$consumerAgeSeconds" in source
    assert "$consumerAgeSeconds -ge $consumerStartupGraceSeconds" in source
    assert "BIG_QMT_CONSUMER_FAILURE_GRACE_SECONDS" in source
    assert "BIG_QMT_CONSUMER_FAILURE_CHECKS" in source
    assert "BIG_QMT_CONSUMER_MAX_SAMPLE_GAP_SECONDS" in source
    assert "$failureCount -ge $consumerFailureChecks" in source
    assert "$failureAgeSeconds -ge $consumerFailureGraceSeconds" in source
    assert "$consumerMaxSampleGapSeconds" in source
    assert "consumer_started_at" in source
    assert "Unknown health must break the consecutive-failure series" in source
    assert "Sync receipt recovered; consumer restart guard reset." in source
    assert "WaitForExit(150000)" in source
    assert source.index("$consumerAgeSeconds -ge $consumerStartupGraceSeconds") < source.index(
        "check_big_qmt_end_to_end_health.py"
    )


def test_big_qmt_consumer_restart_terminates_its_delegated_process_tree():
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    stop_source = source.split("function Stop-ManagedProcess", 1)[1].split(
        "$script:ServiceProcessInventoryLoaded", 1
    )[0]
    assert '$ServiceKey -eq "big_qmt_bridge"' in stop_source
    assert "taskkill.exe /PID $proc.Id /T /F" in stop_source
    assert "Get-Process -Id $proc.Id" in stop_source


def test_launcher_does_not_start_legacy_unbounded_watchdog():
    source = (ROOT / "tools" / "launch_local_live_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert "run_qmt_client_watchdog.ps1" not in source


def test_legacy_watchdog_has_no_three_attempt_daily_stop():
    source = (ROOT / "tools" / "run_qmt_client_watchdog.ps1").read_text(
        encoding="utf-8"
    )
    assert "$minimumBackoffSeconds = 30" in source
    assert "$maximumBackoffSeconds = 900" in source
    assert "$attemptCount -lt 3" not in source
    assert "no_daily_limit" in source
    assert "[System.DayOfWeek]::Saturday" in source
    assert "[TimeSpan]::FromHours(6.5)" in source
    assert "XtItClient.exe" in source


def test_supervisor_checks_the_thirty_second_sla_frequently():
    source = (
        ROOT / "tools" / "run_local_live_supervisor.ps1"
    ).read_text(encoding="utf-8")
    assert "Start-Sleep -Seconds 5" in source


def test_local_status_includes_sanitized_login_diagnostic():
    source = (ROOT / "tools" / "status_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    assert "diagnose_bigqmt_login.py" in source
    assert "sanitized" in source
