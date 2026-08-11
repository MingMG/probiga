from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_qmt_autostart_is_bounded_and_retries_without_daily_limit():
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
    assert "TotalSeconds -lt 120" in source
    assert "Stop-Process -Id $staleClient.Id" in source


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
    assert "WaitOne(0)" in source
    assert "AbandonedMutexException" in source
    assert "StaleTakeover" in source
    assert "TotalSeconds -lt 120" in source
    assert "client is not logged in" in source
    assert "client_started_at" in source
    assert "no daily attempt limit" in source
    assert "QMT 2.1.19" in source
    assert "0.105 0.078" in source
    assert "0.470 0.030" in source
    assert "SearchX = 0.325" in source
    assert "SearchY = 0.065" in source
    assert "EditX = 0.458" in source
    assert "EditY = 0.133" in source
    assert "SearchX = 0.125" in source
    assert "SearchX = 0.380" in source
    assert "0.356 0.150" in source
    assert "last_price" not in source


def test_local_supervisor_invokes_big_qmt_strategy_recovery():
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    assert "BIG_QMT_STRATEGY_AUTO_RECOVER" in source
    assert "ensure_big_qmt_strategy_running.ps1" in source


def test_big_qmt_consumer_gets_cold_start_grace_before_receipt_restart():
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    assert "BIG_QMT_CONSUMER_STARTUP_GRACE_SECONDS" in source
    assert "$consumerStartupGraceSeconds = 300" in source
    assert "$consumerAgeSeconds" in source
    assert "$consumerAgeSeconds -ge $consumerStartupGraceSeconds" in source
    assert source.index("$consumerAgeSeconds -ge $consumerStartupGraceSeconds") < source.index(
        "check_big_qmt_end_to_end_health.py"
    )


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
