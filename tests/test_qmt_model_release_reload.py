from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _source() -> str:
    return (ROOT / "tools" / "reload_big_qmt_strategy.ps1").read_text(
        encoding="utf-8"
    )


def test_release_reload_is_bound_to_clean_registered_exact_main() -> None:
    source = _source()

    assert "[string]$RegisteredRoot" in source
    assert "[string]$ExpectedBuildSha" in source
    assert "$Root -ine $ExpectedRoot" in source
    assert '"rev-parse", "--show-toplevel"' in source
    assert '"remote", "get-url", "origin"' in source
    assert '$Branch -cne "main"' in source
    assert "$Head -cne $ExpectedBuild" in source
    assert "$ExpectedBuild`:$StrategyRepositoryPath" in source
    assert "[string]$Release.strategy_git_blob -cne $Blob" in source
    assert '"status", "--porcelain", "--untracked-files=normal"' in source
    assert '"https://github.com/MingMG/probiga.git"' in source


def test_release_reload_requires_one_logged_in_interactive_qmt_client() -> None:
    source = _source()

    assert 'Get-Process -Name "XtItClient"' in source
    assert "$QmtClients.Count -ne 1" in source
    assert "$QmtClient.SessionId -ne [int]$CurrentSession" in source
    assert '$QmtMainTitle -notmatch "^\\s*\\d+\\s*-\\s*.+QMT"' in source
    assert "Assert-NoUnexpectedVisibleQmtWindow" in source
    assert "login, CAPTCHA, confirmation" in source
    assert '"NEEDS_USER_ACTION"' in source
    assert "$FinalExitCode = if ($NeedsUser) { 3 } else { 2 }" in source


def test_release_reload_never_targets_an_ambiguous_or_other_model() -> None:
    source = _source()

    assert '$StrategyName = "PROBIGA_BIGQMT_BRIDGE"' in source
    assert "$EditorTitle = \"$StrategyName$EditorSuffix\"" in source
    assert "Assert-NoOtherStrategyEditors" in source
    assert "another QMT strategy editor is open" in source
    assert "target QMT strategy editor is not unique" in source
    assert "GetWindowThreadProcessId" in source
    assert "QMT click target identity changed" in source
    assert "Invoke-ExactWindowClick $Editor $EditorTitle" in source
    assert "Stop-Process" not in source
    assert "Start-Process" not in source


def test_release_reload_locates_the_visible_strategy_pane_and_fails_closed() -> None:
    source = _source()

    assert "FindStrategyPaneLeft" in source
    assert "CreateDIBSection" in source
    assert "BitBlt" in source
    assert "Get-QmtStrategyPaneLayout" in source
    assert "$FullWidthList" in source
    assert "$EmbeddedList" in source
    assert "$PaneLeft + 70" in source
    assert "$PaneLeft + 322" in source
    assert "the visible QMT model-research strategy pane is not unique" in source
    assert "Invoke-ExactScreenPointClick" in source
    assert "QMT point-click target identity changed" in source
    assert "QMT point click escapes the exact target window" in source
    assert "SearchX = 0.325" not in source
    assert "EditX = 0.458" not in source


def test_atomic_install_finishes_before_the_old_model_is_stopped() -> None:
    source = _source()
    spool = (ROOT / "integrations" / "bigqmt" / "spool.py").read_text(
        encoding="utf-8"
    )
    installer = (ROOT / "tools" / "run_big_qmt_bridge.py").read_text(
        encoding="utf-8"
    )

    backup = source.index("$Backup = New-ArtifactBackup")
    install = source.index("$Release = Invoke-ExactStrategyInstall")
    still_old = source.index("$StillOld = Get-Heartbeat", install)
    stop = source.index("Stop-ExactStrategy $Editor", still_old)
    close = source.index("Close-ExactStrategyEditor $Editor", stop)
    reopen = source.index("$NewEditor = Open-ExactStrategyEditor", close)
    start = source.index("$Loaded = Start-ExactStrategy", reopen)
    assert backup < install < still_old < stop < close < reopen < start

    assert "temporary_target = installed_target.with_name" in spool
    assert "_replace_with_retry(temporary_target, installed_target)" in spool
    assert spool.index("_replace_with_retry(temporary_target, installed_target)") < spool.index(
        "return target", spool.index("def install_qmt_strategy")
    )
    assert "manifest_temporary" in installer
    assert "os.fsync(handle.fileno())" in installer
    assert "_replace_with_retry(manifest_temporary, manifest_path)" in installer
    assert "Move-Item -LiteralPath $Temporary -Destination $Path -Force" in source


def test_loaded_identity_must_come_from_the_qmt_process_and_match_all_hashes() -> None:
    source = _source()

    expected_checks = (
        '[int]$Heartbeat.pid -eq [int]$QmtClient.Id',
        '[string]$Heartbeat.strategy_build_sha -ceq $ExpectedBuild',
        '[string]$Heartbeat.strategy_git_blob -ceq',
        '[string]$Heartbeat.strategy_source_sha256 -ceq',
        '[string]$Heartbeat.strategy_artifact_sha256 -ceq',
        '[string]$Heartbeat.strategy_loaded_identity_sha256 -ceq',
        '$Heartbeat.strategy_identity_frozen -eq $true',
        '[string]$Heartbeat.strategy_identity_status -eq "BOUND"',
    )
    for expected in expected_checks:
        assert expected in source
    assert 'direct_python_strategy_execution = $false' in source
    assert 'automatic_order_submission = $false' in source
    assert "Test-ExpectedReleaseHeartbeat $Heartbeat $Release" in source


def test_failed_reload_restores_the_previous_artifact_and_model_or_fails_closed() -> None:
    source = _source()

    assert "function New-ArtifactBackup" in source
    assert "function Restore-OriginalArtifact" in source
    assert "function Invoke-ModelRollback" in source
    assert "original-strategy-$Index.bin" in source
    assert "original-manifest.json" in source
    assert "failed-new-manifest.json" in source
    assert "Test-OriginalReleaseHeartbeat" in source
    assert '"OLD_MODEL_RETAINED"' in source
    assert '"OLD_MODEL_RESTORED"' in source
    assert '"FILES_OR_MODEL_UNVERIFIED"' in source
    assert '"FAILED_CLOSED"' in source
    assert '"NEEDS_USER_ACTION"' in source
    assert "$RollbackStatus = Invoke-ModelRollback" in source
    assert "$FinalExitCode = if ($NeedsUser) { 3 } else { 2 }" in source
    assert source.index("Restore-OriginalArtifact") < source.index(
        "$Editor = Open-ExactStrategyEditor", source.index("function Invoke-ModelRollback")
    )
    rollback = source.index("$RollbackStatus = Invoke-ModelRollback")
    classify = source.index("$Status = if ($NeedsUser)", rollback)
    assert rollback < classify


def test_updater_reloads_before_restarting_the_writer_and_bootstrap() -> None:
    updater = (ROOT / "tools" / "update_qmt_windows_edge.ps1").read_text(
        encoding="utf-8"
    )
    register = (
        ROOT / "tools" / "register_qmt_windows_edge_scheduler_task.ps1"
    ).read_text(encoding="utf-8")

    ready_call = updater.index("--check-ready --expected-build-sha $TargetSha")
    ready_branch = updater.index("if ($ReadyExit -eq 0)", ready_call)
    ready_exit = updater.index("exit 0", ready_branch)
    migration_state = updater.index('$PreparedSha = ""', ready_exit)
    unavailable_branch = updater.index("if ($ReadyExit -ne 4)", ready_exit)
    unavailable_exit = updater.index("exit $ReadyExit", unavailable_branch)
    call = updater.index("-ExpectedBuildSha $CurrentSha")
    start = updater.index("Start-EdgeScheduler", call)
    bootstrap = updater.index("--bootstrap --expected-build-sha", start)
    assert ready_call < ready_branch < ready_exit < migration_state
    assert ready_exit < unavailable_branch < unavailable_exit < migration_state
    assert call < start < bootstrap
    assert "$StrategyReloadExit -eq 3" in updater
    assert 'exit 3' in updater
    assert "failed closed" in updater
    assert '"ProBigA\\qmt-model-reload"' in register
    assert "$StrategyReloader" in register

    reloader = (ROOT / "tools" / "reload_big_qmt_strategy.ps1").read_text(
        encoding="utf-8"
    )
    assert "@Arguments 2>$null" in reloader
    assert "@Arguments 2>&1" not in reloader
    assert '$ErrorActionPreference = "Continue"' in reloader
    assert "$ErrorActionPreference = $PreviousPreference" in reloader
    assert "$ExitCode = $LASTEXITCODE" in reloader
    assert "$ExitCode -ne 0" in reloader
