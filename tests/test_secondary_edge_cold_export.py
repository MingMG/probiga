"""Windows PS5 cold export helpers only: no source freeze, network or database."""
from pathlib import Path
import json
import os
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools/secondary_edge/export_cold_package.ps1"
PS = shutil.which("powershell.exe")
pytestmark = pytest.mark.skipif(os.name != "nt" or not PS, reason="Windows PowerShell 5 required")


def run_ps(code):
    return subprocess.run([PS, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command",
                           "$ErrorActionPreference='Stop';" + code], capture_output=True, text=True,
                          encoding="utf-8", errors="replace", timeout=30)


def quoted(value):
    return "'" + str(value).replace("'", "''") + "'"


def helpers(code):
    return run_ps(f". {quoted(SCRIPT)};" + code)


def test_real_ps5_parser_and_ascii_readme():
    result = run_ps("$t=$null;$e=$null;[System.Management.Automation.Language.Parser]::ParseFile("
                    + quoted(SCRIPT) + ",[ref]$t,[ref]$e)|Out-Null;if($e.Count){throw ($e|Out-String)}")
    assert result.returncode == 0, result.stderr
    (SCRIPT.parent / "COLD_README.txt").read_bytes().decode("ascii")


def test_dot_source_never_runs_export():
    result = helpers("Get-Command Invoke-ColdPackageExport|Select-Object -ExpandProperty Name")
    assert result.returncode == 0, result.stderr
    assert "Invoke-ColdPackageExport" in result.stdout


@pytest.mark.parametrize('matching', [True, False])
def test_native_python_identity_uses_real_interpreter_without_inline_quote_loss(matching):
    expected = 'Python ' + '.'.join(str(part) for part in sys.version_info[:3]) if matching else 'Python 0.0.0'
    result = helpers(f"Assert-ColdExportPythonIdentity {quoted(sys.executable)} {quoted(expected)}")
    assert (result.returncode == 0) is matching, result.stdout + result.stderr
    if not matching:
        assert 'Source Python version mismatch.' in result.stderr


def test_source_python_requires_exact_packaged_versions():
    text = SCRIPT.read_text(encoding='ascii')
    assert "@($Python313,'Python 3.13.14')" in text
    assert "@($Python314,'Python 3.14.3')" in text
    assert 'Assert-ColdExportPythonIdentity $version[0] $version[1]' in text
    assert '$actual = & $version[0] -c' not in text


def test_immutable_installers_keep_sha_and_all_six_signed_publishers():
    result = helpers("ConvertTo-Json -InputObject @(Get-ColdExportArtifacts) -Depth 5 -Compress")
    assert result.returncode == 0, result.stderr
    items = json.loads(result.stdout)
    assert len(items) == 6
    specs = {item["name"]: item for item in items}
    assert specs["git.exe"]["publisher"] == "Johannes Schindelin"
    for name in ("python313.exe", "python314.exe", "git.exe"):
        assert len(specs[name]["sha256"]) == 64
    assert all(item["publisher"] and item["url"].startswith("https://") for item in items)


def test_copy_preserves_lock_pid_hidden_and_empty_directories(tmp_path):
    source = tmp_path / "source"
    target = tmp_path / "copied"
    source.mkdir()
    (source / "state.lock").write_bytes(b"private-lock-state")
    (source / "state.pid").write_bytes(b"12345")
    (source / "payload").mkdir()
    (source / "empty").mkdir()
    (source / "payload" / "data.bin").write_bytes(bytes(range(255)))
    result = helpers(f"Copy-ColdExportTree {quoted(source)} {quoted(target)}")
    assert result.returncode == 0, result.stderr
    assert (target / "empty").is_dir()
    for name in ("state.lock", "state.pid", "payload/data.bin"):
        assert (target / name).read_bytes() == (source / name).read_bytes()
    repeat = helpers(f"Copy-ColdExportTree {quoted(source)} {quoted(target)}")
    assert repeat.returncode != 0


@pytest.mark.parametrize("process", [
    "[pscustomobject]@{Name='XtItClient.exe';ExecutablePath=$null}",
    r"[pscustomobject]@{Name='python.exe';ExecutablePath='D:\QMT\python\python.exe'}",
])
def test_any_qmt_process_refuses_snapshot(process):
    result = helpers("function Get-CimInstance {" + process + r"};Assert-ColdExportQmtStopped 'D:\QMT'")
    assert result.returncode != 0


def test_caller_command_substrings_do_not_count_as_qmt():
    result = helpers("function Get-CimInstance {[pscustomobject]@{Name='powershell.exe';"
                     "ExecutablePath='C:\\Windows\\powershell.exe';CommandLine='export_cold_package QMT XtItClient.exe'}};"
                     "Assert-ColdExportQmtStopped 'D:\\QMT'")
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("size,minimum", [(1, 250 * 1024**3), (300 * 1024**3, 330 * 1024**3)])
def test_manifest_measured_storage_and_paused_boundaries(size, minimum):
    result = helpers("$f=@([pscustomobject]@{path='data';bytes=" + str(size) + ";sha256=('a'*64)});"
                     "$m=New-ColdExportManifest ('a'*40) source origin '' @{} $f;"
                     "$m|ConvertTo-Json -Depth 10 -Compress")
    assert result.returncode == 0, result.stderr
    manifest = json.loads(result.stdout)
    assert manifest["minimum_target_free_bytes"] == minimum
    assert manifest["payload_bytes"] == size
    assert manifest["source_paused"] is True
    assert manifest["production_activation"] is False
    assert manifest["restore_requested"] is False
    assert manifest["format"] == "probiga.windows-cold-migration.v2"


@pytest.mark.parametrize("state,mode,expected", [("Running", "Disabled", 1), ("Stopped", "Auto", 1),
                                                ("Stopped", "Manual", 1), ("Stopped", "Disabled", 0)])
def test_receipt_does_not_override_actual_unpaused_service(state, mode, expected):
    result = helpers("$l=[pscustomobject]@{format='probiga.cold-source-layout.v1';"
                     "source=[pscustomobject]@{hostname='source';version='8.4.11';service_name='ProBigA-MySQL84';server_uuid='u'}};"
                     "$p=[pscustomobject]@{format='probiga.source-pause.v1';status='paused';source_host='source';"
                     "source_server_uuid='u';source_service_name='ProBigA-MySQL84';source_service_state='Stopped';"
                     "source_service_startup='Disabled';source_processes_running=$false;shutdown_complete=$true;"
                     "source_qmt_running=$false;"
                     "source_automatically_resume=$false;completed_at_utc='now'};"
                     "function Get-CimInstance {[pscustomobject]@{State='" + state + "';StartMode='" + mode + "';ProcessId=0}};"
                     "Assert-ColdExportPause $l $p source")
    assert (result.returncode != 0) == bool(expected), result.stderr


def test_export_source_contains_only_final_paused_entry_and_snapshot():
    text = SCRIPT.read_text(encoding="ascii")
    assert "'--source-layout',$SourceLayout" in text
    assert "'--pause-receipt',$PauseReceipt" in text
    assert "tools.secondary_edge.cold_database','snapshot'" in text
    for obsolete in ("export_package.ps1", "install_target.ps1", "verify_target.ps1", "database_export", "AdminClientFile", "*.lock", "*.pid"):
        assert obsolete not in text
    assert "Assert-ColdPackage $OutputRoot" in text
    assert "' ' + (Get-Sha256" in text


def test_private_project_archive_preserves_main_db_wal_and_legacy_only_under_audit(tmp_path):
    development = tmp_path / "development"
    production = tmp_path / "production"
    package_root = tmp_path / "package"
    for source in (development, production):
        (source / "data").mkdir(parents=True)
        (source / "data/main.db").write_bytes(b"sqlite-private-snapshot")
        (source / "data/main.db-wal").write_bytes(b"matching-wal")
        (source / "runtime/backfill_evidence").mkdir(parents=True)
        (source / "runtime/backfill_evidence/raw.bin").write_bytes(b"business-evidence")
        (source / "outputs/backtest").mkdir(parents=True)
        (source / "outputs/backtest/result.json").write_bytes(b"private-backtest")
        (source / "artifacts/acceptance").mkdir(parents=True)
        (source / "artifacts/acceptance/evidence.json").write_bytes(b"private-acceptance")
        (source / "runtime/windows-app-credentials").mkdir()
        (source / "runtime/windows-app-credentials/private.bin").write_bytes(b"must-not-transfer")
        (source / ".env").write_bytes(b"must-not-transfer")
        (source / "_archive").mkdir()
        (source / "_archive/capital_flow_restore.sql").write_bytes(b"historical-business-dump")
        (source / "_archive/probiga_remote_dump.sql.gz").write_bytes(b"historical-business-gzip")
        (source / "_archive/old_deploy.py").write_bytes(b"historical-code-not-runtime-scope")
    (development / "runtime/emquant-py36").mkdir()
    (development / "runtime/emquant-py36/python.exe").write_bytes(b"legacy-only")
    (development / "runtime/emquant-py36/python36._pth").write_bytes(b"python36.zip\n.\nimport site\n")
    result = helpers("function Get-CimInstance {return @()};Copy-ColdExportProjectArchive "
                     + " ".join(quoted(path) for path in (development, production, package_root)))
    assert result.returncode == 0, result.stderr
    archive = package_root / "audit/source-project-state"
    for label in ("development", "production"):
        assert (archive / label / "data/main.db").read_bytes() == b"sqlite-private-snapshot"
        assert (archive / label / "data/main.db-wal").read_bytes() == b"matching-wal"
        assert not (archive / label / "runtime/windows-app-credentials").exists()
        assert not (archive / label / ".env").exists()
        assert (archive / label / "outputs/backtest/result.json").read_bytes() == b"private-backtest"
        assert (archive / label / "artifacts/acceptance/evidence.json").read_bytes() == b"private-acceptance"
        assert (archive / label / "_archive/capital_flow_restore.sql").read_bytes() == b"historical-business-dump"
        assert (archive / label / "_archive/probiga_remote_dump.sql.gz").read_bytes() == b"historical-business-gzip"
        assert not (archive / label / "_archive/old_deploy.py").exists()
    assert (archive / "development/runtime/emquant-py36/python.exe").is_file()
    assert not (package_root / "runtime/emquant-py36").exists()
    metadata = json.loads((archive / "archive-metadata.json").read_text(encoding="utf-8"))
    assert metadata["archive_only"] is True
    assert metadata["legacy_runtime_activation"] is False
    assert metadata["historical_source_code_retained"] is True


@pytest.mark.parametrize("argument", [
    r'"--user-data-dir=E:\My Code\ProBigA\data\ai_bridge\deepseek_chrome_profile"',
    r'--user-data-dir="E:\My Code\ProBigA\data\ai_bridge\deepseek_chrome_profile"',
])
def test_source_chrome_exact_profile_prevents_cold_archive(argument):
    code = "function Get-CimInstance {[pscustomobject]@{CommandLine=" + quoted(argument) + "}};"
    result = helpers(code + r"Assert-ColdExportSourceBrowserStopped 'E:\My Code\ProBigA'")
    assert result.returncode != 0


def test_other_chrome_profile_does_not_block_source_archive():
    result = helpers("function Get-CimInstance {[pscustomobject]@{CommandLine="
                     + quoted('--user-data-dir="C:\\OtherProfile"') + "}};"
                     + r"Assert-ColdExportSourceBrowserStopped 'E:\My Code\ProBigA'")
    assert result.returncode == 0, result.stderr
