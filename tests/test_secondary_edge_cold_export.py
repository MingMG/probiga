"""Windows PS5 cold export helpers only: no source freeze, network or database."""
from pathlib import Path
import hashlib
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


@pytest.fixture(autouse=True)
def isolate_optional_machine_asset_paths(tmp_path, monkeypatch):
    user_profile = tmp_path / "fixture-user"
    local_app_data = tmp_path / "fixture-local-app-data"
    user_profile.mkdir()
    local_app_data.mkdir()
    monkeypatch.setenv("USERPROFILE", str(user_profile))
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))
    return user_profile, local_app_data


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


def test_application_requirements_use_stdlib_typing_not_obsolete_backport():
    requirements = (ROOT / 'deploy/windows_app_requirements.txt').read_text(encoding='ascii')
    names = {line.split('==', 1)[0].lower().replace('_', '-')
             for line in requirements.splitlines() if line and not line.startswith('#')}
    assert 'typing' not in names
    assert 'typing-extensions' in names
    assert 'typing-inspection' in names


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


def test_all_signed_installer_caches_are_reused_and_revalidated():
    text = SCRIPT.read_text(encoding="ascii")
    reuse = text.split("$cached = Join-Path $DownloadCache $spec.name", 1)[1].split(
        "$artifactReceipts +=", 1)[0]
    assert "if (Test-Path -LiteralPath $cached -PathType Leaf)" in reuse
    assert "if ($spec.sha256 -and" not in reuse
    assert "Assert-ColdExportPlainTree $cached" in reuse
    assert "Get-SignedArtifact $spec.url $destination $spec.publisher $spec.sha256" in reuse


def test_both_pip_download_caches_are_explicit_global_options_on_download_disk():
    text = SCRIPT.read_text(encoding="ascii")
    for version in ("313", "314"):
        prefix = f"Invoke-Checked $Python{version} @('-m','pip','--isolated','--cache-dir',"
        assert prefix in text
        command = text.split(prefix, 1)[1].split("\n    Invoke-Checked", 1)[0]
        assert f"(Join-Path $DownloadCache 'pip{version}-cache')," in command
        assert command.index("'download'") > command.index(f"'pip{version}-cache'")
        assert "'--index-url','https://pypi.org/simple'" in command
        assert "'--only-binary=:all:'" in command


def test_registration_archive_allowlist_is_exact_and_reviewed():
    result = helpers("ConvertTo-Json -InputObject @(Get-ColdExportRegistrationArtifacts) -Depth 5 -Compress")
    assert result.returncode == 0, result.stderr
    items = json.loads(result.stdout)
    assert {item["name"] for item in items} == {
        "install.ps1", "windows_app_credentials.py", "README.md", "test_windows_app_credentials.py"
    }
    assert all(len(item["sha256"]) == 64 for item in items)


def test_exact_file_copy_verifies_bytes_and_refuses_overwrite_and_partial_attempt(tmp_path):
    source = tmp_path / "source.bin"
    target = tmp_path / "archive" / "target.bin"
    source.write_bytes(b"exact-preservation")
    result = helpers(f"Copy-ColdExportExactFile {quoted(source)} {quoted(target)}|ConvertTo-Json -Compress")
    assert result.returncode == 0, result.stderr
    proof = json.loads(result.stdout)
    assert proof["bytes"] == source.stat().st_size
    assert proof["sha256"].lower() == hashlib.sha256(source.read_bytes()).hexdigest()
    assert target.read_bytes() == source.read_bytes()
    repeat = helpers(f"Copy-ColdExportExactFile {quoted(source)} {quoted(target)}")
    assert repeat.returncode != 0
    assert target.read_bytes() == b"exact-preservation"
    partial_target = target.parent / "other.bin"
    partial_target.with_suffix(".bin.part").write_bytes(b"preserve-interrupted-attempt")
    interrupted = helpers(f"Copy-ColdExportExactFile {quoted(source)} {quoted(partial_target)}")
    assert interrupted.returncode != 0
    assert not partial_target.exists()
    assert partial_target.with_suffix(".bin.part").read_bytes() == b"preserve-interrupted-attempt"


def test_exact_file_copy_rejects_unreviewed_source_and_post_copy_source_change(tmp_path):
    source = tmp_path / "source.bin"
    target = tmp_path / "archive" / "target.bin"
    source.write_bytes(b"reviewed-state")
    wrong = helpers(f"Copy-ColdExportExactFile {quoted(source)} {quoted(target)} ('a'*64)")
    assert wrong.returncode != 0
    assert not target.exists()
    code = ("$global:SourceHashCalls=0;$global:OriginalHash=(Get-Command Get-Sha256).ScriptBlock;"
            "function Get-Sha256([string]$Path) {"
            f"if($Path -eq {quoted(source)}) {{$global:SourceHashCalls++;"
            "if($global:SourceHashCalls -eq 2){[IO.File]::WriteAllBytes($Path,[byte[]]@(42))}};"
            "return (& $global:OriginalHash $Path)};"
            f"Copy-ColdExportExactFile {quoted(source)} {quoted(target)}")
    changed = helpers(code)
    assert changed.returncode != 0
    assert not target.exists()
    assert target.with_suffix(".bin.part").is_file(), changed.stdout + changed.stderr


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


def test_manifest_accepts_the_real_ps5_ordered_file_generator(tmp_path):
    package = tmp_path / "generated-payload"
    (package / "nested").mkdir(parents=True)
    expected = {"empty.bin": b"", "first.bin": bytes(range(128)), "nested/second.bin": b"x" * 9001}
    for name, content in expected.items():
        (package / name).write_bytes(content)
    result = helpers("if($PSVersionTable.PSVersion.Major -ne 5){throw 'Native PS5 required'};"
        + "$root=" + quoted(package) + ";"
        + "$f=@(Get-ChildItem -LiteralPath $root -Recurse -Force -File -ErrorAction Stop|"
        + "Sort-Object FullName|ForEach-Object{[ordered]@{path=$_.FullName.Substring($root.Length+1);"
        + "bytes=[long]$_.Length;sha256=(Get-Sha256 $_.FullName)}});"
        + "if(@($f|Where-Object{$_ -isnot [Collections.Specialized.OrderedDictionary]}).Count){throw 'Wrong generated row type'};"
        + "$m=New-ColdExportManifest ('a'*40) source origin '' @{} $f;"
        + "$m|ConvertTo-Json -Depth 10 -Compress")
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = json.loads(result.stdout)
    assert manifest["payload_bytes"] == sum(map(len, expected.values()))
    assert manifest["minimum_target_free_bytes"] == 250 * 1024**3
    assert len(manifest["files"]) == len(expected)
    for row in manifest["files"]:
        content = expected[row["path"].replace("\\", "/")]
        assert row["bytes"] == len(content)
        assert row["sha256"].lower() == hashlib.sha256(content).hexdigest()


@pytest.mark.parametrize("row_types", [
    ("ordered",), ("pscustomobject",), ("hashtable",),
    ("ordered", "pscustomobject", "hashtable"),
])
def test_manifest_accepts_fresh_and_json_release_row_types_without_mocking_creator(row_types):
    prefixes = {"ordered": "[ordered]@", "pscustomobject": "[pscustomobject]@", "hashtable": "@"}
    rows = [prefixes[kind] + "{path='row" + str(index) + "';bytes=[long]" + str(index + 1) + ";sha256=('a'*64)}"
            for index, kind in enumerate(row_types)]
    result = helpers("$f=@(" + ",".join(rows) + ");"
        + "$m=New-ColdExportManifest ('a'*40) source origin '' @{} $f;"
        + "$m|ConvertTo-Json -Depth 10 -Compress")
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = json.loads(result.stdout)
    assert manifest["payload_bytes"] == sum(range(1, len(rows) + 1))
    assert len(manifest["files"]) == len(rows)


@pytest.mark.parametrize("integer_type", ["sbyte", "byte", "int16", "uint16", "int32", "uint32", "int64", "uint64"])
def test_manifest_accepts_only_exact_clr_integral_bytes(integer_type):
    result = helpers("$f=@([ordered]@{path='integer';bytes=[" + integer_type + "]123;sha256=('a'*64)},"
        + "[pscustomobject]@{path='zero';bytes=[long]0;sha256=('a'*64)});"
        + "$m=New-ColdExportManifest ('a'*40) source origin '' @{} $f;"
        + "$m|ConvertTo-Json -Depth 10 -Compress")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["payload_bytes"] == 123


@pytest.mark.parametrize("row", [
    "$null", "[ordered]@{path='missing'}", "[pscustomobject]@{path='missing'}",
    "[ordered]@{bytes=$null}", "[ordered]@{bytes='123'}", "[ordered]@{bytes=$true}",
    "[ordered]@{bytes=[double]123}", "[ordered]@{bytes=[decimal]123}",
    "[ordered]@{bytes=[float]123}", "[ordered]@{bytes=[long]-1}",
    "[ordered]@{bytes=[double]::NaN}", "[ordered]@{bytes=[double]::PositiveInfinity}",
    "[ordered]@{bytes=[bigint]123}", "123",
])
def test_manifest_rejects_missing_non_integer_and_negative_bytes(row):
    result = helpers("$f=@(" + row + ");New-ColdExportManifest ('a'*40) source origin '' @{} $f")
    assert result.returncode != 0, result.stdout
    assert "COLD_MANIFEST_FILE_BYTES_INVALID" in result.stderr


@pytest.mark.parametrize("rows,expected", [
    ("[ordered]@{bytes=[uint64]9223372036854775808}", "COLD_MANIFEST_FILE_BYTES_OUT_OF_RANGE"),
    ("[ordered]@{bytes=[long]::MaxValue},[pscustomobject]@{bytes=[long]1}", "COLD_MANIFEST_PAYLOAD_OVERFLOW"),
    ("[ordered]@{bytes=([long]::MaxValue-[long]30GB+[long]1)}", "COLD_MANIFEST_CAPACITY_OVERFLOW"),
])
def test_manifest_rejects_each_int64_overflow_before_addition(rows, expected):
    result = helpers("$f=@(" + rows + ");New-ColdExportManifest ('a'*40) source origin '' @{} $f")
    assert result.returncode != 0, result.stdout
    assert expected in result.stderr


@pytest.mark.parametrize("size,minimum", [
    (0, 250 * 1024**3),
    (9007199254740993, 9007199254740993 + 30 * 1024**3),
    (2**63 - 1 - 30 * 1024**3, 2**63 - 1),
])
def test_manifest_exact_capacity_above_double_precision_and_at_int64_boundary(size, minimum):
    result = helpers("$f=@([ordered]@{bytes=[long]" + str(size) + "});"
        + "$m=New-ColdExportManifest ('a'*40) source origin '' @{} $f;"
        + "if($m.payload_bytes -isnot [long] -or $m.minimum_target_free_bytes -isnot [long]){throw 'Inexact capacity type'};"
        + "$m|ConvertTo-Json -Depth 10 -Compress")
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = json.loads(result.stdout)
    assert manifest["payload_bytes"] == size
    assert manifest["minimum_target_free_bytes"] == minimum


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
                     "function Get-CimInstance {param($ClassName,$Filter,$ErrorAction);if($ClassName -eq 'Win32_Service'){[pscustomobject]@{State='" + state + "';StartMode='" + mode + "';ProcessId=0}}};"
                     "Assert-ColdExportPause $l $p source")
    assert (result.returncode != 0) == bool(expected), result.stderr


def test_export_source_contains_only_final_paused_entry_and_snapshot():
    text = SCRIPT.read_text(encoding="ascii")
    assert "'--source-layout',$SourceLayout" in text
    assert "'--pause-receipt',$PauseReceipt" in text
    assert "tools.secondary_edge.cold_database','snapshot'" in text
    for obsolete in ("export_package.ps1", "install_target.ps1", "verify_target.ps1", "database_export", "AdminClientFile", "*.lock", "*.pid"):
        assert obsolete not in text
    assert "Publish-ColdPackage $OutputRoot $manifest $sealGuard" in text
    assert "Assert-ColdPackageContents $Root" in text
    assert "FileMode]::CreateNew" in text


def test_manual_mysqld_cannot_hide_behind_stopped_scm():
    result = helpers("$l=[pscustomobject]@{format='probiga.cold-source-layout.v1';"
                     "source=[pscustomobject]@{hostname='source';version='8.4.11';service_name='ProBigA-MySQL84';server_uuid='u'}};"
                     "$p=[pscustomobject]@{format='probiga.source-pause.v1';status='paused';source_host='source';"
                     "source_server_uuid='u';source_service_name='ProBigA-MySQL84';source_service_state='Stopped';"
                     "source_service_startup='Disabled';source_processes_running=$false;shutdown_complete=$true;"
                     "source_qmt_running=$false;source_automatically_resume=$false;completed_at_utc='now'};"
                     "function Get-CimInstance {param($ClassName,$Filter,$ErrorAction);"
                     "if($ClassName -eq 'Win32_Service'){[pscustomobject]@{State='Stopped';StartMode='Disabled';ProcessId=0}}"
                     "else{[pscustomobject]@{Name='mysqld.exe';ProcessId=123}}};Assert-ColdExportPause $l $p source")
    assert result.returncode != 0
    assert "manually started" in result.stderr


def test_private_project_archive_preserves_main_db_wal_and_legacy_only_under_audit(
    tmp_path, isolate_optional_machine_asset_paths
):
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
        (source / "runtime/windows-app-credentials/installation.json").write_bytes(b"must-not-transfer")
        (source / "runtime/windows-app-credentials/login-verification.json").write_bytes(b"must-not-transfer")
        for name in ("install.ps1", "windows_app_credentials.py", "README.md", "test_windows_app_credentials.py"):
            (source / "runtime/windows-app-credentials" / name).write_bytes(b"reviewed-tool-fixture")
        (source / ".env").write_bytes(b"must-not-transfer")
        (source / "_archive").mkdir()
        (source / "_archive/capital_flow_restore.sql").write_bytes(b"historical-business-dump")
        (source / "_archive/probiga_remote_dump.sql.gz").write_bytes(b"historical-business-gzip")
        (source / "_archive/old_deploy.py").write_bytes(b"historical-code-not-runtime-scope")
    (development / "runtime/emquant-py36").mkdir()
    (development / "runtime/emquant-py36/python.exe").write_bytes(b"legacy-only")
    (development / "runtime/emquant-py36/python36._pth").write_bytes(b"python36.zip\n.\nimport site\n")
    user_profile, local_app_data = isolate_optional_machine_asset_paths
    models = user_profile / ".EasyOCR/model"
    models.mkdir(parents=True)
    for name in ("craft_mlt_25k.pth", "english_g2.pth"):
        (models / name).write_bytes(b"private-offline-model")
    (models / "unselected-model.pth").write_bytes(b"must-not-transfer")
    old_alert = local_app_data / "ProBigA/qmt-wecom-alert-state.json"
    old_alert.parent.mkdir()
    old_alert.write_text('{"events":{"test":{"active":false}},"schema_version":1}', encoding="utf-8")
    digest = hashlib.sha256(b"reviewed-tool-fixture").hexdigest()
    specs = "function Get-ColdExportRegistrationArtifacts {return @(" + ",".join(
        f"[pscustomobject]@{{name='{name}';sha256='{digest}'}}"
        for name in ("install.ps1", "windows_app_credentials.py", "README.md", "test_windows_app_credentials.py")
    ) + ")};"
    specs += ("function Get-ColdExportReviewedAlertStateSha256 {return "
              + quoted(hashlib.sha256(old_alert.read_bytes()).hexdigest()) + "};")
    result = helpers("function Get-CimInstance {return @()};" + specs + "Copy-ColdExportProjectArchive "
                     + " ".join(quoted(path) for path in (development, production, package_root)))
    assert result.returncode == 0, result.stderr
    archive = package_root / "audit/source-project-state"
    for label in ("development", "production"):
        assert (archive / label / "data/main.db").read_bytes() == b"sqlite-private-snapshot"
        assert (archive / label / "data/main.db-wal").read_bytes() == b"matching-wal"
        assert not (archive / label / "runtime/windows-app-credentials").exists()
        tools = archive / label / "operation-tools/windows-app-registration"
        assert {path.name for path in tools.iterdir()} == {
            "install.ps1", "windows_app_credentials.py", "README.md", "test_windows_app_credentials.py"
        }
        assert all(path.read_bytes() == b"reviewed-tool-fixture" for path in tools.iterdir())
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
    assert metadata["runtime_activation"] is False
    assert metadata["credentials_copied"] is False
    assert metadata["target_credentials_enrolled"] is False
    for name in ("craft_mlt_25k.pth", "english_g2.pth"):
        assert (archive / "machine-assets/easyocr/model" / name).read_bytes() == b"private-offline-model"
    assert not (archive / "machine-assets/easyocr/model/unselected-model.pth").exists()
    assert (archive / "machine-assets/legacy-alert-state/qmt-wecom-alert-state.json").read_bytes() == old_alert.read_bytes()
    assert all(entry["archive_only"] and not entry["runtime_activation"] for entry in metadata["entries"])


@pytest.mark.parametrize("contents", [
    '{"password":"fake-secret-fixture"}',
    r'{"nested":{"\u0070assword":"fake-secret-fixture"}}',
    '{"events":[{"token":"fake-secret-fixture"}]}',
    '{"message":"https://example.test/endpoint?key=fake-secret-fixture"}',
    '{"message":"Bearer fake-secret-fixture"}',
    '{"message":"-----BEGIN PRIVATE KEY----- fake-secret-fixture"}',
    '{"message":"eyJfake.fake.fake"}',
    'not-json',
])
def test_unreviewed_legacy_alert_state_is_excluded_without_reading_or_copying_its_values(
    tmp_path, isolate_optional_machine_asset_paths, contents
):
    development, production, package = (tmp_path / name for name in ("development", "production", "package"))
    for source in (development, production):
        (source / "data").mkdir(parents=True)
    _, local_app_data = isolate_optional_machine_asset_paths
    state = local_app_data / "ProBigA/qmt-wecom-alert-state.json"
    state.parent.mkdir()
    state.write_text(contents, encoding="utf-8")
    result = helpers("function Get-CimInstance {return @()};Copy-ColdExportProjectArchive "
                     + " ".join(quoted(path) for path in (development, production, package)))
    assert result.returncode == 0, result.stderr
    archive = package / "audit/source-project-state"
    assert not (archive / "machine-assets/legacy-alert-state/qmt-wecom-alert-state.json").exists()
    metadata = json.loads((archive / "archive-metadata.json").read_text(encoding="utf-8"))
    assert metadata["excluded_assets"] == [{
        "asset": "legacy-qmt-alert-state", "reason": "not-matching-reviewed-non-secret-json",
        "copied": False, "runtime_activation": False
    }]
    assert "fake-secret-fixture" not in result.stdout + result.stderr


def test_registration_tool_change_fails_closed_without_copying_private_material(tmp_path):
    development, production, package = (tmp_path / name for name in ("development", "production", "package"))
    for source in (development, production):
        (source / "data").mkdir(parents=True)
    credential_dir = development / "runtime/windows-app-credentials"
    credential_dir.mkdir(parents=True)
    (credential_dir / "windows_app_credentials.py").write_bytes(b"unreviewed-source-change")
    (credential_dir / "private.bin").write_bytes(b"must-not-transfer")
    result = helpers("function Get-CimInstance {return @()};Copy-ColdExportProjectArchive "
                     + " ".join(quoted(path) for path in (development, production, package)))
    assert result.returncode != 0
    assert "reviewed identity" in result.stderr
    archive = package / "audit/source-project-state"
    assert not list(archive.rglob("private.bin"))
    assert not list(archive.rglob("windows_app_credentials.py"))


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


REPORTS = ("20260728_v3_july_backtest", "20260728_v3_latest_july_backtest")


def create_junction(link, target):
    link.parent.mkdir(parents=True, exist_ok=True)
    result = run_ps("New-Item -ItemType Junction -Path " + quoted(link)
                    + " -Value " + quoted(target) + "|Out-Null")
    assert result.returncode == 0, result.stdout + result.stderr


def report_node_fixture(tmp_path, machine_paths):
    profile, _ = machine_paths
    node = profile / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node"
    modules = node / "node_modules"
    (modules / "@oai/artifact-tool").mkdir(parents=True)
    (modules / "@oai/artifact-tool/package.json").write_bytes(b'{"version":"fixture"}')
    (modules / "empty").mkdir()
    (modules / "state.lock").write_bytes(b"inactive-dependency-lock")
    (node / "bin").mkdir()
    (node / "bin/node.exe").write_bytes(b"archive-only-native-binary-not-executed")
    development, production = (tmp_path / name for name in ("development", "production"))
    for source in (development, production):
        (source / "data").mkdir(parents=True)
        (source / "data/main.db").write_bytes(b"paused-business-data")
    for name in REPORTS:
        report = development / "outputs" / name
        report.mkdir(parents=True)
        (report / "build_workbook.mjs").write_bytes(b"historical-report-script")
        (report / "result.xlsx").write_bytes(b"historical-business-report")
        create_junction(report / "node_modules", modules)
    return development, production, node, modules


def test_reviewed_report_junctions_are_fully_materialized_and_recorded(
    tmp_path, isolate_optional_machine_asset_paths
):
    development, production, node, modules = report_node_fixture(
        tmp_path, isolate_optional_machine_asset_paths
    )
    package = tmp_path / "package"
    result = helpers("function Get-CimInstance {return @()};Copy-ColdExportProjectArchive "
                     + " ".join(quoted(path) for path in (development, production, package)))
    assert result.returncode == 0, result.stdout + result.stderr
    archive = package / "audit/source-project-state"
    for name in REPORTS:
        copied = archive / "development/outputs" / name / "node_modules"
        assert copied.is_dir()
        assert not copied.is_junction()
        assert (copied / "empty").is_dir()
        assert (copied / "state.lock").read_bytes() == b"inactive-dependency-lock"
        assert (copied / "@oai/artifact-tool/package.json").read_bytes() == (
            modules / "@oai/artifact-tool/package.json"
        ).read_bytes()
        assert (copied.parent / "result.xlsx").read_bytes() == b"historical-business-report"
        assert (development / "outputs" / name / "node_modules").is_junction()
    assert (archive / "machine-assets/report-node-runtime/bin/node.exe").read_bytes() == (
        node / "bin/node.exe"
    ).read_bytes()
    metadata = json.loads((archive / "archive-metadata.json").read_text(encoding="utf-8"))
    tree = next(entry for entry in metadata["entries"]
                if entry["label"] == "development" and entry["relative_path"] == "outputs")
    assert len(tree["materialized_junctions"]) == 2
    assert {item["link_type"] for item in tree["materialized_junctions"]} == {"Junction"}
    assert all(entry["archive_only"] and not entry["runtime_activation"] for entry in metadata["entries"])
    assert not (package / "runtime/node.exe").exists()


def test_report_archive_plan_expands_only_reviewed_dependency_and_copy_proof_is_single_object(
    tmp_path, isolate_optional_machine_asset_paths
):
    development, _, _, _ = report_node_fixture(tmp_path, isolate_optional_machine_asset_paths)
    plan_result = helpers("Get-ColdExportArchiveTreePlan " + quoted(development)
                          + " 'outputs'|ConvertTo-Json -Depth 8 -Compress")
    assert plan_result.returncode == 0, plan_result.stderr
    plan = json.loads(plan_result.stdout)
    assert len(plan["junctions"]) == 2
    assert len([entry for entry in plan["entries"] if not entry["directory"]]) == 8
    assert all("source_path" in entry for entry in plan["entries"])
    proof_path = tmp_path / "proof.json"
    copied = tmp_path / "copied"
    copy = helpers("$proof=Copy-ColdExportArchiveTree " + quoted(development)
                   + " 'outputs' " + quoted(copied)
                   + ";Write-Utf8 " + quoted(proof_path)
                   + " ($proof|ConvertTo-Json -Depth 8 -Compress)")
    assert copy.returncode == 0, copy.stdout + copy.stderr
    proof = json.loads(proof_path.read_text(encoding="utf-8"))
    assert isinstance(proof, dict)
    assert proof["files"] == 8
    assert proof["bytes"] > 0
    assert len(proof["junctions"]) == 2


@pytest.mark.parametrize("fault", ["unknown-location", "wrong-target", "inner-junction", "environment-file"])
def test_archive_preflight_refuses_unreviewed_links_or_secret_environment_before_output(
    tmp_path, isolate_optional_machine_asset_paths, fault
):
    development, production, _, modules = report_node_fixture(
        tmp_path, isolate_optional_machine_asset_paths
    )
    if fault == "unknown-location":
        create_junction(development / "outputs/unreviewed/node_modules", modules)
    elif fault == "wrong-target":
        other = tmp_path / "other-dependencies"
        other.mkdir()
        production_report = production / "outputs" / REPORTS[0]
        create_junction(production_report / "node_modules", other)
    elif fault == "inner-junction":
        create_junction(modules / "loop", modules)
    else:
        (modules / ".env").write_bytes(b"never-read-or-copy-secret-fixture")
    result = helpers("function Get-CimInstance {return @()};Assert-ColdExportProjectArchiveSources "
                     + quoted(development) + " " + quoted(production))
    assert result.returncode != 0
    assert "never-read-or-copy-secret-fixture" not in result.stdout + result.stderr
    assert not (tmp_path / "package").exists()


def test_generic_program_copy_still_refuses_even_a_reviewed_report_junction(
    tmp_path, isolate_optional_machine_asset_paths
):
    development, _, _, _ = report_node_fixture(tmp_path, isolate_optional_machine_asset_paths)
    copied = tmp_path / "generic-copy"
    result = helpers("Copy-ColdExportTree " + quoted(development / "outputs") + " " + quoted(copied))
    assert result.returncode != 0
    assert not copied.exists()


def test_archive_materialized_dependency_source_mutation_fails_closed(
    tmp_path, isolate_optional_machine_asset_paths
):
    development, _, _, modules = report_node_fixture(tmp_path, isolate_optional_machine_asset_paths)
    original = modules / "state.lock"
    code = ("$global:SourceHashCalls=0;$global:OriginalHash=(Get-Command Get-Sha256).ScriptBlock;"
            "function Get-Sha256([string]$Path) {"
            f"if($Path -eq {quoted(original)}) {{$global:SourceHashCalls++;"
            "if($global:SourceHashCalls -eq 2){[IO.File]::WriteAllBytes($Path,[byte[]]@(42))}};"
            "return (& $global:OriginalHash $Path)};"
            "Copy-ColdExportArchiveTree " + quoted(development) + " 'outputs' " + quoted(tmp_path / "copied"))
    result = helpers(code)
    assert result.returncode != 0, result.stdout + result.stderr


def test_full_archive_preflight_precedes_output_creation_and_software_copy():
    text = SCRIPT.read_text(encoding="ascii")
    exporter = text.split("function Invoke-ColdPackageExport", 1)[1]
    preflight = "Assert-ColdExportProjectArchiveSources $SourceProjectRoot $SourceProductionRoot"
    assert preflight in exporter
    assert exporter.index(preflight) < exporter.index("New-Item -ItemType Directory -Path $OutputRoot")
    assert exporter.index(preflight) < exporter.index("Copy-ColdExportTree $QmtHome")
    assert exporter.index("Get-ColdExportProductionHistoryFiles") < exporter.index(
        "New-Item -ItemType Directory -Path $OutputRoot"
    )


HISTORY_IDS = ("019fbe02-0390-7663-a7ba-bd150e063fe7", "019fbe02-0a70-7c62-9bf2-9ab439bea770")


def history_day_fixture(machine_paths):
    profile, _ = machine_paths
    day = profile / ".codex/sessions/2026/08/01"
    day.mkdir(parents=True)
    for number, thread in enumerate(HISTORY_IDS):
        (day / f"rollout-2026-08-01T23-47-{10 + number}-{thread}.jsonl").write_bytes(b"private-business-history")
    return profile, day


def test_history_preflight_selects_only_two_reviewed_files_without_personal_session_traversal(
    tmp_path, isolate_optional_machine_asset_paths
):
    profile, day = history_day_fixture(isolate_optional_machine_asset_paths)
    unrelated = profile / ".codex/sessions/2026/10/01"
    unrelated.mkdir(parents=True)
    create_junction(unrelated / "irrelevant-user-link", tmp_path)
    (day / "unrelated-personal-history.jsonl").write_bytes(b"must-not-select-personal-history")
    result = helpers("ConvertTo-Json -InputObject @(Get-ColdExportProductionHistoryFiles) -Depth 5 -Compress")
    assert result.returncode == 0, result.stdout + result.stderr
    files = json.loads(result.stdout)
    assert len(files) == 2
    assert {item["thread_id"] for item in files} == set(HISTORY_IDS)
    assert all(Path(item["source_path"]).parent == day for item in files)
    assert "must-not-select-personal-history" not in result.stdout + result.stderr


@pytest.mark.parametrize("fault", ["missing", "ambiguous", "linked-day"])
def test_history_preflight_refuses_missing_ambiguous_or_linked_sources(
    tmp_path, isolate_optional_machine_asset_paths, fault
):
    profile, _ = isolate_optional_machine_asset_paths
    if fault == "linked-day":
        other = tmp_path / "other-history"
        other.mkdir()
        create_junction(profile / ".codex/sessions/2026/08/01", other)
    else:
        _, day = history_day_fixture(isolate_optional_machine_asset_paths)
        if fault == "missing":
            for item in day.glob(f"*{HISTORY_IDS[1]}.jsonl"):
                item.unlink()
        else:
            (day / f"another-{HISTORY_IDS[0]}.jsonl").write_bytes(b"ambiguous-history")
    result = helpers("Get-ColdExportProductionHistoryFiles")
    assert result.returncode != 0


def test_history_copy_reuses_exact_source_and_is_not_a_loose_copy_item():
    text = SCRIPT.read_text(encoding="ascii")
    stage = text.split("Write-Host '3/6", 1)[1].split("Copy-Item -LiteralPath $PauseReceipt", 1)[0]
    assert "Get-ColdExportProductionHistoryFiles" in stage
    assert "Copy-ColdExportExactFile $history.source_path $copy | Out-Null" in stage
    assert "Copy-Item -LiteralPath $rollouts" not in stage
