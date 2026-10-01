"""Native Windows PS5 release on small fake assets; no installers/accounts/DB."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from test_secondary_edge_package_metadata import metadata_fixture, write_json
from test_secondary_edge_package import package as prerequisite_files

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools/secondary_edge"
PS = shutil.which("powershell.exe")
pytestmark = pytest.mark.skipif(os.name != "nt" or not PS, reason="Native Windows PS5 required")
CODE_FILES = ("package_common.ps1", "migrate_target.ps1", "target_entry.ps1", "start_target_migration.cmd", "COLD_README.txt")


def q(path):
    return "'" + str(path).replace("'", "''") + "'"


def git(repo, *args):
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def ps(code):
    return subprocess.run([PS, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command",
                           "$ErrorActionPreference='Stop';"
                           "$env:PSModulePath=\"$env:SystemRoot\\System32\\WindowsPowerShell\\v1.0\\Modules;$env:ProgramFiles\\WindowsPowerShell\\Modules\";"
                           "Import-Module Microsoft.PowerShell.Security -ErrorAction Stop;" + code],
                          capture_output=True, text=True, timeout=60)


def seal(package, manifest):
    manifest["files"] = [{"path": str(file.relative_to(package)).replace("\\", "/"),
        "bytes": file.stat().st_size, "sha256": hashlib.sha256(file.read_bytes()).hexdigest()}
        for file in package.rglob("*") if file.is_file() and file.name not in {"manifest.json", "READY"}]
    write_json(package / "manifest.json", manifest)
    digest = hashlib.sha256((package / "manifest.json").read_bytes()).hexdigest().upper()
    (package / "READY").write_text(manifest["build_sha"] + " " + digest, encoding="utf-8")


@pytest.fixture
def release(tmp_path):
    host = os.environ["COMPUTERNAME"]
    package, manifest, layout, pause = metadata_fixture(tmp_path, host)
    # Fill the existing real common validator's required assets, never overwrite
    # the independent snapshot receipts or fake physical database.
    fillers = tmp_path / "fillers"
    prerequisite_files(fillers)
    for file in fillers.rglob("*"):
        if file.is_file():
            target = package / file.relative_to(fillers)
            if not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(file, target)
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "--initial-branch=main")
    git(repo, "config", "user.name", "Cold fixture")
    git(repo, "config", "user.email", "fixture@example.invalid")
    git(repo, "remote", "add", "origin", "https://github.com/MingMG/probiga.git")
    for name in (*CODE_FILES, "__init__.py", "cold_database.py", "package_metadata.py"):
        target = repo / "tools/secondary_edge" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(TOOLS / name, target)
    for name in ("qmt_windows_requirements.lock", "windows_app_requirements.txt"):
        target = repo / "deploy" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / "deploy" / name, target)
    git(repo, "add", ".")
    git(repo, "commit", "-m", "Old main")
    previous = git(repo, "rev-parse", "HEAD")
    git(repo, "bundle", "create", str(package / "code.bundle"), "main")
    for name in CODE_FILES:
        shutil.copyfile(repo / "tools/secondary_edge" / name, package / name)
    (repo / "tools/secondary_edge/migrate_target.ps1").write_text(
        (repo / "tools/secondary_edge/migrate_target.ps1").read_text(encoding="utf-8") + "\n# New release fixture\n", encoding="utf-8")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "Permanent Windows installation improvement")
    build = git(repo, "rev-parse", "HEAD")
    git(repo, "update-ref", "refs/remotes/origin/main", build)
    state = tmp_path / "source-state"
    state.mkdir()
    shutil.copyfile(package / "audit/source-layout.json", state / "source-layout.json")
    shutil.copyfile(package / "audit/source-pause.json", state / "source-pause.json")
    sid_result = ps("[Security.Principal.WindowsIdentity]::GetCurrent().User.Value")
    assert sid_result.returncode == 0
    sid = sid_result.stdout.strip()
    write_json(state / "original-state.json", {"format": "probiga.pause-original-state.v1", "user_sid": sid, "source_host": host})
    protected = ps(f". {q(TOOLS / 'package_common.ps1')};Protect-LocalPath {q(state)}")
    assert protected.returncode == 0, protected.stderr
    receipts = []
    for name in ("python313.exe", "python314.exe", "git.exe", "vc_x64.exe", "vc_x86.exe", "chrome.msi"):
        digest = hashlib.sha256((package / "software/installers" / name).read_bytes()).hexdigest().upper()
        receipts.append({"name": name, "url": "https://fixture.invalid/" + name, "publisher": "Fixture", "sha256": digest})
    write_json(package / "audit/official-software.json", receipts)
    manifest.update(format="probiga.windows-cold-migration.v2", build_sha=previous, source_paused=True,
        production_activation=False, restore_requested=False, git_origin_fetch_url="https://github.com/MingMG/probiga.git",
        ai_server_url="", created_at="old-publication", minimum_target_free_bytes=250 * 1024**3)
    seal(package, manifest)
    return {"package": package, "manifest": manifest, "repo": repo, "previous": previous, "build": build,
            "state": state, "sid": sid, "journal": tmp_path / "release-journal", "root": tmp_path}


def run_release(item, extra=""):
    script = TOOLS / "republish_cold_package.ps1"
    item["root"].joinpath("development").mkdir(exist_ok=True)
    item["root"].joinpath("production").mkdir(exist_ok=True)
    code = f". {q(script)} -PackageRoot {q(item['package'])} -JournalRoot {q(item['journal'])} " \
        f"-StateRoot {q(item['state'])} -CodeRoot {q(item['repo'])} -ExpectedPreviousBuild {q(item['previous'])} " \
        f"-ExpectedUserSid {q(item['sid'])} -TargetInstallationNotStarted -QmtHome {q(item['root'] / 'qmt-source')} " \
        f"-SourceProjectRoot {q(item['root'] / 'development')} -SourceProductionRoot {q(item['root'] / 'production')} " \
        f"-Python314 {q(Path(os.sys.executable))};"
    code += """
function Assert-Administrator {}
function Assert-ColdExportPythonIdentity { param($Program,$ExpectedVersion) }
function Get-CimInstance { param($ClassName,$Filter,$ErrorAction)
 if($ClassName -eq 'Win32_Service'){[pscustomobject]@{State='Stopped';StartMode='Disabled';ProcessId=0}} }
function Get-Volume { param($DriveLetter) [pscustomobject]@{FileSystem='NTFS';SizeRemaining=2TB} }
function New-ColdMigrationPowerLease { [pscustomobject]@{fixture=$true} }
function Remove-ColdMigrationPowerLease { param($Lease) }
function Get-SignedArtifact { param($Url,$Path,$Publisher,$Sha256) }
function Get-ColdExportArtifacts {
 $r=Get-Content -LiteralPath (Join-Path $PackageRoot 'audit/official-software.json') -Raw|ConvertFrom-Json
 foreach($entry in $r){[pscustomobject]@{name=$entry.name;url=$entry.url;publisher=$entry.publisher;sha256=$entry.sha256}}
}
"""
    return ps(code + extra + ";Invoke-ColdPackageRepublication")


def test_success_publishes_last_ready_and_preserves_every_non_code_byte(release):
    package = release["package"]
    before = {row["path"]: ((package / row["path"]).read_bytes(), (package / row["path"]).stat().st_mtime_ns)
              for row in release["manifest"]["files"] if row["path"] not in (*CODE_FILES, "code.bundle")}
    extra = """
$global:originalContents=${function:Assert-ColdPackageContents};$global:contentChecks=0
function Assert-ColdPackageContents { param($Root)
 $global:contentChecks++; if($global:contentChecks -gt 1 -and (Test-Path -LiteralPath (Join-Path $Root 'READY'))){throw 'EARLY_READY'}
 & $global:originalContents $Root }
"""
    result = run_release(release, extra)
    assert result.returncode == 0, result.stdout + result.stderr
    updated = json.loads((package / "manifest.json").read_text())
    assert updated["build_sha"] == release["build"]
    assert updated["database"] == release["manifest"]["database"]
    assert updated["created_at"] != release["manifest"]["created_at"]
    assert (package / "READY").read_text() == release["build"] + " " + hashlib.sha256((package / "manifest.json").read_bytes()).hexdigest().upper()
    for path, (content, modified) in before.items():
        assert (package / path).read_bytes() == content
        assert (package / path).stat().st_mtime_ns == modified
    backups = release["journal"] / "previous-release"
    assert {file.name for file in backups.iterdir()} == {*CODE_FILES, "code.bundle", "manifest.json", "READY"}
    assert json.loads((release["journal"] / "release-state.json").read_text())["status"] == "published"
    logs = list(release["state"].glob("republish-*.log"))
    assert len(logs) == 1
    log = logs[0].read_text(encoding="utf-8-sig")
    events = [json.loads(line) for line in log.splitlines()]
    assert [event["event"] for event in events] == ["helper-started", "old-sha-started", "old-sha-verified",
        "ready-withdrawn", "new-sha-started", "new-sha-verified", "published", "cleanup-finished"]
    assert all(event["format"] == "probiga.cold-package-log.v1" and event["helper_pid"] > 0 for event in events)
    assert all(set(event) <= {"format", "event", "helper_pid", "at_utc", "build_sha", "manifest_sha256", "reason"}
               for event in events)


@pytest.mark.parametrize("kind", ["running", "version", "diff", "metadata", "extra", "corrupt", "partial", "existing-journal"])
def test_invalid_sources_and_artifacts_never_enter_mutation(release, kind):
    package = release["package"]
    extra = ""
    if kind == "running":
        extra = "function Get-CimInstance { [pscustomobject]@{State='Running';StartMode='Disabled';ProcessId=123} }"
    elif kind == "version":
        extra = "function Assert-ColdExportPythonIdentity { throw 'SOURCE_PYTHON_VERSION_WRONG' }"
    elif kind == "diff":
        (release["repo"] / "linux-runtime.py").write_text("not allowed")
        git(release["repo"], "add", "."); git(release["repo"], "commit", "-m", "Forbidden boundary")
        git(release["repo"], "update-ref", "refs/remotes/origin/main", "HEAD")
    elif kind == "metadata":
        release["manifest"]["database"]["source"]["hostname"] = "different"
        seal(package, release["manifest"])
    elif kind == "extra":
        (package / "unexpected.txt").write_text("unsealed")
    elif kind == "corrupt":
        (package / "database/data/mysql.ibd").write_text("corrupted")
    elif kind == "partial":
        (package / "READY").unlink()
    else:
        release["journal"].mkdir()
        (release["journal"] / "release-failed.json").write_text("previous failure")
    old_code = (package / "code.bundle").read_bytes()
    result = run_release(release, extra)
    assert result.returncode != 0
    assert (package / "code.bundle").read_bytes() == old_code
    assert not (release["journal"] / "previous-release").exists()


def test_failure_after_withdrawal_keeps_backup_without_ready_or_auto_restore(release):
    extra = "function Publish-ColdPackage { throw 'INJECTED_PUBLICATION_FAILURE' }"
    result = run_release(release, extra)
    assert result.returncode != 0
    assert not (release["package"] / "READY").exists()
    assert (release["journal"] / "previous-release/READY").exists()
    failed = json.loads((release["journal"] / "release-failed.json").read_text())
    assert failed["ready_withdrawn"] is True
    assert failed["automatic_rollback"] is False
    log = next(release["state"].glob("republish-*.log")).read_text(encoding="utf-8-sig")
    assert '"event":"blocked"' in log
    assert '"event":"cleanup-finished"' in log
    retry = run_release(release)
    assert retry.returncode != 0


def test_real_empty_junction_is_not_omitted_from_contents(release):
    empty = release["root"] / "empty-junction-target"
    empty.mkdir()
    junction = release["package"] / "empty-junction"
    result = ps(f"New-Item -ItemType Junction -Path {q(junction)} -Target {q(empty)} | Out-Null")
    assert result.returncode == 0, result.stderr
    checked = run_release(release)
    assert checked.returncode != 0
    assert "COLD_PACKAGE_INTEGRITY_VALIDATION_FAILED" in checked.stdout + checked.stderr
    assert not release["journal"].exists()


@pytest.mark.parametrize("kind", ["extra", "junction", "manifest", "ready"])
def test_late_inventory_manifest_and_seal_changes_fail_closed(release, kind):
    package = release["package"]
    outside = release["root"] / "late-empty-target"
    outside.mkdir()
    injection = {
        "extra": f"[IO.File]::WriteAllText({q(package / 'late-extra.txt')},'late')",
        "junction": f"New-Item -ItemType Junction -Path {q(package / 'late-junction')} -Target {q(outside)} | Out-Null",
        "manifest": "$p=Join-Path $PackageRoot 'manifest.json';$t=(Get-Item -LiteralPath $p).LastWriteTimeUtc;"
                    "$text=[IO.File]::ReadAllText($p).Replace($ExpectedPreviousBuild,('b'*40));"
                    "[IO.File]::WriteAllText($p,$text);(Get-Item -LiteralPath $p).LastWriteTimeUtc=$t",
        "ready": "$p=Join-Path $PackageRoot 'READY';$t=(Get-Item -LiteralPath $p).LastWriteTimeUtc;"
                 "$text=[IO.File]::ReadAllText($p).Replace($ExpectedPreviousBuild,('b'*40));"
                 "[IO.File]::WriteAllText($p,$text);(Get-Item -LiteralPath $p).LastWriteTimeUtc=$t",
    }[kind]
    extra = "$global:oldHasher=${function:Get-Sha256};$global:injected=$false;"
    extra += "function Get-Sha256 {param($Path);$value=& $global:oldHasher $Path;"
    extra += "if(-not $global:injected -and $Path.EndsWith('code.bundle')){$global:injected=$true;" + injection + "};return $value}"
    result = run_release(release, extra)
    assert result.returncode != 0
    assert not release["journal"].exists()
    assert "COLD_PACKAGE_INTEGRITY_VALIDATION_FAILED" in result.stdout + result.stderr


def test_source_guard_failure_after_ready_write_withdraws_only_new_ready(release):
    package = release["package"]
    (package / "READY").unlink()
    script = TOOLS / "export_cold_package.ps1"
    code = f". {q(script)};$m=Get-Content -LiteralPath {q(package / 'manifest.json')} -Raw|ConvertFrom-Json;"
    code += "$global:gates=0;$g={$global:gates++;if($global:gates -eq 4){throw 'FINAL_SOURCE_CHANGED'}};"
    result = ps(code + f"Publish-ColdPackage {q(package)} $m $g")
    assert result.returncode != 0
    assert "FINAL_SOURCE_CHANGED" in result.stdout + result.stderr
    assert not (package / "READY").exists()


@pytest.mark.parametrize("callback", ["source-guard", "acl"])
def test_publication_never_rebaselines_manifest_changed_after_contents(release, callback):
    package = release["package"]
    (package / "READY").unlink()
    script = TOOLS / "export_cold_package.ps1"
    code = f". {q(script)};$m=Get-Content -LiteralPath {q(package / 'manifest.json')} -Raw|ConvertFrom-Json;"
    code += f"$global:manifestToChange={q(package / 'manifest.json')};"
    code += "$global:oldBuild=$m.build_sha;function Change-TestManifest {"
    code += "$p=$global:manifestToChange;$t=(Get-Item -LiteralPath $p).LastWriteTimeUtc;"
    code += "$text=[IO.File]::ReadAllText($p).Replace($global:oldBuild,('b'*40));"
    code += "[IO.File]::WriteAllText($p,$text);(Get-Item -LiteralPath $p).LastWriteTimeUtc=$t};"
    if callback == "source-guard":
        code += "$global:gates=0;$g={$global:gates++;if($global:gates -eq 2){Change-TestManifest}};"
    else:
        code += "$global:oldAcl=${function:Protect-PortablePackageEntry};"
        code += "function Protect-PortablePackageEntry {param($Root);& $global:oldAcl $Root;Change-TestManifest};$g={};"
    result = ps(code + f"Publish-ColdPackage {q(package)} $m $g")
    assert result.returncode != 0
    assert "PUBLICATION_MANIFEST_CHANGED" in result.stdout + result.stderr
    assert not (package / "READY").exists()


@pytest.mark.parametrize("message_style", ["provider-json", "uppercase-prefix"])
@pytest.mark.parametrize("cleanup_failure", [False, True])
def test_provider_error_never_discloses_input_in_any_output_or_evidence(release, message_style, cleanup_failure):
    sentinel = "UNIQUE_FAKE_SECRET_SENTINEL_87DAF216B959"
    provider_input = release["root"] / "fake-provider-error.txt"
    message = f"Malformed provider JSON input: '{{\"password\":\"{sentinel}\"}}'"
    if message_style == "uppercase-prefix":
        message = sentinel + ": private provider input cannot become a reason code"
    provider_input.write_text(message, encoding="utf-8")
    # The mock provider reads private input rather than embedding it in argv.
    extra = "function Publish-ColdPackage {$inputMessage=[IO.File]::ReadAllText(" + q(provider_input) + ");"
    extra += "throw (New-Object FormatException($inputMessage))}"
    if cleanup_failure:
        extra += ";function Remove-ColdMigrationPowerLease {$inputMessage=[IO.File]::ReadAllText(" + q(provider_input) + ");"
        extra += "throw (New-Object FormatException($inputMessage))}"
    result = run_release(release, extra)
    assert result.returncode != 0
    assert "SOURCE_RELEASE_BLOCKED: SOURCE_RELEASE_VERIFICATION_FAILED" in result.stderr
    assert sentinel not in result.stdout + result.stderr
    assert not (release["package"] / "READY").exists()
    assert (release["journal"] / "previous-release/READY").is_file()
    log = next(release["state"].glob("republish-*.log")).read_text(encoding="utf-8-sig")
    failed_text = (release["journal"] / "release-failed.json").read_text(encoding="utf-8")
    assert sentinel not in log + failed_text
    failed = json.loads(failed_text)
    assert failed["reason"] == "SOURCE_RELEASE_VERIFICATION_FAILED"
    assert failed["ready_withdrawn"] is True
    assert failed["automatic_rollback"] is False
    events = [json.loads(line) for line in log.splitlines()]
    assert any(event["event"] == "blocked" and event["reason"] == "SOURCE_RELEASE_VERIFICATION_FAILED" for event in events)
    assert events[-1]["event"] == ("cleanup-failed" if cleanup_failure else "cleanup-finished")


@pytest.mark.parametrize("failure_point", ["initial-provider", "log-init"])
def test_preflight_and_log_initialization_fail_safely_before_any_mutation(release, failure_point):
    sentinel = "UNIQUE_PREFLIGHT_SECRET_SENTINEL_1ECC6F6F6532"
    provider_input = release["root"] / "fake-preflight-error.txt"
    provider_input.write_text("private provider input " + sentinel, encoding="utf-8")
    name = "Assert-Administrator" if failure_point == "initial-provider" else "New-ColdReleaseSafeLog"
    extra = "function " + name + " {$inputMessage=[IO.File]::ReadAllText(" + q(provider_input) + ");"
    extra += "throw (New-Object FormatException($inputMessage))}"
    old_code = (release["package"] / "code.bundle").read_bytes()
    old_ready = (release["package"] / "READY").read_bytes()
    result = run_release(release, extra)
    assert result.returncode != 0
    assert sentinel not in result.stdout + result.stderr
    assert "SOURCE_RELEASE_BLOCKED: SOURCE_RELEASE_VERIFICATION_FAILED" in result.stderr
    assert (release["package"] / "code.bundle").read_bytes() == old_code
    assert (release["package"] / "READY").read_bytes() == old_ready
    assert not release["journal"].exists()
    assert not list(release["state"].glob("republish-*.log"))


@pytest.mark.parametrize("callback", ["source-guard", "acl"])
@pytest.mark.parametrize("mutation", ["extra", "junction", "payload"])
def test_last_publication_metadata_scan_covers_guard_and_acl_mutations(release, callback, mutation):
    package = release["package"]
    (package / "READY").unlink()
    outside = release["root"] / "last-callback-empty-target"
    outside.mkdir()
    modification = {
        "extra": f"[IO.File]::WriteAllText({q(package / 'last-callback-extra.txt')},'late')",
        "junction": f"New-Item -ItemType Junction -Path {q(package / 'last-callback-junction')} -Target {q(outside)} | Out-Null",
        "payload": f"[IO.File]::WriteAllText({q(package / 'database/data/mysql.ibd')},'ordinary callback mutation')",
    }[mutation]
    code = f". {q(TOOLS / 'export_cold_package.ps1')};$m=Get-Content -LiteralPath {q(package / 'manifest.json')} -Raw|ConvertFrom-Json;"
    code += "function Change-TestPayload {" + modification + "};"
    if callback == "source-guard":
        # Mutate after the locked READY has been flushed, the very last window.
        code += "$global:gates=0;$g={$global:gates++;if($global:gates -eq 4){Change-TestPayload}};"
    else:
        code += "$global:oldAcl=${function:Protect-PortablePackageEntry};"
        code += "function Protect-PortablePackageEntry {param($Root);& $global:oldAcl $Root;Change-TestPayload};$g={};"
    result = ps(code + f"Publish-ColdPackage {q(package)} $m $g")
    assert result.returncode != 0
    assert "inventory changed" in result.stdout + result.stderr or "reparse" in result.stdout + result.stderr
    assert not (package / "READY").exists()
