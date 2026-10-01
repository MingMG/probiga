"""Exercise PS5 parsing and the real offline-package integrity gate, no installs."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / 'tools' / 'secondary_edge'
PS = shutil.which('powershell.exe')
pytestmark = pytest.mark.skipif(os.name != 'nt' or not PS, reason='Windows PowerShell 5.1 required')


def run_ps(code):
    return subprocess.run([PS, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
                           '-Command', "$ErrorActionPreference='Stop';" + code],
                          capture_output=True, text=True, timeout=30)


@pytest.mark.parametrize('name', ['package_common', 'prepare_cold_package', 'export_cold_package',
                                 'pause_source', 'migrate_target', 'target_entry'])
def test_actual_windows_powershell_parse(name):
    path = str(TOOLS / (name + '.ps1')).replace("'", "''")
    result = run_ps("$t=$null;$e=$null;[System.Management.Automation.Language.Parser]::ParseFile('"
                    + path + "',[ref]$t,[ref]$e)|Out-Null;if($e.Count){throw ($e|Out-String)}")
    assert result.returncode == 0, result.stderr


def package(tmp_path):
    paths = ['code.bundle', 'database/data/mysql.ibd', 'database/data/auto.cnf', 'database/config/my.ini',
             'database/metadata.json', 'database/certs/ca.pem','database/certs/server-cert.pem','database/certs/server-key.pem',
             'audit/source-pause.json','audit/source-project-state/archive-metadata.json',
             'audit/source-project-state/development/data/main.db', 'package_common.ps1','migrate_target.ps1','target_entry.ps1',
             'start_target_migration.cmd','COLD_README.txt', 'wheels313/fixture.whl','wheels314/fixture.whl',
             'software/qmt/bin.x64/XtItClient.exe', 'software/mysql84/bin/mysqld.exe',
             'software/mysql84/bin/mysql.exe','software/mysql84/bin/mysqladmin.exe',
             'software/codex/codex.exe', 'software/installers/python313.exe',
             'software/codex/codex-code-mode-host.exe','software/codex/codex-command-runner.exe',
             'software/codex/codex-windows-sandbox-setup.exe',
             'software/installers/python314.exe', 'software/installers/git.exe',
             'software/installers/vc_x64.exe', 'software/installers/vc_x86.exe',
             'software/installers/chrome.msi']
    files = []
    for name in paths:
        file = tmp_path / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(b'package-fixture')
        if name == 'audit/source-pause.json':
            file.write_text(json.dumps({'format':'probiga.source-pause.v1','status':'paused','source_host':'source-fixture',
                'source_service_state':'Stopped','source_service_startup':'Disabled','shutdown_complete':True,
                'source_processes_running':False,'source_qmt_running':False,'source_automatically_resume':False,
                'source_server_uuid':'fixture-uuid'}), encoding='utf-8')
        files.append({'path': name.replace('/', '\\'), 'bytes': file.stat().st_size,
                      'sha256': hashlib.sha256(file.read_bytes()).hexdigest()})
    manifest = {'format': 'probiga.windows-cold-migration.v2', 'build_sha': 'a'*40,
                'source_host': 'source-fixture', 'production_activation': False,
                'source_paused':True,'restore_requested':False,'database':{'source':{'server_uuid':'fixture-uuid'}},
                'minimum_target_free_bytes': 250*1024**3, 'files': files}
    return manifest


def verify(tmp_path, manifest):
    (tmp_path / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    if not (tmp_path / 'READY').exists():
        seal=hashlib.sha256((tmp_path / 'manifest.json').read_bytes()).hexdigest().upper()
        (tmp_path / 'READY').write_text('a'*40+' '+seal, encoding='utf-8')
    common = str(TOOLS / 'package_common.ps1').replace("'", "''")
    path = str(tmp_path).replace("'", "''")
    return run_ps(f". '{common}';Assert-ColdPackage '{path}'|Out-Null")


def test_complete_package_passes(tmp_path):
    assert verify(tmp_path, package(tmp_path)).returncode == 0


def test_corrupt_required_file_is_rejected(tmp_path):
    manifest = package(tmp_path)
    (tmp_path / 'code.bundle').write_bytes(b'corrupted')
    assert verify(tmp_path, manifest).returncode != 0


def test_omitted_database_is_rejected(tmp_path):
    manifest = package(tmp_path)
    manifest['files'] = [item for item in manifest['files'] if item['path'] != 'database\\data\\mysql.ibd']
    assert verify(tmp_path, manifest).returncode != 0


def test_traversal_is_rejected(tmp_path):
    manifest = package(tmp_path)
    manifest['files'].append({'path': '..\\outside.sql', 'bytes': 0, 'sha256': '0'*64})
    assert verify(tmp_path, manifest).returncode != 0


def test_mismatched_ready_is_rejected(tmp_path):
    manifest = package(tmp_path)
    (tmp_path / 'READY').write_text('b'*40, encoding='utf-8')
    assert verify(tmp_path, manifest).returncode != 0


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def native_helpers(code):
    # The parent executor can have PS7 module paths; these probes are native PS5.
    prefix = "$env:PSModulePath=\"$env:SystemRoot\\System32\\WindowsPowerShell\\v1.0\\Modules;$env:ProgramFiles\\WindowsPowerShell\\Modules\";"
    prefix += "Import-Module Microsoft.PowerShell.Utility,Microsoft.PowerShell.Management -ErrorAction Stop;"
    prefix += "if($PSVersionTable.PSVersion.Major -ne 5){throw 'NATIVE_PS5_REQUIRED'};"
    return run_ps(prefix + ". " + quote(TOOLS / 'package_common.ps1') + ";" + code)


def sealed_fixture(tmp_path):
    manifest = package(tmp_path)
    manifest_path = tmp_path / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    (tmp_path / 'READY').write_text(
        manifest['build_sha'] + ' ' + hashlib.sha256(manifest_path.read_bytes()).hexdigest().upper(),
        encoding='utf-8')
    return manifest


@pytest.mark.parametrize('length', [0, 1, 55, 56, 63, 64, 65, 65535, 65536, 65537, 4 * 1024**2 + 17])
def test_native_streaming_sha_matches_uppercase_file_hash_and_releases_file(tmp_path, length):
    path = tmp_path / 'sensitive-fixture-only.bin'
    payload = bytes(range(251)) * (length // 251) + bytes(range(length % 251))
    path.write_bytes(payload)
    result = native_helpers(
        "$p=" + quote(path) + ";$events=New-Object 'Collections.Generic.List[long]';"
        + "$callback={param($bytes);if($bytes -isnot [long]){throw 'HASH_BYTE_TYPE_INVALID'};"
        + "[void]$events.Add($bytes);Write-Output 'UNWANTED_HASH_CALLBACK_OUTPUT'};"
        + "$actual=@(Get-Sha256 $p -HashProgressAction $callback);"
        + "$reference=(Get-FileHash -LiteralPath $p -Algorithm SHA256).Hash;"
        + "$exclusive=[IO.File]::Open($p,[IO.FileMode]::Open,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None);"
        + "try{$released=$true}finally{$exclusive.Dispose()};"
        + "[pscustomobject]@{actual=$actual;reference=$reference;released=$released;events=@($events)}|ConvertTo-Json -Compress")
    assert result.returncode == 0, result.stdout + result.stderr
    values = json.loads(result.stdout)
    assert values['actual'] == [hashlib.sha256(payload).hexdigest().upper()]
    assert values['actual'][0] == values['reference']
    assert values['released'] is True
    assert all(0 <= count <= length for count in values['events'])
    assert values['events'] == sorted(values['events'])
    assert 'UNWANTED_HASH_CALLBACK_OUTPUT' not in result.stdout
    assert str(path) not in result.stdout


# .NET 4 BCL-only virtual input: a small deterministic stream with delayed
# reads exercises time-based callbacks inside Compute, without a huge file.
SLOW_STREAM_SOURCE = r'''
using System;
using System.IO;
using System.Threading;
public sealed class ColdShaSlowTestStream : Stream {
    private readonly long length;
    private long position;
    public bool Disposed;
    public int DelayMilliseconds = 2100;
    public ColdShaSlowTestStream(long length) { this.length = length; }
    public override bool CanRead { get { return !Disposed; } }
    public override bool CanSeek { get { return false; } }
    public override bool CanWrite { get { return false; } }
    public override long Length { get { return length; } }
    public override long Position { get { return position; } set { throw new NotSupportedException(); } }
    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) {
        if (Disposed) throw new ObjectDisposedException("ColdShaSlowTestStream");
        if (position == length) return 0;
        Thread.Sleep(DelayMilliseconds);
        int copied = (int)Math.Min(Math.Min((long)count, 65536L), length - position);
        for (int index = 0; index < copied; index++) buffer[offset + index] = (byte)((position + index) % 251);
        position += copied;
        return copied;
    }
    public override long Seek(long offset, SeekOrigin origin) { throw new NotSupportedException(); }
    public override void SetLength(long value) { throw new NotSupportedException(); }
    public override void Write(byte[] buffer, int offset, int count) { throw new NotSupportedException(); }
    protected override void Dispose(bool disposing) { Disposed = true; base.Dispose(disposing); }
}
'''


def slow_stream_prelude(tmp_path):
    empty = tmp_path / 'initialize-sha.bin'
    empty.write_bytes(b'')
    return "Get-Sha256 " + quote(empty) + "|Out-Null;Add-Type -TypeDefinition @'\n" + SLOW_STREAM_SOURCE + "\n'@;"


def test_native_sha_reports_progress_inside_slow_stream_blocks(tmp_path):
    length = 2 * 65536 + 17
    payload = bytes(range(251)) * (length // 251) + bytes(range(length % 251))
    code = slow_stream_prelude(tmp_path)
    code += "$stream=New-Object ColdShaSlowTestStream([long]" + str(length) + ");"
    code += "$events=New-Object 'Collections.Generic.List[long]';"
    code += "$action=[Action[long]]{param($bytes);[void]$events.Add($bytes)};"
    code += "try{$digest=[ProBigA.ColdMigration.ShaNative]::Compute($stream,$action);$callerStillOwns=-not $stream.Disposed}"
    code += "finally{$stream.Dispose()};"
    code += "[pscustomobject]@{digest=$digest;events=@($events);caller_still_owns=$callerStillOwns;disposed=$stream.Disposed}|ConvertTo-Json -Compress"
    result = native_helpers(code)
    assert result.returncode == 0, result.stdout + result.stderr
    values = json.loads(result.stdout)
    assert values['digest'] == hashlib.sha256(payload).hexdigest().upper()
    assert len(values['events']) >= 2
    assert any(0 < count < length for count in values['events'])
    assert all(0 <= count <= length for count in values['events'])
    assert values['events'] == sorted(values['events'])
    assert values['caller_still_owns'] is True
    assert values['disposed'] is True


def test_native_sha_callback_failure_propagates_without_disposing_caller_stream(tmp_path):
    code = slow_stream_prelude(tmp_path)
    code += "$stream=New-Object ColdShaSlowTestStream([long]65537);$caught=$false;"
    code += "$action=[Action[long]]{param($bytes);throw 'EXPECTED_HASH_PROGRESS_EXCEPTION'};"
    code += "try{try{[ProBigA.ColdMigration.ShaNative]::Compute($stream,$action)|Out-Null}"
    code += "catch{if($_.Exception.ToString() -notlike '*EXPECTED_HASH_PROGRESS_EXCEPTION*'){throw};$caught=$true};"
    code += "$callerStillOwns=-not $stream.Disposed;$readBytes=$stream.Position}finally{$stream.Dispose()};"
    code += "[pscustomobject]@{caught=$caught;caller_still_owns=$callerStillOwns;read_bytes=$readBytes;disposed=$stream.Disposed}|ConvertTo-Json -Compress"
    result = native_helpers(code)
    assert result.returncode == 0, result.stdout + result.stderr
    values = json.loads(result.stdout)
    assert values['caught'] is True
    assert values['caller_still_owns'] is True
    assert values['read_bytes'] > 0
    assert values['disposed'] is True


def test_get_sha_owns_file_stream_with_finally_in_native_function_ast():
    code = "$tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseFile("
    code += quote(TOOLS / 'package_common.ps1') + ",[ref]$tokens,[ref]$errors);"
    code += "$function=$ast.Find({param($node)$node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-Sha256'},$true);"
    code += "$tries=@($function.FindAll({param($node)$node -is [Management.Automation.Language.TryStatementAst]},$true));"
    code += "if(-not @($tries|Where-Object{$_.Finally -and $_.Finally.Extent.Text -match '\\$stream\\.Dispose\\('}).Count){throw 'FILE_STREAM_FINALLY_MISSING'};"
    code += "if($function.Extent.Text -notmatch 'ShaNative\\]::Compute'){throw 'UNIFIED_NATIVE_STREAMING_HASH_MISSING'}"
    result = native_helpers(code)
    assert result.returncode == 0, result.stdout + result.stderr


def package_progress_action():
    return "$events=New-Object 'Collections.Generic.List[object]';$callback={param($phase,$bytes,$total,$valid,$files);" \
        "if($phase -isnot [string] -or $bytes -isnot [long] -or $total -isnot [long] -or " \
        "$valid -isnot [int] -or $files -isnot [int]){throw 'PACKAGE_PROGRESS_TYPE_INVALID'};" \
        "[void]$events.Add([pscustomobject][ordered]@{phase=$phase;bytes=$bytes;total=$total;valid=$valid;files=$files});" \
        "Write-Output 'UNWANTED_PACKAGE_CALLBACK_OUTPUT'};"


def test_complete_package_numeric_progress_is_ordered_private_and_output_isolated(tmp_path):
    manifest = sealed_fixture(tmp_path)
    code = package_progress_action()
    code += "$result=@(Assert-ColdPackage " + quote(tmp_path) + " -ProgressAction $callback);"
    code += "[pscustomobject]@{result_count=$result.Count;format=$result[0].format;events=$events.ToArray()}|ConvertTo-Json -Depth 8 -Compress"
    result = native_helpers(code)
    assert result.returncode == 0, result.stdout + result.stderr
    values = json.loads(result.stdout)
    assert values['result_count'] == 1
    assert values['format'] == manifest['format']
    events = values['events']
    phases = [event['phase'] for event in events]
    expected = ['inventory-scanning', 'inventory-refresh', 'hashing-files',
                'final-inventory-scanning', 'final-inventory-refresh', 'verified']
    assert set(phases) == set(expected)
    assert [phase for index, phase in enumerate(phases) if not index or phase != phases[index - 1]] == expected
    assert phases.count('verified') == 1
    measured = sum(row['bytes'] for row in manifest['files'])
    assert events[-1] == {'phase': 'verified', 'bytes': measured, 'total': measured,
                          'valid': len(manifest['files']), 'files': len(manifest['files'])}
    hashing = [event for event in events if event['phase'] == 'hashing-files']
    assert all(event['total'] == measured and event['files'] == len(manifest['files']) for event in hashing)
    assert [event['bytes'] for event in hashing] == sorted(event['bytes'] for event in hashing)
    assert [event['valid'] for event in hashing] == sorted(event['valid'] for event in hashing)
    assert hashing[-1]['bytes'] == measured and hashing[-1]['valid'] == len(manifest['files'])
    assert all(set(event) == {'phase', 'bytes', 'total', 'valid', 'files'} for event in events)
    assert 'UNWANTED_PACKAGE_CALLBACK_OUTPUT' not in result.stdout
    assert str(tmp_path) not in result.stdout
    assert 'server-key.pem' not in result.stdout
    assert 'mysql.ibd' not in result.stdout


@pytest.mark.parametrize('seal_file', ['READY', 'manifest.json'])
def test_final_seal_mutation_never_reports_verified(tmp_path, seal_file):
    sealed_fixture(tmp_path)
    code = "$events=New-Object 'Collections.Generic.List[string]';$mutated=$false;$sealPath=" + quote(tmp_path / seal_file) + ";"
    code += "$callback={param($phase,$bytes,$total,$valid,$files);[void]$events.Add($phase);"
    code += "if($phase -eq 'final-inventory-refresh' -and -not $script:mutated){$script:mutated=$true;"
    code += "$stamp=(Get-Item -LiteralPath $sealPath -Force).LastWriteTimeUtc;$text=[IO.File]::ReadAllText($sealPath);"
    code += "[IO.File]::WriteAllText($sealPath,$text.Replace(('a'*40),('b'*40)));"
    code += "(Get-Item -LiteralPath $sealPath -Force).LastWriteTimeUtc=$stamp}};"
    code += "$blocked=$false;try{Assert-ColdPackage " + quote(tmp_path) + " -ProgressAction $callback|Out-Null}catch{$blocked=$true};"
    code += "[pscustomobject]@{blocked=$blocked;mutated=$script:mutated;events=@($events)}|ConvertTo-Json -Compress"
    result = native_helpers(code)
    assert result.returncode == 0, result.stdout + result.stderr
    values = json.loads(result.stdout)
    assert values['blocked'] is True
    assert values['mutated'] is True
    assert 'verified' not in values['events']


def test_progress_keeps_actual_empty_junction_rejection(tmp_path):
    root = tmp_path / 'package'
    root.mkdir()
    sealed_fixture(root)
    outside = tmp_path / 'outside-empty'
    outside.mkdir()
    code = "New-Item -ItemType Junction -Path " + quote(root / 'private-empty-link') + " -Target " + quote(outside) + "|Out-Null;"
    code += "$events=New-Object 'Collections.Generic.List[string]';$callback={param($phase,$bytes,$total,$valid,$files);[void]$events.Add($phase)};"
    code += "$blocked=$false;try{Assert-ColdPackage " + quote(root) + " -ProgressAction $callback|Out-Null}catch{$blocked=$true};"
    code += "[pscustomobject]@{blocked=$blocked;events=@($events)}|ConvertTo-Json -Compress"
    result = native_helpers(code)
    assert result.returncode == 0, result.stdout + result.stderr
    values = json.loads(result.stdout)
    assert values['blocked'] is True
    assert 'verified' not in values['events']


def scoped_progress_fixture(tmp_path):
    root = tmp_path / 'small-sealed-package'
    root.mkdir()
    manifest = package(root)
    length = 65537
    payload = bytes(range(251)) * (length // 251) + bytes(range(length % 251))
    (root / 'code.bundle').write_bytes(payload)
    row = manifest['files'][0]
    assert row['path'] == 'code.bundle'
    row.update(bytes=length, sha256=hashlib.sha256(payload).hexdigest())
    manifest_path = root / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    (root / 'READY').write_text('a' * 40 + ' ' + hashlib.sha256(manifest_path.read_bytes()).hexdigest().upper(), encoding='utf-8')

    code = "$ErrorActionPreference='Stop';"
    code += "$env:PSModulePath=\"$env:SystemRoot\\System32\\WindowsPowerShell\\v1.0\\Modules;$env:ProgramFiles\\WindowsPowerShell\\Modules\";"
    code += "Import-Module Microsoft.PowerShell.Utility,Microsoft.PowerShell.Management -ErrorAction Stop;"
    code += "if($PSVersionTable.PSVersion.Major -ne 5){throw 'NATIVE_PS5_REQUIRED'};"
    # No global dot-source: both real -File and &{} put common's functions and
    # the consumer callback's named function in the caller's private scope.
    code += ". " + quote(TOOLS / 'package_common.ps1') + ";"
    code += "Add-Type -TypeDefinition @'\n" + SLOW_STREAM_SOURCE + "\n'@;"
    code += "$probeState=[pscustomobject]@{Events=(New-Object 'Collections.Generic.List[object]');Calls=0;HashStream=$null};"
    code += r'''
function Receive-LocalPackageProgress {
    param([string]$phase,[long]$bytes,[long]$total,[int]$valid,[int]$files)
    $probeState.Calls++
    [void]$probeState.Events.Add([pscustomobject][ordered]@{phase=$phase;bytes=$bytes;total=$total;valid=$valid;files=$files})
    Write-Output 'SCOPED_CALLBACK_OUTPUT_MUST_NOT_ESCAPE'
}
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile(COMMON_PATH,[ref]$tokens,[ref]$errors)
$hashFunction=$ast.Find({param($node)$node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-Sha256'},$true)
$hashDefinition=$hashFunction.Extent.Text
$originalOpen='$stream = [IO.File]::OpenRead($Path)'
if(-not $hashDefinition.Contains($originalOpen)){throw 'ACTUAL_HASH_STREAM_OPEN_NOT_FOUND'}
$fixtureOpen='$stream = if([IO.Path]::GetFileName($Path) -eq ''code.bundle''){$probeState.HashStream=New-Object ColdShaSlowTestStream([long]65537);$probeState.HashStream}else{[IO.File]::OpenRead($Path)}'
$hashDefinition=$hashDefinition.Replace($originalOpen,$fixtureOpen)
. ([scriptblock]::Create($hashDefinition))
$callback={param($phase,$bytes,$total,$valid,$files);Receive-LocalPackageProgress $phase $bytes $total $valid $files}
$blocked=$false;$failure='';$actual=@()
try{$actual=@(Assert-ColdPackage PACKAGE_PATH -ProgressAction $callback)}
catch{$blocked=$true;if($_.Exception.ToString() -match 'Invoke-ColdPackageProgress|Receive-LocalPackageProgress'){$failure='LOCAL_CALLBACK_RESOLUTION_FAILED'}else{$failure='UNEXPECTED_TEST_FAILURE'}}
[pscustomobject]@{blocked=$blocked;failure=$failure;result_count=$actual.Count;calls=$probeState.Calls;
    disposed=$probeState.HashStream.Disposed;events=$probeState.Events.ToArray()}|ConvertTo-Json -Depth 8 -Compress
'''
    code = code.replace('COMMON_PATH', quote(TOOLS / 'package_common.ps1')).replace('PACKAGE_PATH', quote(root))
    return code, manifest


def run_scoped_fixture(tmp_path, code, invocation):
    if invocation == 'file':
        path = tmp_path / 'actual-private-script-scope.ps1'
        path.write_text(code, encoding='utf-8')
        return subprocess.run([PS, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', str(path)],
                              capture_output=True, text=True, timeout=30)
    return run_ps('& {\n' + code + '\n}')


@pytest.mark.parametrize('invocation', ['file', 'local-block'])
def test_first_delayed_package_callback_resolves_script_local_functions_and_counts(tmp_path, invocation):
    code, manifest = scoped_progress_fixture(tmp_path)
    result = run_scoped_fixture(tmp_path, code, invocation)
    assert result.returncode == 0, result.stdout + result.stderr
    values = json.loads(result.stdout)
    assert values['blocked'] is False, values['failure']
    assert values['result_count'] == 1
    assert values['disposed'] is True
    events = values['events']
    assert values['calls'] == len(events)
    measured = sum(row['bytes'] for row in manifest['files'])
    assert any(event['phase'] == 'hashing-files' and 0 < event['bytes'] < 65537 and event['valid'] == 0
               and event['total'] == measured and event['files'] == len(manifest['files']) for event in events)
    assert events[-1] == {'phase': 'verified', 'bytes': measured, 'total': measured,
                          'valid': len(manifest['files']), 'files': len(manifest['files'])}
    assert 'SCOPED_CALLBACK_OUTPUT_MUST_NOT_ESCAPE' not in result.stdout
    assert str(tmp_path) not in result.stdout
