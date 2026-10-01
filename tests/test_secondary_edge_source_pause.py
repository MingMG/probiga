from pathlib import Path
import os
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_source_pause_is_durable_and_no_automatic_resume():
    source = (ROOT / 'tools/secondary_edge/pause_source.ps1').read_text(encoding='utf-8')
    assert 'Set-Service -Name $serviceName -StartupType Disabled' in source
    assert 'Stop-Service -Name $serviceName' in source
    assert 'Shutdown complete' in source
    assert 'source_automatically_resume=$false' in source
    assert 'qmt-exit-required' in source
    assert 'Start-Service' not in source
    assert 'Enable-ScheduledTask' not in source
    assert 'Stop-Process -Name' not in source
    assert '$current.CreationDate -eq $proc.CreationDate' in source
    assert '$candidate.ProcessId -in $excluded' in source
    assert "--execute=SELECT @@server_uuid,@@hostname,@@version;" in source
    assert 'automatic_restore=$false' in source


def test_powershell_sources_parse_on_windows():
    import os
    if os.name != 'nt':
        import pytest
        pytest.skip('Windows PowerShell parser')
    for file in ('pause_source.ps1', 'package_common.ps1'):
        path = str(ROOT / 'tools/secondary_edge' / file).replace("'", "''")
        command = f"$t=$null;$e=$null;[System.Management.Automation.Language.Parser]::ParseFile('{path}',[ref]$t,[ref]$e)|Out-Null;if($e.Count){{$e|ForEach-Object Message;exit 1}}"
        result = subprocess.run(['powershell.exe', '-NoProfile', '-Command', command], capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr


def test_native_file_entry_resolves_default_root_before_rejecting_foreign_host():
    import os
    if os.name != 'nt':
        import pytest
        pytest.skip('Windows PowerShell native file entry')
    result = subprocess.run(['powershell.exe','-NoProfile','-ExecutionPolicy','Bypass','-File',
        str(ROOT / 'tools/secondary_edge/prepare_cold_package.ps1'),
        '-ExpectedHost','FOREIGN-NOT-SOURCE'],capture_output=True,text=True,timeout=15)
    assert result.returncode != 0
    assert 'SOURCE-PC ONLY' in result.stderr
    assert 'empty string' not in result.stderr


@pytest.mark.parametrize('registry_state', ['absent-key', 'absent-value', 'present-value'])
def test_native_startup_lookup_accepts_paused_absence_without_suppressing_errors(registry_state):
    ps = shutil.which('powershell.exe')
    if os.name != 'nt' or not ps:
        pytest.skip('Windows PowerShell registry provider')
    source = ROOT / 'tools/secondary_edge/pause_source.ps1'
    path = str(source).replace("'", "''")
    setup = {
        'absent-key': '',
        'absent-value': 'New-Item -Path $keyPath -Force|Out-Null;',
        'present-value': ('New-Item -Path $keyPath -Force|Out-Null;'
                          "New-ItemProperty -LiteralPath $keyPath -Name 'owned' "
                          "-PropertyType ExpandString -Value '%TEMP%\\expected-launcher'|Out-Null;"),
    }[registry_state]
    expected = ("if($value -cne '%TEMP%\\expected-launcher'){throw 'Raw startup value changed.'}"
                if registry_state == 'present-value' else
                "if($null -ne $value){throw 'Absent startup must return null.'}")
    command = (
        "$ErrorActionPreference='Stop';Set-StrictMode -Version Latest;"
        "$t=$null;$e=$null;"
        f"$ast=[System.Management.Automation.Language.Parser]::ParseFile('{path}',[ref]$t,[ref]$e);"
        "$function=$ast.Find({param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] "
        "-and $node.Name -eq 'Get-SourceStartupRunValue'},$true);"
        "if(-not $function){throw 'Missing startup lookup helper.'};Invoke-Expression $function.Extent.Text;"
        "$keyPath='HKCU:\\Software\\ProBigA_Migration_Test_'+[guid]::NewGuid().ToString('N');"
        "try{" + setup + "$value=Get-SourceStartupRunValue $keyPath 'owned';" + expected +
        "}finally{if(Test-Path -LiteralPath $keyPath){Remove-Item -LiteralPath $keyPath -Recurse -Force}}"
    )
    result = subprocess.run([ps, '-NoProfile', '-NonInteractive', '-Command', command],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
