from pathlib import Path
import subprocess

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
