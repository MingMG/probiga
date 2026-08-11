from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "freeze_probiga_business_writers.ps1"


def test_writer_freeze_script_parses_as_powershell() -> None:
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


def test_writer_freeze_never_stores_or_replays_command_lines() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "command_lines_stored = $false" in text
    assert "command_sha256" in text
    assert "writer_processes_started = $false" in text
    assert "Invoke-Expression" not in text
    assert "Start-Process" not in text


def test_writer_freeze_requires_explicit_acks() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "I_CONFIRM_PROBIGA_BUSINESS_WRITERS_MAY_BE_STOPPED" in text
    assert "I_CONFIRM_MYSQL84_POST_CUTOVER_CHECKS_PASSED" in text


def test_writer_freeze_covers_hidden_qmtagent_source_clients() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "Get-NetTCPConnection -RemotePort 3306" in text
    assert 'IndexOf("QMTAgent"' in text
    assert "Stop-ScheduledTask" in text
