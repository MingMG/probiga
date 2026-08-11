from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "transition_mysql84_data_layout.ps1"


def test_transition_script_parses_as_powershell() -> None:
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


def test_large_ibdata_copy_is_unbuffered_and_durably_sealed() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "$rollbackIbdataSeal" in text
    assert 'copy_mode = "robocopy-unbuffered-sha256"' in text
    assert "$legacyIbdataParent $copyRoot $legacyIbdataName" in text
    assert "/J /NFL /NDL /NP" in text
    assert "Legacy ibdata durable seal was not published" in text
    assert "ibdata1.seal.unverified-" in text
    assert "$legacyBytes + $legacyDataBytes" in text
    assert "function Get-FileSha256" in text
    assert "SHA-256 read failed after $Attempts attempts" in text
    assert "resumed_from_verified_partial = $true" in text
    assert 'Where-Object { $_.Name -like ".ibdata1.copying-*" }' in text
