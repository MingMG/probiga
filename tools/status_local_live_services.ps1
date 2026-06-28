$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"

function Resolve-QmtClientPath {
    $explicitCandidates = @()
    if ($env:GJ_QMT_EXE) { $explicitCandidates += $env:GJ_QMT_EXE }
    if ($env:QMT_CLIENT_EXE) { $explicitCandidates += $env:QMT_CLIENT_EXE }
    if ($env:GJ_QMT_HOME) { $explicitCandidates += (Join-Path $env:GJ_QMT_HOME "bin.x64\XtMiniQmt.exe") }
    if ($env:QMT_HOME) { $explicitCandidates += (Join-Path $env:QMT_HOME "bin.x64\XtMiniQmt.exe") }
    foreach ($candidate in $explicitCandidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    foreach ($driveRoot in @("D:\", "C:\")) {
        if (!(Test-Path -LiteralPath $driveRoot)) { continue }
        foreach ($folder in Get-ChildItem -LiteralPath $driveRoot -Directory -ErrorAction SilentlyContinue) {
            $candidate = Join-Path $folder.FullName "bin.x64\XtMiniQmt.exe"
            if (Test-Path -LiteralPath $candidate) {
                return (Resolve-Path -LiteralPath $candidate).Path
            }
        }
    }
    return $null
}

function Resolve-PythonPath {
    $candidates = @(
        "C:\Users\Administrator\AppData\Local\Python\pythoncore-3.14-64\python.exe",
        "C:\Users\Administrator\AppData\Local\Python\bin\python.exe",
        "C:\Users\Administrator\AppData\Local\Microsoft\WindowsApps\python.exe"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source) {
        return $cmd.Source
    }
    throw "python.exe not found"
}

Write-Host "Processes:" -ForegroundColor Cyan
$procs = Get-CimInstance Win32_Process | Where-Object {
    ($_.Name -eq "XtMiniQmt.exe") -or
    (
        $_.Name -eq "python.exe" -and (
            (
                $_.CommandLine -like "*run_guojin_qmt_gateway.py*" -and
                $_.ExecutablePath -like "*runtime\qmt-py313\Scripts\python.exe"
            ) -or
            $_.CommandLine -like "*run_qmt_live_runtime.py*" -or
            $_.CommandLine -like "*run_remote_mysql_tunnel.py*"
        )
    ) -or (
        $_.Name -eq "powershell.exe" -and (
            $_.CommandLine -like "*run_local_live_supervisor.ps1*" -or
            $_.CommandLine -like "*launch_local_live_supervisor.ps1*"
        )
    )
}
if (!$procs) {
    Write-Host "(none)"
}
else {
    ($procs | Select-Object ProcessId, CommandLine | Format-List | Out-String -Width 500) | Write-Host
}

Write-Host "`nQMT client:" -ForegroundColor Cyan
$installedQmt = Resolve-QmtClientPath
Write-Host "XtMiniQmt installed: $([bool]$installedQmt) path=$installedQmt"
$qmtProc = Get-CimInstance Win32_Process | Where-Object { $_.Name -eq "XtMiniQmt.exe" } | Select-Object -First 1
if ($qmtProc) {
    Write-Host "XtMiniQmt running: pid=$($qmtProc.ProcessId) path=$($qmtProc.ExecutablePath)"
}
else {
    Write-Host "XtMiniQmt running: no"
}

Write-Host "`nAuto-run:" -ForegroundColor Cyan
$StartupCmd = Join-Path ([Environment]::GetFolderPath("Startup")) "ProBigA Local Live Services.cmd"
if (Test-Path $StartupCmd) {
    Write-Host "Startup launcher: $StartupCmd"
}
else {
    Write-Host "Startup launcher: (missing)"
}
try {
    $runValue = Get-ItemPropertyValue "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run" -Name "ProBigA Local Live Services" -ErrorAction Stop
    Write-Host "HKCU Run: $runValue"
}
catch {
    Write-Host "HKCU Run: (missing)"
}

Write-Host "`nlocal_live_supervisor.out.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "local_live_supervisor.out.log") -Tail 20

Write-Host "`nlocal_live_supervisor.err.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "local_live_supervisor.err.log") -Tail 20

Write-Host "`nqmt_live_runtime.err.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "qmt_live_runtime.err.log") -Tail 20

Write-Host "`nqmt_gateway.err.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "qmt_gateway.err.log") -Tail 20

Write-Host "`nmysql_tunnel.err.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "mysql_tunnel.err.log") -Tail 20

Write-Host "`nDB freshness:" -ForegroundColor Cyan
$python = Resolve-PythonPath
@'
from datetime import datetime

from sqlalchemy import create_engine, text

from server.common.config import get_mysql_url

engine = create_engine(get_mysql_url(required=True), future=True)
queries = [
    ("sm_stock_current", "stock_code"),
    ("sm_index_current", "index_code"),
]
with engine.connect() as conn:
    for table_name, code_column in queries:
        try:
            row = conn.execute(
                text(
                    f"""
                    SELECT
                        MAX(snapshot_at) AS latest_snapshot_at,
                        COUNT(*) AS total_rows,
                        SUM(CASE WHEN DATE(snapshot_at) = CURDATE() THEN 1 ELSE 0 END) AS today_rows,
                        COUNT(DISTINCT CASE WHEN DATE(snapshot_at) = CURDATE() THEN {code_column} END) AS today_symbols
                    FROM {table_name}
                    """
                )
            ).mappings().first() or {}
        except Exception as exc:
            print(f"{table_name}: ERROR {exc}")
            continue
        latest = row.get("latest_snapshot_at")
        age = None
        if isinstance(latest, datetime):
            age = max(0, int((datetime.now() - latest).total_seconds()))
            latest = latest.strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"{table_name}: latest={latest} age_seconds={age} "
            f"today_rows={int(row.get('today_rows') or 0)} "
            f"today_symbols={int(row.get('today_symbols') or 0)} "
            f"total_rows={int(row.get('total_rows') or 0)}"
        )
'@ | & $python -
