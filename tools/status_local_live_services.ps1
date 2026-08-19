$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"

function Resolve-QmtClientPath {
    $explicitCandidates = @()
    if ($env:BIG_QMT_HOME) { $explicitCandidates += (Join-Path $env:BIG_QMT_HOME "bin.x64\XtItClient.exe") }
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
            $standardCandidate = Join-Path $folder.FullName "bin.x64\XtItClient.exe"
            if (Test-Path -LiteralPath $standardCandidate) {
                return (Resolve-Path -LiteralPath $standardCandidate).Path
            }
            $candidate = Join-Path $folder.FullName "bin.x64\XtMiniQmt.exe"
            if (Test-Path -LiteralPath $candidate) {
                return (Resolve-Path -LiteralPath $candidate).Path
            }
        }
    }
    return $null
}

function Resolve-PythonPath {
    if ($env:PROBIGA_PYTHON_EXE -and (Test-Path -LiteralPath $env:PROBIGA_PYTHON_EXE)) {
        return (Resolve-Path -LiteralPath $env:PROBIGA_PYTHON_EXE).Path
    }
    $venvPython = Join-Path $Root ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venvPython) {
        return (Resolve-Path -LiteralPath $venvPython).Path
    }
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source) {
        return $cmd.Source
    }
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py -and $py.Source) {
        return $py.Source
    }
    $candidates = @(
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python313\python.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python312\python.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python311\python.exe")
    )
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    throw "python.exe not found"
}

function Get-ServiceKey {
    param($Proc)
    if ($Proc.CommandLine -like "*run_big_qmt_bridge.py*") {
        return "big_qmt_bridge"
    }
    if ($Proc.CommandLine -like "*run_guojin_qmt_gateway.py*") {
        return "qmt_gateway"
    }
    if ($Proc.CommandLine -like "*run_qmt_live_runtime.py*") {
        return "qmt_live"
    }
    if ($Proc.CommandLine -like "*run_remote_qmt_tunnel.py*") {
        return "qmt_tunnel"
    }
    if ($Proc.CommandLine -like "*run_scheduler_daemon.py*") {
        return "scheduler"
    }
    if ($Proc.CommandLine -like "*run_remote_mysql_tunnel.py*") {
        return "mysql_tunnel"
    }
    if ($Proc.CommandLine -like "*run_local_live_supervisor.ps1*" -or $Proc.CommandLine -like "*launch_local_live_supervisor.ps1*") {
        return "supervisor"
    }
    return "unknown"
}

function Test-PythonLauncherProcess {
    param($Proc)
    $exe = [string]$Proc.ExecutablePath
    $venvLauncher = Join-Path $Root ".venv\Scripts\python.exe"
    $qmtLauncher = Join-Path $Root "runtime\qmt-py313\Scripts\python.exe"
    return ($exe -ieq $venvLauncher) -or ($exe -ieq $qmtLauncher)
}

Write-Host "Processes:" -ForegroundColor Cyan
$procs = Get-CimInstance Win32_Process | Where-Object {
    ($_.Name -in @("XtMiniQmt.exe", "XtItClient.exe")) -or
    (
        $_.Name -eq "python.exe" -and (
            (
                $_.CommandLine -like "*run_guojin_qmt_gateway.py*"
            ) -or
            $_.CommandLine -like "*run_big_qmt_bridge.py*" -or
            $_.CommandLine -like "*run_qmt_live_runtime.py*" -or
            $_.CommandLine -like "*run_remote_qmt_tunnel.py*" -or
            $_.CommandLine -like "*run_remote_mysql_tunnel.py*" -or
            $_.CommandLine -like "*run_scheduler_daemon.py*"
        ) -and -not (Test-PythonLauncherProcess $_)
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
    $duplicates = $procs | Group-Object { Get-ServiceKey $_ } | Where-Object { $_.Count -gt 1 }
    if ($duplicates) {
        Write-Host "Duplicate service processes:" -ForegroundColor Yellow
        foreach ($group in $duplicates) {
            $pids = ($group.Group | Select-Object -ExpandProperty ProcessId) -join ","
            Write-Host "$($group.Name): $pids"
        }
    }
    else {
        Write-Host "Duplicate service processes: none"
    }
}

Write-Host "`nQMT client:" -ForegroundColor Cyan
$installedQmt = Resolve-QmtClientPath
Write-Host "QMT terminal installed: $([bool]$installedQmt) path=$installedQmt"
$qmtProc = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @("XtMiniQmt.exe", "XtItClient.exe")
} | Select-Object -First 1
if ($qmtProc) {
    Write-Host "QMT client running: name=$($qmtProc.Name) pid=$($qmtProc.ProcessId) path=$($qmtProc.ExecutablePath)"
}
else {
    Write-Host "QMT client running: no"
}

$diagnosticScript = Join-Path $Root "tools\diagnose_bigqmt_login.py"
if (Test-Path -LiteralPath $diagnosticScript) {
    try {
        $diagnosticPython = Resolve-PythonPath
        Write-Host "QMT login diagnostic (sanitized):" -ForegroundColor Cyan
        $diagnosticOutput = & $diagnosticPython $diagnosticScript --json 2>&1
        ($diagnosticOutput | Out-String).Trim() | Write-Host
        # A failed login deliberately returns a non-zero diagnostic code. The
        # status command itself must continue printing the remaining services.
        $global:LASTEXITCODE = 0
    }
    catch {
        Write-Host "QMT login diagnostic unavailable: $($_.Exception.Message)" -ForegroundColor Yellow
    }
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

Write-Host "`nbig_qmt_bridge.out.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "big_qmt_bridge.out.log") -Tail 20

Write-Host "`nbig_qmt_bridge.err.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "big_qmt_bridge.err.log") -Tail 20

$bigQmtHome = if ($env:BIG_QMT_HOME) { $env:BIG_QMT_HOME } elseif ($installedQmt) { Split-Path -Parent (Split-Path -Parent $installedQmt) } else { $null }
if ($bigQmtHome) {
    $bridgeRoot = Join-Path $bigQmtHome "userdata\probiga_bridge"
    Write-Host "`nBig QMT bridge files:" -ForegroundColor Cyan
    foreach ($name in @("heartbeat.json", "consumer_status.json", "full_quotes.json", "tracked_quotes.json")) {
        $path = Join-Path $bridgeRoot $name
        if (Test-Path -LiteralPath $path) {
            $item = Get-Item -LiteralPath $path
            Write-Host "$name size=$($item.Length) updated=$($item.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss'))"
        } else {
            Write-Host "$name (missing)"
        }
    }
    $heartbeatPath = Join-Path $bridgeRoot "heartbeat.json"
    if (Test-Path -LiteralPath $heartbeatPath) {
        try {
            $heartbeat = Get-Content -LiteralPath $heartbeatPath -Raw | ConvertFrom-Json
            $heartbeatAt = [DateTime]::MinValue
            $heartbeatAge = $null
            if ([DateTime]::TryParse([string]$heartbeat.updated_at, [ref]$heartbeatAt)) {
                $heartbeatAge = [int]((Get-Date) - $heartbeatAt).TotalSeconds
            }
            Write-Host (
                "strategy heartbeat: status=$($heartbeat.status) " +
                "age_seconds=$heartbeatAge subscription_id=$($heartbeat.subscription_id) " +
                "all_codes=$($heartbeat.all_code_count)"
            )
        }
        catch {
            Write-Host "strategy heartbeat: unreadable ($($_.Exception.Message))" -ForegroundColor Yellow
        }
    }
}

Write-Host "`nqmt_gateway.err.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "qmt_gateway.err.log") -Tail 20

Write-Host "`nqmt_tunnel.err.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "qmt_tunnel.err.log") -Tail 20

Write-Host "`nscheduler_daemon.out.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "scheduler_daemon.out.log") -Tail 20

Write-Host "`nscheduler_daemon.err.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "scheduler_daemon.err.log") -Tail 20

Write-Host "`nmysql_tunnel.err.log:" -ForegroundColor Cyan
Get-Content -ErrorAction SilentlyContinue (Join-Path $DataDir "mysql_tunnel.err.log") -Tail 20

Write-Host "`nDB freshness:" -ForegroundColor Cyan
$python = Resolve-PythonPath
@'
from datetime import datetime

from sqlalchemy import text

from server.common.batch_db import create_batch_engine

engine = create_batch_engine(future=True)
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
