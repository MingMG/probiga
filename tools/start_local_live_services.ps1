$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"
if (!(Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

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

    $candidates = @(
        "D:\国金证券QMT交易端\bin.x64\XtMiniQmt.exe",
        "C:\国金证券QMT交易端\bin.x64\XtMiniQmt.exe",
        "D:\QMT\bin.x64\XtMiniQmt.exe",
        "C:\QMT\bin.x64\XtMiniQmt.exe"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    $running = Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "XtMiniQmt.exe" -and $_.ExecutablePath
    } | Select-Object -First 1
    if ($running -and $running.ExecutablePath -and (Test-Path -LiteralPath $running.ExecutablePath)) {
        return $running.ExecutablePath
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

function Get-QmtProcesses {
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "XtMiniQmt.exe"
    }
}

function Get-ServiceProcesses {
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "python.exe" -and (
            (
                $_.CommandLine -like "*run_guojin_qmt_gateway.py*" -and
                $_.ExecutablePath -like "*runtime\qmt-py313\Scripts\python.exe"
            ) -or
            $_.CommandLine -like "*run_qmt_live_runtime.py*" -or
            $_.CommandLine -like "*run_scheduler_daemon.py*" -or
            $_.CommandLine -like "*run_remote_mysql_tunnel.py*"
        )
    }
}

function Stop-DuplicateProcesses {
    $seen = @{}
    foreach ($proc in Get-ServiceProcesses | Sort-Object ProcessId) {
        $key = if ($proc.CommandLine -like "*run_guojin_qmt_gateway.py*") {
            "qmt_gateway"
        } elseif ($proc.CommandLine -like "*run_qmt_live_runtime.py*") {
            "qmt_live"
        } elseif ($proc.CommandLine -like "*run_scheduler_daemon.py*") {
            "scheduler"
        } else {
            "tunnel"
        }
        if ($seen.ContainsKey($key)) {
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
        } else {
            $seen[$key] = $true
        }
    }
}

function Ensure-Process {
    param(
        [string]$PythonExe,
        [string]$ScriptName,
        [string]$ArgLine,
        [string]$StdOutPath,
        [string]$StdErrPath
    )
    $running = Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "python.exe" -and $_.CommandLine -like "*$ScriptName*"
    }
    if ($running) {
        return
    }
    Start-Process -FilePath $PythonExe `
        -ArgumentList $ArgLine `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $StdOutPath `
        -RedirectStandardError $StdErrPath
}

function Ensure-QmtClient {
    $running = Get-QmtProcesses | Select-Object -First 1
    if ($running) {
        return
    }
    $clientPath = Resolve-QmtClientPath
    if (!$clientPath) {
        throw "XtMiniQmt.exe not found"
    }
    $workingDir = Split-Path -Parent $clientPath
    Start-Process -FilePath $clientPath `
        -WorkingDirectory $workingDir `
        -WindowStyle Minimized
    Start-Sleep -Seconds 15
}

$python = Resolve-PythonPath
$qmtPython = Join-Path $Root "runtime\qmt-py313\Scripts\python.exe"
if (!(Test-Path -LiteralPath $qmtPython)) {
    throw "Guojin QMT Python runtime not found: $qmtPython"
}
Stop-DuplicateProcesses
Ensure-QmtClient

Ensure-Process `
    -PythonExe $qmtPython `
    -ScriptName "run_guojin_qmt_gateway.py" `
    -ArgLine "tools/run_guojin_qmt_gateway.py" `
    -StdOutPath (Join-Path $DataDir "qmt_gateway.out.log") `
    -StdErrPath (Join-Path $DataDir "qmt_gateway.err.log")

Start-Sleep -Seconds 2

Ensure-Process `
    -PythonExe $python `
    -ScriptName "run_scheduler_daemon.py" `
    -ArgLine "tools/run_scheduler_daemon.py" `
    -StdOutPath (Join-Path $DataDir "scheduler_daemon.out.log") `
    -StdErrPath (Join-Path $DataDir "scheduler_daemon.err.log")

Ensure-Process `
    -PythonExe $python `
    -ScriptName "run_qmt_live_runtime.py" `
    -ArgLine "tools/run_qmt_live_runtime.py" `
    -StdOutPath (Join-Path $DataDir "qmt_live_runtime.out.log") `
    -StdErrPath (Join-Path $DataDir "qmt_live_runtime.err.log")

$sshHost = if ($env:PROBIGA_REMOTE_SSH_HOST) { $env:PROBIGA_REMOTE_SSH_HOST } else { "47.113.123.190" }
$sshUser = if ($env:PROBIGA_REMOTE_SSH_USER) { $env:PROBIGA_REMOTE_SSH_USER } else { "root" }
$sshPassword = $env:PROBIGA_REMOTE_SSH_PASSWORD
if ($sshPassword) {
    $env:PROBIGA_REMOTE_SSH_PASSWORD = $sshPassword
    Ensure-Process `
        -PythonExe $python `
        -ScriptName "run_remote_mysql_tunnel.py" `
        -ArgLine "tools/run_remote_mysql_tunnel.py --ssh-host $sshHost --ssh-user $sshUser --remote-bind-port 13306 --local-port 3306" `
        -StdOutPath (Join-Path $DataDir "mysql_tunnel.out.log") `
        -StdErrPath (Join-Path $DataDir "mysql_tunnel.err.log")
} else {
    Write-Warning "Skip remote MySQL tunnel: set PROBIGA_REMOTE_SSH_PASSWORD to enable it."
}
