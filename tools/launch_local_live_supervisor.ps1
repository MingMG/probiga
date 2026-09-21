$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"
if (!(Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

$SupervisorScript = Join-Path $Root "tools\run_local_live_supervisor.ps1"
$StdOutPath = Join-Path $DataDir "local_live_supervisor.out.log"
$StdErrPath = Join-Path $DataDir "local_live_supervisor.err.log"

$launchMutex = [Threading.Mutex]::new($false, "Local\ProBigA.LocalLiveSupervisorLaunch")
$launchOwned = $false
try {
    try { $launchOwned = $launchMutex.WaitOne(15000) }
    catch [Threading.AbandonedMutexException] { $launchOwned = $true }
    if (!$launchOwned) { throw 'Local live supervisor launch is still pending' }
    # CommandLine is unavailable across some Windows process tokens. The
    # supervisor's lifetime mutex is the authoritative singleton signal.
    $createdNew = $false
    $probe = [Threading.Mutex]::new(
        $false, "Local\ProBigA.LocalLiveSupervisor", [ref]$createdNew
    )
    try { if (!$createdNew) { exit 0 } }
    finally { $probe.Dispose() }

    $started = Start-Process -FilePath "powershell.exe" `
        -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$SupervisorScript`"" `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $StdOutPath `
        -RedirectStandardError $StdErrPath `
        -PassThru
    # Keep launch serialization until the child owns its lifetime mutex, so
    # overlapping updater/logon triggers cannot truncate its active log files.
    $deadline = (Get-Date).AddSeconds(10)
    do {
        if ($started.HasExited) { throw 'Local live supervisor exited during startup' }
        try {
            $ready = [Threading.Mutex]::OpenExisting("Local\ProBigA.LocalLiveSupervisor")
            $ready.Dispose()
            exit 0
        }
        catch [Threading.WaitHandleCannotBeOpenedException] { }
        Start-Sleep -Milliseconds 100
    } while ((Get-Date) -lt $deadline)
    throw 'Local live supervisor startup was not confirmed'
}
finally {
    if ($launchOwned) { $launchMutex.ReleaseMutex() }
    $launchMutex.Dispose()
}
