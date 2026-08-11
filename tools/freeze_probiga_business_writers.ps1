[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("Audit", "Freeze", "EnableScheduledTasks")]
    [string]$Mode,

    [Parameter(Mandatory = $true)]
    [string]$State,

    [Parameter(Mandatory = $true)]
    [string]$Evidence,

    [string]$Ack
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$freezeAck = "I_CONFIRM_PROBIGA_BUSINESS_WRITERS_MAY_BE_STOPPED"
$enableAck = "I_CONFIRM_MYSQL84_POST_CUTOVER_CHECKS_PASSED"
$root = (Resolve-Path -LiteralPath (Split-Path -Parent $PSScriptRoot)).Path
$writerPatterns = @(
    "uvicorn server.api.main",
    "server.api.main:app",
    "run_scheduler_daemon.py",
    "run_qmt_live_runtime.py",
    "run_big_qmt_bridge.py",
    "run_guojin_qmt_gateway.py",
    "QMTAgent",
    "run_scheduler",
    "run_local_live_supervisor.ps1",
    "launch_local_live_supervisor.ps1",
    "sync_",
    "crawl_",
    "fetch_",
    "backfill_",
    "replay_guojin_qmt_pending_writes.py",
    "repair_guojin_qmt_gaps.py"
)

function Get-AbsolutePath {
    param([Parameter(Mandatory = $true)][string]$Path, [bool]$MustExist = $false)
    # Windows PowerShell 5.1 uses .NET Framework, which does not expose
    # Path.IsPathFullyQualified. Accept only drive-rooted or UNC paths so
    # drive-relative values such as C:foo remain rejected.
    $isDriveRooted = $Path -match '^[A-Za-z]:[\\/]'
    $isUncRooted = $Path -match '^\\\\[^\\/]'
    if (-not ($isDriveRooted -or $isUncRooted)) { throw "Path must be absolute: $Path" }
    if ($MustExist) { return (Resolve-Path -LiteralPath $Path).Path }
    return [IO.Path]::GetFullPath($Path)
}

function Write-AtomicJson {
    param([string]$Path, $Value, [bool]$Replace)
    $resolved = Get-AbsolutePath -Path $Path
    if ((Test-Path -LiteralPath $resolved) -and -not $Replace) {
        throw "Refusing to overwrite artifact: $resolved"
    }
    $parent = Split-Path -Parent $resolved
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
    $partial = Join-Path $parent ("." + [IO.Path]::GetFileName($resolved) + "." + [guid]::NewGuid().ToString("N") + ".partial")
    try {
        [IO.File]::WriteAllText(
            $partial,
            (($Value | ConvertTo-Json -Depth 20) + [Environment]::NewLine),
            [Text.UTF8Encoding]::new($false)
        )
        Move-Item -LiteralPath $partial -Destination $resolved -Force:$Replace
    }
    finally {
        if (Test-Path -LiteralPath $partial) { Remove-Item -LiteralPath $partial -Force }
    }
}

function Test-WriterCommand {
    param([string]$Name, [string]$CommandLine)
    if ($Name -notin @("python.exe", "pythonw.exe", "powershell.exe", "pwsh.exe", "cmd.exe")) {
        return $false
    }
    $text = [string]$CommandLine
    foreach ($pattern in $writerPatterns) {
        if ($text.IndexOf($pattern, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            return $true
        }
    }
    return $false
}

function Get-Writers {
    $processes = @(Get-CimInstance Win32_Process)
    $selected = @{}
    foreach ($process in $processes) {
        if ($process.ProcessId -ne $PID -and
            (Test-WriterCommand -Name ([string]$process.Name) -CommandLine ([string]$process.CommandLine))) {
            $selected[[int]$process.ProcessId] = $true
        }
    }
    # Some elevated QMTAgent processes hide their command line from this
    # session. Any process with an established client connection to the legacy
    # 3306 endpoint is nevertheless in the database cutover scope and must be
    # quiesced. Include its descendants so detached scheduler workers cannot
    # write after the initial connection snapshot.
    try {
        foreach ($connection in @(Get-NetTCPConnection -RemotePort 3306 -State Established -ErrorAction Stop)) {
            $owner = [int]$connection.OwningProcess
            if ($owner -gt 0 -and $owner -ne $PID) { $selected[$owner] = $true }
        }
    }
    catch {}
    $changed = $true
    while ($changed) {
        $changed = $false
        foreach ($process in $processes) {
            $id = [int]$process.ProcessId
            $parent = [int]$process.ParentProcessId
            if ($id -ne $PID -and $selected.ContainsKey($parent) -and -not $selected.ContainsKey($id)) {
                $selected[$id] = $true
                $changed = $true
            }
        }
    }
    return @($processes | Where-Object { $selected.ContainsKey([int]$_.ProcessId) } | Sort-Object ProcessId)
}

function Get-ProBigATasks {
    $matches = @()
    foreach ($task in Get-ScheduledTask -ErrorAction SilentlyContinue) {
        $actionText = ($task.Actions | ForEach-Object {
            $action = $_
            $parts = foreach ($name in @("Execute", "Arguments", "WorkingDirectory")) {
                $property = $action.PSObject.Properties[$name]
                if ($null -ne $property) { [string]$property.Value }
            }
            $parts -join " "
        }) -join " | "
        if ($actionText.IndexOf("ProBigA", [StringComparison]::OrdinalIgnoreCase) -ge 0 -or
            $actionText.IndexOf("QMTAgent", [StringComparison]::OrdinalIgnoreCase) -ge 0 -or
            $actionText.IndexOf($root, [StringComparison]::OrdinalIgnoreCase) -ge 0 -or
            ([string]$task.TaskName).StartsWith("QMTAgent", [StringComparison]::OrdinalIgnoreCase)) {
            $matches += [pscustomobject]@{
                TaskName = [string]$task.TaskName
                TaskPath = [string]$task.TaskPath
                Enabled = [bool]($task.State -ne "Disabled")
            }
        }
    }
    return @($matches | Sort-Object TaskPath, TaskName)
}

function Get-SafeWriterSnapshot {
    param($Processes)
    return @(
        $Processes | ForEach-Object {
            $command = [string]$_.CommandLine
            $algorithm = [Security.Cryptography.SHA256]::Create()
            try {
                $hash = ([BitConverter]::ToString($algorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($command)))).Replace("-", "").ToLowerInvariant()
            }
            finally { $algorithm.Dispose() }
            [ordered]@{
                pid = [int]$_.ProcessId
                name = [string]$_.Name
                command_sha256 = $hash
            }
        }
    )
}

$statePath = Get-AbsolutePath -Path $State
$evidencePath = Get-AbsolutePath -Path $Evidence

if ($Mode -eq "Freeze") {
    if ($Ack -cne $freezeAck) { throw "Exact writer-freeze acknowledgement is required" }
    if (Test-Path -LiteralPath $statePath) { throw "Freeze state already exists" }
    if (Test-Path -LiteralPath $evidencePath) { throw "Freeze evidence already exists" }
    $writers = @(Get-Writers)
    $tasks = @(Get-ProBigATasks)
    $planned = [ordered]@{
        schema_version = 1
        tool = "freeze_probiga_business_writers"
        status = "planned"
        started_at_utc = [DateTime]::UtcNow.ToString("o")
        workspace = $root
        writers = @(Get-SafeWriterSnapshot -Processes $writers)
        scheduled_tasks = @($tasks | ForEach-Object {
            [ordered]@{ task_name = $_.TaskName; task_path = $_.TaskPath; was_enabled = $_.Enabled }
        })
        command_lines_stored = $false
    }
    Write-AtomicJson -Path $statePath -Value $planned -Replace $false

    foreach ($task in $tasks | Where-Object { $_.Enabled }) {
        Stop-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath -ErrorAction SilentlyContinue
        Disable-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath | Out-Null
    }
    foreach ($writer in $writers) {
        Stop-Process -Id $writer.ProcessId -ErrorAction SilentlyContinue
    }
    $deadline = (Get-Date).AddSeconds(15)
    do {
        Start-Sleep -Milliseconds 500
        $remaining = @(Get-Writers)
    } while ($remaining.Count -gt 0 -and (Get-Date) -lt $deadline)
    foreach ($writer in $remaining) {
        Stop-Process -Id $writer.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
    $remaining = @(Get-Writers)
    if ($remaining.Count -ne 0) {
        throw "One or more known ProBigA writer processes could not be stopped"
    }
    $enabledTasks = @(Get-ProBigATasks | Where-Object { $_.Enabled })
    if ($enabledTasks.Count -ne 0) {
        throw "One or more ProBigA scheduled tasks remain enabled"
    }
    $result = [ordered]@{
        schema_version = 1
        tool = "freeze_probiga_business_writers"
        status = "frozen"
        finished_at_utc = [DateTime]::UtcNow.ToString("o")
        stopped_writer_count = $writers.Count
        disabled_task_count = @($tasks | Where-Object { $_.Enabled }).Count
        remaining_writer_count = 0
        enabled_probiga_task_count = 0
        state = $statePath
    }
    Write-AtomicJson -Path $statePath -Value (@{ planned = $planned; result = $result }) -Replace $true
    Write-AtomicJson -Path $evidencePath -Value $result -Replace $false
    $result | ConvertTo-Json -Depth 20
    exit 0
}

if ($Mode -eq "Audit") {
    if (Test-Path -LiteralPath $evidencePath) { throw "Audit evidence already exists" }
    $writers = @(Get-Writers)
    $enabledTasks = @(Get-ProBigATasks | Where-Object { $_.Enabled })
    $result = [ordered]@{
        schema_version = 1
        tool = "freeze_probiga_business_writers"
        status = if ($writers.Count -eq 0 -and $enabledTasks.Count -eq 0) { "frozen" } else { "not_frozen" }
        finished_at_utc = [DateTime]::UtcNow.ToString("o")
        writers = @(Get-SafeWriterSnapshot -Processes $writers)
        enabled_tasks = @($enabledTasks | ForEach-Object { [ordered]@{ task_name = $_.TaskName; task_path = $_.TaskPath } })
    }
    Write-AtomicJson -Path $evidencePath -Value $result -Replace $false
    $result | ConvertTo-Json -Depth 20
    if ($result.status -ne "frozen") { exit 2 }
    exit 0
}

if ($Mode -eq "EnableScheduledTasks") {
    if ($Ack -cne $enableAck) { throw "Exact post-cutover acknowledgement is required" }
    if (-not (Test-Path -LiteralPath $statePath -PathType Leaf)) { throw "Freeze state is missing" }
    if (Test-Path -LiteralPath $evidencePath) { throw "Enable evidence already exists" }
    $saved = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    $planned = if ($saved.PSObject.Properties.Name -contains "planned") { $saved.planned } else { $saved }
    $enabled = 0
    foreach ($task in $planned.scheduled_tasks | Where-Object { $_.was_enabled -eq $true }) {
        Enable-ScheduledTask -TaskName ([string]$task.task_name) -TaskPath ([string]$task.task_path) | Out-Null
        $enabled += 1
    }
    $result = [ordered]@{
        schema_version = 1
        tool = "freeze_probiga_business_writers"
        status = "scheduled_tasks_enabled"
        finished_at_utc = [DateTime]::UtcNow.ToString("o")
        enabled_task_count = $enabled
        writer_processes_started = $false
    }
    Write-AtomicJson -Path $evidencePath -Value $result -Replace $false
    $result | ConvertTo-Json -Depth 20
    exit 0
}

throw "Unsupported mode"
