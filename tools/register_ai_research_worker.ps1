[CmdletBinding(SupportsShouldProcess=$true)]
param(
    [Parameter(Mandatory=$true)][string]$RegisteredRoot,
    [Parameter(Mandatory=$true)][string]$ServerUrl,
    [string]$StateRoot = (Join-Path $env:LOCALAPPDATA 'ProBigA\research-worker')
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$taskName = 'ProBigA AI Research Worker'
$root = [IO.Path]::GetFullPath($RegisteredRoot).TrimEnd('\')
if ($root -ine [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot)).TrimEnd('\')) {
    throw 'Register the worker from the target production checkout.'
}
if ($root -notmatch '^[A-Za-z]:\\' -or $root.Contains('"')) {
    throw 'Research production root must be an ordinary absolute local path.'
}
$worker = Join-Path $root 'tools\run_codex_web_bridge.py'
$wrapper = Join-Path $root 'tools\run_ai_research_worker.ps1'
$python = Join-Path $root '.venv\Scripts\python.exe'
foreach ($path in @($root, $worker, $wrapper, $python)) {
    $item = Get-Item -LiteralPath $path -Force -ErrorAction Stop
    if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Research deployment refuses reparse points.' }
}
$head = (& git -C $root rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $head -notmatch '^[0-9a-f]{40}$') { throw 'Research worker HEAD cannot be resolved.' }
$production = (& git -C $root rev-parse refs/remotes/origin/main).Trim()
if ($LASTEXITCODE -ne 0 -or $head -cne $production) { throw 'Research worker HEAD must exactly equal fetched origin/main.' }
$dirty = & git -C $root status --porcelain --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $dirty) { throw 'Research worker tracked production files must be clean.' }
$uri = [uri]$ServerUrl
if (-not $uri.IsAbsoluteUri -or $uri.Scheme -notin @('http','https') -or $uri.UserInfo -or $uri.Query -or $uri.Fragment) {
    throw 'Research server must be a credential-free absolute HTTP origin.'
}
$state = [IO.Path]::GetFullPath($StateRoot).TrimEnd('\')
if ($state.Contains('"') -or $state -eq [IO.Path]::GetPathRoot($state).TrimEnd('\') -or
    $state -eq $root -or ($state+'\').StartsWith($root+'\',[StringComparison]::OrdinalIgnoreCase)) {
    throw 'Research state must be a dedicated path outside the code checkout.'
}
$workerHash = (Get-FileHash -LiteralPath $worker -Algorithm SHA256).Hash.ToLowerInvariant()
$powershell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$arguments = '-WindowStyle Hidden -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + $wrapper +
    '" -RegisteredRoot "' + $root + '" -ExpectedWorkerSha256 ' + $workerHash +
    ' -ServerUrl "' + $uri.AbsoluteUri.TrimEnd('/') + '" -StateRoot "' + $state + '"'
$user = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction -Execute $powershell -Argument $arguments -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
if ($PSCmdlet.ShouldProcess($taskName, 'Register independent hidden AI research worker; do not start it')) {
    Register-ScheduledTask -TaskName $taskName -TaskPath '\' -Action $action -Trigger $trigger `
        -Principal $principal -Settings $settings -Force | Out-Null
    $registered = Get-ScheduledTask -TaskName $taskName -TaskPath '\' -ErrorAction Stop
    if (@($registered.Actions).Count -ne 1 -or $registered.Actions[0].Execute -ine $powershell -or
        $registered.Actions[0].Arguments -cne $arguments -or $registered.Actions[0].WorkingDirectory -ine $root) {
        throw 'Research task registration readback differs from intended runtime identity.'
    }
}
[pscustomobject]@{task_name=$taskName;registered_root=$root;state_root=$state;worker_sha256=$workerHash;
    provider='codex_ephemeral';purpose='research';started=$false;account=$user}
