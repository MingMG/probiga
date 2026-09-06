param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$RegisteredRoot,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9A-Fa-f]{40}$")]
    [string]$ExpectedBuildSha,

    [int]$StopTimeoutSeconds = 15,
    [int]$StartTimeoutSeconds = 90,

    [ValidateRange(1, 300)]
    [int]$HeartbeatMaxAgeSeconds = 30,

    [switch]$PreflightOnly,

    [switch]$ColdStartRecovery
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$SecurityModuleManifest = Join-Path `
    $PSHOME `
    "Modules\Microsoft.PowerShell.Security\Microsoft.PowerShell.Security.psd1"
Import-Module -Name $SecurityModuleManifest -ErrorAction Stop

$StrategyName = "PROBIGA_BIGQMT_BRIDGE"
$EditorSuffix = "-" + (
    [string][char]0x7B56 + [char]0x7565 + [char]0x7F16 +
    [char]0x8F91 + [char]0x5668
)
$EditorTitle = "$StrategyName$EditorSuffix"
$ExpectedOrigin = "https://github.com/MingMG/probiga.git"
$ReleaseManifestName = "probiga_big_qmt_bridge.release.json"
$DirectModelFilePrefix = "probiga_direct_acquisition_"
$ReleaseManifestSchema = "probiga.bigqmt-strategy-manifest.v1"
$ReleaseProtocol = "probiga.bigqmt-strategy-release.v2"
$IdentityProtocol = "probiga.bigqmt-loaded-strategy-identity.v1"
$ExpectedBuild = $ExpectedBuildSha.Trim().ToLowerInvariant()
$ExpectedRoot = [System.IO.Path]::GetFullPath($RegisteredRoot)
$Root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$PythonExe = Join-Path $ExpectedRoot ".venv\Scripts\python.exe"
$Installer = Join-Path $ExpectedRoot "tools\run_big_qmt_bridge.py"
$ReleaseBootstrap = Join-Path `
    $ExpectedRoot `
    "tools\run_qmt_windows_edge_release_bootstrap.py"
$StrategyRepositoryPath = (
    "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py"
)
$ProgramDataRoot = [System.IO.Path]::GetFullPath($env:ProgramData)
$ReloadStateRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $ProgramDataRoot "ProBigA\qmt-model-reload")
)
$RecoveryStateSchema = "probiga.bigqmt-cold-start-recovery.v1"
$RecoveryStatePath = Join-Path $ReloadStateRoot "cold-start-recovery.json"
$TrustedReloadStateOwnerSids = @()

$QmtClient = $null
$QmtMainHandle = [IntPtr]::Zero
$QmtMainTitle = ""
$QmtPythonRoot = ""
$HeartbeatPath = ""
$PreviousHeartbeat = $null
$Backup = $null
$InstallAttempted = $false
$OldModelStopped = $false
$OldEditorClosed = $false
$NewModelStarted = $false
$StartAttempted = $false
$UiActionsAttempted = $false
$QmtCallsAttempted = $false
$ControlledColdStart = $false
$ColdStartEvidence = $null
$WasMinimized = $false
$PreviousForeground = [IntPtr]::Zero
$RecoveryMutex = $null
$RecoveryMutexOwned = $false
$ReleaseMutex = $null
$ReleaseMutexOwned = $false
$FinalPayload = $null
$FinalExitCode = 1
$PersistedRecovery = $null
$AttemptedRelease = $null
$RecoveredRunningIdempotently = $false

function Throw-NeedsUserAction(
    [string]$Reason,
    [string]$ReasonCode = "QMT_USER_ACTION_REQUIRED"
) {
    throw "NEEDS_USER_ACTION:${ReasonCode}:$Reason"
}

function Get-SafeErrorText($Failure) {
    $Text = [string]$Failure
    $Text = $Text -replace "\r|\n", " "
    if ($Text.Length -gt 500) {
        return $Text.Substring(0, 500)
    }
    return $Text
}

function Invoke-Git([string[]]$Arguments) {
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $Output = & git -C $ExpectedRoot @Arguments 2>$null
        $ExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousPreference
    }
    if ($ExitCode -ne 0) {
        throw "git command failed: git $($Arguments -join ' ')"
    }
    return @($Output)
}

function Assert-OrdinaryFile([string]$Path, [string]$Description) {
    if (!(Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Description is missing"
    }
    $Item = Get-Item -LiteralPath $Path -Force
    if (
        ($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
            -ne 0
    ) {
        throw "$Description cannot be a reparse point"
    }
}

function Assert-OrdinaryDirectory([string]$Path, [string]$Description) {
    if (!(Test-Path -LiteralPath $Path -PathType Container)) {
        throw "$Description is missing"
    }
    $Item = Get-Item -LiteralPath $Path -Force
    if (
        !$Item.PSIsContainer -or
        ($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
            -ne 0
    ) {
        throw "$Description must be an ordinary directory"
    }
}

function Test-PathInside([string]$Path, [string]$Directory) {
    $ResolvedPath = [System.IO.Path]::GetFullPath($Path)
    $ResolvedDirectory = [System.IO.Path]::GetFullPath($Directory)
    return $ResolvedPath.StartsWith(
        $ResolvedDirectory + [System.IO.Path]::DirectorySeparatorChar,
        [System.StringComparison]::OrdinalIgnoreCase
    )
}

function Get-PathOwnerSid([string]$Path, [string]$Description) {
    try {
        $Acl = Get-Acl -LiteralPath $Path -ErrorAction Stop
        return $Acl.GetOwner(
            [System.Security.Principal.SecurityIdentifier]
        ).Value
    }
    catch {
        throw "$Description owner is unavailable"
    }
}

function Assert-ProtectedPathOwner([string]$Path, [string]$Description) {
    if ($TrustedReloadStateOwnerSids.Count -eq 0) {
        throw "QMT reload trusted owner set is unavailable"
    }
    $OwnerSid = Get-PathOwnerSid $Path $Description
    if ($OwnerSid -cnotin $TrustedReloadStateOwnerSids) {
        throw "$Description owner is not a trusted QMT release identity"
    }
}

function Initialize-ProtectedStateOwnerContract {
    $CurrentIdentity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    if ($null -eq $CurrentIdentity.User) {
        throw "QMT reload current Windows identity SID is unavailable"
    }
    $CurrentSid = [string]$CurrentIdentity.User.Value
    $RootOwnerSid = Get-PathOwnerSid `
        $ReloadStateRoot `
        "QMT reload protected state root"
    $AllowedAclSids = @(
        "S-1-5-18",
        "S-1-5-32-544",
        $CurrentSid
    ) | Sort-Object -Unique
    $RootAcl = Get-Acl -LiteralPath $ReloadStateRoot -ErrorAction Stop
    if (!$RootAcl.AreAccessRulesProtected) {
        throw "QMT reload protected state root ACL inheritance is enabled"
    }
    $Rules = $RootAcl.GetAccessRules(
        $true,
        $true,
        [System.Security.Principal.SecurityIdentifier]
    )
    foreach ($Rule in $Rules) {
        if (
            $Rule.AccessControlType -eq `
                [System.Security.AccessControl.AccessControlType]::Allow -and
            [string]$Rule.IdentityReference.Value -cnotin $AllowedAclSids
        ) {
            throw "QMT reload protected state root ACL grants an unknown identity"
        }
    }
    if ($RootOwnerSid -cnotin @("S-1-5-32-544", $CurrentSid)) {
        throw "QMT reload protected state root owner is not trusted"
    }
    $script:TrustedReloadStateOwnerSids = @(
        $RootOwnerSid,
        $CurrentSid
    ) | Sort-Object -Unique
}

function Assert-QmtClientProcessOwner($Client) {
    $CurrentIdentity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $ProcessRecord = Get-CimInstance Win32_Process `
        -Filter "ProcessId = $([int]$Client.Id)" `
        -ErrorAction SilentlyContinue
    if ($null -eq $ProcessRecord) {
        Throw-NeedsUserAction `
            "the QMT client process identity disappeared" `
            "QMT_CLIENT_CHANGED"
    }
    try {
        $Owner = Invoke-CimMethod `
            -InputObject $ProcessRecord `
            -MethodName GetOwnerSid `
            -ErrorAction Stop
    }
    catch {
        Throw-NeedsUserAction `
            "the QMT client process owner is unavailable" `
            "QMT_CLIENT_OWNER_UNAVAILABLE"
    }
    if (
        [uint32]$Owner.ReturnValue -ne 0 -or
        [string]$Owner.Sid -cne [string]$CurrentIdentity.User.Value
    ) {
        Throw-NeedsUserAction `
            "the QMT client owner differs from the updater identity" `
            "QMT_CLIENT_OWNER_MISMATCH"
    }
}

function Get-FileSha256([string]$Path) {
    return (
        Get-FileHash -LiteralPath $Path -Algorithm SHA256
    ).Hash.ToLowerInvariant()
}

function Write-AtomicJson([string]$Path, $Payload) {
    if (!(Test-PathInside $Path $ReloadStateRoot)) {
        throw "QMT reload receipt escapes protected state root"
    }
    $Parent = [System.IO.Path]::GetDirectoryName(
        [System.IO.Path]::GetFullPath($Path)
    )
    Assert-OrdinaryDirectory $Parent "QMT reload receipt directory"
    Assert-ProtectedPathOwner $Parent "QMT reload receipt directory"
    if (Test-Path -LiteralPath $Path) {
        Assert-OrdinaryFile $Path "existing QMT reload receipt"
        Assert-ProtectedPathOwner $Path "existing QMT reload receipt"
    }
    $Temporary = "$Path.$PID.$([Guid]::NewGuid().ToString('N')).tmp"
    [System.IO.File]::WriteAllText(
        $Temporary,
        ($Payload | ConvertTo-Json -Depth 12 -Compress),
        [System.Text.UTF8Encoding]::new($false)
    )
    Assert-OrdinaryFile $Temporary "temporary QMT reload receipt"
    Assert-ProtectedPathOwner $Temporary "temporary QMT reload receipt"
    Move-Item -LiteralPath $Temporary -Destination $Path -Force
    Assert-OrdinaryFile $Path "published QMT reload receipt"
    Assert-ProtectedPathOwner $Path "published QMT reload receipt"
}

function Test-QmtReleaseActivationPayload(
    $Payload,
    [string]$ExpectedBuildSha
) {
    $TopLevelFields = @(
        "mode", "status", "build_sha", "deployment_attempt_id",
        "activation_granted", "reason_code", "hold", "grant",
        "database_writes"
    )
    $HoldFields = @(
        "schema", "build_sha", "deployment_attempt_id", "hold_run_uid",
        "request_run_uid", "requested_at", "real_order", "hold_hash"
    )
    $GrantFields = @(
        "schema", "build_sha", "deployment_attempt_id", "grant_run_uid",
        "hold_run_uid", "hold_hash", "granted_at",
        "schema_cutover_verified", "real_order", "grant_hash"
    )
    $ExactFields = {
        param($Value, [string[]]$ExpectedFields)
        if (
            $null -eq $Value -or
            $Value -isnot [System.Management.Automation.PSCustomObject]
        ) {
            return $false
        }
        $ActualFields = @($Value.PSObject.Properties.Name)
        if ($ActualFields.Count -ne $ExpectedFields.Count) {
            return $false
        }
        foreach ($Field in $ExpectedFields) {
            if ($ActualFields -cnotcontains $Field) {
                return $false
            }
        }
        return $true
    }
    $ParseTimestamp = {
        param($Value)
        if (
            $Value -isnot [string] -or
            $Value -cnotmatch (
                "^[0-9]{4}-[0-9]{2}-[0-9]{2}T" +
                "[0-9]{2}:[0-9]{2}:[0-9]{2}" +
                "(?:[+-][0-9]{2}:[0-9]{2})?$"
            )
        ) {
            return $null
        }
        try {
            return [DateTimeOffset]::ParseExact(
                $Value,
                [string[]]@(
                    "yyyy-MM-dd'T'HH:mm:ss",
                    "yyyy-MM-dd'T'HH:mm:sszzz"
                ),
                [Globalization.CultureInfo]::InvariantCulture,
                [Globalization.DateTimeStyles]::None
            )
        }
        catch {
            return $null
        }
    }
    $CanonicalTimestamp = {
        param($Value)
        if ($Value -is [string]) {
            if ($null -eq (& $ParseTimestamp $Value)) {
                return ""
            }
            return $Value
        }
        if ($Value -is [DateTimeOffset]) {
            return $Value.ToString(
                "yyyy-MM-dd'T'HH:mm:sszzz",
                [Globalization.CultureInfo]::InvariantCulture
            )
        }
        if ($Value -is [DateTime]) {
            if ($Value.Kind -eq [DateTimeKind]::Unspecified) {
                return $Value.ToString(
                    "yyyy-MM-dd'T'HH:mm:ss",
                    [Globalization.CultureInfo]::InvariantCulture
                )
            }
            return ([DateTimeOffset]$Value).ToString(
                "yyyy-MM-dd'T'HH:mm:sszzz",
                [Globalization.CultureInfo]::InvariantCulture
            )
        }
        return ""
    }
    $CanonicalDigest = {
        param([System.Collections.IDictionary]$Unsigned)
        $Json = $Unsigned | ConvertTo-Json -Depth 4 -Compress
        $Bytes = [System.Text.Encoding]::UTF8.GetBytes($Json)
        $Hasher = [System.Security.Cryptography.SHA256]::Create()
        try {
            return (($Hasher.ComputeHash($Bytes) | ForEach-Object {
                $_.ToString("x2")
            }) -join "")
        }
        finally {
            $Hasher.Dispose()
        }
    }
    $ValidateHold = {
        param($Hold, [string]$BuildSha, [string]$AttemptId)
        if (!(& $ExactFields $Hold $HoldFields)) {
            return $false
        }
        foreach ($Field in @(
            "schema", "build_sha", "deployment_attempt_id", "hold_run_uid",
            "request_run_uid", "hold_hash"
        )) {
            if ($Hold.$Field -isnot [string]) {
                return $false
            }
        }
        $RequestedAtText = & $CanonicalTimestamp $Hold.requested_at
        $RequestedAt = & $ParseTimestamp $RequestedAtText
        if (
            $Hold.schema -cne `
                "probiga.qmt-windows-edge-release-quiescence.v1" -or
            $Hold.build_sha -cne $BuildSha -or
            $Hold.deployment_attempt_id -cne $AttemptId -or
            $Hold.hold_run_uid -cne "qmt-edge-hold-${AttemptId}" -or
            $Hold.request_run_uid -cne "qmt-edge-request-${BuildSha}" -or
            $Hold.real_order -isnot [bool] -or
            $Hold.real_order -ne $false -or
            $null -eq $RequestedAt -or
            $Hold.hold_hash -cnotmatch "^[0-9a-f]{64}$"
        ) {
            return $false
        }
        $Unsigned = [ordered]@{
            build_sha = $Hold.build_sha
            deployment_attempt_id = $Hold.deployment_attempt_id
            hold_run_uid = $Hold.hold_run_uid
            real_order = $Hold.real_order
            request_run_uid = $Hold.request_run_uid
            requested_at = $RequestedAtText
            schema = $Hold.schema
        }
        return $Hold.hold_hash -ceq (& $CanonicalDigest $Unsigned)
    }
    $ValidateGrant = {
        param($Grant, $Hold, [string]$BuildSha, [string]$AttemptId)
        if (!(& $ExactFields $Grant $GrantFields)) {
            return $false
        }
        foreach ($Field in @(
            "schema", "build_sha", "deployment_attempt_id", "grant_run_uid",
            "hold_run_uid", "hold_hash", "grant_hash"
        )) {
            if ($Grant.$Field -isnot [string]) {
                return $false
            }
        }
        $GrantedAtText = & $CanonicalTimestamp $Grant.granted_at
        $RequestedAtText = & $CanonicalTimestamp $Hold.requested_at
        $GrantedAt = & $ParseTimestamp $GrantedAtText
        $RequestedAt = & $ParseTimestamp $RequestedAtText
        if (
            $Grant.schema -cne `
                "probiga.qmt-windows-edge-release-activation.v1" -or
            $Grant.build_sha -cne $BuildSha -or
            $Grant.deployment_attempt_id -cne $AttemptId -or
            $Grant.grant_run_uid -cne "qmt-edge-grant-${AttemptId}" -or
            $Grant.hold_run_uid -cne $Hold.hold_run_uid -or
            $Grant.hold_hash -cne $Hold.hold_hash -or
            $Grant.schema_cutover_verified -isnot [bool] -or
            $Grant.schema_cutover_verified -ne $true -or
            $Grant.real_order -isnot [bool] -or
            $Grant.real_order -ne $false -or
            $null -eq $GrantedAt -or
            $null -eq $RequestedAt -or
            $GrantedAt -lt $RequestedAt -or
            $Grant.grant_hash -cnotmatch "^[0-9a-f]{64}$"
        ) {
            return $false
        }
        $Unsigned = [ordered]@{
            build_sha = $Grant.build_sha
            deployment_attempt_id = $Grant.deployment_attempt_id
            grant_run_uid = $Grant.grant_run_uid
            granted_at = $GrantedAtText
            hold_hash = $Grant.hold_hash
            hold_run_uid = $Grant.hold_run_uid
            real_order = $Grant.real_order
            schema = $Grant.schema
            schema_cutover_verified = $Grant.schema_cutover_verified
        }
        return $Grant.grant_hash -ceq (& $CanonicalDigest $Unsigned)
    }

    if (!(& $ExactFields $Payload $TopLevelFields)) {
        return ""
    }
    foreach ($Field in @(
        "mode", "status", "build_sha", "deployment_attempt_id", "reason_code"
    )) {
        if ($Payload.$Field -isnot [string]) {
            return ""
        }
    }
    $ExpectedBuild = $ExpectedBuildSha.Trim().ToLowerInvariant()
    if (
        $Payload.mode -cne "check-activation" -or
        $Payload.build_sha -cne $ExpectedBuild -or
        $Payload.activation_granted -isnot [bool] -or
        $Payload.database_writes -isnot [bool] -or
        $Payload.database_writes -ne $false
    ) {
        return ""
    }
    $AttemptId = $Payload.deployment_attempt_id
    $ValidAttempt = (
        $AttemptId -cmatch "^[0-9a-f]{32}$" -and
        $AttemptId -cne ("0" * 32)
    )
    if ($Payload.status -ceq "PENDING") {
        if (
            $Payload.activation_granted -ne $false -or
            $Payload.reason_code -cne `
                "QMT_EDGE_RELEASE_ACTIVATION_PENDING" -or
            $null -ne $Payload.grant
        ) {
            return ""
        }
        if ($null -eq $Payload.hold) {
            if ($AttemptId -ceq "") {
                return "PENDING"
            }
            return ""
        }
        if (
            $ValidAttempt -and
            (& $ValidateHold $Payload.hold $ExpectedBuild $AttemptId)
        ) {
            return "PENDING"
        }
        return ""
    }
    if (
        $Payload.status -ceq "READY" -and
        $Payload.activation_granted -eq $true -and
        $Payload.reason_code -ceq "" -and
        $ValidAttempt -and
        (& $ValidateHold $Payload.hold $ExpectedBuild $AttemptId) -and
        (& $ValidateGrant `
            $Payload.grant $Payload.hold $ExpectedBuild $AttemptId)
    ) {
        return "READY"
    }
    return ""
}

function Get-QmtReleaseActivation([string]$BuildSha) {
    $ActivationOutput = @()
    $ActivationExit = -1
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        # Windows PowerShell updates native exit status in the runspace-global
        # automatic variable, including when this helper runs in function scope.
        $global:LASTEXITCODE = -1
        try {
            $ActivationOutput = & $PythonExe -P $ReleaseBootstrap `
                --check-activation --expected-build-sha $BuildSha `
                --compact 2>&1
            $ActivationExit = $global:LASTEXITCODE
        }
        catch {
            $ActivationOutput = @($_)
            $ActivationExit = -1
        }
    }
    finally {
        $ErrorActionPreference = $PreviousPreference
    }
    try {
        $Payload = ($ActivationOutput -join "`n") |
            ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        throw "QMT release activation proof is malformed"
    }
    $ExpectedActivationBuild = $BuildSha.Trim().ToLowerInvariant()
    # Nested receipt/hash validation belongs to the Python checker.  Avoid
    # re-hashing timestamps after PowerShell has coerced the JSON values.
    $Ready = (
        $ActivationExit -eq 0 -and
        [string]$Payload.mode -ceq "check-activation" -and
        [string]$Payload.status -ceq "READY" -and
        [string]$Payload.build_sha -ceq $ExpectedActivationBuild -and
        $Payload.activation_granted -eq $true -and
        $Payload.database_writes -eq $false
    )
    if ($Ready) {
        return [pscustomobject]@{
            granted = $true
            payload = $Payload
        }
    }
    $Pending = (
        $ActivationExit -eq 4 -and
        [string]$Payload.mode -ceq "check-activation" -and
        [string]$Payload.status -ceq "PENDING" -and
        [string]$Payload.build_sha -ceq $ExpectedActivationBuild -and
        $Payload.activation_granted -eq $false -and
        $Payload.database_writes -eq $false
    )
    if ($Pending) {
        return [pscustomobject]@{
            granted = $false
            payload = $Payload
        }
    }
    throw "QMT release activation proof failed closed"
}

# The updater that initiated the first coordinated release may still be
# executing its old in-memory script after it fast-forwards this checkout.
# Gate this new reloader itself before Add-Type, QMT discovery, mutex/state
# creation, UI actions or artifact writes.  PreflightOnly remains the original
# read-only probe and deliberately does not require an activation grant.
if (!$PreflightOnly) {
    $env:PROBIGA_DEPLOYMENT_MODE = "production"
    if ($Root -ine $ExpectedRoot) {
        throw "QMT reload tool differs from its registered production root"
    }
    Assert-OrdinaryDirectory $ExpectedRoot "QMT registered production root"
    Assert-OrdinaryFile $PythonExe "QMT production Python"
    Assert-OrdinaryFile $ReleaseBootstrap "QMT release activation checker"
    $ActivationTopLevel = ((
        Invoke-Git @("rev-parse", "--show-toplevel")
    ) -join "").Trim()
    $ActivationOrigin = ((
        Invoke-Git @("remote", "get-url", "origin")
    ) -join "").Trim()
    $ActivationBranch = ((
        Invoke-Git @("symbolic-ref", "--short", "HEAD")
    ) -join "").Trim()
    $ActivationHead = ((
        Invoke-Git @("rev-parse", "HEAD")
    ) -join "").Trim().ToLowerInvariant()
    $ActivationDirty = ((
        Invoke-Git @("status", "--porcelain", "--untracked-files=normal")
    ) -join "`n").Trim()
    if (
        [System.IO.Path]::GetFullPath($ActivationTopLevel) -ine $ExpectedRoot -or
        $ActivationOrigin -ine $ExpectedOrigin -or
        $ActivationBranch -cne "main" -or
        $ActivationHead -cne $ExpectedBuild -or
        $ActivationDirty
    ) {
        throw "QMT activation gate checkout is not clean exact-main"
    }
    $Activation = Get-QmtReleaseActivation $ExpectedBuild
    if (!$Activation.granted) {
        [Console]::Out.WriteLine(
            ($Activation.payload | ConvertTo-Json -Depth 12 -Compress)
        )
        exit 4
    }
}

if (!$PreflightOnly -and -not ("ProBigAQmtReleaseWindow" -as [type])) {
    Add-Type @'
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

public static class ProBigAQmtReleaseWindow
{
    public delegate bool EnumWindowsProc(IntPtr handle, IntPtr state);

    [StructLayout(LayoutKind.Sequential)]
    public struct RECT
    {
        public int Left;
        public int Top;
        public int Right;
        public int Bottom;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct MONITORINFO
    {
        public int cbSize;
        public RECT rcMonitor;
        public RECT rcWork;
        public uint dwFlags;
    }

    [DllImport("user32.dll")]
    public static extern bool EnumWindows(
        EnumWindowsProc callback,
        IntPtr state
    );

    [DllImport("user32.dll")]
    public static extern uint GetWindowThreadProcessId(
        IntPtr handle,
        out uint processId
    );

    [DllImport("user32.dll", CharSet = CharSet.Unicode)]
    public static extern int GetWindowText(
        IntPtr handle,
        StringBuilder text,
        int count
    );

    [DllImport("user32.dll")]
    public static extern bool IsWindow(IntPtr handle);

    [DllImport("user32.dll")]
    public static extern bool IsWindowVisible(IntPtr handle);

    [DllImport("user32.dll")]
    public static extern bool IsWindowEnabled(IntPtr handle);

    [DllImport("user32.dll")]
    public static extern bool IsIconic(IntPtr handle);

    [DllImport("user32.dll")]
    public static extern bool GetWindowRect(IntPtr handle, out RECT rect);

    [DllImport("user32.dll")]
    public static extern IntPtr MonitorFromWindow(
        IntPtr handle,
        uint flags
    );

    [DllImport("user32.dll")]
    public static extern bool GetMonitorInfo(
        IntPtr monitor,
        ref MONITORINFO info
    );

    [DllImport("user32.dll")]
    public static extern bool ShowWindow(IntPtr handle, int command);

    [DllImport("user32.dll")]
    public static extern bool SetForegroundWindow(IntPtr handle);

    [DllImport("user32.dll")]
    public static extern IntPtr GetForegroundWindow();

    [DllImport("user32.dll")]
    public static extern bool SetCursorPos(int x, int y);

    [DllImport("user32.dll")]
    public static extern void mouse_event(
        uint flags,
        uint x,
        uint y,
        uint data,
        UIntPtr extraInfo
    );

    [StructLayout(LayoutKind.Sequential)]
    private struct BITMAPINFOHEADER
    {
        public uint biSize;
        public int biWidth;
        public int biHeight;
        public ushort biPlanes;
        public ushort biBitCount;
        public uint biCompression;
        public uint biSizeImage;
        public int biXPelsPerMeter;
        public int biYPelsPerMeter;
        public uint biClrUsed;
        public uint biClrImportant;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct RGBQUAD
    {
        public byte rgbBlue;
        public byte rgbGreen;
        public byte rgbRed;
        public byte rgbReserved;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct BITMAPINFO
    {
        public BITMAPINFOHEADER bmiHeader;
        public RGBQUAD bmiColors;
    }

    [DllImport("user32.dll")]
    private static extern IntPtr GetDC(IntPtr handle);

    [DllImport("user32.dll")]
    private static extern int ReleaseDC(IntPtr handle, IntPtr dc);

    [DllImport("gdi32.dll")]
    private static extern IntPtr CreateCompatibleDC(IntPtr dc);

    [DllImport("gdi32.dll")]
    private static extern bool DeleteDC(IntPtr dc);

    [DllImport("gdi32.dll")]
    private static extern IntPtr CreateDIBSection(
        IntPtr dc,
        ref BITMAPINFO info,
        uint usage,
        out IntPtr bits,
        IntPtr section,
        uint offset
    );

    [DllImport("gdi32.dll")]
    private static extern IntPtr SelectObject(IntPtr dc, IntPtr value);

    [DllImport("gdi32.dll")]
    private static extern bool DeleteObject(IntPtr value);

    [DllImport("gdi32.dll")]
    private static extern bool BitBlt(
        IntPtr target,
        int x,
        int y,
        int width,
        int height,
        IntPtr source,
        int sourceX,
        int sourceY,
        uint operation
    );

    private static int Luminance(byte[] pixels, int width, int x, int y)
    {
        int index = ((y * width) + x) * 4;
        int blue = pixels[index];
        int green = pixels[index + 1];
        int red = pixels[index + 2];
        return ((2126 * red) + (7152 * green) + (722 * blue)) / 10000;
    }

    public static int FindStrategyPaneLeft(
        int left,
        int top,
        int right,
        int bottom
    )
    {
        int width = right - left;
        int height = bottom - top;
        if (width < 900 || height < 500)
        {
            return -1;
        }
        IntPtr screen = GetDC(IntPtr.Zero);
        IntPtr memory = IntPtr.Zero;
        IntPtr bitmap = IntPtr.Zero;
        IntPtr previous = IntPtr.Zero;
        try
        {
            if (screen == IntPtr.Zero)
            {
                return -1;
            }
            memory = CreateCompatibleDC(screen);
            if (memory == IntPtr.Zero)
            {
                return -1;
            }
            BITMAPINFO info = new BITMAPINFO();
            info.bmiHeader.biSize = (uint)Marshal.SizeOf(
                typeof(BITMAPINFOHEADER)
            );
            info.bmiHeader.biWidth = width;
            info.bmiHeader.biHeight = -height;
            info.bmiHeader.biPlanes = 1;
            info.bmiHeader.biBitCount = 32;
            IntPtr bits;
            bitmap = CreateDIBSection(
                screen,
                ref info,
                0,
                out bits,
                IntPtr.Zero,
                0
            );
            if (bitmap == IntPtr.Zero || bits == IntPtr.Zero)
            {
                return -1;
            }
            previous = SelectObject(memory, bitmap);
            if (previous == IntPtr.Zero)
            {
                return -1;
            }
            if (!BitBlt(
                memory,
                0,
                0,
                width,
                height,
                screen,
                left,
                top,
                0x40CC0020
            ))
            {
                return -1;
            }
            byte[] pixels = new byte[width * height * 4];
            Marshal.Copy(bits, pixels, 0, pixels.Length);
            int[] rows = new int[] { 100, 180, 300 };
            bool inRun = false;
            int runCount = 0;
            int paneLeft = -1;
            int limit = Math.Min(width - 100, 1500);
            for (int x = 250; x < limit; x += 2)
            {
                int dark = 0;
                int light = 0;
                foreach (int row in rows)
                {
                    dark += Luminance(pixels, width, x - 24, row);
                    light += Luminance(pixels, width, x + 24, row);
                }
                bool transition = dark / rows.Length < 80 &&
                    light / rows.Length > 220;
                if (transition && !inRun)
                {
                    runCount += 1;
                    paneLeft = x + 24;
                    inRun = true;
                }
                else if (!transition)
                {
                    inRun = false;
                }
            }
            if (runCount == 1)
            {
                return left + paneLeft;
            }
            int fullWidthLight = 0;
            int[] fullWidthColumns = new int[] { 60, 250, 400, 600 };
            int[] fullWidthRows = new int[] { 180, 300 };
            foreach (int column in fullWidthColumns)
            {
                foreach (int row in fullWidthRows)
                {
                    fullWidthLight += Luminance(
                        pixels,
                        width,
                        column,
                        row
                    );
                }
            }
            int fullWidthSamples = fullWidthColumns.Length *
                fullWidthRows.Length;
            if (runCount == 0 && fullWidthLight / fullWidthSamples > 220)
            {
                // QMT's dedicated model-research list starts immediately to
                // the right of the 51-pixel navigation rail.
                return left + 51;
            }
            return -1;
        }
        finally
        {
            if (previous != IntPtr.Zero && memory != IntPtr.Zero)
            {
                SelectObject(memory, previous);
            }
            if (bitmap != IntPtr.Zero)
            {
                DeleteObject(bitmap);
            }
            if (memory != IntPtr.Zero)
            {
                DeleteDC(memory);
            }
            if (screen != IntPtr.Zero)
            {
                ReleaseDC(IntPtr.Zero, screen);
            }
        }
    }

    [DllImport("user32.dll")]
    public static extern bool PostMessage(
        IntPtr handle,
        uint message,
        IntPtr word,
        IntPtr value
    );

    public static string Title(IntPtr handle)
    {
        StringBuilder text = new StringBuilder(512);
        GetWindowText(handle, text, text.Capacity);
        return text.ToString();
    }

    public static uint Owner(IntPtr handle)
    {
        uint processId;
        GetWindowThreadProcessId(handle, out processId);
        return processId;
    }

    public static List<IntPtr> ExactWindows(uint processId, string title)
    {
        List<IntPtr> result = new List<IntPtr>();
        EnumWindows(delegate(IntPtr handle, IntPtr state)
        {
            if (Owner(handle) == processId && Title(handle) == title)
            {
                result.Add(handle);
            }
            return true;
        }, IntPtr.Zero);
        return result;
    }

    public static List<IntPtr> TitleSuffixWindows(
        uint processId,
        string suffix
    )
    {
        List<IntPtr> result = new List<IntPtr>();
        EnumWindows(delegate(IntPtr handle, IntPtr state)
        {
            string title = Title(handle);
            if (
                Owner(handle) == processId &&
                title.EndsWith(suffix, StringComparison.Ordinal)
            )
            {
                result.Add(handle);
            }
            return true;
        }, IntPtr.Zero);
        return result;
    }

    public static List<IntPtr> VisibleTitledWindows(uint processId)
    {
        List<IntPtr> result = new List<IntPtr>();
        EnumWindows(delegate(IntPtr handle, IntPtr state)
        {
            if (
                Owner(handle) == processId &&
                IsWindowVisible(handle) &&
                Title(handle).Length > 0
            )
            {
                result.Add(handle);
            }
            return true;
        }, IntPtr.Zero);
        return result;
    }
}
'@
}

function Get-ExactEditorWindows {
    return @(
        [ProBigAQmtReleaseWindow]::ExactWindows(
            [uint32]$QmtClient.Id,
            $EditorTitle
        )
    )
}

function Assert-NoOtherStrategyEditors {
    $Editors = @(
        [ProBigAQmtReleaseWindow]::TitleSuffixWindows(
            [uint32]$QmtClient.Id,
            $EditorSuffix
        )
    )
    $Other = @(
        $Editors | Where-Object {
            [ProBigAQmtReleaseWindow]::Title($_) -cne $EditorTitle
        }
    )
    if ($Other.Count -ne 0) {
        Throw-NeedsUserAction (
            "another QMT strategy editor is open; close it before release"
        )
    }
    $Exact = @(
        $Editors | Where-Object {
            [ProBigAQmtReleaseWindow]::Title($_) -ceq $EditorTitle
        }
    )
    if ($Exact.Count -gt 1) {
        Throw-NeedsUserAction "target QMT strategy editor is not unique"
    }
}

function Assert-NoUnexpectedVisibleQmtWindow {
    $Visible = @(
        [ProBigAQmtReleaseWindow]::VisibleTitledWindows(
            [uint32]$QmtClient.Id
        )
    )
    foreach ($Handle in $Visible) {
        $Title = [ProBigAQmtReleaseWindow]::Title($Handle)
        if ($Title -cnotin @($QmtMainTitle, $EditorTitle)) {
            Throw-NeedsUserAction (
                "QMT has a login, CAPTCHA, confirmation, or other modal window"
            )
        }
    }
}

function Get-Heartbeat {
    if (!(Test-Path -LiteralPath $HeartbeatPath -PathType Leaf)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $HeartbeatPath -Raw |
            ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Get-HeartbeatProperty($Heartbeat, [string]$Name) {
    if ($null -eq $Heartbeat -or [string]::IsNullOrWhiteSpace($Name)) {
        return $null
    }
    $Property = $Heartbeat.PSObject.Properties[$Name]
    if ($null -eq $Property) {
        return $null
    }
    return $Property.Value
}

function Test-HeartbeatProperty($Heartbeat, [string]$Name) {
    if ($null -eq $Heartbeat -or [string]::IsNullOrWhiteSpace($Name)) {
        return $false
    }
    return $null -ne $Heartbeat.PSObject.Properties[$Name]
}

function Test-HeartbeatProperties($Heartbeat, [string[]]$Names) {
    foreach ($Name in $Names) {
        if (!(Test-HeartbeatProperty $Heartbeat $Name)) {
            return $false
        }
    }
    return $true
}

function Assert-QmtInteractiveClientReady(
    [object[]]$QmtClients,
    [int]$CurrentSession
) {
    if ($QmtClients.Count -ne 1) {
        Throw-NeedsUserAction `
            "exactly one QMT client must be running" `
            "QMT_CLIENT_COUNT_INVALID"
    }
    $QmtClient = $QmtClients[0]
    if (
        $QmtClient.MainWindowHandle -eq [IntPtr]::Zero -or
        [string]::IsNullOrWhiteSpace([string]$QmtClient.Path)
    ) {
        Throw-NeedsUserAction `
            "the QMT interactive client window is unavailable" `
            "QMT_INTERACTIVE_WINDOW_UNAVAILABLE"
    }
    if ([int]$QmtClient.SessionId -ne [int]$CurrentSession) {
        Throw-NeedsUserAction `
            "QMT is not in the updater interactive session" `
            "QMT_SESSION_MISMATCH"
    }
    $QmtMainTitle = [string]$QmtClient.MainWindowTitle
    if ($QmtMainTitle -notmatch "^\s*\d+\s*-\s*.+QMT") {
        Throw-NeedsUserAction `
            "QMT login or broker authentication is required" `
            "QMT_LOGIN_REQUIRED"
    }
    return $QmtClient
}

function Assert-QmtClientHeartbeatReady(
    $Heartbeat,
    [int]$ClientPid,
    [int]$MaxAgeSeconds,
    [double]$NowUnixSeconds = [double]::NaN
) {
    if ($null -eq $Heartbeat) {
        Throw-NeedsUserAction `
            "the QMT strategy heartbeat is missing or malformed" `
            "QMT_HEARTBEAT_MISSING_OR_MALFORMED"
    }
    if (!(Test-HeartbeatProperties $Heartbeat @(
        "status", "source", "pid", "updated_ts"
    ))) {
        Throw-NeedsUserAction `
            "the QMT strategy heartbeat contract is incomplete" `
            "QMT_HEARTBEAT_CONTRACT_INVALID"
    }
    try {
        $HeartbeatPid = [int](Get-HeartbeatProperty $Heartbeat "pid")
    }
    catch {
        Throw-NeedsUserAction `
            "the QMT strategy heartbeat PID is invalid" `
            "QMT_HEARTBEAT_PID_INVALID"
    }
    if ($HeartbeatPid -ne $ClientPid) {
        Throw-NeedsUserAction `
            "the QMT strategy heartbeat belongs to a different client process" `
            "QMT_HEARTBEAT_PID_MISMATCH"
    }
    if (
        [string](Get-HeartbeatProperty $Heartbeat "source") -cne `
            "gj_big_qmt_inner" -or
        [string](Get-HeartbeatProperty $Heartbeat "status") -cnotin @(
            "running", "busy"
        )
    ) {
        Throw-NeedsUserAction `
            "the exact QMT strategy is not running" `
            "QMT_STRATEGY_NOT_RUNNING"
    }
    try {
        $UpdatedTs = [double](Get-HeartbeatProperty $Heartbeat "updated_ts")
    }
    catch {
        Throw-NeedsUserAction `
            "the QMT strategy heartbeat timestamp is invalid" `
            "QMT_HEARTBEAT_TIMESTAMP_INVALID"
    }
    if (
        [double]::IsNaN($UpdatedTs) -or
        [double]::IsInfinity($UpdatedTs) -or
        $UpdatedTs -le 0
    ) {
        Throw-NeedsUserAction `
            "the QMT strategy heartbeat timestamp is invalid" `
            "QMT_HEARTBEAT_TIMESTAMP_INVALID"
    }
    if ([double]::IsNaN($NowUnixSeconds)) {
        $NowUnixSeconds = (
            [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
        )
    }
    $HeartbeatAgeSeconds = $NowUnixSeconds - $UpdatedTs
    if (
        [double]::IsNaN($HeartbeatAgeSeconds) -or
        [double]::IsInfinity($HeartbeatAgeSeconds) -or
        $HeartbeatAgeSeconds -lt -5.0 -or
        $HeartbeatAgeSeconds -gt [double]$MaxAgeSeconds
    ) {
        Throw-NeedsUserAction `
            "the QMT strategy heartbeat is stale" `
            "QMT_HEARTBEAT_STALE"
    }
    return [Math]::Round($HeartbeatAgeSeconds, 3)
}

function Assert-QmtColdStartEvidence($Heartbeat, $Client) {
    if (!(Test-HeartbeatProperties $Heartbeat @(
        "status", "source", "pid", "updated_ts"
    ))) {
        Throw-NeedsUserAction `
            "the prior QMT heartbeat cannot prove a client restart" `
            "QMT_COLD_START_EVIDENCE_INVALID"
    }
    try {
        $HeartbeatPid = [int](Get-HeartbeatProperty $Heartbeat "pid")
        $HeartbeatUpdatedTs = [double](
            Get-HeartbeatProperty $Heartbeat "updated_ts"
        )
        $ClientStartedTs = (
            [DateTimeOffset]$Client.StartTime.ToUniversalTime()
        ).ToUnixTimeMilliseconds() / 1000.0
    }
    catch {
        Throw-NeedsUserAction `
            "the prior QMT heartbeat cannot prove a client restart" `
            "QMT_COLD_START_EVIDENCE_INVALID"
    }
    if (
        [string](Get-HeartbeatProperty $Heartbeat "source") -cne `
            "gj_big_qmt_inner" -or
        [string](Get-HeartbeatProperty $Heartbeat "status") -cnotin @(
            "running", "busy"
        ) -or
        $HeartbeatPid -le 0 -or
        $HeartbeatPid -eq [int]$Client.Id -or
        [double]::IsNaN($HeartbeatUpdatedTs) -or
        [double]::IsInfinity($HeartbeatUpdatedTs) -or
        $HeartbeatUpdatedTs -le 0 -or
        $HeartbeatUpdatedTs -gt ($ClientStartedTs + 5.0)
    ) {
        Throw-NeedsUserAction `
            "the prior QMT heartbeat does not predate the current client" `
            "QMT_COLD_START_EVIDENCE_INVALID"
    }
    return [pscustomobject]@{
        heartbeat_pid = $HeartbeatPid
        heartbeat_updated_ts = $HeartbeatUpdatedTs
        qmt_client_started_ts = $ClientStartedTs
    }
}

function Get-StrategyIdentityHeartbeatPropertyNames {
    return @(
        "strategy_release_protocol",
        "strategy_identity_protocol",
        "strategy_identity_frozen",
        "strategy_identity_status",
        "strategy_build_sha",
        "strategy_git_blob",
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256"
    )
}

function Test-SameHeartbeatPropertyShape($Left, $Right) {
    if ($null -eq $Left -or $null -eq $Right) {
        return $false
    }
    $LeftNames = @(
        $Left.PSObject.Properties | ForEach-Object { $_.Name } | Sort-Object
    )
    $RightNames = @(
        $Right.PSObject.Properties | ForEach-Object { $_.Name } | Sort-Object
    )
    if ($LeftNames.Count -ne $RightNames.Count) {
        return $false
    }
    for ($Index = 0; $Index -lt $LeftNames.Count; $Index += 1) {
        if ([string]$LeftNames[$Index] -cne [string]$RightNames[$Index]) {
            return $false
        }
    }
    return $true
}

function Test-RunningHeartbeat($Heartbeat) {
    if (!(Test-HeartbeatProperties $Heartbeat @("status", "source", "pid"))) {
        return $false
    }
    return (
        [string](Get-HeartbeatProperty $Heartbeat "status") -in @(
            "running", "busy"
        ) -and
        [string](Get-HeartbeatProperty $Heartbeat "source") -eq `
            "gj_big_qmt_inner" -and
        [int](Get-HeartbeatProperty $Heartbeat "pid") -eq [int]$QmtClient.Id
    )
}

function Test-ExpectedReleaseHeartbeat($Heartbeat, $Release) {
    if (!(Test-RunningHeartbeat $Heartbeat)) {
        return $false
    }
    $Required = @(
        "bridge_version",
        "direct_acquisition_model_sha256",
        "direct_acquisition_status"
    ) + @(
        Get-StrategyIdentityHeartbeatPropertyNames
    )
    if (!(Test-HeartbeatProperties $Heartbeat $Required)) {
        return $false
    }
    return (
        [string](Get-HeartbeatProperty $Heartbeat "bridge_version") -eq `
            "bigqmt_inner_v2" -and
        [string](Get-HeartbeatProperty `
            $Heartbeat "strategy_release_protocol") -eq $ReleaseProtocol -and
        [string](Get-HeartbeatProperty `
            $Heartbeat "strategy_identity_protocol") -eq $IdentityProtocol -and
        (Get-HeartbeatProperty $Heartbeat "strategy_identity_frozen") -eq `
            $true -and
        [string](Get-HeartbeatProperty `
            $Heartbeat "strategy_identity_status") -eq "BOUND" -and
        [string](Get-HeartbeatProperty `
            $Heartbeat "strategy_build_sha") -ceq $ExpectedBuild -and
        [string](Get-HeartbeatProperty `
            $Heartbeat "strategy_git_blob") -ceq `
            [string]$Release.strategy_git_blob -and
        [string](Get-HeartbeatProperty `
            $Heartbeat "strategy_source_sha256") -ceq `
            [string]$Release.strategy_source_sha256 -and
        [string](Get-HeartbeatProperty `
            $Heartbeat "strategy_artifact_sha256") -ceq `
            [string]$Release.strategy_artifact_sha256 -and
        [string](Get-HeartbeatProperty `
            $Heartbeat "strategy_loaded_identity_sha256") -ceq `
            [string]$Release.strategy_loaded_identity_sha256 -and
        [string](Get-HeartbeatProperty `
            $Heartbeat "direct_acquisition_model_sha256") -cmatch `
            "^[0-9a-f]{64}$" -and
        [string](Get-HeartbeatProperty `
            $Heartbeat "direct_acquisition_status") -in @(
                "idle", "busy", "awaiting_commit"
            )
    )
}

function Test-OriginalReleaseHeartbeat($Heartbeat) {
    if (!(Test-RunningHeartbeat $Heartbeat)) {
        return $false
    }
    if (!(Test-RunningHeartbeat $PreviousHeartbeat)) {
        return $false
    }
    $IdentityNames = @(Get-StrategyIdentityHeartbeatPropertyNames)
    $PreviousHasIdentityStatus = Test-HeartbeatProperty `
        $PreviousHeartbeat "strategy_identity_status"
    if (!$PreviousHasIdentityStatus) {
        foreach ($Name in $IdentityNames) {
            if (
                (Test-HeartbeatProperty $PreviousHeartbeat $Name) -or
                (Test-HeartbeatProperty $Heartbeat $Name)
            ) {
                return $false
            }
        }
        $LegacyStable = @("schema_version", "bridge_version", "source", "pid")
        if (
            !(Test-HeartbeatProperties $PreviousHeartbeat $LegacyStable) -or
            !(Test-HeartbeatProperties $Heartbeat $LegacyStable) -or
            !(Test-SameHeartbeatPropertyShape $PreviousHeartbeat $Heartbeat)
        ) {
            return $false
        }
        return (
            [int](Get-HeartbeatProperty $Heartbeat "schema_version") -eq 2 -and
            [int](Get-HeartbeatProperty $PreviousHeartbeat "schema_version") `
                -eq 2 -and
            [string](Get-HeartbeatProperty $Heartbeat "bridge_version") `
                -ceq "bigqmt_inner_v2" -and
            [string](Get-HeartbeatProperty $Heartbeat "bridge_version") `
                -ceq [string](Get-HeartbeatProperty `
                    $PreviousHeartbeat "bridge_version") -and
            [string](Get-HeartbeatProperty $Heartbeat "source") -ceq `
                [string](Get-HeartbeatProperty $PreviousHeartbeat "source") -and
            [int](Get-HeartbeatProperty $Heartbeat "pid") -eq `
                [int](Get-HeartbeatProperty $PreviousHeartbeat "pid")
        )
    }
    if (
        [string](Get-HeartbeatProperty `
            $PreviousHeartbeat "strategy_identity_status") -cne "BOUND" -or
        !(Test-HeartbeatProperties $PreviousHeartbeat $IdentityNames) -or
        !(Test-HeartbeatProperties $Heartbeat $IdentityNames)
    ) {
        return $false
    }
    foreach ($Name in $IdentityNames) {
        if (
            [string](Get-HeartbeatProperty $Heartbeat $Name) -cne `
                [string](Get-HeartbeatProperty $PreviousHeartbeat $Name)
        ) {
            return $false
        }
    }
    return (
        (Get-HeartbeatProperty $Heartbeat "strategy_identity_frozen") -eq `
            $true -and
        (Get-HeartbeatProperty `
            $PreviousHeartbeat "strategy_identity_frozen") -eq $true
    )
}

function Wait-ForHeartbeat([scriptblock]$Predicate, [int]$TimeoutSeconds) {
    $Deadline = (Get-Date).AddSeconds([Math]::Max(1, $TimeoutSeconds))
    do {
        Start-Sleep -Milliseconds 250
        $Heartbeat = Get-Heartbeat
        if (& $Predicate $Heartbeat) {
            return $Heartbeat
        }
    } while ((Get-Date) -lt $Deadline)
    return $null
}

function Invoke-ExactWindowClick(
    [IntPtr]$Handle,
    [string]$ExpectedTitle,
    [double]$XRatio,
    [double]$YRatio,
    [switch]$UseMonitorWorkArea
) {
    if (
        ![ProBigAQmtReleaseWindow]::IsWindow($Handle) -or
        [ProBigAQmtReleaseWindow]::Owner($Handle) -ne [uint32]$QmtClient.Id -or
        [ProBigAQmtReleaseWindow]::Title($Handle) -cne $ExpectedTitle
    ) {
        throw "QMT click target identity changed"
    }
    $Rect = New-Object ProBigAQmtReleaseWindow+RECT
    if (![ProBigAQmtReleaseWindow]::GetWindowRect($Handle, [ref]$Rect)) {
        throw "QMT click target bounds are unavailable"
    }
    $ClickLeft = $Rect.Left
    $ClickTop = $Rect.Top
    $ClickRight = $Rect.Right
    $ClickBottom = $Rect.Bottom
    if ($UseMonitorWorkArea) {
        # The QMT main wrapper may cover multiple monitors.  Navigation is
        # anchored to the owning monitor's work area; normalizing against the
        # wrapper union can otherwise click another QMT command or monitor.
        $Monitor = [ProBigAQmtReleaseWindow]::MonitorFromWindow($Handle, 2)
        if ($Monitor -eq [IntPtr]::Zero) {
            throw "QMT navigation monitor is unavailable"
        }
        $MonitorInfo = New-Object ProBigAQmtReleaseWindow+MONITORINFO
        $MonitorInfo.cbSize = [Runtime.InteropServices.Marshal]::SizeOf(
            $MonitorInfo
        )
        if (
            ![ProBigAQmtReleaseWindow]::GetMonitorInfo(
                $Monitor,
                [ref]$MonitorInfo
            )
        ) {
            throw "QMT navigation monitor bounds are unavailable"
        }
        $ClickLeft = $MonitorInfo.rcWork.Left
        $ClickTop = $MonitorInfo.rcWork.Top
        $ClickRight = $MonitorInfo.rcWork.Right
        $ClickBottom = $MonitorInfo.rcWork.Bottom
        $BoundsTolerance = 8
        if (
            $ClickLeft -lt $Rect.Left - $BoundsTolerance -or
            $ClickTop -lt $Rect.Top - $BoundsTolerance -or
            $ClickRight -gt $Rect.Right + $BoundsTolerance -or
            $ClickBottom -gt $Rect.Bottom + $BoundsTolerance
        ) {
            throw "QMT main window does not cover its navigation monitor"
        }
    }
    $Width = $ClickRight - $ClickLeft
    $Height = $ClickBottom - $ClickTop
    if ($Width -lt 900 -or $Height -lt 500) {
        throw "QMT click target bounds are unsafe"
    }
    [ProBigAQmtReleaseWindow]::SetForegroundWindow($Handle) | Out-Null
    Start-Sleep -Milliseconds 200
    [ProBigAQmtReleaseWindow]::SetCursorPos(
        $ClickLeft + [int]($Width * $XRatio),
        $ClickTop + [int]($Height * $YRatio)
    ) | Out-Null
    [ProBigAQmtReleaseWindow]::mouse_event(
        0x0002, 0, 0, 0, [UIntPtr]::Zero
    )
    [ProBigAQmtReleaseWindow]::mouse_event(
        0x0004, 0, 0, 0, [UIntPtr]::Zero
    )
}

function Show-QmtMainWindow {
    [ProBigAQmtReleaseWindow]::ShowWindow($QmtMainHandle, 9) | Out-Null
    [ProBigAQmtReleaseWindow]::SetForegroundWindow($QmtMainHandle) | Out-Null
    Start-Sleep -Milliseconds 500
}

function Get-QmtMainWorkArea {
    $Rect = New-Object ProBigAQmtReleaseWindow+RECT
    if (
        ![ProBigAQmtReleaseWindow]::GetWindowRect(
            $QmtMainHandle,
            [ref]$Rect
        )
    ) {
        throw "QMT main window bounds are unavailable"
    }
    $Monitor = [ProBigAQmtReleaseWindow]::MonitorFromWindow(
        $QmtMainHandle,
        2
    )
    if ($Monitor -eq [IntPtr]::Zero) {
        throw "QMT navigation monitor is unavailable"
    }
    $MonitorInfo = New-Object ProBigAQmtReleaseWindow+MONITORINFO
    $MonitorInfo.cbSize = [Runtime.InteropServices.Marshal]::SizeOf(
        $MonitorInfo
    )
    if (
        ![ProBigAQmtReleaseWindow]::GetMonitorInfo(
            $Monitor,
            [ref]$MonitorInfo
        )
    ) {
        throw "QMT navigation monitor bounds are unavailable"
    }
    $BoundsTolerance = 8
    if (
        $MonitorInfo.rcWork.Left -lt $Rect.Left - $BoundsTolerance -or
        $MonitorInfo.rcWork.Top -lt $Rect.Top - $BoundsTolerance -or
        $MonitorInfo.rcWork.Right -gt $Rect.Right + $BoundsTolerance -or
        $MonitorInfo.rcWork.Bottom -gt $Rect.Bottom + $BoundsTolerance
    ) {
        throw "QMT main window does not cover its navigation monitor"
    }
    return [pscustomobject]@{
        Left = [int]$MonitorInfo.rcWork.Left
        Top = [int]$MonitorInfo.rcWork.Top
        Right = [int]$MonitorInfo.rcWork.Right
        Bottom = [int]$MonitorInfo.rcWork.Bottom
    }
}

function Invoke-ExactScreenPointClick(
    [IntPtr]$Handle,
    [string]$ExpectedTitle,
    [int]$X,
    [int]$Y
) {
    if (
        ![ProBigAQmtReleaseWindow]::IsWindow($Handle) -or
        [ProBigAQmtReleaseWindow]::Owner($Handle) -ne [uint32]$QmtClient.Id -or
        [ProBigAQmtReleaseWindow]::Title($Handle) -cne $ExpectedTitle
    ) {
        throw "QMT point-click target identity changed"
    }
    $Rect = New-Object ProBigAQmtReleaseWindow+RECT
    if (![ProBigAQmtReleaseWindow]::GetWindowRect($Handle, [ref]$Rect)) {
        throw "QMT point-click target bounds are unavailable"
    }
    if (
        $X -lt $Rect.Left -or
        $X -ge $Rect.Right -or
        $Y -lt $Rect.Top -or
        $Y -ge $Rect.Bottom
    ) {
        throw "QMT point click escapes the exact target window"
    }
    [ProBigAQmtReleaseWindow]::SetForegroundWindow($Handle) | Out-Null
    Start-Sleep -Milliseconds 200
    [ProBigAQmtReleaseWindow]::SetCursorPos($X, $Y) | Out-Null
    [ProBigAQmtReleaseWindow]::mouse_event(
        0x0002, 0, 0, 0, [UIntPtr]::Zero
    )
    [ProBigAQmtReleaseWindow]::mouse_event(
        0x0004, 0, 0, 0, [UIntPtr]::Zero
    )
}

function Get-QmtStrategyPaneLayout {
    Assert-NoUnexpectedVisibleQmtWindow
    $Work = Get-QmtMainWorkArea
    $PaneLeft = [ProBigAQmtReleaseWindow]::FindStrategyPaneLeft(
        $Work.Left,
        $Work.Top,
        $Work.Right,
        $Work.Bottom
    )
    $RelativeLeft = $PaneLeft - $Work.Left
    $FullWidthList = $RelativeLeft -ge 45 -and $RelativeLeft -le 60
    $EmbeddedList = $RelativeLeft -ge 400 -and $RelativeLeft -le 1000
    if (!$FullWidthList -and !$EmbeddedList) {
        Throw-NeedsUserAction (
            "the visible QMT model-research strategy pane is not unique"
        )
    }
    return [pscustomobject]@{
        SearchX = [int]($PaneLeft + 70)
        SearchY = [int]($Work.Top + 67)
        EditX = [int]($PaneLeft + 322)
        EditY = [int]($Work.Top + 137)
    }
}

function Open-ExactStrategyEditor {
    Assert-NoOtherStrategyEditors
    $Existing = @(Get-ExactEditorWindows)
    if ($Existing.Count -eq 1) {
        return $Existing[0]
    }

    Add-Type -AssemblyName System.Windows.Forms
    Show-QmtMainWindow
    # QMT 2.1.19 leaves the client on a model backtest page after an editor
    # closes.  This exact location is the read-only "return to home" link.
    # The old 0.056/0.039 point lands on the account badge in 2.1.19 and opens
    # the modal "About QMT", which prevents unattended release recovery.
    Invoke-ExactWindowClick `
        $QmtMainHandle `
        $QmtMainTitle `
        0.107 `
        0.077 `
        -UseMonitorWorkArea
    Start-Sleep -Milliseconds 1200
    # The model-research header tracks the full QMT wrapper, while the search
    # pane is anchored to the owning monitor. This normalizes spanning layouts.
    Invoke-ExactWindowClick $QmtMainHandle $QmtMainTitle 0.470 0.015
    Start-Sleep -Milliseconds 1000
    $Layout = Get-QmtStrategyPaneLayout
    Invoke-ExactScreenPointClick `
        $QmtMainHandle `
        $QmtMainTitle `
        $Layout.SearchX `
        $Layout.SearchY
    [System.Windows.Forms.SendKeys]::SendWait("^a")
    [System.Windows.Forms.SendKeys]::SendWait($StrategyName)
    [System.Windows.Forms.SendKeys]::SendWait("{ENTER}")
    Start-Sleep -Milliseconds 1000
    Invoke-ExactScreenPointClick `
        $QmtMainHandle `
        $QmtMainTitle `
        $Layout.EditX `
        $Layout.EditY
    $Deadline = (Get-Date).AddSeconds(8)
    do {
        Start-Sleep -Milliseconds 250
        Assert-NoOtherStrategyEditors
        $Opened = @(Get-ExactEditorWindows)
    } while ($Opened.Count -ne 1 -and (Get-Date) -lt $Deadline)
    if ($Opened.Count -eq 1) {
        return $Opened[0]
    }
    Throw-NeedsUserAction (
        "the exact QMT strategy editor could not be opened safely"
    )
}

function Stop-ExactStrategy([IntPtr]$Editor) {
    $Before = Get-Heartbeat
    if ([string](Get-HeartbeatProperty $Before "status") -eq "stopped") {
        return $Before
    }
    if (!(Test-RunningHeartbeat $Before)) {
        throw "target QMT strategy heartbeat is not running before stop"
    }
    for ($Attempt = 0; $Attempt -lt 2; $Attempt += 1) {
        Show-QmtMainWindow
        Invoke-ExactWindowClick $Editor $EditorTitle 0.477 0.154
        $Stopped = Wait-ForHeartbeat {
            param($Heartbeat)
            return (
                $Heartbeat -and
                [string](Get-HeartbeatProperty $Heartbeat "status") -eq `
                    "stopped" -and
                [int](Get-HeartbeatProperty $Heartbeat "pid") -eq `
                    [int]$QmtClient.Id -and
                [string](Get-HeartbeatProperty $Heartbeat "updated_at") `
                    -cne [string](Get-HeartbeatProperty $Before "updated_at")
            )
        } $StopTimeoutSeconds
        if ($Stopped) {
            return $Stopped
        }
    }
    throw "exact QMT strategy stop control did not produce a stopped heartbeat"
}

function Close-ExactStrategyEditor([IntPtr]$Editor) {
    if (
        [ProBigAQmtReleaseWindow]::Owner($Editor) -ne [uint32]$QmtClient.Id -or
        [ProBigAQmtReleaseWindow]::Title($Editor) -cne $EditorTitle
    ) {
        throw "QMT editor close target identity changed"
    }
    if (![ProBigAQmtReleaseWindow]::PostMessage(
        $Editor, 0x0010, [IntPtr]::Zero, [IntPtr]::Zero
    )) {
        throw "QMT editor close message was rejected"
    }
    $Deadline = (Get-Date).AddSeconds(10)
    do {
        Start-Sleep -Milliseconds 250
        $Remaining = @(Get-ExactEditorWindows)
    } while ($Remaining.Count -ne 0 -and (Get-Date) -lt $Deadline)
    if ($Remaining.Count -ne 0) {
        throw "exact QMT strategy editor did not close"
    }
    Assert-NoUnexpectedVisibleQmtWindow
}

function Start-ExactStrategy(
    [IntPtr]$Editor,
    [scriptblock]$HeartbeatPredicate
) {
    # Starting the bridge may service an already queued read request. Track
    # that conservatively so a later failure receipt never claims zero QMT
    # calls after run control was attempted.
    $script:QmtCallsAttempted = $true
    for ($Attempt = 0; $Attempt -lt 3; $Attempt += 1) {
        Show-QmtMainWindow
        # QMT 2.1.19's run action is the second toolbar command.  The click
        # is accepted only on the exact, uniquely titled target editor and is
        # subsequently proven by the model's own in-process heartbeat.
        Invoke-ExactWindowClick $Editor $EditorTitle 0.339 0.151
        $Running = Wait-ForHeartbeat $HeartbeatPredicate $StartTimeoutSeconds
        if ($Running) {
            return $Running
        }
    }
    throw "exact QMT strategy run control did not produce its expected heartbeat"
}

function Get-InstalledStrategyAliases {
    return @(
        Get-ChildItem -LiteralPath $QmtPythonRoot -File -Force |
            Where-Object {
                $_.Name.ToLowerInvariant() -eq `
                    "probiga_big_qmt_bridge.py"
            } |
            Sort-Object FullName
    )
}

function Test-ExactPropertySet($Payload, [string[]]$ExpectedNames) {
    if ($null -eq $Payload) {
        return $false
    }
    $Actual = @(
        $Payload.PSObject.Properties |
            ForEach-Object { [string]$_.Name } |
            Sort-Object
    )
    $Expected = @($ExpectedNames | Sort-Object -Unique)
    if ($Actual.Count -ne $Expected.Count) {
        return $false
    }
    for ($Index = 0; $Index -lt $Expected.Count; $Index += 1) {
        if ($Actual[$Index] -cne $Expected[$Index]) {
            return $false
        }
    }
    return $true
}

function Assert-StrictFlatJsonKeys(
    [string]$RawJson,
    [string[]]$ExpectedNames
) {
    if ([string]::IsNullOrWhiteSpace($RawJson)) {
        throw "QMT recovery state JSON is empty"
    }
    $Matches = [regex]::Matches(
        $RawJson,
        '(?<!\\)"(?<name>[A-Za-z0-9_]+)"\s*:',
        [System.Text.RegularExpressions.RegexOptions]::CultureInvariant
    )
    $ActualNames = @($Matches | ForEach-Object { $_.Groups["name"].Value })
    $Expected = @($ExpectedNames | Sort-Object -Unique)
    if ($ActualNames.Count -ne $Expected.Count) {
        throw "QMT recovery state JSON key count differs"
    }
    foreach ($Name in $Expected) {
        if (@($ActualNames | Where-Object { $_ -ceq $Name }).Count -ne 1) {
            throw "QMT recovery state JSON keys are duplicated or differ"
        }
    }
}

function Get-QmtClientStartedTs($Client) {
    try {
        $StartedTs = (
            [DateTimeOffset]$Client.StartTime.ToUniversalTime()
        ).ToUnixTimeMilliseconds() / 1000.0
    }
    catch {
        throw "QMT client start identity is unavailable"
    }
    if (
        [double]::IsNaN($StartedTs) -or
        [double]::IsInfinity($StartedTs) -or
        $StartedTs -le 0
    ) {
        throw "QMT client start identity is invalid"
    }
    return [double]$StartedTs
}

function Test-TrustedStoppedHeartbeat(
    $Heartbeat,
    $Client,
    [double]$MinimumUpdatedTs
) {
    if (!(Test-HeartbeatProperties $Heartbeat @(
        "status", "source", "pid", "updated_ts"
    ))) {
        return $false
    }
    try {
        $HeartbeatPid = [int](Get-HeartbeatProperty $Heartbeat "pid")
        $UpdatedTs = [double](Get-HeartbeatProperty $Heartbeat "updated_ts")
    }
    catch {
        return $false
    }
    $NowTs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
    return (
        [string](Get-HeartbeatProperty $Heartbeat "source") -ceq `
            "gj_big_qmt_inner" -and
        [string](Get-HeartbeatProperty $Heartbeat "status") -ceq "stopped" -and
        $HeartbeatPid -eq [int]$Client.Id -and
        ![double]::IsNaN($UpdatedTs) -and
        ![double]::IsInfinity($UpdatedTs) -and
        $UpdatedTs -ge ($MinimumUpdatedTs - 0.001) -and
        $UpdatedTs -le ($NowTs + 5.0)
    )
}

function Read-RecoveryBackup([string]$TransactionId) {
    if ($TransactionId -notmatch "^[0-9a-f]{40}-\d{8}T\d{9}Z-\d+$") {
        throw "QMT recovery transaction identity is malformed"
    }
    $Directory = [System.IO.Path]::GetFullPath(
        (Join-Path $ReloadStateRoot $TransactionId)
    )
    if (!(Test-PathInside $Directory $ReloadStateRoot)) {
        throw "QMT recovery transaction escapes protected state root"
    }
    Assert-OrdinaryDirectory $Directory "QMT recovery transaction directory"
    Assert-ProtectedPathOwner $Directory "QMT recovery transaction directory"
    $BackupPath = Join-Path $Directory "backup.json"
    Assert-OrdinaryFile $BackupPath "QMT recovery backup manifest"
    Assert-ProtectedPathOwner $BackupPath "QMT recovery backup manifest"
    try {
        $Snapshot = Get-Content -LiteralPath $BackupPath -Raw |
            ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        throw "QMT recovery backup manifest is malformed"
    }
    if (!(Test-ExactPropertySet $Snapshot @(
        "schema", "transaction_id", "expected_build_sha", "created_at",
        "directory", "aliases", "manifest_path", "manifest_backup_path",
        "manifest_sha256"
    ))) {
        throw "QMT recovery backup manifest contract differs"
    }
    if (
        [string]$Snapshot.schema -cne "probiga.bigqmt-ui-reload-backup.v1" -or
        [string]$Snapshot.transaction_id -cne $TransactionId -or
        [string]$Snapshot.expected_build_sha -cne $ExpectedBuild -or
        [System.IO.Path]::GetFullPath([string]$Snapshot.directory) -ine $Directory
    ) {
        throw "QMT recovery backup identity differs"
    }
    $Aliases = @($Snapshot.aliases)
    if ($Aliases.Count -eq 0) {
        throw "QMT recovery backup contains no strategy aliases"
    }
    foreach ($Entry in $Aliases) {
        if (!(Test-ExactPropertySet $Entry @(
            "target_path", "backup_path", "sha256"
        ))) {
            throw "QMT recovery backup alias contract differs"
        }
        $TargetPath = [System.IO.Path]::GetFullPath(
            [string]$Entry.target_path
        )
        $BackupFile = [System.IO.Path]::GetFullPath(
            [string]$Entry.backup_path
        )
        if (
            !(Test-PathInside $TargetPath $QmtPythonRoot) -or
            !(Test-PathInside $BackupFile $Directory) -or
            [string]$Entry.sha256 -notmatch "^[0-9a-f]{64}$"
        ) {
            throw "QMT recovery backup alias identity differs"
        }
        Assert-OrdinaryFile $BackupFile "QMT recovery backup artifact"
        Assert-ProtectedPathOwner $BackupFile "QMT recovery backup artifact"
        if ((Get-FileSha256 $BackupFile) -cne [string]$Entry.sha256) {
            throw "QMT recovery backup artifact hash differs"
        }
    }
    $ManifestPath = [System.IO.Path]::GetFullPath(
        [string]$Snapshot.manifest_path
    )
    if (
        !(Test-PathInside $ManifestPath $QmtPythonRoot) -or
        [System.IO.Path]::GetFileName($ManifestPath) -cne $ReleaseManifestName
    ) {
        throw "QMT recovery backup release manifest path differs"
    }
    if ([string]$Snapshot.manifest_backup_path) {
        $ManifestBackup = [System.IO.Path]::GetFullPath(
            [string]$Snapshot.manifest_backup_path
        )
        if (
            !(Test-PathInside $ManifestBackup $Directory) -or
            [string]$Snapshot.manifest_sha256 -notmatch "^[0-9a-f]{64}$"
        ) {
            throw "QMT recovery backup release manifest identity differs"
        }
        Assert-OrdinaryFile $ManifestBackup "QMT recovery manifest backup"
        Assert-ProtectedPathOwner $ManifestBackup "QMT recovery manifest backup"
        if (
            (Get-FileSha256 $ManifestBackup) -cne `
                [string]$Snapshot.manifest_sha256
        ) {
            throw "QMT recovery manifest backup hash differs"
        }
    }
    elseif ([string]$Snapshot.manifest_sha256) {
        throw "QMT recovery manifest absence proof differs"
    }
    return [pscustomobject]@{
        snapshot = $Snapshot
        directory = $Directory
        backup_path = $BackupPath
        backup_sha256 = Get-FileSha256 $BackupPath
    }
}

function Assert-AttemptedArtifactMatchesRecovery($Recovery) {
    $Aliases = @(Get-InstalledStrategyAliases)
    if ($Aliases.Count -eq 0) {
        throw "QMT unverified-start artifact is missing"
    }
    foreach ($Alias in $Aliases) {
        if (
            ($Alias.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
                -ne 0 -or
            !(Test-PathInside $Alias.FullName $QmtPythonRoot) -or
            (Get-FileSha256 $Alias.FullName) -cne `
                [string]$Recovery.payload.attempted_strategy_artifact_sha256
        ) {
            throw "QMT unverified-start artifact identity differs"
        }
    }
}

function Assert-OriginalArtifactMatchesBackup($BackupEnvelope) {
    $Snapshot = $BackupEnvelope.snapshot
    $ExpectedAliases = @{}
    foreach ($Entry in @($Snapshot.aliases)) {
        $ExpectedAliases[
            ([System.IO.Path]::GetFullPath(
                [string]$Entry.target_path
            )).ToLowerInvariant()
        ] = [string]$Entry.sha256
    }
    $CurrentAliases = @(Get-InstalledStrategyAliases)
    if ($CurrentAliases.Count -ne $ExpectedAliases.Count) {
        throw "QMT restored strategy alias set differs"
    }
    foreach ($Alias in $CurrentAliases) {
        $Key = ([System.IO.Path]::GetFullPath(
            $Alias.FullName
        )).ToLowerInvariant()
        if (
            !$ExpectedAliases.ContainsKey($Key) -or
            (Get-FileSha256 $Alias.FullName) -cne $ExpectedAliases[$Key]
        ) {
            throw "QMT restored strategy artifact hash differs"
        }
    }
    $ManifestPath = [System.IO.Path]::GetFullPath(
        [string]$Snapshot.manifest_path
    )
    if ([string]$Snapshot.manifest_backup_path) {
        Assert-OrdinaryFile $ManifestPath "restored QMT release manifest"
        if (
            (Get-FileSha256 $ManifestPath) -cne `
                [string]$Snapshot.manifest_sha256
        ) {
            throw "QMT restored release manifest hash differs"
        }
    }
    elseif (Test-Path -LiteralPath $ManifestPath) {
        throw "QMT restored release manifest absence proof differs"
    }
}

function Read-PersistedRecoveryState($Client) {
    if (!(Test-Path -LiteralPath $RecoveryStatePath)) {
        return $null
    }
    try {
        Assert-OrdinaryFile $RecoveryStatePath "QMT cold-start recovery state"
        Assert-ProtectedPathOwner `
            $RecoveryStatePath `
            "QMT cold-start recovery state"
        $RecoveryPropertyNames = @(
            "schema", "state", "expected_build_sha", "qmt_client_pid",
            "qmt_client_path", "qmt_client_started_ts",
            "prior_heartbeat_pid", "transaction_id",
            "backup_manifest_sha256", "attempted_strategy_artifact_sha256",
            "attempted_at_ts", "stopped_heartbeat_updated_ts",
            "updated_at_utc"
        )
        $RawRecoveryState = Get-Content -LiteralPath $RecoveryStatePath -Raw
        Assert-StrictFlatJsonKeys $RawRecoveryState $RecoveryPropertyNames
        $Payload = $RawRecoveryState | ConvertFrom-Json -ErrorAction Stop
        if (!(Test-ExactPropertySet $Payload $RecoveryPropertyNames)) {
            throw "recovery state contract differs"
        }
        $ClientStartedTs = Get-QmtClientStartedTs $Client
        $AttemptedTs = [double]$Payload.attempted_at_ts
        $StoppedTs = [double]$Payload.stopped_heartbeat_updated_ts
        $NowTs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
        if (
            [string]$Payload.schema -cne $RecoveryStateSchema -or
            [string]$Payload.state -cnotin @(
                "UNVERIFIED_START", "STOPPED_FILES_RESTORED"
            ) -or
            [string]$Payload.expected_build_sha -cne $ExpectedBuild -or
            [int]$Payload.qmt_client_pid -ne [int]$Client.Id -or
            [System.IO.Path]::GetFullPath(
                [string]$Payload.qmt_client_path
            ) -ine [System.IO.Path]::GetFullPath([string]$Client.Path) -or
            [Math]::Abs(
                [double]$Payload.qmt_client_started_ts - $ClientStartedTs
            ) -gt 0.01 -or
            [int]$Payload.prior_heartbeat_pid -le 0 -or
            [int]$Payload.prior_heartbeat_pid -eq [int]$Client.Id -or
            [string]$Payload.backup_manifest_sha256 -notmatch `
                "^[0-9a-f]{64}$" -or
            [string]$Payload.attempted_strategy_artifact_sha256 -notmatch `
                "^[0-9a-f]{64}$" -or
            [double]::IsNaN($AttemptedTs) -or
            [double]::IsInfinity($AttemptedTs) -or
            $AttemptedTs -lt ($ClientStartedTs - 5.0) -or
            $AttemptedTs -gt ($NowTs + 5.0)
        ) {
            throw "recovery state identity differs"
        }
        if (
            ([string]$Payload.state -ceq "UNVERIFIED_START" -and
                $StoppedTs -ne 0.0) -or
            ([string]$Payload.state -ceq "STOPPED_FILES_RESTORED" -and (
                [double]::IsNaN($StoppedTs) -or
                [double]::IsInfinity($StoppedTs) -or
                $StoppedTs -lt $AttemptedTs
            ))
        ) {
            throw "recovery state transition evidence differs"
        }
        try {
            $UpdatedAt = [DateTimeOffset]::Parse(
                [string]$Payload.updated_at_utc
            ).ToUniversalTime()
        }
        catch {
            throw "recovery state timestamp is malformed"
        }
        if (
            $UpdatedAt -gt [DateTimeOffset]::UtcNow.AddSeconds(5) -or
            $UpdatedAt.ToUnixTimeMilliseconds() / 1000.0 -lt `
                ($AttemptedTs - 5.0)
        ) {
            throw "recovery state timestamp differs"
        }
        $BackupEnvelope = Read-RecoveryBackup `
            ([string]$Payload.transaction_id)
        if (
            [string]$BackupEnvelope.backup_sha256 -cne `
                [string]$Payload.backup_manifest_sha256
        ) {
            throw "recovery backup manifest hash differs"
        }
        $Recovery = [pscustomobject]@{
            payload = $Payload
            backup = $BackupEnvelope.snapshot
            backup_envelope = $BackupEnvelope
            disk_state = ""
        }
        if ([string]$Payload.state -ceq "UNVERIFIED_START") {
            $AttemptedMatches = $false
            $OriginalMatches = $false
            try {
                Assert-AttemptedArtifactMatchesRecovery $Recovery
                $AttemptedMatches = $true
            }
            catch {
                $AttemptedMatches = $false
            }
            try {
                Assert-OriginalArtifactMatchesBackup $BackupEnvelope
                $OriginalMatches = $true
            }
            catch {
                $OriginalMatches = $false
            }
            if (!$AttemptedMatches -and !$OriginalMatches) {
                throw "unverified recovery disk is mixed or differs"
            }
            $Recovery.disk_state = if ($OriginalMatches) {
                "ORIGINAL"
            }
            else {
                "ATTEMPTED"
            }
        }
        else {
            Assert-OriginalArtifactMatchesBackup $BackupEnvelope
            $Recovery.disk_state = "ORIGINAL"
        }
        return $Recovery
    }
    catch {
        $Reason = Get-SafeErrorText $_.Exception.Message
        Throw-NeedsUserAction `
            "the persisted QMT recovery state is invalid: $Reason" `
            "QMT_RECOVERY_STATE_INVALID"
    }
}

function Write-PersistedRecoveryState(
    [string]$State,
    $StoppedHeartbeat = $null
) {
    if ($State -cnotin @("UNVERIFIED_START", "STOPPED_FILES_RESTORED")) {
        throw "QMT recovery state transition is invalid"
    }
    if ($null -eq $Backup -or $null -eq $AttemptedRelease) {
        throw "QMT recovery transaction identity is unavailable"
    }
    $TransactionId = [string]$Backup.transaction_id
    $BackupEnvelope = Read-RecoveryBackup $TransactionId
    $NowTs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
    $AttemptedTs = $NowTs
    if (
        $State -ceq "STOPPED_FILES_RESTORED" -and
        $null -ne $PersistedRecovery
    ) {
        $AttemptedTs = [double]$PersistedRecovery.payload.attempted_at_ts
    }
    $StoppedTs = 0.0
    if ($State -ceq "STOPPED_FILES_RESTORED") {
        if (!(Test-TrustedStoppedHeartbeat `
            $StoppedHeartbeat $QmtClient $AttemptedTs
        )) {
            throw "QMT recovery cannot persist an untrusted stopped heartbeat"
        }
        $StoppedTs = [double](Get-HeartbeatProperty `
            $StoppedHeartbeat "updated_ts")
    }
    $PriorPid = 0
    if ($null -ne $ColdStartEvidence) {
        $PriorPid = [int]$ColdStartEvidence.heartbeat_pid
    }
    elseif ($null -ne $PersistedRecovery) {
        $PriorPid = [int]$PersistedRecovery.payload.prior_heartbeat_pid
    }
    if ($PriorPid -le 0 -or $PriorPid -eq [int]$QmtClient.Id) {
        throw "QMT recovery prior heartbeat identity is unavailable"
    }
    $Payload = [ordered]@{
        schema = $RecoveryStateSchema
        state = $State
        expected_build_sha = $ExpectedBuild
        qmt_client_pid = [int]$QmtClient.Id
        qmt_client_path = [System.IO.Path]::GetFullPath(
            [string]$QmtClient.Path
        )
        qmt_client_started_ts = Get-QmtClientStartedTs $QmtClient
        prior_heartbeat_pid = $PriorPid
        transaction_id = $TransactionId
        backup_manifest_sha256 = [string]$BackupEnvelope.backup_sha256
        attempted_strategy_artifact_sha256 = `
            [string]$AttemptedRelease.strategy_artifact_sha256
        attempted_at_ts = [double]$AttemptedTs
        stopped_heartbeat_updated_ts = [double]$StoppedTs
        updated_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    }
    Write-AtomicJson $RecoveryStatePath $Payload
    $script:PersistedRecovery = [pscustomobject]@{
        payload = [pscustomobject]$Payload
        backup = $BackupEnvelope.snapshot
        backup_envelope = $BackupEnvelope
    }
}

function Clear-PersistedRecoveryState {
    if (!(Test-Path -LiteralPath $RecoveryStatePath)) {
        $script:PersistedRecovery = $null
        return
    }
    Assert-OrdinaryFile $RecoveryStatePath "QMT cold-start recovery state"
    Assert-ProtectedPathOwner `
        $RecoveryStatePath `
        "QMT cold-start recovery state"
    Remove-Item -LiteralPath $RecoveryStatePath -Force
    $script:PersistedRecovery = $null
}

function Read-AttemptedReleaseIdentity($Recovery) {
    if ([string]$Recovery.disk_state -cne "ATTEMPTED") {
        throw "QMT attempted release identity requires candidate disk state"
    }
    $ManifestPath = Join-Path $QmtPythonRoot $ReleaseManifestName
    Assert-OrdinaryFile $ManifestPath "attempted QMT release manifest"
    $ManifestKeys = @(
        "schema", "strategy_release_protocol", "strategy_identity_protocol",
        "strategy_build_sha", "strategy_git_blob", "strategy_source_sha256",
        "strategy_artifact_sha256", "strategy_loaded_identity_sha256"
    )
    try {
        $RawManifest = Get-Content -LiteralPath $ManifestPath -Raw
        Assert-StrictFlatJsonKeys $RawManifest $ManifestKeys
        $Manifest = $RawManifest | ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        throw "attempted QMT release manifest is malformed"
    }
    if (
        !(Test-ExactPropertySet $Manifest $ManifestKeys) -or
        [string]$Manifest.schema -cne $ReleaseManifestSchema -or
        [string]$Manifest.strategy_release_protocol -cne $ReleaseProtocol -or
        [string]$Manifest.strategy_identity_protocol -cne $IdentityProtocol -or
        [string]$Manifest.strategy_build_sha -cne $ExpectedBuild -or
        [string]$Manifest.strategy_git_blob -cne $Blob -or
        [string]$Manifest.strategy_artifact_sha256 -cne `
            [string]$Recovery.payload.attempted_strategy_artifact_sha256
    ) {
        throw "attempted QMT release manifest identity differs"
    }
    foreach ($Name in @(
        "strategy_source_sha256", "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256"
    )) {
        if ([string]$Manifest.$Name -notmatch "^[0-9a-f]{64}$") {
            throw "attempted QMT release manifest hash is malformed"
        }
    }
    Assert-AttemptedArtifactMatchesRecovery $Recovery
    return $Manifest
}

function Test-ExpectedRecoveryHeartbeat(
    $Heartbeat,
    $Release,
    [double]$AttemptedTs
) {
    if (!(Test-ExpectedReleaseHeartbeat $Heartbeat $Release)) {
        return $false
    }
    try {
        $UpdatedTs = [double](Get-HeartbeatProperty $Heartbeat "updated_ts")
    }
    catch {
        return $false
    }
    $NowTs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
    $HeartbeatAgeSeconds = $NowTs - $UpdatedTs
    return (
        ![double]::IsNaN($UpdatedTs) -and
        ![double]::IsInfinity($UpdatedTs) -and
        $UpdatedTs -gt $AttemptedTs -and
        $UpdatedTs -le ($NowTs + 5.0) -and
        ![double]::IsNaN($HeartbeatAgeSeconds) -and
        ![double]::IsInfinity($HeartbeatAgeSeconds) -and
        $HeartbeatAgeSeconds -le [double]$HeartbeatMaxAgeSeconds
    )
}

function Complete-ColdStartRecovery($Release, $Loaded) {
    $Receipt = [ordered]@{
        schema = "probiga.bigqmt-ui-release-reload.v1"
        mode = "COLD_START_RECOVERY"
        status = "COLD_START_COMPLETE"
        data_status = "AVAILABLE"
        reason_code = "QMT_CLIENT_RESTART_RECOVERED"
        completed_at = [DateTime]::UtcNow.ToString("o")
        expected_build_sha = $ExpectedBuild
        strategy_git_blob = [string]$Release.strategy_git_blob
        strategy_source_sha256 = [string]$Release.strategy_source_sha256
        strategy_artifact_sha256 = [string]$Release.strategy_artifact_sha256
        strategy_loaded_identity_sha256 = `
            [string]$Release.strategy_loaded_identity_sha256
        qmt_client_pid = [int]$QmtClient.Id
        prior_heartbeat_pid = [int]$ColdStartEvidence.heartbeat_pid
        qmt_client_started_ts = `
            [double]$ColdStartEvidence.qmt_client_started_ts
        loaded_heartbeat_status = `
            [string](Get-HeartbeatProperty $Loaded "status")
        loaded_heartbeat_updated_at = `
            [string](Get-HeartbeatProperty $Loaded "updated_at")
        qmt_client_count = 1
        target_editor_count = 1
        qmt_calls = $QmtCallsAttempted
        database_writes = $false
        ui_actions_attempted = $UiActionsAttempted
        authentication_attempted = $false
        automatic_order_submission = $false
        direct_python_strategy_execution = $false
        rollback_snapshot = [string]$Backup.transaction_id
    }
    $CompletionPath = Join-Path ([string]$Backup.directory) "complete.json"
    Write-AtomicJson $CompletionPath $Receipt
    Assert-OrdinaryFile $CompletionPath "QMT cold-start completion receipt"
    Assert-ProtectedPathOwner `
        $CompletionPath `
        "QMT cold-start completion receipt"
    try {
        $CompletionReadback = Get-Content `
            -LiteralPath $CompletionPath `
            -Raw | ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        throw "QMT cold-start completion receipt is malformed"
    }
    if (
        !(Test-ExactPropertySet $CompletionReadback @($Receipt.Keys)) -or
        [string]$CompletionReadback.status -cne "COLD_START_COMPLETE" -or
        [string]$CompletionReadback.expected_build_sha -cne $ExpectedBuild -or
        [int]$CompletionReadback.qmt_client_pid -ne [int]$QmtClient.Id -or
        [string]$CompletionReadback.strategy_artifact_sha256 -cne `
            [string]$Release.strategy_artifact_sha256 -or
        [string]$CompletionReadback.rollback_snapshot -cne `
            [string]$Backup.transaction_id -or
        $CompletionReadback.database_writes -ne $false -or
        $CompletionReadback.authentication_attempted -ne $false -or
        $CompletionReadback.automatic_order_submission -ne $false -or
        $CompletionReadback.direct_python_strategy_execution -ne $false
    ) {
        throw "QMT cold-start completion receipt readback differs"
    }
    Clear-PersistedRecoveryState
    return $Receipt
}


function New-ArtifactBackup {
    $Aliases = @(Get-InstalledStrategyAliases)
    if ($Aliases.Count -eq 0) {
        throw "installed QMT bridge strategy is missing before release"
    }
    foreach ($Alias in $Aliases) {
        if (
            ($Alias.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
                -ne 0 -or
            !(Test-PathInside $Alias.FullName $QmtPythonRoot)
        ) {
            throw "installed QMT bridge strategy path is unsafe"
        }
    }
    $TransactionId = (
        $ExpectedBuild + "-" +
        [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssfffZ") + "-" + $PID
    )
    $Directory = Join-Path $ReloadStateRoot $TransactionId
    New-Item -ItemType Directory -Path $Directory | Out-Null
    Assert-OrdinaryDirectory $Directory "QMT reload transaction directory"
    $Entries = @()
    for ($Index = 0; $Index -lt $Aliases.Count; $Index += 1) {
        $BackupPath = Join-Path $Directory "original-strategy-$Index.bin"
        [System.IO.File]::WriteAllBytes(
            $BackupPath,
            [System.IO.File]::ReadAllBytes($Aliases[$Index].FullName)
        )
        $Entries += [ordered]@{
            target_path = $Aliases[$Index].FullName
            backup_path = $BackupPath
            sha256 = Get-FileSha256 $Aliases[$Index].FullName
        }
    }
    $ManifestPath = Join-Path $QmtPythonRoot $ReleaseManifestName
    $ManifestBackupPath = ""
    $ManifestHash = ""
    if (Test-Path -LiteralPath $ManifestPath -PathType Leaf) {
        Assert-OrdinaryFile $ManifestPath "existing QMT release manifest"
        $ManifestBackupPath = Join-Path $Directory "original-manifest.json"
        [System.IO.File]::WriteAllBytes(
            $ManifestBackupPath,
            [System.IO.File]::ReadAllBytes($ManifestPath)
        )
        $ManifestHash = Get-FileSha256 $ManifestPath
    }
    $Snapshot = [ordered]@{
        schema = "probiga.bigqmt-ui-reload-backup.v1"
        transaction_id = $TransactionId
        expected_build_sha = $ExpectedBuild
        created_at = [DateTime]::UtcNow.ToString("o")
        directory = $Directory
        aliases = $Entries
        manifest_path = $ManifestPath
        manifest_backup_path = $ManifestBackupPath
        manifest_sha256 = $ManifestHash
    }
    Write-AtomicJson (Join-Path $Directory "backup.json") $Snapshot
    return [pscustomobject]$Snapshot
}

function Invoke-ExactStrategyInstall {
    $PreviousBuildEnvironment = $env:PROBIGA_BUILD_COMMIT_SHA
    try {
        $env:PROBIGA_BUILD_COMMIT_SHA = $ExpectedBuild
        $RawOutput = & $PythonExe -P $Installer `
            --install-strategy --install-only `
            --expected-build-sha $ExpectedBuild --json 2>&1
        $InstallExit = $LASTEXITCODE
    }
    finally {
        $env:PROBIGA_BUILD_COMMIT_SHA = $PreviousBuildEnvironment
    }
    if ($InstallExit -ne 0) {
        throw "exact BigQMT strategy atomic install failed"
    }
    try {
        $Release = ($RawOutput -join "`n") | ConvertFrom-Json
    }
    catch {
        throw "exact BigQMT strategy installer returned invalid JSON"
    }
    if (
        [string]$Release.strategy_git_blob -notmatch `
            "^[0-9a-f]{40}$|^[0-9a-f]{64}$" -or
        [string]$Release.strategy_git_blob -cne $Blob
    ) {
        throw "exact BigQMT strategy Git blob identity differs"
    }
    foreach ($Name in @(
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256",
        "direct_model_source_sha256"
    )) {
        if ([string]$Release.$Name -notmatch "^[0-9a-f]{64}$") {
            throw "exact BigQMT strategy installer identity is malformed"
        }
    }
    if (
        [string]$Release.direct_model_git_blob -notmatch `
            "^[0-9a-f]{40}$|^[0-9a-f]{64}$"
    ) {
        throw "exact BigQMT direct model Git blob identity is malformed"
    }
    if (
        [string]$Release.schema -ne "probiga.bigqmt-strategy-install.v1" -or
        [string]$Release.status -ne "installed" -or
        [string]$Release.build_sha -cne $ExpectedBuild -or
        $Release.database_writes -ne $false -or
        $Release.automatic_order_submission -ne $false
    ) {
        throw "exact BigQMT strategy installer contract differs"
    }
    $ManifestPath = [System.IO.Path]::GetFullPath(
        [string]$Release.strategy_release_manifest
    )
    if (
        !(Test-PathInside $ManifestPath $QmtPythonRoot) -or
        [System.IO.Path]::GetFileName($ManifestPath) -cne `
            $ReleaseManifestName
    ) {
        throw "BigQMT release manifest path differs"
    }
    Assert-OrdinaryFile $ManifestPath "BigQMT release manifest"
    $Manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
    if (
        [string]$Manifest.schema -ne $ReleaseManifestSchema -or
        [string]$Manifest.strategy_release_protocol -ne $ReleaseProtocol -or
        [string]$Manifest.strategy_identity_protocol -ne $IdentityProtocol -or
        [string]$Manifest.strategy_build_sha -cne $ExpectedBuild
    ) {
        throw "BigQMT release manifest contract differs"
    }
    foreach ($Name in @(
        "strategy_git_blob",
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256"
    )) {
        if ([string]$Manifest.$Name -cne [string]$Release.$Name) {
            throw "BigQMT release manifest identity differs: $Name"
        }
    }
    $InstalledPaths = @($Release.installed_paths)
    if ($InstalledPaths.Count -eq 0) {
        throw "BigQMT release installer returned no strategy aliases"
    }
    foreach ($InstalledPathValue in $InstalledPaths) {
        $InstalledPath = [System.IO.Path]::GetFullPath(
            [string]$InstalledPathValue
        )
        if (!(Test-PathInside $InstalledPath $QmtPythonRoot)) {
            throw "BigQMT installed strategy path escapes QMT Python root"
        }
        Assert-OrdinaryFile $InstalledPath "installed BigQMT strategy alias"
        if (
            (Get-FileSha256 $InstalledPath) -cne `
                [string]$Release.strategy_artifact_sha256
        ) {
            throw "BigQMT installed strategy alias hash differs"
        }
    }
    $DirectModelPath = [System.IO.Path]::GetFullPath(
        [string]$Release.direct_model_path
    )
    $ExpectedDirectModelName = (
        $DirectModelFilePrefix +
        [string]$Release.direct_model_source_sha256 +
        ".py"
    )
    if (
        !(Test-PathInside $DirectModelPath $QmtPythonRoot) -or
        [System.IO.Path]::GetFileName($DirectModelPath) -cne `
            $ExpectedDirectModelName
    ) {
        throw "BigQMT direct model path differs"
    }
    Assert-OrdinaryFile $DirectModelPath "installed BigQMT direct model"
    if (
        (Get-FileSha256 $DirectModelPath) -cne `
            [string]$Release.direct_model_source_sha256
    ) {
        throw "BigQMT installed direct model hash differs"
    }
    return $Release
}

function Restore-OriginalArtifact {
    if (!$Backup) {
        throw "QMT reload backup is unavailable"
    }
    $OriginalTargets = @{}
    foreach ($Entry in @($Backup.aliases)) {
        $TargetPath = [System.IO.Path]::GetFullPath(
            [string]$Entry.target_path
        )
        if (!(Test-PathInside $TargetPath $QmtPythonRoot)) {
            throw "QMT rollback target escapes QMT Python root"
        }
        $OriginalTargets[$TargetPath.ToLowerInvariant()] = $true
    }
    $FailedIndex = 0
    foreach ($Current in @(Get-InstalledStrategyAliases)) {
        $CurrentPath = [System.IO.Path]::GetFullPath($Current.FullName)
        if (!$OriginalTargets.ContainsKey($CurrentPath.ToLowerInvariant())) {
            $FailedPath = Join-Path (
                [string]$Backup.directory
            ) "failed-new-strategy-$FailedIndex.bin"
            Move-Item -LiteralPath $CurrentPath -Destination $FailedPath
            $FailedIndex += 1
        }
    }
    foreach ($Entry in @($Backup.aliases)) {
        $TargetPath = [System.IO.Path]::GetFullPath(
            [string]$Entry.target_path
        )
        $Temporary = "$TargetPath.rollback.$PID.tmp"
        [System.IO.File]::WriteAllBytes(
            $Temporary,
            [System.IO.File]::ReadAllBytes([string]$Entry.backup_path)
        )
        Move-Item -LiteralPath $Temporary -Destination $TargetPath -Force
        if ((Get-FileSha256 $TargetPath) -cne [string]$Entry.sha256) {
            throw "QMT rollback strategy hash differs"
        }
    }
    $ManifestPath = [System.IO.Path]::GetFullPath(
        [string]$Backup.manifest_path
    )
    if ([string]$Backup.manifest_backup_path) {
        $TemporaryManifest = "$ManifestPath.rollback.$PID.tmp"
        [System.IO.File]::WriteAllBytes(
            $TemporaryManifest,
            [System.IO.File]::ReadAllBytes(
                [string]$Backup.manifest_backup_path
            )
        )
        Move-Item -LiteralPath $TemporaryManifest `
            -Destination $ManifestPath -Force
        if ((Get-FileSha256 $ManifestPath) -cne [string]$Backup.manifest_sha256) {
            throw "QMT rollback manifest hash differs"
        }
    }
    elseif (Test-Path -LiteralPath $ManifestPath -PathType Leaf) {
        $FailedManifest = Join-Path (
            [string]$Backup.directory
        ) "failed-new-manifest.json"
        Move-Item -LiteralPath $ManifestPath -Destination $FailedManifest
    }
}

function Invoke-ModelRollback {
    Restore-OriginalArtifact
    $CurrentHeartbeat = Get-Heartbeat
    if (Test-OriginalReleaseHeartbeat $CurrentHeartbeat) {
        return "OLD_MODEL_RETAINED"
    }
    $Editors = @(Get-ExactEditorWindows)
    if ($Editors.Count -eq 1) {
        if (
            [string](Get-HeartbeatProperty $CurrentHeartbeat "status") -in `
                @("running", "busy")
        ) {
            try {
                Stop-ExactStrategy $Editors[0] | Out-Null
            }
            catch {
                # A model whose heartbeat cannot be stopped is never closed.
                throw "QMT rollback could not safely stop the loaded model"
            }
        }
        $StoppedHeartbeat = Get-Heartbeat
        if (
            [string](Get-HeartbeatProperty $StoppedHeartbeat "status") -eq `
                "stopped"
        ) {
            Close-ExactStrategyEditor $Editors[0]
        }
    }
    $Editor = Open-ExactStrategyEditor
    Start-ExactStrategy $Editor {
        param($Heartbeat)
        return Test-OriginalReleaseHeartbeat $Heartbeat
    } | Out-Null
    return "OLD_MODEL_RESTORED"
}

function Invoke-ColdStartRollback {
    # The active marker normally exists before Start-ExactStrategy. Recreate it
    # conservatively if a later receipt/cleanup failure occurs after success.
    if ($StartAttempted -and $null -eq $PersistedRecovery) {
        Write-PersistedRecoveryState "UNVERIFIED_START"
    }
    $CurrentHeartbeat = Get-Heartbeat
    $Editors = @(Get-ExactEditorWindows)
    if ($Editors.Count -gt 1) {
        throw "QMT cold-start rollback editor is not unique"
    }

    if (Test-RunningHeartbeat $CurrentHeartbeat) {
        if ($Editors.Count -ne 1) {
            throw "QMT cold-start rollback cannot identify the running editor"
        }
        Stop-ExactStrategy $Editors[0] | Out-Null
        $CurrentHeartbeat = Get-Heartbeat
    }

    $MinimumStoppedTs = if ($null -ne $PersistedRecovery) {
        [double]$PersistedRecovery.payload.attempted_at_ts
    }
    else {
        0.0
    }
    $CurrentModelProvenStopped = Test-TrustedStoppedHeartbeat `
        $CurrentHeartbeat `
        $QmtClient `
        $MinimumStoppedTs
    if ($StartAttempted -and !$CurrentModelProvenStopped) {
        # Never rewrite a file that an unverified in-process model may have
        # loaded after the run control was clicked. The durable UNVERIFIED_START
        # marker makes the next updater fail before any UI or file mutation.
        throw "QMT cold-start rollback cannot prove the new model stopped"
    }
    if ($null -ne $PersistedRecovery -and !$CurrentModelProvenStopped) {
        throw "QMT persisted recovery lost its stopped heartbeat proof"
    }
    if ($Editors.Count -eq 1) {
        Close-ExactStrategyEditor $Editors[0]
    }
    Restore-OriginalArtifact
    $BackupEnvelope = Read-RecoveryBackup ([string]$Backup.transaction_id)
    Assert-OriginalArtifactMatchesBackup $BackupEnvelope
    if ($null -ne $PersistedRecovery) {
        Write-PersistedRecoveryState `
            "STOPPED_FILES_RESTORED" `
            $CurrentHeartbeat
    }
    if ($StartAttempted -or $null -ne $PersistedRecovery) {
        return "COLD_START_MODEL_STOPPED_FILES_RESTORED"
    }
    return "COLD_START_FILES_RESTORED"
}

try {
    if ($PreflightOnly -and $ColdStartRecovery) {
        throw "QMT reload modes are mutually exclusive"
    }
    if ($Root -ine $ExpectedRoot) {
        throw "QMT reload tool differs from its registered production root"
    }
    Assert-OrdinaryDirectory $ExpectedRoot "QMT registered production root"
    Assert-OrdinaryFile $PythonExe "QMT production Python"
    Assert-OrdinaryFile $Installer "BigQMT strategy installer"
    Assert-OrdinaryDirectory $ReloadStateRoot "QMT reload protected state root"
    if (!(Test-PathInside $ReloadStateRoot $ProgramDataRoot)) {
        throw "QMT reload protected state root escapes ProgramData"
    }
    Initialize-ProtectedStateOwnerContract

    $TopLevel = ((Invoke-Git @("rev-parse", "--show-toplevel")) -join "").Trim()
    $Origin = ((Invoke-Git @("remote", "get-url", "origin")) -join "").Trim()
    $Branch = ((Invoke-Git @("symbolic-ref", "--short", "HEAD")) -join "").Trim()
    $Head = ((Invoke-Git @("rev-parse", "HEAD")) -join "").Trim().ToLowerInvariant()
    $Blob = ((
        Invoke-Git @(
            "rev-parse",
            "$ExpectedBuild`:$StrategyRepositoryPath"
        )
    ) -join "").Trim().ToLowerInvariant()
    $Dirty = ((
        Invoke-Git @("status", "--porcelain", "--untracked-files=normal")
    ) -join "`n").Trim()
    if (
        [System.IO.Path]::GetFullPath($TopLevel) -ine $ExpectedRoot -or
        $Origin -ine $ExpectedOrigin -or
        $Branch -cne "main" -or
        $Head -cne $ExpectedBuild -or
        $Blob -notmatch "^[0-9a-f]{40}$|^[0-9a-f]{64}$" -or
        $Dirty
    ) {
        throw "QMT reload checkout is not clean exact-main"
    }

    $RecoveryMutexCreated = $false
    $RecoveryMutex = [System.Threading.Mutex]::new(
        $false,
        "Local\ProBigA.BigQmtStrategyRecovery",
        [ref]$RecoveryMutexCreated
    )
    try {
        $RecoveryMutexOwned = $RecoveryMutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $RecoveryMutexOwned = $true
    }
    if (!$RecoveryMutexOwned) {
        throw "QMT strategy recovery is active; release reload refused"
    }
    $ReleaseMutexCreated = $false
    $ReleaseMutex = [System.Threading.Mutex]::new(
        $false,
        "Local\ProBigA.BigQmtStrategyReleaseReload",
        [ref]$ReleaseMutexCreated
    )
    try {
        $ReleaseMutexOwned = $ReleaseMutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $ReleaseMutexOwned = $true
    }
    if (!$ReleaseMutexOwned) {
        throw "QMT strategy release reload is already active"
    }

    $QmtClients = @(
        Get-Process -Name "XtItClient" -ErrorAction SilentlyContinue
    )
    $CurrentSession = (Get-Process -Id $PID).SessionId
    $QmtClient = Assert-QmtInteractiveClientReady `
        $QmtClients `
        ([int]$CurrentSession)
    Assert-QmtClientProcessOwner $QmtClient
    $QmtMainTitle = [string]$QmtClient.MainWindowTitle
    $QmtMainHandle = $QmtClient.MainWindowHandle
    $QmtRoot = [System.IO.Path]::GetFullPath(
        (Split-Path -Parent (Split-Path -Parent $QmtClient.Path))
    )
    Assert-OrdinaryDirectory $QmtRoot "QMT installation root"
    $ExpectedClientPath = Join-Path $QmtRoot "bin.x64\XtItClient.exe"
    if ([System.IO.Path]::GetFullPath($QmtClient.Path) -ine $ExpectedClientPath) {
        Throw-NeedsUserAction `
            "QMT client executable path differs" `
            "QMT_CLIENT_PATH_MISMATCH"
    }
    $QmtPythonRoot = [System.IO.Path]::GetFullPath(
        (Join-Path $QmtRoot "python")
    )
    Assert-OrdinaryDirectory $QmtPythonRoot "QMT strategy directory"
    $HeartbeatPath = Join-Path (
        $QmtRoot
    ) "userdata\probiga_bridge\heartbeat.json"
    $PreviousHeartbeat = Get-Heartbeat
    $HeartbeatAgeSeconds = $null
    # A persisted unresolved start always outranks the ordinary PID-mismatch
    # route. It can advance only after this same authenticated QMT process has
    # published a stopped heartbeat newer than the pre-click marker.
    $PersistedRecovery = Read-PersistedRecoveryState $QmtClient
    if (
        $null -ne $PersistedRecovery -and
        [string]$PersistedRecovery.payload.state -ceq "UNVERIFIED_START" -and
        [string]$PersistedRecovery.disk_state -ceq "ATTEMPTED"
    ) {
        $AttemptedRelease = Read-AttemptedReleaseIdentity $PersistedRecovery
        if (Test-ExpectedRecoveryHeartbeat `
            $PreviousHeartbeat `
            $AttemptedRelease `
            ([double]$PersistedRecovery.payload.attempted_at_ts)
        ) {
            if ($PreflightOnly -or !$ColdStartRecovery) {
                Throw-NeedsUserAction `
                    "the exact running QMT recovery is ready for explicit finalization" `
                    "QMT_COLD_START_RUNNING_FINALIZE_READY"
            }
            # The prior Run succeeded but the process ended before receipt
            # publication/marker cleanup. Complete the transaction without
            # touching QMT UI or installed strategy files.
            $Backup = $PersistedRecovery.backup
            $ColdStartEvidence = [pscustomobject]@{
                heartbeat_pid = `
                    [int]$PersistedRecovery.payload.prior_heartbeat_pid
                heartbeat_updated_ts = `
                    [double]$PersistedRecovery.payload.attempted_at_ts
                qmt_client_started_ts = `
                    [double]$PersistedRecovery.payload.qmt_client_started_ts
            }
            $FinalPayload = Complete-ColdStartRecovery `
                $AttemptedRelease `
                $PreviousHeartbeat
            $FinalExitCode = 0
            $RecoveredRunningIdempotently = $true
        }
    }
    if ($RecoveredRunningIdempotently) {
        # The durable transaction is complete; no ordinary reload path may run.
    }
    elseif ($null -ne $PersistedRecovery) {
        $RecoveryMinimumStoppedTs = if (
            [string]$PersistedRecovery.payload.state -ceq `
                "STOPPED_FILES_RESTORED"
        ) {
            [double]$PersistedRecovery.payload.stopped_heartbeat_updated_ts
        }
        else {
            [double]$PersistedRecovery.payload.attempted_at_ts
        }
        if (!(Test-TrustedStoppedHeartbeat `
            $PreviousHeartbeat $QmtClient $RecoveryMinimumStoppedTs
        )) {
            Throw-NeedsUserAction `
                "an earlier QMT start remains unverified; no UI or file mutation is allowed" `
                "QMT_UNVERIFIED_START_PENDING"
        }
        if ($PreflightOnly -or !$ColdStartRecovery) {
            Throw-NeedsUserAction `
                "the persisted QMT recovery is safely stopped and ready to resume" `
                "QMT_COLD_START_RETRY_READY"
        }
        $ControlledColdStart = $true
        $ColdStartEvidence = [pscustomobject]@{
            heartbeat_pid = `
                [int]$PersistedRecovery.payload.prior_heartbeat_pid
            heartbeat_updated_ts = $RecoveryMinimumStoppedTs
            qmt_client_started_ts = `
                [double]$PersistedRecovery.payload.qmt_client_started_ts
        }
    }
    else {
        try {
            $HeartbeatAgeSeconds = Assert-QmtClientHeartbeatReady `
                $PreviousHeartbeat `
                ([int]$QmtClient.Id) `
                $HeartbeatMaxAgeSeconds
        }
        catch {
            $HeartbeatFailure = [string]$_.Exception.Message
            $ControlledColdStart = (
                $ColdStartRecovery -and
                !$PreflightOnly -and
                $HeartbeatFailure.StartsWith(
                    "NEEDS_USER_ACTION:QMT_HEARTBEAT_PID_MISMATCH:",
                    [System.StringComparison]::Ordinal
                )
            )
            if (!$ControlledColdStart) {
                throw
            }
            $ColdStartEvidence = Assert-QmtColdStartEvidence `
                $PreviousHeartbeat `
                $QmtClient
        }
    }

    if ($RecoveredRunningIdempotently) {
        # Completion was proven and finalized above with zero UI/QMT calls.
    }
    elseif ($PreflightOnly) {
        $FinalPayload = [ordered]@{
            schema = "probiga.bigqmt-ui-release-reload.v1"
            mode = "PREFLIGHT_ONLY"
            status = "READY"
            data_status = "AVAILABLE"
            reason_code = "QMT_PREFLIGHT_READY"
            expected_build_sha = $ExpectedBuild
            qmt_client_pid = [int]$QmtClient.Id
            heartbeat_pid = [int](Get-HeartbeatProperty `
                $PreviousHeartbeat "pid")
            heartbeat_age_seconds = $HeartbeatAgeSeconds
            qmt_calls = $false
            database_writes = $false
            ui_actions_attempted = $false
            authentication_attempted = $false
            automatic_order_submission = $false
            direct_python_strategy_execution = $false
        }
        $FinalExitCode = 0
    }
    else {
        # Re-read every interactive prerequisite immediately before the first
        # UI operation so a client restart/login transition cannot race the
        # earlier read-only decision.
        $UiQmtClient = Assert-QmtInteractiveClientReady `
            @(Get-Process -Name "XtItClient" -ErrorAction SilentlyContinue) `
            ([int]$CurrentSession)
        Assert-QmtClientProcessOwner $UiQmtClient
        if (
            [int]$UiQmtClient.Id -ne [int]$QmtClient.Id -or
            [System.IO.Path]::GetFullPath($UiQmtClient.Path) -ine `
                $ExpectedClientPath
        ) {
            Throw-NeedsUserAction `
                "QMT client identity changed before UI recovery" `
                "QMT_CLIENT_CHANGED"
        }
        $QmtClient = $UiQmtClient
        $QmtMainTitle = [string]$QmtClient.MainWindowTitle
        $QmtMainHandle = $QmtClient.MainWindowHandle
        $PreviousHeartbeat = Get-Heartbeat
        if ($ControlledColdStart) {
            if ($null -ne $PersistedRecovery) {
                $PersistedRecovery = Read-PersistedRecoveryState $QmtClient
                $RecoveryMinimumStoppedTs = if (
                    [string]$PersistedRecovery.payload.state -ceq `
                        "STOPPED_FILES_RESTORED"
                ) {
                    [double]$PersistedRecovery.payload.stopped_heartbeat_updated_ts
                }
                else {
                    [double]$PersistedRecovery.payload.attempted_at_ts
                }
                if (!(Test-TrustedStoppedHeartbeat `
                    $PreviousHeartbeat $QmtClient $RecoveryMinimumStoppedTs
                )) {
                    Throw-NeedsUserAction `
                        "the QMT stopped proof changed before recovery" `
                        "QMT_RECOVERY_STOP_PROOF_LOST"
                }
            }
            else {
                $ColdStartEvidence = Assert-QmtColdStartEvidence `
                    $PreviousHeartbeat `
                    $QmtClient
            }
        }
        else {
            $HeartbeatAgeSeconds = Assert-QmtClientHeartbeatReady `
                $PreviousHeartbeat `
                ([int]$QmtClient.Id) `
                $HeartbeatMaxAgeSeconds
        }
        $WasMinimized = [ProBigAQmtReleaseWindow]::IsIconic($QmtMainHandle)
        $PreviousForeground = [ProBigAQmtReleaseWindow]::GetForegroundWindow()
        $UiActionsAttempted = $true
        Show-QmtMainWindow
        Assert-NoOtherStrategyEditors
        Assert-NoUnexpectedVisibleQmtWindow

        if ($ControlledColdStart) {
            $StoppedRecoveryHeartbeat = Get-Heartbeat
            if ($null -ne $PersistedRecovery) {
                $RecoveryMinimumStoppedTs = if (
                    [string]$PersistedRecovery.payload.state -ceq `
                        "STOPPED_FILES_RESTORED"
                ) {
                    [double]$PersistedRecovery.payload.stopped_heartbeat_updated_ts
                }
                else {
                    [double]$PersistedRecovery.payload.attempted_at_ts
                }
                if (!(Test-TrustedStoppedHeartbeat `
                    $StoppedRecoveryHeartbeat `
                    $QmtClient `
                    $RecoveryMinimumStoppedTs
                )) {
                    Throw-NeedsUserAction `
                        "the QMT stopped proof changed before file recovery" `
                        "QMT_RECOVERY_STOP_PROOF_LOST"
                }
                # Reuse the original transaction. Never take a new backup of
                # candidate bytes left by an interrupted prior start.
                $Backup = $PersistedRecovery.backup
                $AttemptedRelease = [pscustomobject]@{
                    strategy_artifact_sha256 = [string](
                        $PersistedRecovery.payload.`
                            attempted_strategy_artifact_sha256
                    )
                }
            }
            else {
                # The ordinary cold-start path is authorized only by an old
                # heartbeat that predates this authenticated client process.
                $ColdStartEvidence = Assert-QmtColdStartEvidence `
                    $StoppedRecoveryHeartbeat `
                    $QmtClient
            }
            $Editors = @(Get-ExactEditorWindows)
            if ($Editors.Count -eq 1) {
                Close-ExactStrategyEditor $Editors[0]
                $OldEditorClosed = $true
            }
            if ($null -ne $PersistedRecovery) {
                Restore-OriginalArtifact
                Assert-OriginalArtifactMatchesBackup `
                    $PersistedRecovery.backup_envelope
                Write-PersistedRecoveryState `
                    "STOPPED_FILES_RESTORED" `
                    $StoppedRecoveryHeartbeat
            }
            else {
                $Backup = New-ArtifactBackup
            }
            $InstallAttempted = $true
            $Release = Invoke-ExactStrategyInstall
            $AttemptedRelease = $Release
            $NewEditor = Open-ExactStrategyEditor
            Assert-NoOtherStrategyEditors
            Assert-NoUnexpectedVisibleQmtWindow
            # Persist before the first Run click. A process crash after this
            # point must block every later close/install until a newer stopped
            # heartbeat from this exact QMT PID is observed.
            Write-PersistedRecoveryState "UNVERIFIED_START"
            $StartAttempted = $true
            $Loaded = Start-ExactStrategy $NewEditor {
                param($Heartbeat)
                return Test-ExpectedReleaseHeartbeat $Heartbeat $Release
            }
            $NewModelStarted = $true
            $Receipt = Complete-ColdStartRecovery $Release $Loaded
            $FinalPayload = $Receipt
            $FinalExitCode = 0
        }
        else {
            $Editors = @(Get-ExactEditorWindows)
            if ($Editors.Count -eq 0) {
                $Editor = Open-ExactStrategyEditor
            }
            else {
                $Editor = $Editors[0]
            }
            Assert-NoOtherStrategyEditors
            Assert-NoUnexpectedVisibleQmtWindow
            $PreviousHeartbeat = Get-Heartbeat
            if (!(Test-RunningHeartbeat $PreviousHeartbeat)) {
                Throw-NeedsUserAction (
                    "the existing exact QMT strategy must be running before release"
                ) "QMT_STRATEGY_CHANGED_DURING_RELOAD"
            }

            $Backup = New-ArtifactBackup
            $InstallAttempted = $true
            $Release = Invoke-ExactStrategyInstall

            # Installation is deliberately completed and fully hash-verified
            # while the old in-memory model remains running. Only then may UI
            # control stop that model.
            $StillOld = Get-Heartbeat
            if (!(Test-RunningHeartbeat $StillOld)) {
                throw "old QMT model changed state during atomic release install"
            }
            if (Test-ExpectedReleaseHeartbeat $StillOld $Release) {
                $NewModelStarted = $true
                $FinalPayload = [ordered]@{
                    schema = "probiga.bigqmt-ui-release-reload.v1"
                    mode = "RELOAD"
                    status = "IDEMPOTENT"
                    expected_build_sha = $ExpectedBuild
                    strategy_git_blob = [string]$Release.strategy_git_blob
                    strategy_source_sha256 = `
                        [string]$Release.strategy_source_sha256
                    strategy_artifact_sha256 = `
                        [string]$Release.strategy_artifact_sha256
                    strategy_loaded_identity_sha256 = `
                        [string]$Release.strategy_loaded_identity_sha256
                    qmt_client_count = 1
                    target_editor_count = 1
                    qmt_calls = $QmtCallsAttempted
                    database_writes = $false
                    ui_actions_attempted = $true
                    authentication_attempted = $false
                    automatic_order_submission = $false
                    direct_python_strategy_execution = $false
                    rollback_snapshot = [string]$Backup.transaction_id
                }
                $FinalExitCode = 0
            }
            else {
                Stop-ExactStrategy $Editor | Out-Null
                $OldModelStopped = $true
                Close-ExactStrategyEditor $Editor
                $OldEditorClosed = $true
                $NewEditor = Open-ExactStrategyEditor
                $StartAttempted = $true
                $Loaded = Start-ExactStrategy $NewEditor {
                    param($Heartbeat)
                    return Test-ExpectedReleaseHeartbeat $Heartbeat $Release
                }
                $NewModelStarted = $true
                $Receipt = [ordered]@{
                    schema = "probiga.bigqmt-ui-release-reload.v1"
                    mode = "RELOAD"
                    status = "COMPLETE"
                    completed_at = [DateTime]::UtcNow.ToString("o")
                    expected_build_sha = $ExpectedBuild
                    strategy_git_blob = [string]$Release.strategy_git_blob
                    strategy_source_sha256 = `
                        [string]$Release.strategy_source_sha256
                    strategy_artifact_sha256 = `
                        [string]$Release.strategy_artifact_sha256
                    strategy_loaded_identity_sha256 = `
                        [string]$Release.strategy_loaded_identity_sha256
                    qmt_client_pid = [int]$QmtClient.Id
                    loaded_heartbeat_status = [string]$Loaded.status
                    loaded_heartbeat_updated_at = [string]$Loaded.updated_at
                    qmt_client_count = 1
                    target_editor_count = 1
                    qmt_calls = $QmtCallsAttempted
                    database_writes = $false
                    ui_actions_attempted = $true
                    authentication_attempted = $false
                    automatic_order_submission = $false
                    direct_python_strategy_execution = $false
                    rollback_snapshot = [string]$Backup.transaction_id
                }
                Write-AtomicJson (
                    Join-Path ([string]$Backup.directory) "complete.json"
                ) $Receipt
                $FinalPayload = $Receipt
                $FinalExitCode = 0
            }
        }
    }
}
catch {
    $FailureText = Get-SafeErrorText $_.Exception.Message
    $NeedsUser = $FailureText.StartsWith(
        "NEEDS_USER_ACTION:",
        [System.StringComparison]::Ordinal
    )
    $FailureReasonCode = if ($ControlledColdStart) {
        "QMT_COLD_START_RECOVERY_FAILED"
    }
    else {
        "QMT_RELEASE_RELOAD_FAILED"
    }
    if ($NeedsUser) {
        $EncodedFailure = $FailureText.Substring(
            "NEEDS_USER_ACTION:".Length
        )
        $SeparatorIndex = $EncodedFailure.IndexOf(":")
        if ($SeparatorIndex -gt 0) {
            $CandidateReasonCode = $EncodedFailure.Substring(0, $SeparatorIndex)
            if ($CandidateReasonCode -match "^[A-Z0-9_]+$") {
                $FailureReasonCode = $CandidateReasonCode
                $FailureText = $EncodedFailure.Substring($SeparatorIndex + 1)
            }
            else {
                $FailureText = $EncodedFailure
            }
        }
        else {
            $FailureText = $EncodedFailure
        }
    }
    $RollbackStatus = "NOT_REQUIRED"
    $RollbackError = ""
    if ($InstallAttempted -and $Backup) {
        try {
            $RollbackStatus = if ($ControlledColdStart) {
                Invoke-ColdStartRollback
            }
            else {
                Invoke-ModelRollback
            }
        }
        catch {
            $RollbackStatus = "FILES_OR_MODEL_UNVERIFIED"
            $RollbackError = Get-SafeErrorText $_.Exception.Message
        }
    }
    $Status = if ($NeedsUser) {
        "NEEDS_USER_ACTION"
    }
    elseif ($RollbackStatus -in @(
        "OLD_MODEL_RETAINED",
        "OLD_MODEL_RESTORED",
        "COLD_START_FILES_RESTORED",
        "COLD_START_MODEL_STOPPED_FILES_RESTORED"
    )) {
        "ROLLED_BACK"
    }
    else {
        "FAILED_CLOSED"
    }
    $FinalPayload = [ordered]@{
        schema = "probiga.bigqmt-ui-release-reload.v1"
        mode = if ($PreflightOnly) {
            "PREFLIGHT_ONLY"
        }
        elseif ($ControlledColdStart -or $ColdStartRecovery) {
            "COLD_START_RECOVERY"
        }
        else {
            "RELOAD"
        }
        status = $Status
        data_status = "DATA_BLOCKED"
        reason_code = $FailureReasonCode
        expected_build_sha = $ExpectedBuild
        reason = $FailureText
        rollback_status = $RollbackStatus
        rollback_error = $RollbackError
        old_model_was_stopped = $OldModelStopped
        old_editor_was_closed = $OldEditorClosed
        new_model_was_started = $NewModelStarted
        qmt_calls = $QmtCallsAttempted
        database_writes = $false
        ui_actions_attempted = $UiActionsAttempted
        authentication_attempted = $false
        automatic_order_submission = $false
        direct_python_strategy_execution = $false
    }
    $FinalExitCode = if ($NeedsUser) { 3 } else { 2 }
}
finally {
    if ($UiActionsAttempted -and $QmtMainHandle -ne [IntPtr]::Zero) {
        if ($WasMinimized) {
            [ProBigAQmtReleaseWindow]::ShowWindow(
                $QmtMainHandle, 6
            ) | Out-Null
        }
        elseif (
            $PreviousForeground -ne [IntPtr]::Zero -and
            [ProBigAQmtReleaseWindow]::IsWindow($PreviousForeground)
        ) {
            [ProBigAQmtReleaseWindow]::SetForegroundWindow(
                $PreviousForeground
            ) | Out-Null
        }
    }
    if ($ReleaseMutexOwned) {
        $ReleaseMutex.ReleaseMutex()
    }
    if ($ReleaseMutex) {
        $ReleaseMutex.Dispose()
    }
    if ($RecoveryMutexOwned) {
        $RecoveryMutex.ReleaseMutex()
    }
    if ($RecoveryMutex) {
        $RecoveryMutex.Dispose()
    }
}

if (!$FinalPayload) {
    $FinalPayload = [ordered]@{
        schema = "probiga.bigqmt-ui-release-reload.v1"
        status = "FAILED_CLOSED"
        expected_build_sha = $ExpectedBuild
        reason = "QMT reload ended without a release receipt"
    }
    $FinalExitCode = 2
}
[Console]::Out.WriteLine(
    ($FinalPayload | ConvertTo-Json -Depth 12 -Compress)
)
exit $FinalExitCode
