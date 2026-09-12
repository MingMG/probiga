param(
    [string]$StateInitializationRoot = '',
    [string]$StateInitializationBuildSha = ''
)

# One fixed state layout shared by registration and every release application.
# Dot-sourcing only defines functions; readiness probes never initialize state.
. (Join-Path $PSScriptRoot 'deploy_preflight.ps1')

function Get-QmtWindowsStatePaths() {
    if ($env:ProgramData -notmatch '^[A-Za-z]:[\\/]') {
        throw 'QMT_STATE_PROGRAMDATA_INVALID'
    }
    $Base = [IO.Path]::GetFullPath($env:ProgramData)
    foreach ($Scope in @(
        'qmt-local-gap-repair', 'qmt-model-reload', 'scheduler', 'jobs',
        'qmt-full-market-history'
    )) {
        $Path = [IO.Path]::GetFullPath((Join-Path $Base "ProBigA\$Scope"))
        if (!$Path.StartsWith($Base.TrimEnd('\', '/') + '\', [StringComparison]::OrdinalIgnoreCase)) {
            throw 'QMT_STATE_PATH_OUTSIDE_PROGRAMDATA'
        }
        $Path
    }
}

function Assert-QmtWindowsStateAncestors([string]$Path) {
    # Check all existing components, including a junction above an existing leaf.
    $Current = [IO.Path]::GetFullPath($Path)
    while ($Current) {
        if (Test-Path -LiteralPath $Current) {
            $Item = Get-Item -LiteralPath $Current -Force -ErrorAction Stop
            if (!$Item.PSIsContainer -or
                ($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'QMT_STATE_UNSAFE_DIRECTORY'
            }
        }
        $Current = [IO.Path]::GetDirectoryName($Current)
    }
}

function Assert-QmtWindowsStateDirectories() {
    foreach ($Path in @(Get-QmtWindowsStatePaths)) {
        Assert-QmtWindowsStateAncestors $Path
        # Uses AccessCheck against the actual token, including deny ACEs. The
        # registered updater is Limited; membership in Administrators alone
        # would neither prove access nor authorize changing the task principal.
        Assert-DeployStateDirectoryAccess $Path
    }
}

function Test-QmtWindowsStateAcl($Acl, [string[]]$AllowedSids) {
    if (!$Acl.AreAccessRulesProtected) { return $false }
    $Rules = @($Acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier]))
    if ($Rules.Count -ne $AllowedSids.Count) { return $false }
    $Seen = @()
    foreach ($Rule in $Rules) {
        $Sid = $Rule.IdentityReference.Value
        if ($Sid -cnotin $AllowedSids -or $Sid -cin $Seen -or $Rule.IsInherited -or
            $Rule.AccessControlType -ne [Security.AccessControl.AccessControlType]::Allow -or
            $Rule.FileSystemRights -ne [Security.AccessControl.FileSystemRights]::FullControl -or
            $Rule.InheritanceFlags -ne ([Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
                [Security.AccessControl.InheritanceFlags]::ObjectInherit) -or
            $Rule.PropagationFlags -ne [Security.AccessControl.PropagationFlags]::None) {
            return $false
        }
        $Seen += $Sid
    }
    return $true
}

function Initialize-QmtWindowsStateDirectories() {
    $ErrorActionPreference = 'Stop'
    # Preflight every scope before the first mutation, not just the new scope.
    Assert-QmtWindowsStateDirectories
    $Identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    try { $AllowedSids = @('S-1-5-18', 'S-1-5-32-544', $Identity.User.Value | Select-Object -Unique) }
    finally { $Identity.Dispose() }
    $Acl = [Security.AccessControl.DirectorySecurity]::new()
    $Acl.SetAccessRuleProtection($true, $false)
    foreach ($SidValue in $AllowedSids) {
        $Rule = [Security.AccessControl.FileSystemAccessRule]::new(
            [Security.Principal.SecurityIdentifier]::new($SidValue),
            [Security.AccessControl.FileSystemRights]::FullControl,
            ([Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
                [Security.AccessControl.InheritanceFlags]::ObjectInherit),
            [Security.AccessControl.PropagationFlags]::None,
            [Security.AccessControl.AccessControlType]::Allow
        )
        $Acl.AddAccessRule($Rule)
    }
    foreach ($Path in @(Get-QmtWindowsStatePaths)) {
        Assert-QmtWindowsStateAncestors $Path
        if (!(Test-Path -LiteralPath $Path)) {
            # Apply the protected DACL at creation, before any job can use it.
            [IO.Directory]::CreateDirectory($Path, $Acl) | Out-Null
        } elseif (!(Test-QmtWindowsStateAcl (Get-Acl -LiteralPath $Path) $AllowedSids)) {
            Set-Acl -LiteralPath $Path -AclObject $Acl -ErrorAction Stop
        }
        Assert-QmtWindowsStateAncestors $Path
        if (!(Test-QmtWindowsStateAcl (Get-Acl -LiteralPath $Path) $AllowedSids)) {
            throw 'QMT_STATE_ACL_READBACK_INVALID'
        }
        $Path
    }
}

if ($MyInvocation.InvocationName -ne '.') {
    $ErrorActionPreference = 'Stop'
    Set-StrictMode -Version Latest
    [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
    if ($StateInitializationRoot -notmatch '^[A-Za-z]:[\\/]' -or
        $StateInitializationBuildSha -cnotmatch '^[0-9a-f]{40}$') {
        throw 'QMT_STATE_RELEASE_ARGUMENTS_INVALID'
    }
    $StateCodeRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
    if ($StateCodeRoot -ine [IO.Path]::GetFullPath($StateInitializationRoot)) {
        throw 'QMT_STATE_RELEASE_ROOT_INVALID'
    }
    Assert-QmtWindowsStateAncestors $StateCodeRoot
    $StateGitTop = Invoke-DeployGit -Root $StateCodeRoot -Arguments @('rev-parse', '--show-toplevel') -Stage 'state.root' -TimeoutSeconds 10
    $StateGitBranch = Invoke-DeployGit -Root $StateCodeRoot -Arguments @('symbolic-ref', '--short', 'HEAD') -Stage 'state.branch' -TimeoutSeconds 10
    $StateGitSha = Invoke-DeployGit -Root $StateCodeRoot -Arguments @('rev-parse', 'HEAD') -Stage 'state.head' -TimeoutSeconds 10
    if ([IO.Path]::GetFullPath(($StateGitTop -join '').Trim()) -ine $StateCodeRoot -or
        ($StateGitBranch -join '').Trim() -cne 'main' -or
        ($StateGitSha -join '').Trim() -cne $StateInitializationBuildSha) {
        throw 'QMT_STATE_RELEASE_IDENTITY_INVALID'
    }
    $PreparedStateRoots = @(Initialize-QmtWindowsStateDirectories)
    [ordered]@{
        schema = 'probiga.qmt-windows-state.v1'
        status = 'ready'
        build_sha = $StateInitializationBuildSha
        production_root = $StateCodeRoot
        state_roots = $PreparedStateRoots
    } | ConvertTo-Json -Compress
}
