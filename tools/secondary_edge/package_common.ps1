Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-Sha256([string]$Path) {
    $stream = [IO.File]::OpenRead($Path)
    $hasher = [Security.Cryptography.SHA256]::Create()
    try { return [BitConverter]::ToString($hasher.ComputeHash($stream)).Replace('-','') }
    finally { $hasher.Dispose(); $stream.Dispose() }
}

function Assert-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Run this script from an Administrator PowerShell window.'
    }
}

function Write-Utf8([string]$Path, [string]$Content) {
    [IO.File]::WriteAllText($Path, $Content, (New-Object Text.UTF8Encoding($false)))
}

function Invoke-Checked([string]$Exe, [string[]]$Arguments) {
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Command failed: $Exe (exit $LASTEXITCODE)" }
}

function Protect-LocalPath([string]$Path) {
    $acl = Get-Acl -LiteralPath $Path
    $acl.SetAccessRuleProtection($true, $false)
    foreach ($sid in @('S-1-5-18', 'S-1-5-32-544', [Security.Principal.WindowsIdentity]::GetCurrent().User.Value)) {
        $rule = New-Object Security.AccessControl.FileSystemAccessRule(
            (New-Object Security.Principal.SecurityIdentifier($sid)),
            'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
        $acl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $Path -AclObject $acl
}

function Copy-Tree([string]$From, [string]$To, [string[]]$Extra = @()) {
    if (-not (Test-Path -LiteralPath $From -PathType Container)) { throw "Missing source: $From" }
    # Never /MIR: retrying must not delete unrelated target files.
    & robocopy.exe $From $To /E /COPY:DAT /DCOPY:DAT /R:2 /W:2 /XJ /NP /NFL /NDL @Extra
    if ($LASTEXITCODE -ge 8) { throw "Copy failed: $From (robocopy $LASTEXITCODE)" }
}

function Get-SignedArtifact([string]$Url, [string]$Path, [string]$Publisher, [string]$Sha256 = '') {
    if (-not (Test-Path -LiteralPath $Path)) {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri $Url -OutFile ($Path + '.part') -UseBasicParsing
        Move-Item -LiteralPath ($Path + '.part') -Destination $Path
    }
    if ($Sha256 -and (Get-Sha256 $Path) -ne $Sha256) {
        throw "SHA256 mismatch: $Path"
    }
    $signature = Get-AuthenticodeSignature -LiteralPath $Path
    if ($signature.Status -ne 'Valid' -or $signature.SignerCertificate.Subject -notlike "*$Publisher*") {
        throw "Invalid publisher signature: $Path"
    }
}

function Assert-Package([string]$Root) {
    $manifestPath = Join-Path $Root 'manifest.json'
    if (-not (Test-Path -LiteralPath (Join-Path $Root 'READY'))) { throw 'Package is incomplete: no READY seal.' }
    $manifest = Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($manifest.format -ne 'probiga.windows-edge-offline.v1') { throw 'Unknown package format.' }
    if ($manifest.build_sha -notmatch '^[0-9a-f]{40}$' -or -not $manifest.source_host -or
        $manifest.production_activation -ne $false -or $manifest.minimum_target_free_bytes -lt 450GB) {
        throw 'Invalid package identity or activation boundary.'
    }
    if ((Get-Content -LiteralPath (Join-Path $Root 'READY') -Raw).Trim() -ne $manifest.build_sha) { throw 'Invalid READY seal.' }
    $required = @('code.bundle','database\app.sql','database\metadata.json',
        'software\qmt\bin.x64\XtItClient.exe','software\mysql84\bin\mysqld.exe','software\codex\codex.exe',
        'software\installers\python313.exe','software\installers\python314.exe','software\installers\git.exe',
        'software\installers\vc_x64.exe','software\installers\vc_x86.exe','software\installers\chrome.msi')
    foreach ($entry in $required) {
        if ($entry -notin @($manifest.files | ForEach-Object path)) { throw "Unsealed required file: $entry" }
    }
    $rootFull = [IO.Path]::GetFullPath($Root).TrimEnd('\') + '\'
    foreach ($file in $manifest.files) {
        $full = [IO.Path]::GetFullPath((Join-Path $rootFull $file.path))
        if (-not $full.StartsWith($rootFull, [StringComparison]::OrdinalIgnoreCase)) { throw 'Unsafe manifest path.' }
        if (-not (Test-Path -LiteralPath $full -PathType Leaf)) { throw "Missing package file: $($file.path)" }
        if ((Get-Item -LiteralPath $full).Length -ne $file.bytes -or
            (Get-Sha256 $full) -ne $file.sha256) {
            throw "Package corruption: $($file.path)"
        }
    }
    return $manifest
}
