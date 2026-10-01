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

function Assert-MergedMain([string]$CodeRoot) {
    $branch = (& git -C $CodeRoot branch --show-current).Trim()
    $head = (& git -C $CodeRoot rev-parse HEAD).Trim()
    $main = (& git -C $CodeRoot rev-parse origin/main).Trim()
    if ($LASTEXITCODE -ne 0 -or $branch -ne 'main' -or $head -ne $main -or
        (& git -C $CodeRoot status --porcelain)) {
        throw 'Use only a clean checkout of merged production main.'
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
    foreach ($entry in @($acl.Access)) { [void]$acl.RemoveAccessRuleSpecific($entry) }
    foreach ($sid in @('S-1-5-18', 'S-1-5-32-544', [Security.Principal.WindowsIdentity]::GetCurrent().User.Value)) {
        $rule = New-Object Security.AccessControl.FileSystemAccessRule(
            (New-Object Security.Principal.SecurityIdentifier($sid)),
            'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
        $acl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $Path -AclObject $acl
}

function Protect-PortablePackageEntry([string]$Root) {
    # Local source-user SIDs do not travel to another Windows installation.
    # Only harmless launch/seal files are public-readable; database payload,
    # keys, histories and software remain administrator-only on the new PC.
    $users = New-Object Security.Principal.SecurityIdentifier('S-1-5-32-545')
    foreach ($name in @('', 'start_target_migration.cmd','target_entry.ps1','migrate_target.ps1',
        'package_common.ps1','COLD_README.txt','manifest.json','READY')) {
        $path = if ($name) { Join-Path $Root $name } else { $Root }
        $acl = Get-Acl -LiteralPath $path
        $rule = New-Object Security.AccessControl.FileSystemAccessRule($users,'ReadAndExecute','None','None','Allow')
        $acl.AddAccessRule($rule)
        Set-Acl -LiteralPath $path -AclObject $acl
    }
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

function Assert-ColdPackage([string]$Root) {
    $rootFull = [IO.Path]::GetFullPath($Root).TrimEnd('\') + '\'
    $manifestPath = Join-Path $rootFull 'manifest.json'
    $readyPath = Join-Path $rootFull 'READY'
    foreach ($path in @($rootFull.TrimEnd('\'),$manifestPath,$readyPath)) {
        if (-not (Test-Path -LiteralPath $path) -or
            ((Get-Item -LiteralPath $path -Force).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw 'Cold package is incomplete or has an unsafe seal path.'
        }
    }
    $manifest = Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($manifest.format -ne 'probiga.windows-cold-migration.v2' -or
        $manifest.build_sha -notmatch '^[0-9a-f]{40}$' -or -not $manifest.source_host -or
        $manifest.source_paused -ne $true -or $manifest.production_activation -ne $false -or
        $manifest.restore_requested -ne $false -or $manifest.minimum_target_free_bytes -lt 250GB) {
        throw 'Invalid paused cold migration manifest.'
    }
    $seal = $manifest.build_sha + ' ' + (Get-Sha256 $manifestPath)
    if ((Get-Content -LiteralPath $readyPath -Raw -Encoding UTF8).Trim() -cne $seal) {
        throw 'Cold package manifest seal does not match.'
    }
    $required = @('code.bundle','database/metadata.json','database/data/mysql.ibd','database/config/my.ini',
        'database/data/auto.cnf','database/certs/ca.pem','database/certs/server-cert.pem','database/certs/server-key.pem',
        'audit/source-pause.json','audit/source-project-state/archive-metadata.json',
        'audit/source-project-state/development/data/main.db','software/qmt/bin.x64/XtItClient.exe',
        'software/mysql84/bin/mysqld.exe','software/mysql84/bin/mysql.exe','software/mysql84/bin/mysqladmin.exe',
        'software/codex/codex.exe','software/codex/codex-code-mode-host.exe',
        'software/codex/codex-command-runner.exe','software/codex/codex-windows-sandbox-setup.exe','software/installers/python313.exe',
        'software/installers/python314.exe','software/installers/git.exe','software/installers/vc_x64.exe',
        'software/installers/vc_x86.exe','software/installers/chrome.msi','package_common.ps1',
        'migrate_target.ps1','target_entry.ps1','start_target_migration.cmd','COLD_README.txt')
    $seen = New-Object 'Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)
    $measured = [long]0
    foreach ($file in @($manifest.files)) {
        $relative = ([string]$file.path).Replace('\','/')
        if (-not $relative -or $relative.StartsWith('/') -or $relative.Contains(':') -or
            $relative.Split('/') -contains '..' -or $relative.Split('/') -contains '.' -or
            $relative.Split('/') -contains '' -or -not $seen.Add($relative) -or
            $relative -in @('manifest.json','READY') -or $file.sha256 -notmatch '^[0-9A-Fa-f]{64}$' -or
            $file.bytes -lt 0) { throw 'Unsafe or duplicate cold manifest file entry.' }
        $full = [IO.Path]::GetFullPath((Join-Path $rootFull $relative))
        if (-not $full.StartsWith($rootFull,[StringComparison]::OrdinalIgnoreCase) -or
            -not (Test-Path -LiteralPath $full -PathType Leaf)) { throw 'Cold package file is missing or outside the package.' }
        $walk = Get-Item -LiteralPath $full -Force
        while ($walk -and $walk.FullName.Length -ge $rootFull.TrimEnd('\').Length) {
            if ($walk.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Cold package contains a reparse point.' }
            if ($walk -is [IO.DirectoryInfo]) { $walk = $walk.Parent } else { $walk = $walk.Directory }
        }
        if ((Get-Item -LiteralPath $full -Force).Length -ne $file.bytes -or
            (Get-Sha256 $full) -ine $file.sha256) { throw ('Cold package corruption: '+$relative) }
        $measured += [long]$file.bytes
    }
    foreach ($relative in $required) { if (-not $seen.Contains($relative)) { throw ('Unsealed cold package prerequisite: '+$relative) } }
    foreach ($house in @('wheels313/','wheels314/')) {
        if (-not @($seen | Where-Object { $_.StartsWith($house,[StringComparison]::OrdinalIgnoreCase) -and $_.EndsWith('.whl') }).Count) {
            throw 'A sealed offline wheelhouse is missing.'
        }
    }
    if ($manifest.minimum_target_free_bytes -lt $measured + 30GB) { throw 'Cold package target capacity estimate is incomplete.' }
    # Reject additional files rather than executing code that was not sealed.
    foreach ($actual in Get-ChildItem -LiteralPath $rootFull -Recurse -File -Force) {
        $relative = $actual.FullName.Substring($rootFull.Length).Replace('\','/')
        if ($relative -notin @('manifest.json','READY') -and -not $seen.Contains($relative)) { throw ('Unsealed extra file: '+$relative) }
    }
    $pause = Get-Content -LiteralPath (Join-Path $rootFull 'audit/source-pause.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($pause.format -ne 'probiga.source-pause.v1' -or $pause.status -ne 'paused' -or
        $pause.source_host -ine $manifest.source_host -or $pause.source_service_state -ne 'Stopped' -or
        $pause.source_service_startup -ne 'Disabled' -or $pause.shutdown_complete -ne $true -or
        $pause.source_processes_running -ne $false -or $pause.source_qmt_running -ne $false -or $pause.source_automatically_resume -ne $false -or
        $pause.source_server_uuid -ne $manifest.database.source.server_uuid) {
        throw 'Cold package has no matching durable source pause receipt.'
    }
    return $manifest
}
