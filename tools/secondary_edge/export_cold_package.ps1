#Requires -Version 5.1
[CmdletBinding()]
param(
    [string]$OutputRoot,
    [string]$SourceLayout,
    [string]$PauseReceipt,
    [string]$CodeRoot = '',
    [string]$QmtHome = '',
    [string]$MySqlHome = 'D:\MySQL84\software\mysql-8.4.11-winx64',
    [string]$Python313 = 'C:\Users\Administrator\AppData\Local\Programs\Python\Python313\python.exe',
    [string]$Python314 = 'E:\My Code\ProBigA\.venv\Scripts\python.exe',
    [string]$CodexBundle = 'C:\Users\Administrator\AppData\Local\OpenAI\Codex\bin\c6fe824d725f02d7',
    [string]$DownloadCache = 'F:\ProBigA-Migration-Download-20261001',
    [string]$SourceProjectRoot = 'E:\My Code\ProBigA',
    [string]$SourceProductionRoot = 'E:\My Code\ProBigA-qmt-production',
    [string]$AiServerUrl = ''
)
. (Join-Path $PSScriptRoot 'package_common.ps1')
if (-not $CodeRoot) { $CodeRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent }

function Get-ColdExportArtifacts {
    # Immutable versioned installers are pinned before execution. Mutable official
    # vendor endpoints are Authenticode checked, then pinned by the package seal.
    return @(
        [pscustomobject]@{name='python313.exe';url='https://www.python.org/ftp/python/3.13.14/python-3.13.14-amd64.exe';publisher='Python Software Foundation';sha256='c54d9b9bbb8a36e6489363ddd01139707fd781d72f1f9e90c7ec65d0061368e0'},
        [pscustomobject]@{name='python314.exe';url='https://www.python.org/ftp/python/3.14.3/python-3.14.3-amd64.exe';publisher='Python Software Foundation';sha256='b68ad91421afbbd1a628105199c8c5f6179b21ba799067a8d8c0bbac3b7defb0'},
        [pscustomobject]@{name='git.exe';url='https://github.com/git-for-windows/git/releases/download/v2.53.0.windows.2/Git-2.53.0.2-64-bit.exe';publisher='Johannes Schindelin';sha256='194362cf24cd0db4b573096108460a34c7f80a20c5f2aa60d06ef817be9f73a1'},
        [pscustomobject]@{name='vc_x64.exe';url='https://aka.ms/vc14/vc_redist.x64.exe';publisher='Microsoft Corporation';sha256=''},
        [pscustomobject]@{name='vc_x86.exe';url='https://aka.ms/vc14/vc_redist.x86.exe';publisher='Microsoft Corporation';sha256=''},
        [pscustomobject]@{name='chrome.msi';url='https://dl.google.com/dl/chrome/install/googlechromestandaloneenterprise64.msi';publisher='Google';sha256=''}
    )
}

function Assert-ColdExportPlainTree([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { throw "Missing source: $Path" }
    $cursor = [IO.Path]::GetFullPath($Path)
    while ($cursor) {
        if ((Get-Item -LiteralPath $cursor -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw 'Cold export refuses reparse-point paths.'
        }
        $cursor = Split-Path $cursor -Parent
    }
    if (Test-Path -LiteralPath $Path -PathType Container) {
        if (@(Get-ChildItem -LiteralPath $Path -Recurse -Force -ErrorAction Stop |
            Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint }).Count) {
            throw 'Cold export refuses reparse-point trees; files must not be silently omitted.'
        }
    }
}

function Get-ColdExportInventory([string]$Root) {
    $prefix = [IO.Path]::GetFullPath($Root).TrimEnd('\') + '\'
    return @(Get-ChildItem -LiteralPath $Root -Recurse -Force -ErrorAction Stop |
        Sort-Object FullName | ForEach-Object {
            [pscustomobject]@{path=$_.FullName.Substring($prefix.Length);directory=$_.PSIsContainer;
                bytes=$(if ($_.PSIsContainer) { [long]0 } else { [long]$_.Length });
                modified=$_.LastWriteTimeUtc.Ticks}
        })
}

function Copy-ColdExportTree([string]$From, [string]$To) {
    Assert-ColdExportPlainTree $From
    if (Test-Path -LiteralPath $To) { throw 'Copy destination must not already exist.' }
    $before = @(Get-ColdExportInventory $From)
    # No /XF exclusions: stale .lock/.pid files are data, not an excuse to copy
    # a running QMT installation. /E also preserves empty directories.
    Copy-Tree $From $To
    $after = @(Get-ColdExportInventory $From)
    if (($before | ConvertTo-Json -Depth 4 -Compress) -cne ($after | ConvertTo-Json -Depth 4 -Compress)) {
        throw 'Source tree changed during copy; preserve this attempt and retry.'
    }
    $target = @(Get-ColdExportInventory $To)
    if (($before | Select-Object path,directory,bytes | ConvertTo-Json -Depth 4 -Compress) -cne
        ($target | Select-Object path,directory,bytes | ConvertTo-Json -Depth 4 -Compress)) {
        throw 'Incomplete cold tree copy.'
    }
    foreach ($file in @($before | Where-Object { -not $_.directory })) {
        $source = Join-Path $From $file.path
        $copied = Join-Path $To $file.path
        if ((Get-Sha256 $source) -ne (Get-Sha256 $copied) -or
            (Get-Item -LiteralPath $source -Force).LastWriteTimeUtc.Ticks -ne $file.modified) {
            throw 'Cold tree byte verification failed.'
        }
    }
}

function Copy-ColdExportExactFile([string]$From, [string]$To, [string]$ExpectedSha256 = '') {
    Assert-ColdExportPlainTree $From
    if (-not (Test-Path -LiteralPath $From -PathType Leaf)) { throw 'Exact archive source must be a file.' }
    $before = Get-Item -LiteralPath $From -Force
    $bytes = [long]$before.Length
    $modified = $before.LastWriteTimeUtc.Ticks
    $digest = Get-Sha256 $From
    if ($ExpectedSha256 -and $digest -ine $ExpectedSha256) {
        throw 'Exact archive source differs from its reviewed identity; review it before exporting.'
    }
    $parent = Split-Path $To -Parent
    $existingParent = $parent
    while (-not (Test-Path -LiteralPath $existingParent)) { $existingParent = Split-Path $existingParent -Parent }
    Assert-ColdExportPlainTree $existingParent
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
    Assert-ColdExportPlainTree $parent
    $partial = $To + '.part'
    if ((Test-Path -LiteralPath $To) -or (Test-Path -LiteralPath $partial)) {
        throw 'Exact archive copy must not overwrite an existing file or partial attempt.'
    }
    # CreateNew prevents even a raced-in partial file from being overwritten.
    $inputFile = $null
    $outputFile = $null
    try {
        $inputFile = [IO.File]::Open($From,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::Read)
        $outputFile = [IO.File]::Open($partial,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
        $inputFile.CopyTo($outputFile)
        $outputFile.Flush($true)
    }
    finally {
        if ($outputFile) { $outputFile.Dispose() }
        if ($inputFile) { $inputFile.Dispose() }
    }
    Assert-ColdExportPlainTree $From
    Assert-ColdExportPlainTree $partial
    $after = Get-Item -LiteralPath $From -Force
    if ($after.Length -ne $bytes -or $after.LastWriteTimeUtc.Ticks -ne $modified -or
        (Get-Sha256 $From) -ne $digest -or (Get-Sha256 $partial) -ne $digest) {
        throw 'Exact archive source changed during copy or copied bytes differ.'
    }
    Move-Item -LiteralPath $partial -Destination $To
    Assert-ColdExportPlainTree $To
    if ((Get-Item -LiteralPath $To -Force).Length -ne $bytes -or (Get-Sha256 $To) -ne $digest) {
        throw 'Exact archive final byte verification failed.'
    }
    return [pscustomobject]@{bytes=$bytes;sha256=$digest}
}

function Get-ColdExportRegistrationArtifacts {
    # Only these reviewed installation sources are archiveable. The source
    # directory also contains private verification receipts and must not be
    # copied as a tree. A changed tool requires an explicit non-secret review.
    return @(
        [pscustomobject]@{name='install.ps1';sha256='B403E6CBB6453CDFB5F09E9B2B52F94E0608EBC12FF3EA23F2FB7BE2199453C6'},
        [pscustomobject]@{name='windows_app_credentials.py';sha256='3C3049D5BE507B4A84B54E3AF219330D4C5984D8D6F4BEF03B9B1DC80C445D93'},
        [pscustomobject]@{name='README.md';sha256='7D49B0527F2B1FE079C2B3EF9D2940096BDCCB16911059299DEFA0DDD04A5677'},
        [pscustomobject]@{name='test_windows_app_credentials.py';sha256='612731CA75090A83E25F7B69ECFB4E58B3D49CC74A6FDA2CF279C18BDF39A279'}
    )
}

function Get-ColdExportReviewedAlertStateSha256 {
    # This exact JSON was reviewed as historical status/fingerprint data, not
    # credentials. Do not infer that arbitrary later JSON is safe to archive.
    return '37C169D25D7DFB04984E71BB9B4674BDA5B1562FC1BFA2A50FBF3F9C4131DF55'
}

function Assert-ColdExportQmtStopped([string]$QmtRootPath) {
    $prefix = [IO.Path]::GetFullPath($QmtRootPath).TrimEnd('\') + '\'
    foreach ($process in @(Get-CimInstance Win32_Process -ErrorAction Stop)) {
        # Refuse even an inaccessible/foreign XtItClient process. Do not match
        # the caller's command text (which can itself contain this script name).
        if ($process.Name -match '^(?i)XtItClient\.exe$' -or
            ($process.ExecutablePath -and $process.ExecutablePath.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase))) {
            throw 'QMT must remain completely stopped before and throughout export.'
        }
    }
}

function Assert-ColdExportSourceBrowserStopped([string]$ProjectRoot) {
    $profile = [IO.Path]::GetFullPath((Join-Path $ProjectRoot 'data\ai_bridge\deepseek_chrome_profile')).TrimEnd('\')
    foreach ($process in @(Get-CimInstance Win32_Process -Filter "Name='chrome.exe'" -ErrorAction Stop)) {
        if (-not $process.CommandLine) { throw 'Cannot prove that an inaccessible Chrome process is outside the source profile.' }
        # Windows can quote the complete --user-data-dir=VALUE argument or just
        # VALUE. Compare the parsed complete path, never an arbitrary substring.
        $patterns = @('"--user-data-dir=(?<profile>[^"]+)"',
            '--user-data-dir="(?<profile>[^"]+)"', '--user-data-dir\s+"(?<profile>[^"]+)"',
            '--user-data-dir=(?<profile>[^"\s]+)')
        foreach ($pattern in $patterns) {
            $match = [regex]::Match($process.CommandLine,$pattern,[Text.RegularExpressions.RegexOptions]::IgnoreCase)
            if ($match.Success -and [IO.Path]::GetFullPath($match.Groups['profile'].Value).TrimEnd('\') -ieq $profile) {
                throw 'The source DeepSeek Chrome profile is still running; a complete cold archive requires normal browser exit.'
            }
        }
    }
}

function Get-ColdExportProjectArchivePaths {
    return @('data','runtime\backfill_evidence','runtime\cache','runtime\logs','cache','logs','reports',
        'output','outputs','artifacts','runtime\emquant-py36','runtime\emquant-wheels','runtime\installers')
}

function Get-ColdExportProductionHistoryFiles {
    if (-not $env:USERPROFILE) { throw 'The exact production history archive requires USERPROFILE.' }
    # Both reviewed business rollouts belong to this exact day. Do not scan
    # the entire personal session store, or follow unrelated history links.
    $day = Join-Path $env:USERPROFILE '.codex\sessions\2026\08\01'
    Assert-ColdExportPlainAncestors $day
    if (-not (Test-Path -LiteralPath $day -PathType Container)) { throw 'The reviewed production history day is missing.' }
    $files = @(Get-ChildItem -LiteralPath $day -File -Force -ErrorAction Stop)
    foreach ($thread in @('019fbe02-0390-7663-a7ba-bd150e063fe7','019fbe02-0a70-7c62-9bf2-9ab439bea770')) {
        $rollouts = @($files | Where-Object Name -Like "*$thread.jsonl")
        if ($rollouts.Count -ne 1) { throw 'The exact production history archive is missing or ambiguous.' }
        Assert-ColdExportPlainTree $rollouts[0].FullName
        [pscustomobject]@{thread_id=$thread;name=$rollouts[0].Name;source_path=$rollouts[0].FullName}
    }
}

function Get-ColdExportReportNodeModules {
    if (-not $env:USERPROFILE) { throw 'The reviewed report Node dependency root requires USERPROFILE.' }
    return [IO.Path]::GetFullPath((Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\node\node_modules')).TrimEnd('\')
}

function Assert-ColdExportPlainAncestors([string]$Path) {
    $cursor = [IO.Path]::GetFullPath($Path)
    while ($cursor) {
        if (-not (Test-Path -LiteralPath $cursor) -or
            ((Get-Item -LiteralPath $cursor -Force).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw 'Archive paths and ancestors must be ordinary existing filesystem entries.'
        }
        $cursor = Split-Path $cursor -Parent
    }
}

function Assert-ColdExportArchiveName([string]$Name) {
    if ($Name -like '.env*' -or $Name -ieq 'windows-app-credentials' -or
        $Name -ieq 'CredentialVault' -or $Name -ieq 'auth.json' -or $Name -ieq 'private.bin') {
        throw 'Source state archive contains an environment/credential store requiring separate review.'
    }
}

function Get-ColdExportArchiveTreePlan([string]$ProjectRoot, [string]$Relative) {
    $project = [IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\')
    $root = [IO.Path]::GetFullPath((Join-Path $project $Relative)).TrimEnd('\')
    if (-not ($root + '\').StartsWith(($project + '\'),[StringComparison]::OrdinalIgnoreCase) -or
        -not (Test-Path -LiteralPath $root -PathType Container)) {
        throw 'Archive tree must be an existing directory inside its source project.'
    }
    Assert-ColdExportPlainAncestors $root
    $entries = New-Object 'System.Collections.Generic.List[object]'
    $junctions = New-Object 'System.Collections.Generic.List[object]'
    $pending = New-Object 'System.Collections.Generic.Stack[object]'
    $pending.Push([pscustomobject]@{source_path=$root;path='';expanded=$false})
    while ($pending.Count) {
        $current = $pending.Pop()
        # A queued ordinary directory may have changed since its parent was
        # listed. Recheck before listing it; never inspect a new link target.
        Assert-ColdExportPlainAncestors $current.source_path
        foreach ($item in @(Get-ChildItem -LiteralPath $current.source_path -Force -ErrorAction Stop)) {
            Assert-ColdExportArchiveName $item.Name
            $path = $(if ($current.path) { $current.path + '\' + $item.Name } else { $item.Name })
            $sourcePath = $item.FullName
            $expanded = [bool]$current.expanded
            if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
                # No generic link traversal, including inside the reviewed Node
                # runtime. Only these two historical report links are data.
                $projectRelative = $root.Substring($project.Length+1) + '\' + $path
                $approved = @('outputs\20260728_v3_july_backtest\node_modules',
                    'outputs\20260728_v3_latest_july_backtest\node_modules')
                if ($expanded -or $approved -notcontains $projectRelative -or
                    -not $item.PSIsContainer -or $item.LinkType -cne 'Junction') {
                    throw 'Unreviewed archive reparse point; only the two exact report Directory Junctions are allowed.'
                }
                $targets = @($item.Target)
                $expected = Get-ColdExportReportNodeModules
                if ($targets.Count -ne 1 -or
                    [IO.Path]::GetFullPath([string]$targets[0]).TrimEnd('\') -ine $expected) {
                    throw 'The reviewed report junction target changed; do not follow it.'
                }
                if (-not (Test-Path -LiteralPath $expected -PathType Container)) {
                    throw 'The complete reviewed report Node dependency root is missing.'
                }
                Assert-ColdExportPlainAncestors $expected
                $junctions.Add([pscustomobject]@{relative_path=$projectRelative;path=$path;
                    source_path=$item.FullName;target_path=$expected;link_type='Junction';
                    materialized_as='ordinary-directory';archive_only=$true;runtime_activation=$false})
                $sourcePath = $expected
                $expanded = $true
            }
            $entries.Add([pscustomobject]@{path=$path;directory=[bool]$item.PSIsContainer;
                bytes=$(if ($item.PSIsContainer) { [long]0 } else { [long]$item.Length });
                modified=$item.LastWriteTimeUtc.Ticks;source_path=$sourcePath})
            if ($item.PSIsContainer) {
                $pending.Push([pscustomobject]@{source_path=$sourcePath;path=$path;expanded=$expanded})
            }
        }
    }
    return [pscustomobject]@{format='probiga.source-archive-tree-plan.v1';root=$root;
        relative_path=$Relative;entries=@($entries.ToArray() | Sort-Object path);
        junctions=@($junctions.ToArray() | Sort-Object path)}
}

function Copy-ColdExportArchiveTree([string]$ProjectRoot, [string]$Relative, [string]$To) {
    if (Test-Path -LiteralPath $To) { throw 'Archive tree destination must not already exist.' }
    $targetParent = Split-Path ([IO.Path]::GetFullPath($To)) -Parent
    while (-not (Test-Path -LiteralPath $targetParent)) { $targetParent = Split-Path $targetParent -Parent }
    Assert-ColdExportPlainAncestors $targetParent
    $before = Get-ColdExportArchiveTreePlan $ProjectRoot $Relative
    $sourceHashes = @{}
    foreach ($file in @($before.entries | Where-Object { -not $_.directory })) {
        $sourceHashes[$file.path] = Get-Sha256 $file.source_path
    }
    # /XJ on the ordinary project tree never follows the source links. Then the
    # separately reviewed dependency root is copied to each ordinary target.
    Copy-Tree $before.root $To | Out-Host
    foreach ($junction in @($before.junctions)) {
        $destination = Join-Path $To $junction.path
        if (Test-Path -LiteralPath $destination) { throw 'A report dependency destination unexpectedly already exists.' }
        Assert-ColdExportPlainAncestors $junction.target_path
        Copy-Tree $junction.target_path $destination | Out-Host
    }
    $after = Get-ColdExportArchiveTreePlan $ProjectRoot $Relative
    if (($before | ConvertTo-Json -Depth 8 -Compress) -cne ($after | ConvertTo-Json -Depth 8 -Compress)) {
        throw 'Source archive tree or report junction changed during copy; preserve this attempt.'
    }
    Assert-ColdExportPlainTree $To
    $target = @(Get-ColdExportInventory $To)
    if (($before.entries | Select-Object path,directory,bytes | ConvertTo-Json -Depth 4 -Compress) -cne
        ($target | Select-Object path,directory,bytes | ConvertTo-Json -Depth 4 -Compress)) {
        throw 'Incomplete materialized source archive tree.'
    }
    $bytes = [long]0
    $files = 0
    foreach ($file in @($before.entries | Where-Object { -not $_.directory })) {
        $source = $file.source_path
        $copied = Join-Path $To $file.path
        Assert-ColdExportPlainAncestors $source
        $sourceFile = Get-Item -LiteralPath $source -Force
        $targetFile = Get-Item -LiteralPath $copied -Force
        if ($sourceFile.Length -ne $file.bytes -or $sourceFile.LastWriteTimeUtc.Ticks -ne $file.modified -or
            $targetFile.Length -ne $file.bytes -or $targetFile.LastWriteTimeUtc.Ticks -ne $file.modified -or
            (Get-Sha256 $source) -ne $sourceHashes[$file.path] -or
            (Get-Sha256 $copied) -ne $sourceHashes[$file.path]) {
            throw 'Source archive byte verification failed.'
        }
        $bytes += [long]$file.bytes
        $files++
    }
    # A final no-follow inventory revalidates every link after byte reads, not
    # merely before copying, and rejects any raced-in file/directory/link.
    $final = Get-ColdExportArchiveTreePlan $ProjectRoot $Relative
    if (($before | ConvertTo-Json -Depth 8 -Compress) -cne ($final | ConvertTo-Json -Depth 8 -Compress)) {
        throw 'Source archive changed during final verification.'
    }
    Assert-ColdExportPlainTree $To
    return [pscustomobject]@{files=$files;bytes=$bytes;junctions=@($before.junctions)}
}

function Assert-ColdExportProjectArchiveSources([string]$Development, [string]$Production) {
    $needsReportNode = $false
    foreach ($project in @($Development,$Production)) {
        if (-not (Test-Path -LiteralPath $project -PathType Container)) { throw 'A required source project root is missing.' }
        Assert-ColdExportPlainAncestors $project
        Assert-ColdExportSourceBrowserStopped $project
        foreach ($relative in @(Get-ColdExportProjectArchivePaths)) {
            $from = Join-Path $project $relative
            if (-not (Test-Path -LiteralPath $from -PathType Container)) { continue }
            $plan = Get-ColdExportArchiveTreePlan $project $relative
            if (@($plan.junctions).Count) { $needsReportNode = $true }
        }
        foreach ($artifact in @(Get-ColdExportRegistrationArtifacts)) {
            $from = Join-Path $project ('runtime\windows-app-credentials\' + $artifact.name)
            if (-not (Test-Path -LiteralPath $from -PathType Leaf)) { continue }
            Assert-ColdExportPlainTree $from
            if ((Get-Sha256 $from) -ine $artifact.sha256) { throw 'An operation tool differs from its reviewed identity.' }
        }
        foreach ($name in @('capital_flow_restore.sql','probiga_remote_dump.sql.gz')) {
            $from = Join-Path $project ('_archive\' + $name)
            if (Test-Path -LiteralPath $from -PathType Leaf) { Assert-ColdExportPlainTree $from }
        }
    }
    if ($env:USERPROFILE) {
        foreach ($name in @('craft_mlt_25k.pth','english_g2.pth')) {
            $from = Join-Path $env:USERPROFILE ('.EasyOCR\model\' + $name)
            if (Test-Path -LiteralPath $from -PathType Leaf) { Assert-ColdExportPlainTree $from }
        }
    }
    if ($env:LOCALAPPDATA) {
        $from = Join-Path $env:LOCALAPPDATA 'ProBigA\qmt-wecom-alert-state.json'
        if (Test-Path -LiteralPath $from -PathType Leaf) { Assert-ColdExportPlainTree $from }
    }
    if ($needsReportNode) {
        $from = Join-Path (Split-Path (Get-ColdExportReportNodeModules) -Parent) 'bin\node.exe'
        Assert-ColdExportPlainTree $from
        if (-not (Test-Path -LiteralPath $from -PathType Leaf)) { throw 'The exact report Node executable is missing.' }
    }
}

function Copy-ColdExportProjectArchive([string]$Development, [string]$Production, [string]$PackageRoot) {
    Assert-ColdExportProjectArchiveSources $Development $Production
    $archive = Join-Path $PackageRoot 'audit\source-project-state'
    if (Test-Path -LiteralPath $archive) { throw 'Project state archive must be a new directory.' }
    New-Item -ItemType Directory -Path $archive | Out-Null
    $entries = @()
    $excluded = @()
    $sources = @([pscustomobject]@{label='development';root=$Development},
        [pscustomobject]@{label='production';root=$Production})
    foreach ($source in $sources) {
        if (-not (Test-Path -LiteralPath $source.root -PathType Container)) { throw 'A required source project root is missing.' }
        Assert-ColdExportSourceBrowserStopped $source.root
        foreach ($relative in @(Get-ColdExportProjectArchivePaths)) {
            $from = Join-Path $source.root $relative
            if (-not (Test-Path -LiteralPath $from -PathType Container)) { continue }
            $to = Join-Path (Join-Path $archive $source.label) $relative
            New-Item -ItemType Directory -Path (Split-Path $to -Parent) -Force | Out-Null
            $proof = Copy-ColdExportArchiveTree $source.root $relative $to
            $entries += [ordered]@{label=$source.label;source_root=$source.root;relative_path=$relative;
                package_path=$to.Substring($PackageRoot.TrimEnd('\').Length+1);
                files=$proof.files;bytes=$proof.bytes;materialized_junctions=@($proof.junctions);
                active_runtime=$false;runtime_activation=$false;archive_only=$true;kind='complete-tree'}
        }
        foreach ($artifact in @(Get-ColdExportRegistrationArtifacts)) {
            $relative = 'runtime\windows-app-credentials\' + $artifact.name
            $from = Join-Path $source.root $relative
            if (-not (Test-Path -LiteralPath $from -PathType Leaf)) { continue }
            $destination = 'operation-tools\windows-app-registration\' + $artifact.name
            $to = Join-Path (Join-Path $archive $source.label) $destination
            $proof = Copy-ColdExportExactFile $from $to $artifact.sha256
            $entries += [ordered]@{label=$source.label;source_root=$source.root;relative_path=$relative;
                package_path=$to.Substring($PackageRoot.TrimEnd('\').Length+1);
                files=1;bytes=$proof.bytes;sha256=$proof.sha256;active_runtime=$false;
                runtime_activation=$false;archive_only=$true;kind='reviewed-operation-tool';
                credentials_copied=$false;target_credentials_enrolled=$false}
        }
        foreach ($name in @('capital_flow_restore.sql','probiga_remote_dump.sql.gz')) {
            $relative = '_archive\' + $name
            $from = Join-Path $source.root $relative
            if (-not (Test-Path -LiteralPath $from -PathType Leaf)) { continue }
            $to = Join-Path (Join-Path $archive $source.label) $relative
            $proof = Copy-ColdExportExactFile $from $to
            $entries += [ordered]@{label=$source.label;source_root=$source.root;relative_path=$relative;
                package_path=$to.Substring($PackageRoot.TrimEnd('\').Length+1);
                files=1;bytes=$proof.bytes;sha256=$proof.sha256;active_runtime=$false;
                runtime_activation=$false;archive_only=$true;kind='exact-file'}
        }
        Assert-ColdExportSourceBrowserStopped $source.root
    }
    if (@($entries | Where-Object { $_.Contains('materialized_junctions') -and @($_.materialized_junctions).Count }).Count) {
        $relative = '.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe'
        $from = Join-Path $env:USERPROFILE $relative
        $to = Join-Path $archive 'machine-assets\report-node-runtime\bin\node.exe'
        $proof = Copy-ColdExportExactFile $from $to
        $entries += [ordered]@{label='machine-assets';source_root=$env:USERPROFILE;relative_path=$relative;
            package_path=$to.Substring($PackageRoot.TrimEnd('\').Length+1);
            files=1;bytes=$proof.bytes;sha256=$proof.sha256;active_runtime=$false;
            runtime_activation=$false;archive_only=$true;kind='report-node-runtime-preservation'}
    }
    # Machine assets are exact optional files, not a copy of the source user
    # directory. They stay private/immutable and are never installed or run.
    if ($env:USERPROFILE) {
        foreach ($name in @('craft_mlt_25k.pth','english_g2.pth')) {
            $relative = '.EasyOCR\model\' + $name
            $from = Join-Path $env:USERPROFILE $relative
            if (-not (Test-Path -LiteralPath $from -PathType Leaf)) { continue }
            $to = Join-Path $archive ('machine-assets\easyocr\model\' + $name)
            $proof = Copy-ColdExportExactFile $from $to
            $entries += [ordered]@{label='machine-assets';source_root=$env:USERPROFILE;relative_path=$relative;
                package_path=$to.Substring($PackageRoot.TrimEnd('\').Length+1);
                files=1;bytes=$proof.bytes;sha256=$proof.sha256;active_runtime=$false;
                runtime_activation=$false;archive_only=$true;kind='offline-model-preservation'}
        }
    }
    if ($env:LOCALAPPDATA) {
        $relative = 'ProBigA\qmt-wecom-alert-state.json'
        $from = Join-Path $env:LOCALAPPDATA $relative
        if (Test-Path -LiteralPath $from -PathType Leaf) {
            Assert-ColdExportPlainTree $from
            $digest = Get-Sha256 $from
            if ($digest -ieq (Get-ColdExportReviewedAlertStateSha256)) {
                $to = Join-Path $archive 'machine-assets\legacy-alert-state\qmt-wecom-alert-state.json'
                $proof = Copy-ColdExportExactFile $from $to $digest
                $entries += [ordered]@{label='machine-assets';source_root=$env:LOCALAPPDATA;relative_path=$relative;
                    package_path=$to.Substring($PackageRoot.TrimEnd('\').Length+1);
                    files=1;bytes=$proof.bytes;sha256=$proof.sha256;active_runtime=$false;
                    runtime_activation=$false;archive_only=$true;kind='non-secret-legacy-alert-state'}
            }
            else {
                $excluded += [ordered]@{asset='legacy-qmt-alert-state';
                    reason='not-matching-reviewed-non-secret-json';copied=$false;runtime_activation=$false}
            }
        }
    }
    Write-Utf8 (Join-Path $archive 'archive-metadata.json') ([ordered]@{
        format='probiga.source-project-private-archive.v1';source_host=$env:COMPUTERNAME;
        source_paused=$true;archive_only=$true;production_activation=$false;restore_requested=$false;
        legacy_runtime_activation=$false;runtime_activation=$false;browser_login_reuse_authorized=$false;
        credentials_copied=$false;target_credentials_enrolled=$false;entries=$entries;excluded_assets=$excluded;
        historical_source_code_retained=$true;
        excluded_legacy_code_reason='Historical _archive code and deployment scripts remain on the source, are not deleted and are outside runtime migration scope.'
    } | ConvertTo-Json -Depth 8)
}

function Assert-ColdExportPause($Layout, $Pause, [string]$HostName) {
    if ($Layout.format -ne 'probiga.cold-source-layout.v1' -or
        $Layout.source.hostname -ine $HostName -or $Layout.source.version -ne '8.4.11' -or
        $Layout.source.service_name -ne 'ProBigA-MySQL84' -or
        $Pause.format -ne 'probiga.source-pause.v1' -or $Pause.status -ne 'paused' -or
        $Pause.source_host -ine $HostName -or $Pause.source_server_uuid -ne $Layout.source.server_uuid -or
        $Pause.source_service_name -ne 'ProBigA-MySQL84' -or
        $Pause.source_service_state -ne 'Stopped' -or $Pause.source_service_startup -ne 'Disabled' -or
        $Pause.source_processes_running -ne $false -or $Pause.source_qmt_running -ne $false -or $Pause.shutdown_complete -ne $true -or
        $Pause.source_automatically_resume -ne $false -or -not $Pause.completed_at_utc) {
        throw 'A matching durable source-paused receipt is mandatory.'
    }
    $service = Get-CimInstance Win32_Service -Filter "Name='ProBigA-MySQL84'" -ErrorAction Stop
    if (-not $service -or $service.State -ne 'Stopped' -or $service.StartMode -ne 'Disabled' -or
        [int]$service.ProcessId -ne 0) { throw 'Source MySQL is not durably stopped/disabled.' }
}

function New-ColdExportManifest([string]$Build, [string]$HostName, [string]$Origin, [string]$AiUrl, $Database, [object[]]$Files) {
    if (-not $Files.Count) { throw 'An empty package cannot be sealed.' }
    $payload = [long](($Files | Measure-Object -Property bytes -Sum).Sum)
    $minimum = [long][Math]::Max([long]250GB, ($payload + [long]30GB))
    return [ordered]@{
        format='probiga.windows-cold-migration.v2';build_sha=$Build;source_host=$HostName;
        created_at=(Get-Date).ToUniversalTime().ToString('o');source_paused=$true;
        production_activation=$false;restore_requested=$false;
        git_origin_fetch_url=$Origin;ai_server_url=$AiUrl;database=$Database;files=$Files;
        payload_bytes=$payload;minimum_target_free_bytes=$minimum;
        manual_gates=@('Original-user QMT broker login','Original-user Codex official login',
            'Original-user DeepSeek login','Verified archived stock/general history continuity',
            'Explicit later resume authorization and coordinated host/database identity rehome',
            'Sole production reverse tunnel and Windows/AI writer ownership')
    }
}

function Assert-ColdExportPythonIdentity([string]$Program, [string]$ExpectedVersion) {
    # Native Windows PowerShell strips embedded quotes in Python -c arguments.
    # Use the interpreter's own version switch and require the packaged version.
    $actual = & $Program --version
    if ($LASTEXITCODE -ne 0 -or ([string]$actual).Trim() -cne $ExpectedVersion) {
        throw 'Source Python version mismatch.'
    }
}

function Invoke-ColdPackageExport {
    Assert-Administrator
    foreach ($value in @($OutputRoot,$SourceLayout,$PauseReceipt)) {
        if (-not $value) { throw 'OutputRoot, SourceLayout and PauseReceipt are required.' }
    }
    $CodeRoot = [IO.Path]::GetFullPath($CodeRoot).TrimEnd('\')
    $OutputRoot = [IO.Path]::GetFullPath($OutputRoot).TrimEnd('\')
    if (Test-Path -LiteralPath $OutputRoot) { throw 'Use a NEW output directory; never overwrite a previous attempt.' }
    foreach ($path in @($SourceLayout,$PauseReceipt,$Python313,$Python314)) { Assert-ColdExportPlainTree $path }
    $layout = Get-Content -LiteralPath $SourceLayout -Raw -Encoding UTF8 | ConvertFrom-Json
    $pause = Get-Content -LiteralPath $PauseReceipt -Raw -Encoding UTF8 | ConvertFrom-Json
    $layoutHash = Get-Sha256 $SourceLayout
    $pauseHash = Get-Sha256 $PauseReceipt
    Assert-ColdExportPause $layout $pause $env:COMPUTERNAME
    if (-not $QmtHome) {
        $candidates = @(Get-ChildItem -LiteralPath 'D:\' -Directory -ErrorAction Stop |
            Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName 'bin.x64\XtItClient.exe') -PathType Leaf })
        if ($candidates.Count -ne 1) { throw 'Specify -QmtHome; exactly one installed QMT directory is required.' }
        $QmtHome = $candidates[0].FullName
    }
    Assert-ColdExportQmtStopped $QmtHome
    foreach ($tree in @($CodeRoot,$QmtHome,$MySqlHome,$CodexBundle)) {
        Assert-ColdExportPlainTree $tree
        $prefix = [IO.Path]::GetFullPath($tree).TrimEnd('\') + '\'
        if (($OutputRoot + '\').StartsWith($prefix,[StringComparison]::OrdinalIgnoreCase)) {
            throw 'Output must be outside all source trees.'
        }
    }
    foreach ($tree in @($SourceProjectRoot,$SourceProductionRoot)) {
        if (-not (Test-Path -LiteralPath $tree -PathType Container)) { throw 'Both known source project roots are required.' }
        $prefix = [IO.Path]::GetFullPath($tree).TrimEnd('\') + '\'
        if (($OutputRoot + '\').StartsWith($prefix,[StringComparison]::OrdinalIgnoreCase)) { throw 'Output must be outside the source project state.' }
        Assert-ColdExportSourceBrowserStopped $tree
    }
    foreach ($entry in @('bin.x64\XtItClient.exe')) {
        if (-not (Test-Path -LiteralPath (Join-Path $QmtHome $entry) -PathType Leaf)) { throw 'Incomplete QMT source.' }
    }
    foreach ($entry in @('codex.exe','codex-code-mode-host.exe','codex-command-runner.exe','codex-windows-sandbox-setup.exe')) {
        if (-not (Test-Path -LiteralPath (Join-Path $CodexBundle $entry) -PathType Leaf)) { throw 'Incomplete Codex CLI source bundle.' }
    }
    $mysqlVersion = & (Join-Path $MySqlHome 'bin\mysqld.exe') '--no-defaults' '--version'
    if ($LASTEXITCODE -ne 0 -or [string]$mysqlVersion -notmatch '\bVer 8\.4\.11\b') {
        throw 'Cold physical migration requires the exact source MySQL 8.4.11 binaries.'
    }
    foreach ($command in @(@('branch','--show-current'),@('rev-parse','HEAD'),@('rev-parse','origin/main'),@('status','--porcelain'))) {
        $result = & git -C $CodeRoot @command
        if ($LASTEXITCODE -ne 0) { throw 'Cannot verify source code revision.' }
        if ($command[0] -eq 'branch') { $branch = [string]$result }
        elseif ($command[1] -eq 'HEAD') { $head = [string]$result }
        elseif ($command[0] -eq 'rev-parse') { $main = [string]$result }
        elseif ($result) { throw 'Source main has uncommitted changes.' }
    }
    if ($branch.Trim() -ne 'main' -or $head.Trim() -ne $main.Trim() -or $head.Trim() -notmatch '^[0-9a-f]{40}$') {
        throw 'Export code only from clean, fetched, merged production main.'
    }
    $head = $head.Trim()
    $origin = & git -C $CodeRoot remote get-url origin
    if ($LASTEXITCODE -ne 0 -or [string]$origin -ne 'https://github.com/MingMG/probiga.git') {
        throw 'Source main must use the exact approved production Git fetch URL.'
    }
    foreach ($version in @(@($Python313,'Python 3.13.14'),@($Python314,'Python 3.14.3'))) {
        Assert-ColdExportPythonIdentity $version[0] $version[1]
    }
    $drive = [IO.Path]::GetPathRoot($OutputRoot)
    if ($drive -notmatch '^[A-Za-z]:\\$') { throw 'Use a local NTFS output drive, not a network share.' }
    $volume = Get-Volume -DriveLetter $drive.Substring(0,1)
    if ($volume.FileSystem -ne 'NTFS' -or $volume.SizeRemaining -lt 250GB) { throw 'Export needs at least 250 GiB free on NTFS.' }
    # Validate all selected private archive sources before creating an output
    # attempt or spending hours copying QMT and the physical database.
    Assert-ColdExportProjectArchiveSources $SourceProjectRoot $SourceProductionRoot
    $historySources = @(Get-ColdExportProductionHistoryFiles)
    if ($env:USERPROFILE) {
        $nodeRoot = Split-Path (Get-ColdExportReportNodeModules) -Parent
        if ((Test-Path -LiteralPath $nodeRoot) -and
            ($OutputRoot + '\').StartsWith(($nodeRoot.TrimEnd('\') + '\'),[StringComparison]::OrdinalIgnoreCase)) {
            throw 'Output must be outside the reviewed source report Node runtime.'
        }
    }
    New-Item -ItemType Directory -Path $OutputRoot | Out-Null
    Protect-LocalPath $OutputRoot
    foreach ($name in @('software','software\installers','wheels313','wheels314','audit','audit\codex-production-threads')) {
        New-Item -ItemType Directory -Path (Join-Path $OutputRoot $name) | Out-Null
    }
    Write-Host '1/6 Build signed offline software and both complete Windows wheelhouses.'
    $artifactReceipts = @()
    foreach ($spec in @(Get-ColdExportArtifacts)) {
        $destination = Join-Path $OutputRoot ('software\installers\' + $spec.name)
        $cached = Join-Path $DownloadCache $spec.name
        if (Test-Path -LiteralPath $cached -PathType Leaf) {
            Assert-ColdExportPlainTree $cached
            Copy-Item -LiteralPath $cached -Destination $destination
        }
        Get-SignedArtifact $spec.url $destination $spec.publisher $spec.sha256
        $artifactReceipts += [ordered]@{name=$spec.name;url=$spec.url;publisher=$spec.publisher;sha256=(Get-Sha256 $destination)}
    }
    Write-Utf8 (Join-Path $OutputRoot 'audit\official-software.json') ($artifactReceipts | ConvertTo-Json -Depth 5)
    Invoke-Checked $Python313 @('-m','pip','--isolated','--cache-dir',(Join-Path $DownloadCache 'pip313-cache'),
        'download','--index-url','https://pypi.org/simple','--only-binary=:all:',
        '--require-hashes','-r',(Join-Path $CodeRoot 'deploy\qmt_windows_requirements.lock'),'-d',(Join-Path $OutputRoot 'wheels313'))
    Invoke-Checked $Python314 @('-m','pip','--isolated','--cache-dir',(Join-Path $DownloadCache 'pip314-cache'),
        'download','--index-url','https://pypi.org/simple','--only-binary=:all:',
        '-r',(Join-Path $CodeRoot 'deploy\windows_app_requirements.txt'),'-d',(Join-Path $OutputRoot 'wheels314'))
    foreach ($wheelhouse in @('wheels313','wheels314')) {
        $wheels = @(Get-ChildItem -LiteralPath (Join-Path $OutputRoot $wheelhouse) -File -Force)
        if (-not $wheels.Count -or @($wheels | Where-Object Extension -ne '.whl').Count) { throw 'Only complete binary wheelhouses are allowed.' }
    }
    Write-Host '2/6 Copy stopped QMT, exact MySQL binaries and the four-executable Codex bundle.'
    Assert-ColdExportPause $layout $pause $env:COMPUTERNAME
    Assert-ColdExportQmtStopped $QmtHome
    Copy-ColdExportTree $QmtHome (Join-Path $OutputRoot 'software\qmt')
    Copy-ColdExportTree $MySqlHome (Join-Path $OutputRoot 'software\mysql84')
    Copy-ColdExportTree $CodexBundle (Join-Path $OutputRoot 'software\codex')
    Write-Host '3/6 Archive merged main and the two exact production histories.'
    Invoke-Checked 'git' @('-C',$CodeRoot,'bundle','create',(Join-Path $OutputRoot 'code.bundle'),'main')
    Invoke-Checked 'git' @('-C',$CodeRoot,'bundle','verify',(Join-Path $OutputRoot 'code.bundle'))
    $bundleHeads = & git -C $CodeRoot bundle list-heads (Join-Path $OutputRoot 'code.bundle')
    if ($LASTEXITCODE -ne 0 -or @($bundleHeads).Count -ne 1 -or
        ([string]$bundleHeads).Trim() -ne ($head + ' refs/heads/main')) { throw 'Code bundle revision changed during export.' }
    foreach ($name in @('package_common.ps1','migrate_target.ps1','target_entry.ps1','start_target_migration.cmd','COLD_README.txt')) {
        $source = Join-Path (Join-Path $CodeRoot 'tools\secondary_edge') $name
        if (-not (Test-Path -LiteralPath $source -PathType Leaf)) { throw "Missing final migration entry: $name" }
        Copy-Item -LiteralPath $source -Destination (Join-Path $OutputRoot $name)
    }
    $currentHistorySources = @(Get-ColdExportProductionHistoryFiles)
    if (($historySources | ConvertTo-Json -Depth 4 -Compress) -cne
        ($currentHistorySources | ConvertTo-Json -Depth 4 -Compress)) {
        throw 'The exact production history file selection changed during export.'
    }
    foreach ($history in $currentHistorySources) {
        $copy = Join-Path (Join-Path $OutputRoot 'audit\codex-production-threads') $history.name
        Copy-ColdExportExactFile $history.source_path $copy | Out-Null
    }
    Copy-Item -LiteralPath $PauseReceipt -Destination (Join-Path $OutputRoot 'audit\source-pause.json')
    Copy-Item -LiteralPath $SourceLayout -Destination (Join-Path $OutputRoot 'audit\source-layout.json')
    Assert-ColdExportPause $layout $pause $env:COMPUTERNAME
    Copy-ColdExportProjectArchive $SourceProjectRoot $SourceProductionRoot $OutputRoot
    Write-Host '4/6 Snapshot the entire stopped database, logs, formal configuration and certificates.'
    Push-Location $CodeRoot
    try {
        Invoke-Checked $Python314 @('-m','tools.secondary_edge.cold_database','snapshot',
            '--source-layout',$SourceLayout,'--destination',(Join-Path $OutputRoot 'database'),'--pause-receipt',$PauseReceipt)
    } finally { Pop-Location }
    $database = Get-Content -LiteralPath (Join-Path $OutputRoot 'database\metadata.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($database.format -ne 'probiga.cold-database-snapshot.v1' -or $database.status -ne 'ready' -or
        $database.production_active -ne $false -or $database.source_automatically_resume -ne $false) {
        throw 'Cold snapshot did not finish with a verified paused receipt.'
    }
    Write-Host '5/6 Seal every file. Large database verification can take hours.'
    $files = @(Get-ChildItem -LiteralPath $OutputRoot -Recurse -Force -File -ErrorAction Stop |
        Sort-Object FullName | ForEach-Object {
            [ordered]@{path=$_.FullName.Substring($OutputRoot.Length+1);bytes=[long]$_.Length;sha256=(Get-Sha256 $_.FullName)}
        })
    $manifest = New-ColdExportManifest $head $env:COMPUTERNAME ([string]$origin) $AiServerUrl $database $files
    Write-Utf8 (Join-Path $OutputRoot 'manifest.json') ($manifest | ConvertTo-Json -Depth 30)
    Assert-ColdExportPause $layout $pause $env:COMPUTERNAME
    Assert-ColdExportQmtStopped $QmtHome
    foreach ($tree in @($SourceProjectRoot,$SourceProductionRoot)) { Assert-ColdExportSourceBrowserStopped $tree }
    if ((Get-Sha256 $SourceLayout) -ne $layoutHash -or (Get-Sha256 $PauseReceipt) -ne $pauseHash) {
        throw 'Source layout/pause receipt changed during export; no seal may be created.'
    }
    $finalHead = & git -C $CodeRoot rev-parse HEAD
    if ($LASTEXITCODE -ne 0 -or ([string]$finalHead).Trim() -ne $head) { throw 'Source Git revision changed during export.' }
    $finalMain = & git -C $CodeRoot rev-parse origin/main
    if ($LASTEXITCODE -ne 0 -or ([string]$finalMain).Trim() -ne $head) { throw 'Source fetched main changed during export.' }
    $finalBranch = & git -C $CodeRoot branch --show-current
    if ($LASTEXITCODE -ne 0 -or ([string]$finalBranch).Trim() -ne 'main') { throw 'Source branch changed during export.' }
    $finalDirty = & git -C $CodeRoot status --porcelain
    if ($LASTEXITCODE -ne 0 -or $finalDirty) { throw 'Source code changed during export.' }
    Write-Utf8 (Join-Path $OutputRoot 'READY') ($head + ' ' + (Get-Sha256 (Join-Path $OutputRoot 'manifest.json')))
    try {
        Assert-ColdPackage $OutputRoot | Out-Null
        Protect-PortablePackageEntry $OutputRoot
    }
    catch {
        Remove-Item -LiteralPath (Join-Path $OutputRoot 'READY') -Force
        throw 'Final package validation failed. The source remains paused; preserve this attempt.'
    }
    Write-Host "6/6 PAUSED MIGRATION PACKAGE: $OutputRoot"
    Write-Host 'Source remains stopped/disabled. No automatic restart or production activation.'
    Write-Host 'This confidential physical copy contains original database account hashes, TLS keys and private data.'
    Write-Host 'Copy the ENTIRE directory to the mobile disk; on the OLD PC run start_target_migration.cmd.'
}

# Dot-sourcing exposes ordinary reusable validation helpers without exporting.
if ($MyInvocation.InvocationName -ne '.') { Invoke-ColdPackageExport }
