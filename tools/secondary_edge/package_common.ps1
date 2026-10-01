Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Initialize-ColdMigrationPowerNative {
    if ('ProBigA.ColdMigration.PowerNative' -as [type]) { return }
    # Windows PS5/.NET4 C# only. The explicit union contains the complete
    # Detailed layout so REASON_CONTEXT is 24 bytes on x86 and 32 on x64.
    # All queried power settings are read-only; no power plan is modified.
    Add-Type -Language CSharp -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace ProBigA.ColdMigration {
    [StructLayout(LayoutKind.Sequential)]
    public struct PowerStatus {
        public byte ACLineStatus;
        public byte BatteryFlag;
        public byte BatteryLifePercent;
        public byte SystemStatusFlag;
        public uint BatteryLifeTime;
        public uint BatteryFullLifeTime;
    }
    [StructLayout(LayoutKind.Sequential)]
    public struct DetailedPowerReason {
        public IntPtr LocalizedReasonModule;
        public uint LocalizedReasonId;
        public uint ReasonStringCount;
        public IntPtr ReasonStrings;
    }
    [StructLayout(LayoutKind.Explicit)]
    public struct PowerReasonUnion {
        [FieldOffset(0)] public IntPtr SimpleReasonString;
        [FieldOffset(0)] public DetailedPowerReason Detailed;
    }
    [StructLayout(LayoutKind.Sequential)]
    public struct PowerReasonContext {
        public uint Version;
        public uint Flags;
        public PowerReasonUnion Reason;
    }
    public sealed class PowerRequestHandle : SafeHandleZeroOrMinusOneIsInvalid {
        private bool active;
        public int ClearError { get; private set; }
        public int CloseError { get; private set; }
        public bool ReleaseSucceeded { get; private set; }
        public bool RequestActive { get { return active && !IsClosed; } }
        internal PowerRequestHandle(IntPtr value) : base(true) { SetHandle(value); }
        internal void MarkActive() { active = true; }
        protected override bool ReleaseHandle() {
            bool cleared = true;
            bool closed = false;
            try {
                if (active) {
                    cleared = PowerNative.PowerClearRequest(handle, 1);
                    if (!cleared) ClearError = Marshal.GetLastWin32Error();
                    active = false;
                }
            } finally {
                closed = PowerNative.CloseHandle(handle);
                if (!closed) CloseError = Marshal.GetLastWin32Error();
            }
            ReleaseSucceeded = cleared && closed;
            return ReleaseSucceeded;
        }
    }
    public static class PowerNative {
        [DllImport("kernel32.dll", SetLastError=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetSystemPowerStatus(out PowerStatus status);
        [DllImport("kernel32.dll", SetLastError=true)]
        private static extern IntPtr PowerCreateRequest(ref PowerReasonContext context);
        [DllImport("kernel32.dll", SetLastError=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool PowerSetRequest(PowerRequestHandle request, int requestType);
        [DllImport("kernel32.dll", SetLastError=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static extern bool PowerClearRequest(IntPtr request, int requestType);
        [DllImport("kernel32.dll", SetLastError=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static extern bool CloseHandle(IntPtr handle);
        [DllImport("powrprof.dll")]
        private static extern uint PowerGetActiveScheme(IntPtr userRoot, out IntPtr scheme);
        [DllImport("powrprof.dll")]
        private static extern uint PowerReadACValueIndex(IntPtr root, ref Guid scheme,
            ref Guid subgroup, ref Guid setting, out uint value);
        [DllImport("kernel32.dll", SetLastError=true)]
        private static extern IntPtr LocalFree(IntPtr memory);

        public static byte GetAcLineStatus() {
            PowerStatus status;
            if (!GetSystemPowerStatus(out status))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "MIGRATION_POWER_STATUS_API_FAILED");
            return status.ACLineStatus;
        }
        public static uint GetAcSystemRequiredPolicy() {
            IntPtr memory = IntPtr.Zero;
            try {
                uint result = PowerGetActiveScheme(IntPtr.Zero, out memory);
                if (result != 0 || memory == IntPtr.Zero)
                    throw new Win32Exception((int)result, "MIGRATION_POWER_SCHEME_API_FAILED");
                Guid scheme = (Guid)Marshal.PtrToStructure(memory, typeof(Guid));
                Guid subgroup = new Guid("238c9fa8-0aad-41ed-83f4-97be242c8f20");
                Guid setting = new Guid("a4b195f5-8225-47d8-8012-9d41369786e2");
                uint value;
                result = PowerReadACValueIndex(IntPtr.Zero, ref scheme, ref subgroup, ref setting, out value);
                if (result != 0)
                    throw new Win32Exception((int)result, "MIGRATION_POWER_POLICY_API_FAILED");
                return value;
            } finally {
                if (memory != IntPtr.Zero && LocalFree(memory) != IntPtr.Zero)
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "MIGRATION_POWER_SCHEME_RELEASE_FAILED");
            }
        }
        public static PowerRequestHandle Acquire() {
            // Recheck natively even if a caller invokes this class directly.
            if (GetAcLineStatus() != 1) throw new InvalidOperationException("MIGRATION_AC_POWER_REQUIRED");
            if (GetAcSystemRequiredPolicy() != 1)
                throw new InvalidOperationException("MIGRATION_POWER_POLICY_REFUSES_SYSTEM_REQUESTS");
            IntPtr text = Marshal.StringToHGlobalUni("ProBigA cold migration: verified offline installation");
            PowerRequestHandle request = null;
            try {
                PowerReasonContext context = new PowerReasonContext();
                context.Version = 0; // POWER_REQUEST_CONTEXT_VERSION
                context.Flags = 1; // POWER_REQUEST_CONTEXT_SIMPLE_STRING
                context.Reason.SimpleReasonString = text;
                IntPtr value = PowerCreateRequest(ref context);
                if (value == IntPtr.Zero || value == new IntPtr(-1))
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "MIGRATION_POWER_CREATE_API_FAILED");
                request = new PowerRequestHandle(value);
                if (!PowerSetRequest(request, 1)) // PowerRequestSystemRequired
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "MIGRATION_POWER_SET_API_FAILED");
                request.MarkActive();
                if (GetAcLineStatus() != 1) throw new InvalidOperationException("MIGRATION_AC_POWER_REQUIRED");
                if (GetAcSystemRequiredPolicy() != 1)
                    throw new InvalidOperationException("MIGRATION_POWER_POLICY_REFUSES_SYSTEM_REQUESTS");
                return request;
            } catch {
                if (request != null) request.Dispose();
                throw;
            } finally { Marshal.FreeHGlobal(text); }
        }
    }
}
'@
}

function Get-ColdMigrationAcLineStatus {
    Initialize-ColdMigrationPowerNative
    return [ProBigA.ColdMigration.PowerNative]::GetAcLineStatus()
}

function Get-ColdMigrationAcPolicy {
    Initialize-ColdMigrationPowerNative
    return [ProBigA.ColdMigration.PowerNative]::GetAcSystemRequiredPolicy()
}

function Assert-ColdMigrationPowerPrerequisites {
    if ((Get-ColdMigrationAcLineStatus) -ne 1) {
        Write-Host 'Connect this PC to AC power. Battery or unknown power status cannot start this migration.'
        throw 'MIGRATION_AC_POWER_REQUIRED'
    }
    if ((Get-ColdMigrationAcPolicy) -ne 1) {
        Write-Host 'The active AC power plan does not accept system-required requests. Migration is blocked; no power plan was changed.'
        throw 'MIGRATION_POWER_POLICY_REFUSES_SYSTEM_REQUESTS'
    }
}

function New-ColdMigrationNativePowerRequest {
    Initialize-ColdMigrationPowerNative
    return [ProBigA.ColdMigration.PowerNative]::Acquire()
}

function New-ColdMigrationPowerLease {
    Assert-ColdMigrationPowerPrerequisites
    $lease = $null
    try {
        $lease = New-ColdMigrationNativePowerRequest
        if (-not $lease) { throw 'MIGRATION_POWER_REQUEST_NOT_ACQUIRED' }
        Assert-ColdMigrationPowerPrerequisites
        return $lease
    } catch {
        if ($lease) { $lease.Dispose() }
        throw
    }
}

function Remove-ColdMigrationPowerLease($Lease) {
    if (-not $Lease) { return }
    $Lease.Dispose()
    if (-not $Lease.ReleaseSucceeded -or $Lease.ClearError -ne 0 -or $Lease.CloseError -ne 0) {
        Write-Warning 'MIGRATION_POWER_REQUEST_RELEASE_REQUIRES_ATTENTION'
    }
}

function Initialize-ColdMigrationShaNative {
    if ('ProBigA.ColdMigration.ShaNative' -as [type]) { return }
    # One hashing implementation for every caller. A reusable buffer avoids
    # allocating a megabyte for each of the many small QMT files. Observers run
    # on this same thread, never on a background thread without a PS runspace.
    Add-Type -Language CSharp -TypeDefinition @'
using System;
using System.IO;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Security.Cryptography;
namespace ProBigA.ColdMigration {
    public static class ShaNative {
        private static readonly ConcurrentBag<byte[]> buffers = new ConcurrentBag<byte[]>();
        public static string Compute(Stream stream, Action<long> progress) {
            if (stream == null) throw new ArgumentNullException("stream");
            byte[] buffer;
            if (!buffers.TryTake(out buffer)) buffer = new byte[1024 * 1024];
            try {
                using (SHA256 sha = SHA256.Create()) {
                    long readBytes = 0;
                    Stopwatch clock = Stopwatch.StartNew();
                    int count;
                    while ((count = stream.Read(buffer, 0, buffer.Length)) != 0) {
                        sha.TransformBlock(buffer, 0, count, buffer, 0);
                        readBytes = checked(readBytes + count);
                        if (progress != null && clock.ElapsedMilliseconds >= 2000) {
                            progress(readBytes);
                            clock.Restart();
                        }
                    }
                    sha.TransformFinalBlock(new byte[0], 0, 0);
                    return BitConverter.ToString(sha.Hash).Replace("-", "");
                }
            } finally { buffers.Add(buffer); }
        }
    }
}
'@
}

function Get-Sha256([string]$Path, [scriptblock]$HashProgressAction) {
    Initialize-ColdMigrationShaNative
    $stream = [IO.File]::OpenRead($Path)
    try {
        $observer = $null
        if ($HashProgressAction) {
            # Compute invokes this synchronously while the current function and
            # its caller are still active. Do not create a dynamic-module
            # closure: it loses functions dot-sourced into a script's scope.
            $observer = [Action[long]]{
                param([long]$readBytes)
                & $HashProgressAction $readBytes | Out-Null
            }
        }
        return [ProBigA.ColdMigration.ShaNative]::Compute($stream, $observer)
    } finally { $stream.Dispose() }
}

function Invoke-ColdPackageProgress {
    param([scriptblock]$ProgressAction,
        [ValidateSet('inventory-scanning','inventory-refresh','hashing-files',
            'final-inventory-scanning','final-inventory-refresh','verified')][string]$Phase,
        [long]$BytesRead=0, [long]$TotalBytes=0, [int]$ValidFiles=0, [int]$TotalFiles=0)
    # Observation is not a receipt or an authorization gate. Never include a
    # file name, native output or exception text, and never pollute the return.
    if ($ProgressAction) {
        & $ProgressAction $Phase $BytesRead $TotalBytes $ValidFiles $TotalFiles | Out-Null
    }
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
        # READY is created only after all content/identity/ACL checks finish.
        if ($name -eq 'READY' -and -not (Test-Path -LiteralPath $path)) { continue }
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

function Get-ColdPackagePlainInventory([string]$Root, [switch]$ExcludeReady,
        [scriptblock]$ProgressAction, [ValidateSet('initial','final')][string]$InventoryKind='initial') {
    $rootFull = [IO.Path]::GetFullPath($Root).TrimEnd('\') + '\'
    $ancestor = Get-Item -LiteralPath $rootFull.TrimEnd('\') -Force -ErrorAction Stop
    if ($ancestor -isnot [IO.DirectoryInfo]) { throw 'Cold package root must be an ordinary directory.' }
    while ($ancestor) {
        $ancestor.Refresh()
        if ($ancestor.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Cold package has an unsafe ancestor.' }
        $ancestor = $ancestor.Parent
    }
    # Every directory counts, including empty junctions. Refresh actual metadata
    # rather than trusting stale NTFS child entries from parent enumeration.
    $scanPhase = if ($InventoryKind -eq 'final') { 'final-inventory-scanning' } else { 'inventory-scanning' }
    $refreshPhase = if ($InventoryKind -eq 'final') { 'final-inventory-refresh' } else { 'inventory-refresh' }
    $inventoryClock = [Diagnostics.Stopwatch]::StartNew()
    $inventoryCount = 0
    Invoke-ColdPackageProgress $ProgressAction $scanPhase
    $entries = @(Get-ChildItem -LiteralPath $rootFull -Recurse -Force -ErrorAction Stop | ForEach-Object {
        $inventoryCount++
        if ($ProgressAction -and $inventoryClock.ElapsedMilliseconds -ge 2000) {
            Invoke-ColdPackageProgress $ProgressAction $scanPhase 0 0 $inventoryCount 0
            $inventoryClock.Restart()
        }
        $_
    })
    if (@($entries | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint }).Count) {
        throw 'Cold package contains a reparse point.'
    }
    $inventoryCount = 0
    Invoke-ColdPackageProgress $ProgressAction $refreshPhase 0 0 0 $entries.Count
    foreach ($entry in @($entries | Sort-Object FullName)) {
        $entry.Refresh()
        if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Cold package contains a refreshed reparse point.' }
        $relative = $entry.FullName.Substring($rootFull.Length)
        # Exclusion is only for the publisher's newly created, locked root seal.
        # Its path/reparse safety was still checked above, before skipping it.
        if ($ExcludeReady -and $relative -ieq 'READY') { continue }
        $inventoryCount++
        if ($ProgressAction -and $inventoryClock.ElapsedMilliseconds -ge 2000) {
            Invoke-ColdPackageProgress $ProgressAction $refreshPhase 0 0 $inventoryCount $entries.Count
            $inventoryClock.Restart()
        }
        [pscustomobject][ordered]@{path=$relative;directory=$entry.PSIsContainer;
            bytes=$(if ($entry.PSIsContainer) { [long]0 } else { [long]$entry.Length });modified=$entry.LastWriteTimeUtc.Ticks}
    }
}

function Assert-ColdPackageInventoryStable([object[]]$Expected, [object[]]$Actual) {
    if ($Expected.Count -ne $Actual.Count) { throw 'Cold package inventory changed during verification.' }
    for ($index=0; $index -lt $Expected.Count; $index++) {
        $before = $Expected[$index]; $after = $Actual[$index]
        if ($before.path -cne $after.path -or $before.directory -ne $after.directory -or
            $before.bytes -ne $after.bytes -or $before.modified -ne $after.modified) {
            throw ('Cold package inventory changed during verification: '+$before.path)
        }
    }
}

function Assert-ColdPackageContents([string]$Root, [scriptblock]$ProgressAction) {
    $rootFull = [IO.Path]::GetFullPath($Root).TrimEnd('\') + '\'
    $manifestPath = Join-Path $rootFull 'manifest.json'
    foreach ($path in @($rootFull.TrimEnd('\'),$manifestPath)) {
        if (-not (Test-Path -LiteralPath $path) -or
            ((Get-Item -LiteralPath $path -Force).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw 'Cold package is incomplete or has an unsafe seal path.'
        }
    }
    $initialManifestHash = Get-Sha256 $manifestPath
    $initialInventory = @(Get-ColdPackagePlainInventory $Root -ProgressAction $ProgressAction)
    $manifest = Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($manifest.format -ne 'probiga.windows-cold-migration.v2' -or
        $manifest.build_sha -notmatch '^[0-9a-f]{40}$' -or -not $manifest.source_host -or
        $manifest.source_paused -ne $true -or $manifest.production_activation -ne $false -or
        $manifest.restore_requested -ne $false -or $manifest.minimum_target_free_bytes -lt 250GB) {
        throw 'Invalid paused cold migration manifest.'
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
    [long]$progressTotal = 0
    foreach ($row in @($manifest.files)) {
        [long]$rowBytes = $row.bytes
        if ($rowBytes -lt 0 -or $progressTotal -gt ([long]::MaxValue - $rowBytes)) {
            throw 'Invalid cold package payload byte count.'
        }
        $progressTotal += $rowBytes
    }
    $progressState = [pscustomobject]@{Action=$ProgressAction;CompletedBytes=[long]0;
        TotalBytes=$progressTotal;ValidFiles=0;TotalFiles=@($manifest.files).Count}
    $fileProgress = $null
    if ($ProgressAction) {
        # This callback cannot outlive Assert-ColdPackageContents: the native
        # reader invokes it synchronously inside the active Get-Sha256 call.
        $fileProgress = {
            param([long]$inFileBytes)
            Invoke-ColdPackageProgress $progressState.Action 'hashing-files' `
                ([long]$progressState.CompletedBytes + $inFileBytes) $progressState.TotalBytes `
                $progressState.ValidFiles $progressState.TotalFiles
        }
    }
    $fileProgressClock = [Diagnostics.Stopwatch]::StartNew()
    Invoke-ColdPackageProgress $ProgressAction 'hashing-files' 0 $progressTotal 0 $progressState.TotalFiles
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
            (Get-Sha256 $full -HashProgressAction $fileProgress) -ine $file.sha256) { throw ('Cold package corruption: '+$relative) }
        $measured += [long]$file.bytes
        $progressState.CompletedBytes = $measured
        $progressState.ValidFiles++
        if ($ProgressAction -and $fileProgressClock.ElapsedMilliseconds -ge 2000) {
            Invoke-ColdPackageProgress $ProgressAction 'hashing-files' $measured $progressTotal `
                $progressState.ValidFiles $progressState.TotalFiles
            $fileProgressClock.Restart()
        }
    }
    foreach ($relative in $required) { if (-not $seen.Contains($relative)) { throw ('Unsealed cold package prerequisite: '+$relative) } }
    foreach ($house in @('wheels313/','wheels314/')) {
        if (-not @($seen | Where-Object { $_.StartsWith($house,[StringComparison]::OrdinalIgnoreCase) -and $_.EndsWith('.whl') }).Count) {
            throw 'A sealed offline wheelhouse is missing.'
        }
    }
    if ($manifest.minimum_target_free_bytes -lt $measured + 30GB) { throw 'Cold package target capacity estimate is incomplete.' }
    # Reject additional files rather than executing code that was not sealed.
    foreach ($actual in @($initialInventory | Where-Object { -not $_.directory })) {
        $relative = $actual.path.Replace('\','/')
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
    Invoke-ColdPackageProgress $ProgressAction 'hashing-files' $measured $progressTotal `
        $progressState.ValidFiles $progressState.TotalFiles
    $finalInventory = @(Get-ColdPackagePlainInventory $Root -ProgressAction $ProgressAction -InventoryKind 'final')
    Assert-ColdPackageInventoryStable $initialInventory $finalInventory
    if ((Get-Sha256 $manifestPath) -cne $initialManifestHash) { throw 'Cold package manifest changed during verification.' }
    return $manifest
}

function Assert-ColdPackage([string]$Root, [scriptblock]$ProgressAction) {
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
    $manifestHash = Get-Sha256 $manifestPath
    $seal = $manifest.build_sha + ' ' + $manifestHash
    if ((Get-Content -LiteralPath $readyPath -Raw -Encoding UTF8).Trim() -cne $seal) {
        throw 'Cold package manifest seal does not match.'
    }
    $verified = Assert-ColdPackageContents $Root -ProgressAction $ProgressAction
    if ((Get-Sha256 $manifestPath) -cne $manifestHash -or
        ((Get-Item -LiteralPath $readyPath -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) -or
        (Get-Content -LiteralPath $readyPath -Raw -Encoding UTF8).Trim() -cne $seal) {
        throw 'Cold package seal changed during verification.'
    }
    [long]$verifiedBytes = 0
    foreach ($row in @($verified.files)) { $verifiedBytes += [long]$row.bytes }
    Invoke-ColdPackageProgress $ProgressAction 'verified' $verifiedBytes $verifiedBytes `
        @($verified.files).Count @($verified.files).Count
    return $verified
}
