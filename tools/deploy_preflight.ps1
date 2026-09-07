# Shared by the registered updater and the reviewed recovery controller (PS 5.1).
# Never writes Git config or changes the caller's environment.
function Protect-DeployDiagnostic([string]$Text) {
    $Text = $Text -replace '(?i)([a-z][a-z0-9+.-]*://)[^\s/@]+@', '$1[REDACTED]@'
    $Text = $Text -replace '(?i)([?&](?:[^=\s&]+)=)[^\s&#]+', '$1[REDACTED]'
    $Text = $Text -replace '(?im)((?:proxy-)?authorization\s*[:=]\s*).*', '$1[REDACTED]'
    $Text = $Text -replace '(?i)((?:password|passwd|token|secret|credential)\s*[:=]\s*)[^\s;,]+', '$1[REDACTED]'
    $Text = $Text -replace '(?i)\b(?:gh[pousr]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+)\b', '[REDACTED]'
    $Text = ($Text -replace '[\r\n\x00-\x1f]+', ' ').Trim()
    if ($Text.Length -gt 1200) { $Text = $Text.Substring(0, 1200) }
    return $Text
}

function ConvertTo-DeployArgument([string]$Value) {
    # CommandLineToArgvW quoting; no shell, expression, or command substitution.
    return '"' + (($Value -replace '(\\*)"', '$1$1\"') -replace '(\\+)$', '$1$1') + '"'
}

function Assert-DeployProxy([string]$GitHubProxy) {
    if (!$GitHubProxy -or $GitHubProxy -ceq 'direct') { return }
    $Uri = $null
    if (![Uri]::TryCreate($GitHubProxy, [UriKind]::Absolute, [ref]$Uri) -or
        $Uri.Scheme -cnotin @('http','https','socks5','socks5h') -or
        !$Uri.Host -or $Uri.UserInfo -or $Uri.Query -or $Uri.Fragment -or
        $GitHubProxy -match '[\s\x00-\x1f]') {
        throw 'deploy_preflight stage=proxy reason=INVALID_PROXY use a proxy URL without credentials or direct'
    }
}

function Invoke-DeployGit {
    param([string]$Root, [string[]]$Arguments, [string]$Stage = 'git',
        [ValidateRange(1, 300)][int]$TimeoutSeconds = 45,
        [string]$GitHubProxy = '', [int[]]$AllowedExitCodes = @(0),
        [switch]$ReturnResult)
    Assert-DeployProxy $GitHubProxy
    $Process = $null; $Started = $false
    try {
        $Executable = @(Get-Command git.exe -CommandType Application -ErrorAction Stop)[0].Source
        $Info = New-Object Diagnostics.ProcessStartInfo
        $Info.FileName = $Executable
        $Info.WorkingDirectory = $Root
        $Info.UseShellExecute = $false
        $Info.CreateNoWindow = $true
        $Info.RedirectStandardOutput = $true
        $Info.RedirectStandardError = $true
        $Info.StandardOutputEncoding = [Text.Encoding]::UTF8
        $Info.StandardErrorEncoding = [Text.Encoding]::UTF8
        $Info.EnvironmentVariables['GIT_TERMINAL_PROMPT'] = '0'
        $Info.EnvironmentVariables['GCM_INTERACTIVE'] = 'Never'
        # Diagnostics must not inherit verbose HTTP headers or TLS bypasses.
        foreach ($Key in @('GIT_TRACE','GIT_TRACE_CURL','GIT_CURL_VERBOSE','GIT_SSL_NO_VERIFY')) {
            $Info.EnvironmentVariables.Remove($Key)
        }
        $Options = @('-c', 'http.sslVerify=true', '-c', 'http.lowSpeedLimit=1024',
            '-c', 'http.lowSpeedTime=15', '-c', 'credential.interactive=false',
            '-c', 'http.https://github.com/MingMG/probiga.git.sslVerify=true')
        if ($GitHubProxy) {
            $Proxy = if ($GitHubProxy -ceq 'direct') { '' } else { $GitHubProxy }
            # An exact repository URL beats more-specific persistent http.proxy;
            # remote.origin.proxy otherwise takes precedence over http.proxy.
            $Options += @('-c', "http.https://github.com/MingMG/probiga.git.proxy=$Proxy",
                '-c', "remote.origin.proxy=$Proxy")
            if ($Proxy) { $Info.EnvironmentVariables['NO_PROXY'] = '' }
        }
        $Info.Arguments = (@($Options + @('-C', $Root) + $Arguments) |
            ForEach-Object { ConvertTo-DeployArgument $_ }) -join ' '
        $Process = New-Object Diagnostics.Process
        $Process.StartInfo = $Info
        if (!$Process.Start()) { throw 'git process did not start' }
        $Started = $true
        $OutputTask = $Process.StandardOutput.ReadToEndAsync()
        $ErrorTask = $Process.StandardError.ReadToEndAsync()
        if (!$Process.WaitForExit($TimeoutSeconds * 1000)) {
            throw "deploy_preflight stage=$Stage reason=TIMEOUT timeout_seconds=$TimeoutSeconds"
        }
        # Git helpers can inherit the output handles. Bound their drain as well.
        if (!$OutputTask.Wait(1000) -or !$ErrorTask.Wait(1000)) {
            throw "deploy_preflight stage=$Stage reason=OUTPUT_TIMEOUT"
        }
        $Output = $OutputTask.Result.Trim()
        $ErrorText = Protect-DeployDiagnostic $ErrorTask.Result
        $Code = $Process.ExitCode
        if ($Code -notin $AllowedExitCodes) {
            $Reason = switch -Regex ($ErrorText) {
                '(?i)proxy|CONNECT tunnel' { 'PROXY'; break }
                '(?i)authentication|permission denied \(publickey\)|could not read Username|invalid credentials|403|401' { 'AUTHENTICATION'; break }
                '(?i)certificate|SSL|TLS|host key verification' { 'TLS_OR_HOST_KEY'; break }
                '(?i)resolve host|resolve hostname' { 'DNS'; break }
                '(?i)timed out|timeout|operation too slow' { 'TIMEOUT'; break }
                '(?i)connect|connection|network|reset|unable to access' { 'NETWORK'; break }
                '(?i)permission denied|access is denied|dubious ownership' { 'PERMISSION'; break }
                '(?i)not a git repository|bad revision|unknown revision|not a valid object' { 'REPOSITORY'; break }
                default { 'GIT_FAILED' }
            }
            throw "deploy_preflight stage=$Stage reason=$Reason exit=$Code detail=$ErrorText"
        }
        if ($ReturnResult) { return [pscustomobject]@{ Output=$Output; ExitCode=$Code } }
        return $Output
    }
    catch {
        $Message = Protect-DeployDiagnostic $_.Exception.Message
        if ($Message -notlike 'deploy_preflight *') {
            $Message = "deploy_preflight stage=$Stage reason=PROCESS_START_OR_IO detail=$Message"
        }
        throw $Message
    }
    finally {
        if ($null -ne $Process) {
            try {
                if ($Started -and !$Process.HasExited) {
                    # Only the process tree created above is terminated; no name matching.
                    $TaskKill = Join-Path ([Environment]::GetFolderPath('System')) 'taskkill.exe'
                    & $TaskKill /PID $Process.Id /T /F 2>&1 | Out-Null
                    if (!$Process.HasExited) { $Process.Kill() }
                    $null = $Process.WaitForExit(3000)
                }
            } finally { $Process.Dispose() }
        }
    }
}

function Write-DeployGitContext([string]$Root, [string]$Stage, [string]$GitHubProxy = '') {
    $RemoteProxy = Invoke-DeployGit -Root $Root -Stage "$Stage.proxy.remote" `
        -Arguments @('config','--get','remote.origin.proxy') -AllowedExitCodes @(0,1) -ReturnResult
    $HttpProxy = Invoke-DeployGit -Root $Root -Stage "$Stage.proxy.http" `
        -Arguments @('config','--get-urlmatch','http.proxy','https://github.com/MingMG/probiga.git') `
        -AllowedExitCodes @(0,1) -ReturnResult
    $Source = 'environment'; $Value = 'direct'
    foreach ($Name in @('ALL_PROXY','HTTPS_PROXY')) {
        $Candidate = [Environment]::GetEnvironmentVariable($Name)
        if ($Candidate) { $Value = $Candidate }
    }
    if ($HttpProxy.ExitCode -eq 0) { $Source='git.http'; $Value=$HttpProxy.Output }
    if ($RemoteProxy.ExitCode -eq 0) { $Source='git.remote.origin'; $Value=$RemoteProxy.Output }
    if ($GitHubProxy) { $Source='invocation'; $Value=$GitHubProxy }
    if (!$Value) { $Value='direct' }
    $Bypass = if ([Environment]::GetEnvironmentVariable('NO_PROXY') -and
        (!$GitHubProxy -or $GitHubProxy -ceq 'direct')) { 'present' } else { 'absent' }
    $Version = Invoke-DeployGit -Root $Root -Arguments @('--version') -Stage "$Stage.version"
    [Console]::Error.WriteLine((Protect-DeployDiagnostic (
        "deploy_preflight stage=$Stage host=$([Environment]::MachineName) git=$Version proxy_source=$Source proxy=$Value no_proxy=$Bypass")))
}

function Assert-DeployAdministrator() {
    $Identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    try {
        $Principal = New-Object Security.Principal.WindowsPrincipal($Identity)
        if (!$Principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
            throw 'deploy_preflight stage=permissions.token reason=ELEVATION_REQUIRED use the standard UAC launcher'
        }
    } finally { $Identity.Dispose() }
}

function Assert-DeployTaskAccess([string[]]$TaskNames) {
    # Task Scheduler documents file read/write/execute rights for querying,
    # changing and running a task. AccessCheck uses the actual current token,
    # including deny ACEs and disabled groups; querying alone proves nothing.
    if (!('ProBigADeployAccess' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;
public static class ProBigADeployAccess {
    [StructLayout(LayoutKind.Sequential)] struct Mapping { public uint Read, Write, Execute, All; }
    [DllImport("advapi32.dll", SetLastError=true)] static extern bool DuplicateToken(IntPtr token, int level, out IntPtr copy);
    [DllImport("advapi32.dll", SetLastError=true)] static extern bool AccessCheck(byte[] sd, IntPtr token, uint desired, ref Mapping mapping, IntPtr privileges, ref uint length, out uint granted, out bool allowed);
    [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr value);
    public static bool CanManage(string sddl) { return CanAccess(sddl, 0x1201bf); }
    public static bool CanAccess(string sddl, uint desired) {
        var sd = new RawSecurityDescriptor(sddl); byte[] bytes = new byte[sd.BinaryLength]; sd.GetBinaryForm(bytes,0);
        using (var identity = WindowsIdentity.GetCurrent()) {
            IntPtr token; if (!DuplicateToken(identity.Token, 2, out token)) throw new Win32Exception();
            IntPtr privileges = Marshal.AllocHGlobal(4096);
            try {
                var mapping = new Mapping { Read=0x120089, Write=0x120116, Execute=0x1200a0, All=0x1f01ff };
                uint size=4096, granted; bool allowed;
                if (!AccessCheck(bytes,token,desired,ref mapping,privileges,ref size,out granted,out allowed)) throw new Win32Exception();
                return allowed;
            } finally { Marshal.FreeHGlobal(privileges); CloseHandle(token); }
        }
    }
}
'@
    }
    foreach ($Name in $TaskNames) {
        try {
            $Service = New-Object -ComObject 'Schedule.Service'
            $Service.Connect()
            $Task = $Service.GetFolder('\').GetTask($Name)
            if (![ProBigADeployAccess]::CanManage($Task.GetSecurityDescriptor(7))) {
                throw 'task DACL does not allow read/write/execute'
            }
        } catch {
            throw "deploy_preflight stage=permissions.task reason=TASK_ACCESS_DENIED task=$Name detail=$(Protect-DeployDiagnostic $_.Exception.Message)"
        }
    }
}

function Assert-DeployDirectoryWritable([string]$Path, [string]$Stage) {
    $Probe = Join-Path $Path ('.deploy-preflight-' + [Guid]::NewGuid().ToString('N'))
    try {
        $Stream = [IO.FileStream]::new($Probe, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write,
            [IO.FileShare]::None, 1, [IO.FileOptions]::DeleteOnClose)
        $Stream.Dispose()
    } catch {
        throw "deploy_preflight stage=$Stage reason=WRITE_ACCESS_DENIED detail=$(Protect-DeployDiagnostic $_.Exception.Message)"
    }
}

function Assert-DeployStateDirectoryAccess([string]$Path) {
    Assert-DeployTaskAccess @() # Initialize the same native access checker.
    $Existing = [IO.Path]::GetFullPath($Path)
    while (!(Test-Path -LiteralPath $Existing)) { $Existing = [IO.Path]::GetDirectoryName($Existing) }
    $Item = Get-Item -LiteralPath $Existing -Force
    if (!$Item.PSIsContainer -or ($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw 'deploy_preflight stage=permissions.state-directory reason=UNSAFE_DIRECTORY'
    }
    $Sddl = (Get-Acl -LiteralPath $Existing).GetSecurityDescriptorSddlForm([Security.AccessControl.AccessControlSections]::All)
    # Existing installer creates state files/directories and sets their DACL.
    # Check both abilities before stopping any task; never rewrite the ACL here.
    if (![ProBigADeployAccess]::CanAccess($Sddl, 0x1601bf)) {
        throw "deploy_preflight stage=permissions.state-directory reason=STATE_DIRECTORY_ACCESS_DENIED path=$Existing"
    }
}

function Assert-DeployRepositoryIdle([string]$Root, [int]$TimeoutSeconds = 45) {
    foreach ($Name in @('index.lock','HEAD.lock','refs/heads/main.lock','packed-refs.lock')) {
        $Lock = Invoke-DeployGit -Root $Root -Arguments @('rev-parse','--git-path',$Name) `
            -Stage 'repository.lock-path' -TimeoutSeconds $TimeoutSeconds
        if (![IO.Path]::IsPathRooted($Lock)) { $Lock = Join-Path $Root $Lock }
        if (Test-Path -LiteralPath $Lock) {
            throw "deploy_preflight stage=repository.locks reason=REPOSITORY_BUSY lock=$Name existing lock was retained"
        }
    }
}
