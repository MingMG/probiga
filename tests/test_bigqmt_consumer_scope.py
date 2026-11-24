"""Exercise the narrow controller with temporary PID records and fake processes."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _probe(tmp_path, scenario):
    shell = shutil.which("powershell.exe")
    if not shell:
        pytest.skip("Windows PowerShell required")
    script = tmp_path / "scope.ps1"
    script.write_text(r'''
param([string]$Source,[string]$WorkspacePath,[string]$Scenario)
$ErrorActionPreference='Stop'
$Root=Join-Path $WorkspacePath 'production';$DataDir=Join-Path $Root 'data'
$duplicate=Join-Path $WorkspacePath 'development'
New-Item -ItemType Directory -Force $DataDir,(Join-Path $duplicate 'data'),(Join-Path $Root 'runtime/qmt-py313/Scripts')|Out-Null
[IO.File]::WriteAllText((Join-Path $Root 'runtime/qmt-py313/Scripts/python.exe'),'unused')
$original="OTHER_FLAG=keep`r`nBIG_QMT_BRIDGE_ENABLED=true`r`nBIG_QMT_STRATEGY_AUTO_RECOVER=true`r`n"
[IO.File]::WriteAllText((Join-Path $Root '.env'),$original)
[IO.File]::WriteAllText((Join-Path $duplicate '.env'),$original)
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($Source,[ref]$tokens,[ref]$errors)
if($errors.Count){throw ($errors|Out-String)}
$names=@('Get-ManagedPidPath','Get-ManagedProcess','Set-ManagedProcess','Get-ServiceKeyFromScriptName',
 'Enter-BigQmtConsumerControl','Exit-BigQmtConsumerControl','Assert-BigQmtConsumerIdentity',
 'New-PrivateConsumerEnvFile','Disable-BigQmtConsumer','Test-BigQmtBridgeEnabled','Get-ConsumerCheckoutBuild',
 'Stop-ManagedProcess','Ensure-Process','Assert-ConsumerProductionRoot','Invoke-BigQmtConsumerScope')
foreach($name in $names){
 $node=$ast.Find({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name},$true)
 . ([scriptblock]::Create($node.Extent.Text))
}
if($Scenario -eq 'mutex'){
 Add-Type -TypeDefinition @'
using System;
using System.Threading;
public static class ConsumerMutexCheck {
 public static bool CanAcquire(Mutex mutex) {
  bool acquired=false;
  var thread=new Thread(()=>{ acquired=mutex.WaitOne(0); if(acquired)mutex.ReleaseMutex(); });
  thread.Start();thread.Join();return acquired;
 }
}
'@
 $first=Enter-BigQmtConsumerControl
 try{
  $Root=$Root.ToUpperInvariant();$same=Enter-BigQmtConsumerControl
  $same.ReleaseMutex()
  try{$blocked= ![ConsumerMutexCheck]::CanAcquire($same)}finally{$same.Dispose()}
  $Root=Join-Path $WorkspacePath 'other';$other=Enter-BigQmtConsumerControl
  $other.ReleaseMutex()
  try{$independent=[ConsumerMutexCheck]::CanAcquire($other)}finally{$other.Dispose()}
  @{same_root_blocked=$blocked;other_root_independent=$independent}|ConvertTo-Json -Compress
 }finally{Exit-BigQmtConsumerControl $first}
 return
}
$build='a'*40;$old='b'*40;$AuthorityRoot=$Root
$script:TempAclMatched=$false;$script:TempBeforeBytes=-1
if($Scenario -eq 'private_acl'){
 $everyone=[Security.Principal.SecurityIdentifier]::new('S-1-1-0')
 $parentAcl=[IO.Directory]::GetAccessControl($duplicate)
 $parentAcl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($everyone,
  [Security.AccessControl.FileSystemRights]::ReadAndExecute,
  ([Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [Security.AccessControl.InheritanceFlags]::ObjectInherit),
  [Security.AccessControl.PropagationFlags]::None,[Security.AccessControl.AccessControlType]::Allow))
 [IO.Directory]::SetAccessControl($duplicate,$parentAcl)
 $private=[Security.AccessControl.FileSecurity]::new()
 $private.SetAccessRuleProtection($true,$false)
 $sid=[Security.Principal.WindowsIdentity]::GetCurrent().User
 $private.SetOwner($sid)
 $private.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','Allow'))
 [IO.File]::SetAccessControl((Join-Path $duplicate '.env'),$private)
 $script:AclSections=[Security.AccessControl.AccessControlSections]::Access -bor [Security.AccessControl.AccessControlSections]::Owner
 $script:ExpectedAcl=([IO.File]::GetAccessControl((Join-Path $duplicate '.env'))).GetSecurityDescriptorSddlForm($script:AclSections)
 function Get-AclProof($acl){
  $rules=@($acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])|ForEach-Object{
   $_.IdentityReference.Value+'|'+$_.FileSystemRights+'|'+$_.AccessControlType+'|'+$_.IsInherited+'|'+$_.InheritanceFlags+'|'+$_.PropagationFlags
  }|Sort-Object)
  return $acl.GetOwner([Security.Principal.SecurityIdentifier]).Value+'|'+$acl.AreAccessRulesProtected+'|'+($rules -join ';')
 }
 $script:ExpectedAclProof=Get-AclProof ([IO.File]::GetAccessControl((Join-Path $duplicate '.env')))
 $script:PrivateFactory=${function:New-PrivateConsumerEnvFile}
 function New-PrivateConsumerEnvFile {
  param([string]$Path,[Security.AccessControl.FileSecurity]$Security)
  $stream=& $script:PrivateFactory $Path $Security
  $script:TempBeforeBytes=$stream.Length
  $actual=Get-AclProof ([IO.File]::GetAccessControl($Path))
  $script:TempAclMatched=$actual -ceq $script:ExpectedAclProof
  return $stream
 }
}
$script:Current=[pscustomobject]@{Id=123;ProcessName='python';StartTime=[DateTime]::UtcNow.AddHours(-1)}
$birth=$script:Current.StartTime.ToUniversalTime().ToFileTimeUtc()
$script:ChildAlive=$true
$child=[pscustomobject]@{ProcessId=234;ParentProcessId=123;CreationDate=$script:Current.StartTime}
$maintenance=[pscustomobject]@{ProcessId=345;ParentProcessId=234;CreationDate=$script:Current.StartTime}
$script:Starts=0;$script:Stops=0;$script:OtherCalls=0;$script:HelperKills=0;$script:HelperTerminal=$false
function Ensure-QmtClient { $script:OtherCalls++;throw 'QMT touched' }
function Stop-PublicQuoteService { $script:OtherCalls++;throw 'Other service touched' }
function Get-ScheduledTask { [CmdletBinding()]param($TaskName)
 return [pscustomobject]@{Actions=@([pscustomobject]@{Arguments=('-RegisteredRoot "'+$AuthorityRoot+'"')})}
}
function git { $global:LASTEXITCODE=0;return $build }
function Get-Process { [CmdletBinding()]param([int]$Id)
 if($script:Current -and $Id -eq $script:Current.Id){return $script:Current}
 if($script:ChildAlive -and $Id -in @(234,345)){
  return [pscustomobject]@{Id=$Id;ProcessName='python';StartTime=$child.CreationDate}
 }
 return $null
}
function Get-CimInstance { [CmdletBinding()]param($ClassName,$Filter,$OperationTimeoutSec)
 return @($child,$maintenance)
}
function Stop-Process { [CmdletBinding()]param($Id,[switch]$Force)
 throw 'Unexpected fallback stop'
}
function Start-Process {param($FilePath,$ArgumentList,$WorkingDirectory,$WindowStyle,
 $RedirectStandardOutput,$RedirectStandardError,[switch]$PassThru)
 if($WindowStyle -ne 'Hidden'){throw 'Visible process'}
 if($FilePath -like '*taskkill.exe'){
  if(($ArgumentList -join ' ') -ne '/PID 123 /T /F'){throw 'Wrong process tree'}
  $script:Stops++
  if($Scenario -ne 'stop_deadline'){$script:Current=$null;$script:ChildAlive=($Scenario -eq 'orphan')}
  $result=[pscustomobject]@{HasExited=($Scenario -ne 'stop_deadline')}
  $result|Add-Member ScriptMethod WaitForExit {param($ms)
   if($this.HasExited){$script:HelperTerminal=$true;return $true};return $false
  }
  $result|Add-Member ScriptMethod Kill {$script:HelperKills++;$this.HasExited=$true}
  return $result
 }
 if($WorkingDirectory -ne $AuthorityRoot -or $ArgumentList -ne ('tools/run_big_qmt_bridge.py --expected-build-sha '+$build)){throw 'Wrong startup identity'}
 $script:Starts++
 $script:Current=[pscustomobject]@{Id=456;ProcessName='python';StartTime=[DateTime]::UtcNow}
 return $script:Current
}
$action='Restart';$target=''
if($Scenario -in @('retire','retire_bad_birth','orphan','private_acl')){$action='RetireDuplicate';$target=$duplicate;$savedRoot=$Root;$savedData=$DataDir;$Root=$duplicate;$DataDir=Join-Path $Root 'data'}
$recordBuild=if($Scenario -eq 'already_target'){$build}else{$old}
Set-ManagedProcess 'big_qmt_bridge' 'run_big_qmt_bridge.py' $script:Current $recordBuild
if($Scenario -eq 'bad_script'){
 [IO.File]::WriteAllText((Get-ManagedPidPath 'big_qmt_bridge'),"123|$birth|different.py|$old")
}
if($Scenario -eq 'pid_reused'){$script:Current.StartTime=[DateTime]::UtcNow}
if($Scenario -in @('retire','retire_bad_birth','orphan','private_acl')){$Root=$savedRoot;$DataDir=$savedData}
$expectedBirth=if($Scenario -eq 'retire_bad_birth'){$birth+1}else{$birth}
$registered=if($Scenario -eq 'bad_authority'){$duplicate}else{$Root}
$failure='';$result=$null
try{
 $result=Invoke-BigQmtConsumerScope -Action $action -RegisteredRoot $registered -ExpectedBuildSha $build `
  -ExpectedProcessId 123 -ExpectedStartFileTimeUtc $expectedBirth -ExpectedProcessBuildSha $old -DuplicateRoot $target
 if($Scenario -eq 'retire'){
  $repeat=Invoke-BigQmtConsumerScope -Action $action -RegisteredRoot $registered -ExpectedBuildSha $build `
   -ExpectedProcessId 123 -ExpectedStartFileTimeUtc $expectedBirth -ExpectedProcessBuildSha $old -DuplicateRoot $target
  if($repeat.changed){throw 'Completed retirement was repeated'}
 }
}catch{$failure=$_.Exception.Message}
@{result=$result;failure=$failure;starts=$script:Starts;stops=$script:Stops;other_calls=$script:OtherCalls;
 helper_kills=$script:HelperKills;helper_terminal=$script:HelperTerminal;
 temporary_acl_matched=$script:TempAclMatched;temporary_before_bytes=$script:TempBeforeBytes;
 final_acl_matched=($Scenario -eq 'private_acl' -and ([IO.File]::GetAccessControl((Join-Path $duplicate '.env'))).GetSecurityDescriptorSddlForm($script:AclSections) -ceq $script:ExpectedAcl);
 production_env=[IO.File]::ReadAllText((Join-Path $Root '.env'));
 duplicate_env=[IO.File]::ReadAllText((Join-Path $duplicate '.env'));
 duplicate_record_exists=(Test-Path -LiteralPath (Join-Path $duplicate 'data/big_qmt_bridge.pid'));
 production_record_exists=(Test-Path -LiteralPath (Join-Path $DataDir 'big_qmt_bridge.pid'))}|ConvertTo-Json -Depth 4 -Compress
''', encoding="utf8")
    p = subprocess.run([shell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
                        "-File", str(script), "-Source", str(ROOT / "tools/start_local_live_services.ps1"),
                        "-WorkspacePath", str(tmp_path), "-Scenario", scenario],
                       text=True, capture_output=True, timeout=25, check=False)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout.splitlines()[-1])


def test_exact_restart_stops_complete_tree_and_starts_only_consumer(tmp_path):
    r = _probe(tmp_path, "restart")
    assert r["failure"] == ""
    assert (r["stops"], r["starts"], r["other_calls"]) == (1, 1, 0)
    assert r["result"]["pid"] == 456
    assert r["result"]["build_sha"] == "a" * 40


def test_controller_serializes_same_root_with_supervisor_but_not_other_services(tmp_path):
    r = _probe(tmp_path, "mutex")
    assert r == {"same_root_blocked": True, "other_root_independent": True}


def test_natural_supervisor_rollover_does_not_restart_again(tmp_path):
    r = _probe(tmp_path, "already_target")
    assert r["failure"] == ""
    assert (r["stops"], r["starts"], r["other_calls"]) == (0, 0, 0)
    assert r["result"]["changed"] is False


@pytest.mark.parametrize("scenario", ["bad_authority", "bad_script", "pid_reused", "retire_bad_birth"])
def test_identity_mismatch_never_stops_starts_or_disables(tmp_path, scenario):
    r = _probe(tmp_path, scenario)
    assert r["failure"]
    assert (r["stops"], r["starts"], r["other_calls"]) == (0, 0, 0)
    assert "BIG_QMT_BRIDGE_ENABLED=true" in r["duplicate_env"]


def test_retire_disables_only_duplicate_and_removes_verified_tree(tmp_path):
    r = _probe(tmp_path, "retire")
    assert r["failure"] == ""
    assert (r["stops"], r["starts"], r["other_calls"]) == (1, 0, 0)
    assert r["duplicate_env"] == r["production_env"].replace("BIG_QMT_BRIDGE_ENABLED=true", "BIG_QMT_BRIDGE_ENABLED=false")
    assert r["duplicate_record_exists"] is False


def test_private_env_replacement_has_original_acl_and_owner_before_any_bytes(tmp_path):
    r = _probe(tmp_path, "private_acl")
    assert r["failure"] == ""
    assert r["temporary_before_bytes"] == 0
    assert r["temporary_acl_matched"] is True
    assert r["final_acl_matched"] is True


@pytest.mark.parametrize("scenario", ["orphan", "stop_deadline"])
def test_incomplete_tree_stop_keeps_identity_and_never_starts_replacement(tmp_path, scenario):
    r = _probe(tmp_path, scenario)
    assert r["failure"]
    assert (r["stops"], r["starts"], r["other_calls"]) == (1, 0, 0)
    assert r["duplicate_record_exists"] if scenario == "orphan" else r["production_record_exists"]
    if scenario == "stop_deadline":
        assert r["helper_kills"] == 1
        assert r["helper_terminal"] is True
