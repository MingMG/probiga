Option Explicit

Dim arguments
Dim command
Dim exitCode
Dim fileSystem
Dim launcherRoot
Dim powerShellExe
Dim registeredRoot
Dim shell
Dim updaterScript

Set arguments = WScript.Arguments
If arguments.Count <> 1 Then
    WScript.Quit 2
End If

registeredRoot = CStr(arguments(0))
If Len(registeredRoot) < 3 Then
    WScript.Quit 2
End If
If Mid(registeredRoot, 2, 2) <> ":\" Then
    WScript.Quit 2
End If
If InStr(registeredRoot, Chr(34)) <> 0 Then
    WScript.Quit 2
End If

Set fileSystem = CreateObject("Scripting.FileSystemObject")
registeredRoot = fileSystem.GetAbsolutePathName(registeredRoot)
launcherRoot = fileSystem.GetParentFolderName( _
    fileSystem.GetParentFolderName(WScript.ScriptFullName) _
)
If StrComp(launcherRoot, registeredRoot, 1) <> 0 Then
    WScript.Quit 2
End If

powerShellExe = fileSystem.BuildPath( _
    CreateObject("WScript.Shell").ExpandEnvironmentStrings("%SystemRoot%"), _
    "System32\WindowsPowerShell\v1.0\powershell.exe" _
)
updaterScript = fileSystem.BuildPath( _
    registeredRoot, _
    "tools\update_qmt_windows_edge.ps1" _
)
If Not fileSystem.FileExists(powerShellExe) Then
    WScript.Quit 2
End If
If Not fileSystem.FileExists(updaterScript) Then
    WScript.Quit 2
End If

command = QuoteArgument(powerShellExe) & _
    " -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass" & _
    " -WindowStyle Hidden -File " & QuoteArgument(updaterScript) & _
    " -RegisteredRoot " & QuoteArgument(registeredRoot)

Set shell = CreateObject("WScript.Shell")
exitCode = shell.Run(command, 0, True)
WScript.Quit exitCode

Function QuoteArgument(ByVal value)
    QuoteArgument = Chr(34) & Replace(CStr(value), Chr(34), Chr(34) & Chr(34)) & Chr(34)
End Function
