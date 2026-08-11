param(
    [Parameter(Mandatory = $true)]
    [int]$RelativeX,
    [Parameter(Mandatory = $true)]
    [int]$RelativeY,
    [string]$OutputPath = "",
    [int]$WaitMilliseconds = 1000,
    [ValidateRange(1, 2)]
    [int]$ClickCount = 1,
    [ValidateSet("Left", "Right")]
    [string]$Button = "Left"
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $projectRoot = Split-Path -Parent $PSScriptRoot
    $OutputPath = Join-Path $projectRoot ".tmp\qmt_after_click.png"
}

$nativeSource = @"
using System;
using System.Text;
using System.Runtime.InteropServices;
public static class ProBigAQmtClick {
    [StructLayout(LayoutKind.Sequential)]
    public struct RECT {
        public int Left;
        public int Top;
        public int Right;
        public int Bottom;
    }

    [DllImport("user32.dll")]
    public static extern bool ShowWindowAsync(IntPtr hWnd, int nCmdShow);

    [DllImport("user32.dll")]
    public static extern bool SetForegroundWindow(IntPtr hWnd);

    [DllImport("user32.dll")]
    public static extern bool GetWindowRect(IntPtr hWnd, out RECT rect);

    [DllImport("user32.dll")]
    public static extern bool SetCursorPos(int x, int y);

    [DllImport("user32.dll")]
    public static extern void mouse_event(
        uint dwFlags,
        uint dx,
        uint dy,
        uint dwData,
        UIntPtr dwExtraInfo
    );

    private delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);

    [DllImport("user32.dll")]
    private static extern bool EnumWindows(EnumWindowsProc callback, IntPtr lParam);

    [DllImport("user32.dll")]
    private static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint processId);

    [DllImport("user32.dll", CharSet = CharSet.Unicode)]
    private static extern int GetWindowText(IntPtr hWnd, StringBuilder text, int maxCount);

    public static IntPtr FindTradingWindow(uint processId) {
        IntPtr result = IntPtr.Zero;
        EnumWindows(delegate (IntPtr hWnd, IntPtr lParam) {
            uint ownerProcessId;
            GetWindowThreadProcessId(hWnd, out ownerProcessId);
            if (ownerProcessId != processId) {
                return true;
            }
            StringBuilder title = new StringBuilder(256);
            GetWindowText(hWnd, title, title.Capacity);
            if (title.ToString().Contains("QMT") && title.ToString().Contains("2.1.19.0")) {
                result = hWnd;
                return false;
            }
            return true;
        }, IntPtr.Zero);
        return result;
    }
}
"@

Add-Type -TypeDefinition $nativeSource
Add-Type -AssemblyName System.Drawing
Add-Type -AssemblyName System.Windows.Forms

$process = Get-Process -Name "XtItClient" -ErrorAction Stop |
    Where-Object { $_.MainWindowHandle -ne 0 } |
    Select-Object -First 1
if (-not $process) {
    throw "Guojin QMT main window was not found."
}
$windowHandle = [ProBigAQmtClick]::FindTradingWindow([uint32]$process.Id)
if ($windowHandle -eq [IntPtr]::Zero) {
    $windowHandle = $process.MainWindowHandle
}

[ProBigAQmtClick]::ShowWindowAsync($windowHandle, 9) | Out-Null
[ProBigAQmtClick]::SetForegroundWindow($windowHandle) | Out-Null
Start-Sleep -Milliseconds 300

$rect = New-Object ProBigAQmtClick+RECT
if (-not [ProBigAQmtClick]::GetWindowRect($windowHandle, [ref]$rect)) {
    throw "Unable to read the Guojin QMT window rectangle."
}

$screenX = $rect.Left + $RelativeX
$screenY = $rect.Top + $RelativeY
[ProBigAQmtClick]::SetCursorPos($screenX, $screenY) | Out-Null
for ($clickIndex = 0; $clickIndex -lt $ClickCount; $clickIndex++) {
    $downFlag = if ($Button -eq "Right") { 0x0008 } else { 0x0002 }
    $upFlag = if ($Button -eq "Right") { 0x0010 } else { 0x0004 }
    [ProBigAQmtClick]::mouse_event($downFlag, 0, 0, 0, [UIntPtr]::Zero)
    [ProBigAQmtClick]::mouse_event($upFlag, 0, 0, 0, [UIntPtr]::Zero)
    if ($clickIndex + 1 -lt $ClickCount) {
        Start-Sleep -Milliseconds 100
    }
}
Start-Sleep -Milliseconds ([Math]::Max(100, $WaitMilliseconds))

$bounds = [System.Windows.Forms.SystemInformation]::VirtualScreen
$directory = [System.IO.Path]::GetDirectoryName($OutputPath)
[System.IO.Directory]::CreateDirectory($directory) | Out-Null
$bitmap = New-Object System.Drawing.Bitmap($bounds.Width, $bounds.Height)
$graphics = [System.Drawing.Graphics]::FromImage($bitmap)
try {
    $graphics.CopyFromScreen(
        $bounds.Left,
        $bounds.Top,
        0,
        0,
        $bitmap.Size,
        [System.Drawing.CopyPixelOperation]::SourceCopy
    )
    $bitmap.Save($OutputPath, [System.Drawing.Imaging.ImageFormat]::Png)
}
finally {
    $graphics.Dispose()
    $bitmap.Dispose()
}

Write-Output "clicked=$RelativeX,$RelativeY count=$ClickCount button=$Button captured=$OutputPath"
