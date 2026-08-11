param(
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $projectRoot = Split-Path -Parent $PSScriptRoot
    $OutputPath = Join-Path $projectRoot ".tmp\qmt_window_admin.png"
}

$nativeSource = @"
using System;
using System.Text;
using System.Runtime.InteropServices;
public static class ProBigAQmtWindow {
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
    public static extern bool IsIconic(IntPtr hWnd);

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

$process = Get-Process -Name "XtItClient" -ErrorAction Stop |
    Where-Object { $_.MainWindowHandle -ne 0 } |
    Select-Object -First 1
if (-not $process) {
    throw "Guojin QMT main window was not found."
}
$windowHandle = [ProBigAQmtWindow]::FindTradingWindow([uint32]$process.Id)
if ($windowHandle -eq [IntPtr]::Zero) {
    $windowHandle = $process.MainWindowHandle
}

[ProBigAQmtWindow]::ShowWindowAsync($windowHandle, 9) | Out-Null
[ProBigAQmtWindow]::SetForegroundWindow($windowHandle) | Out-Null
Start-Sleep -Milliseconds 1000

$rect = New-Object ProBigAQmtWindow+RECT
if (-not [ProBigAQmtWindow]::GetWindowRect($windowHandle, [ref]$rect)) {
    throw "Unable to read the Guojin QMT window rectangle."
}
$width = $rect.Right - $rect.Left
$height = $rect.Bottom - $rect.Top
if ($width -lt 300 -or $height -lt 200) {
    throw "Guojin QMT window is still minimized: ${width}x${height}."
}

$directory = [System.IO.Path]::GetDirectoryName($OutputPath)
[System.IO.Directory]::CreateDirectory($directory) | Out-Null
$bitmap = New-Object System.Drawing.Bitmap($width, $height)
$graphics = [System.Drawing.Graphics]::FromImage($bitmap)
try {
    $graphics.CopyFromScreen(
        $rect.Left,
        $rect.Top,
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

Write-Output "captured=$OutputPath width=$width height=$height pid=$($process.Id)"
