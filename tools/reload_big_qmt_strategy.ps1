param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$RegisteredRoot,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9A-Fa-f]{40}$")]
    [string]$ExpectedBuildSha,

    [int]$StopTimeoutSeconds = 15,
    [int]$StartTimeoutSeconds = 90
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$StrategyName = "PROBIGA_BIGQMT_BRIDGE"
$EditorSuffix = "-" + (
    [string][char]0x7B56 + [char]0x7565 + [char]0x7F16 +
    [char]0x8F91 + [char]0x5668
)
$EditorTitle = "$StrategyName$EditorSuffix"
$ExpectedOrigin = "https://github.com/MingMG/probiga.git"
$ReleaseManifestName = "probiga_big_qmt_bridge.release.json"
$ReleaseManifestSchema = "probiga.bigqmt-strategy-manifest.v1"
$ReleaseProtocol = "probiga.bigqmt-strategy-release.v2"
$IdentityProtocol = "probiga.bigqmt-loaded-strategy-identity.v1"
$ExpectedBuild = $ExpectedBuildSha.Trim().ToLowerInvariant()
$ExpectedRoot = [System.IO.Path]::GetFullPath($RegisteredRoot)
$Root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$PythonExe = Join-Path $ExpectedRoot ".venv\Scripts\python.exe"
$Installer = Join-Path $ExpectedRoot "tools\run_big_qmt_bridge.py"
$StrategyRepositoryPath = (
    "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py"
)
$ProgramDataRoot = [System.IO.Path]::GetFullPath($env:ProgramData)
$ReloadStateRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $ProgramDataRoot "ProBigA\qmt-model-reload")
)

$QmtClient = $null
$QmtMainHandle = [IntPtr]::Zero
$QmtMainTitle = ""
$QmtPythonRoot = ""
$HeartbeatPath = ""
$PreviousHeartbeat = $null
$Backup = $null
$InstallAttempted = $false
$OldModelStopped = $false
$OldEditorClosed = $false
$NewModelStarted = $false
$WasMinimized = $false
$PreviousForeground = [IntPtr]::Zero
$RecoveryMutex = $null
$RecoveryMutexOwned = $false
$ReleaseMutex = $null
$ReleaseMutexOwned = $false
$FinalPayload = $null
$FinalExitCode = 1

function Throw-NeedsUserAction([string]$Reason) {
    throw "NEEDS_USER_ACTION:$Reason"
}

function Get-SafeErrorText($Failure) {
    $Text = [string]$Failure
    $Text = $Text -replace "\r|\n", " "
    if ($Text.Length -gt 500) {
        return $Text.Substring(0, 500)
    }
    return $Text
}

function Invoke-Git([string[]]$Arguments) {
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $Output = & git -C $ExpectedRoot @Arguments 2>$null
        $ExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousPreference
    }
    if ($ExitCode -ne 0) {
        throw "git command failed: git $($Arguments -join ' ')"
    }
    return @($Output)
}

function Assert-OrdinaryFile([string]$Path, [string]$Description) {
    if (!(Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Description is missing"
    }
    $Item = Get-Item -LiteralPath $Path -Force
    if (
        ($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
            -ne 0
    ) {
        throw "$Description cannot be a reparse point"
    }
}

function Assert-OrdinaryDirectory([string]$Path, [string]$Description) {
    if (!(Test-Path -LiteralPath $Path -PathType Container)) {
        throw "$Description is missing"
    }
    $Item = Get-Item -LiteralPath $Path -Force
    if (
        !$Item.PSIsContainer -or
        ($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
            -ne 0
    ) {
        throw "$Description must be an ordinary directory"
    }
}

function Test-PathInside([string]$Path, [string]$Directory) {
    $ResolvedPath = [System.IO.Path]::GetFullPath($Path)
    $ResolvedDirectory = [System.IO.Path]::GetFullPath($Directory)
    return $ResolvedPath.StartsWith(
        $ResolvedDirectory + [System.IO.Path]::DirectorySeparatorChar,
        [System.StringComparison]::OrdinalIgnoreCase
    )
}

function Get-FileSha256([string]$Path) {
    return (
        Get-FileHash -LiteralPath $Path -Algorithm SHA256
    ).Hash.ToLowerInvariant()
}

function Write-AtomicJson([string]$Path, $Payload) {
    if (!(Test-PathInside $Path $ReloadStateRoot)) {
        throw "QMT reload receipt escapes protected state root"
    }
    $Temporary = "$Path.$PID.tmp"
    [System.IO.File]::WriteAllText(
        $Temporary,
        ($Payload | ConvertTo-Json -Depth 12 -Compress),
        [System.Text.UTF8Encoding]::new($false)
    )
    Move-Item -LiteralPath $Temporary -Destination $Path -Force
}

if (-not ("ProBigAQmtReleaseWindow" -as [type])) {
    Add-Type @'
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

public static class ProBigAQmtReleaseWindow
{
    public delegate bool EnumWindowsProc(IntPtr handle, IntPtr state);

    [StructLayout(LayoutKind.Sequential)]
    public struct RECT
    {
        public int Left;
        public int Top;
        public int Right;
        public int Bottom;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct MONITORINFO
    {
        public int cbSize;
        public RECT rcMonitor;
        public RECT rcWork;
        public uint dwFlags;
    }

    [DllImport("user32.dll")]
    public static extern bool EnumWindows(
        EnumWindowsProc callback,
        IntPtr state
    );

    [DllImport("user32.dll")]
    public static extern uint GetWindowThreadProcessId(
        IntPtr handle,
        out uint processId
    );

    [DllImport("user32.dll", CharSet = CharSet.Unicode)]
    public static extern int GetWindowText(
        IntPtr handle,
        StringBuilder text,
        int count
    );

    [DllImport("user32.dll")]
    public static extern bool IsWindow(IntPtr handle);

    [DllImport("user32.dll")]
    public static extern bool IsWindowVisible(IntPtr handle);

    [DllImport("user32.dll")]
    public static extern bool IsWindowEnabled(IntPtr handle);

    [DllImport("user32.dll")]
    public static extern bool IsIconic(IntPtr handle);

    [DllImport("user32.dll")]
    public static extern bool GetWindowRect(IntPtr handle, out RECT rect);

    [DllImport("user32.dll")]
    public static extern IntPtr MonitorFromWindow(
        IntPtr handle,
        uint flags
    );

    [DllImport("user32.dll")]
    public static extern bool GetMonitorInfo(
        IntPtr monitor,
        ref MONITORINFO info
    );

    [DllImport("user32.dll")]
    public static extern bool ShowWindow(IntPtr handle, int command);

    [DllImport("user32.dll")]
    public static extern bool SetForegroundWindow(IntPtr handle);

    [DllImport("user32.dll")]
    public static extern IntPtr GetForegroundWindow();

    [DllImport("user32.dll")]
    public static extern bool SetCursorPos(int x, int y);

    [DllImport("user32.dll")]
    public static extern void mouse_event(
        uint flags,
        uint x,
        uint y,
        uint data,
        UIntPtr extraInfo
    );

    [StructLayout(LayoutKind.Sequential)]
    private struct BITMAPINFOHEADER
    {
        public uint biSize;
        public int biWidth;
        public int biHeight;
        public ushort biPlanes;
        public ushort biBitCount;
        public uint biCompression;
        public uint biSizeImage;
        public int biXPelsPerMeter;
        public int biYPelsPerMeter;
        public uint biClrUsed;
        public uint biClrImportant;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct RGBQUAD
    {
        public byte rgbBlue;
        public byte rgbGreen;
        public byte rgbRed;
        public byte rgbReserved;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct BITMAPINFO
    {
        public BITMAPINFOHEADER bmiHeader;
        public RGBQUAD bmiColors;
    }

    [DllImport("user32.dll")]
    private static extern IntPtr GetDC(IntPtr handle);

    [DllImport("user32.dll")]
    private static extern int ReleaseDC(IntPtr handle, IntPtr dc);

    [DllImport("gdi32.dll")]
    private static extern IntPtr CreateCompatibleDC(IntPtr dc);

    [DllImport("gdi32.dll")]
    private static extern bool DeleteDC(IntPtr dc);

    [DllImport("gdi32.dll")]
    private static extern IntPtr CreateDIBSection(
        IntPtr dc,
        ref BITMAPINFO info,
        uint usage,
        out IntPtr bits,
        IntPtr section,
        uint offset
    );

    [DllImport("gdi32.dll")]
    private static extern IntPtr SelectObject(IntPtr dc, IntPtr value);

    [DllImport("gdi32.dll")]
    private static extern bool DeleteObject(IntPtr value);

    [DllImport("gdi32.dll")]
    private static extern bool BitBlt(
        IntPtr target,
        int x,
        int y,
        int width,
        int height,
        IntPtr source,
        int sourceX,
        int sourceY,
        uint operation
    );

    private static int Luminance(byte[] pixels, int width, int x, int y)
    {
        int index = ((y * width) + x) * 4;
        int blue = pixels[index];
        int green = pixels[index + 1];
        int red = pixels[index + 2];
        return ((2126 * red) + (7152 * green) + (722 * blue)) / 10000;
    }

    public static int FindStrategyPaneLeft(
        int left,
        int top,
        int right,
        int bottom
    )
    {
        int width = right - left;
        int height = bottom - top;
        if (width < 900 || height < 500)
        {
            return -1;
        }
        IntPtr screen = GetDC(IntPtr.Zero);
        IntPtr memory = IntPtr.Zero;
        IntPtr bitmap = IntPtr.Zero;
        IntPtr previous = IntPtr.Zero;
        try
        {
            if (screen == IntPtr.Zero)
            {
                return -1;
            }
            memory = CreateCompatibleDC(screen);
            if (memory == IntPtr.Zero)
            {
                return -1;
            }
            BITMAPINFO info = new BITMAPINFO();
            info.bmiHeader.biSize = (uint)Marshal.SizeOf(
                typeof(BITMAPINFOHEADER)
            );
            info.bmiHeader.biWidth = width;
            info.bmiHeader.biHeight = -height;
            info.bmiHeader.biPlanes = 1;
            info.bmiHeader.biBitCount = 32;
            IntPtr bits;
            bitmap = CreateDIBSection(
                screen,
                ref info,
                0,
                out bits,
                IntPtr.Zero,
                0
            );
            if (bitmap == IntPtr.Zero || bits == IntPtr.Zero)
            {
                return -1;
            }
            previous = SelectObject(memory, bitmap);
            if (previous == IntPtr.Zero)
            {
                return -1;
            }
            if (!BitBlt(
                memory,
                0,
                0,
                width,
                height,
                screen,
                left,
                top,
                0x40CC0020
            ))
            {
                return -1;
            }
            byte[] pixels = new byte[width * height * 4];
            Marshal.Copy(bits, pixels, 0, pixels.Length);
            int[] rows = new int[] { 100, 180, 300 };
            bool inRun = false;
            int runCount = 0;
            int paneLeft = -1;
            int limit = Math.Min(width - 100, 1500);
            for (int x = 250; x < limit; x += 2)
            {
                int dark = 0;
                int light = 0;
                foreach (int row in rows)
                {
                    dark += Luminance(pixels, width, x - 24, row);
                    light += Luminance(pixels, width, x + 24, row);
                }
                bool transition = dark / rows.Length < 80 &&
                    light / rows.Length > 220;
                if (transition && !inRun)
                {
                    runCount += 1;
                    paneLeft = x + 24;
                    inRun = true;
                }
                else if (!transition)
                {
                    inRun = false;
                }
            }
            if (runCount == 1)
            {
                return left + paneLeft;
            }
            int fullWidthLight = 0;
            int[] fullWidthColumns = new int[] { 60, 250, 400, 600 };
            int[] fullWidthRows = new int[] { 180, 300 };
            foreach (int column in fullWidthColumns)
            {
                foreach (int row in fullWidthRows)
                {
                    fullWidthLight += Luminance(
                        pixels,
                        width,
                        column,
                        row
                    );
                }
            }
            int fullWidthSamples = fullWidthColumns.Length *
                fullWidthRows.Length;
            if (runCount == 0 && fullWidthLight / fullWidthSamples > 220)
            {
                // QMT's dedicated model-research list starts immediately to
                // the right of the 51-pixel navigation rail.
                return left + 51;
            }
            return -1;
        }
        finally
        {
            if (previous != IntPtr.Zero && memory != IntPtr.Zero)
            {
                SelectObject(memory, previous);
            }
            if (bitmap != IntPtr.Zero)
            {
                DeleteObject(bitmap);
            }
            if (memory != IntPtr.Zero)
            {
                DeleteDC(memory);
            }
            if (screen != IntPtr.Zero)
            {
                ReleaseDC(IntPtr.Zero, screen);
            }
        }
    }

    [DllImport("user32.dll")]
    public static extern bool PostMessage(
        IntPtr handle,
        uint message,
        IntPtr word,
        IntPtr value
    );

    public static string Title(IntPtr handle)
    {
        StringBuilder text = new StringBuilder(512);
        GetWindowText(handle, text, text.Capacity);
        return text.ToString();
    }

    public static uint Owner(IntPtr handle)
    {
        uint processId;
        GetWindowThreadProcessId(handle, out processId);
        return processId;
    }

    public static List<IntPtr> ExactWindows(uint processId, string title)
    {
        List<IntPtr> result = new List<IntPtr>();
        EnumWindows(delegate(IntPtr handle, IntPtr state)
        {
            if (Owner(handle) == processId && Title(handle) == title)
            {
                result.Add(handle);
            }
            return true;
        }, IntPtr.Zero);
        return result;
    }

    public static List<IntPtr> TitleSuffixWindows(
        uint processId,
        string suffix
    )
    {
        List<IntPtr> result = new List<IntPtr>();
        EnumWindows(delegate(IntPtr handle, IntPtr state)
        {
            string title = Title(handle);
            if (
                Owner(handle) == processId &&
                title.EndsWith(suffix, StringComparison.Ordinal)
            )
            {
                result.Add(handle);
            }
            return true;
        }, IntPtr.Zero);
        return result;
    }

    public static List<IntPtr> VisibleTitledWindows(uint processId)
    {
        List<IntPtr> result = new List<IntPtr>();
        EnumWindows(delegate(IntPtr handle, IntPtr state)
        {
            if (
                Owner(handle) == processId &&
                IsWindowVisible(handle) &&
                Title(handle).Length > 0
            )
            {
                result.Add(handle);
            }
            return true;
        }, IntPtr.Zero);
        return result;
    }
}
'@
}

function Get-ExactEditorWindows {
    return @(
        [ProBigAQmtReleaseWindow]::ExactWindows(
            [uint32]$QmtClient.Id,
            $EditorTitle
        )
    )
}

function Assert-NoOtherStrategyEditors {
    $Editors = @(
        [ProBigAQmtReleaseWindow]::TitleSuffixWindows(
            [uint32]$QmtClient.Id,
            $EditorSuffix
        )
    )
    $Other = @(
        $Editors | Where-Object {
            [ProBigAQmtReleaseWindow]::Title($_) -cne $EditorTitle
        }
    )
    if ($Other.Count -ne 0) {
        Throw-NeedsUserAction (
            "another QMT strategy editor is open; close it before release"
        )
    }
    $Exact = @(
        $Editors | Where-Object {
            [ProBigAQmtReleaseWindow]::Title($_) -ceq $EditorTitle
        }
    )
    if ($Exact.Count -gt 1) {
        Throw-NeedsUserAction "target QMT strategy editor is not unique"
    }
}

function Assert-NoUnexpectedVisibleQmtWindow {
    $Visible = @(
        [ProBigAQmtReleaseWindow]::VisibleTitledWindows(
            [uint32]$QmtClient.Id
        )
    )
    foreach ($Handle in $Visible) {
        $Title = [ProBigAQmtReleaseWindow]::Title($Handle)
        if ($Title -cnotin @($QmtMainTitle, $EditorTitle)) {
            Throw-NeedsUserAction (
                "QMT has a login, CAPTCHA, confirmation, or other modal window"
            )
        }
    }
}

function Get-Heartbeat {
    if (!(Test-Path -LiteralPath $HeartbeatPath -PathType Leaf)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $HeartbeatPath -Raw |
            ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Test-RunningHeartbeat($Heartbeat) {
    if (!$Heartbeat) {
        return $false
    }
    return (
        [string]$Heartbeat.status -in @("running", "busy") -and
        [string]$Heartbeat.source -eq "gj_big_qmt_inner" -and
        [int]$Heartbeat.pid -eq [int]$QmtClient.Id
    )
}

function Test-ExpectedReleaseHeartbeat($Heartbeat, $Release) {
    if (!(Test-RunningHeartbeat $Heartbeat)) {
        return $false
    }
    return (
        [string]$Heartbeat.bridge_version -eq "bigqmt_inner_v2" -and
        [string]$Heartbeat.strategy_release_protocol -eq $ReleaseProtocol -and
        [string]$Heartbeat.strategy_identity_protocol -eq $IdentityProtocol -and
        $Heartbeat.strategy_identity_frozen -eq $true -and
        [string]$Heartbeat.strategy_identity_status -eq "BOUND" -and
        [string]$Heartbeat.strategy_build_sha -ceq $ExpectedBuild -and
        [string]$Heartbeat.strategy_git_blob -ceq `
            [string]$Release.strategy_git_blob -and
        [string]$Heartbeat.strategy_source_sha256 -ceq `
            [string]$Release.strategy_source_sha256 -and
        [string]$Heartbeat.strategy_artifact_sha256 -ceq `
            [string]$Release.strategy_artifact_sha256 -and
        [string]$Heartbeat.strategy_loaded_identity_sha256 -ceq `
            [string]$Release.strategy_loaded_identity_sha256
    )
}

function Test-OriginalReleaseHeartbeat($Heartbeat) {
    if (!(Test-RunningHeartbeat $Heartbeat)) {
        return $false
    }
    $PreviousStatus = [string]$PreviousHeartbeat.strategy_identity_status
    if ($PreviousStatus -ne "BOUND") {
        return [string]$Heartbeat.strategy_build_sha -ceq `
            [string]$PreviousHeartbeat.strategy_build_sha
    }
    foreach ($Name in @(
        "strategy_release_protocol",
        "strategy_identity_protocol",
        "strategy_identity_status",
        "strategy_build_sha",
        "strategy_git_blob",
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256"
    )) {
        if ([string]$Heartbeat.$Name -cne [string]$PreviousHeartbeat.$Name) {
            return $false
        }
    }
    return $Heartbeat.strategy_identity_frozen -eq $true
}

function Wait-ForHeartbeat([scriptblock]$Predicate, [int]$TimeoutSeconds) {
    $Deadline = (Get-Date).AddSeconds([Math]::Max(1, $TimeoutSeconds))
    do {
        Start-Sleep -Milliseconds 250
        $Heartbeat = Get-Heartbeat
        if (& $Predicate $Heartbeat) {
            return $Heartbeat
        }
    } while ((Get-Date) -lt $Deadline)
    return $null
}

function Invoke-ExactWindowClick(
    [IntPtr]$Handle,
    [string]$ExpectedTitle,
    [double]$XRatio,
    [double]$YRatio,
    [switch]$UseMonitorWorkArea
) {
    if (
        ![ProBigAQmtReleaseWindow]::IsWindow($Handle) -or
        [ProBigAQmtReleaseWindow]::Owner($Handle) -ne [uint32]$QmtClient.Id -or
        [ProBigAQmtReleaseWindow]::Title($Handle) -cne $ExpectedTitle
    ) {
        throw "QMT click target identity changed"
    }
    $Rect = New-Object ProBigAQmtReleaseWindow+RECT
    if (![ProBigAQmtReleaseWindow]::GetWindowRect($Handle, [ref]$Rect)) {
        throw "QMT click target bounds are unavailable"
    }
    $ClickLeft = $Rect.Left
    $ClickTop = $Rect.Top
    $ClickRight = $Rect.Right
    $ClickBottom = $Rect.Bottom
    if ($UseMonitorWorkArea) {
        # The QMT main wrapper may cover multiple monitors.  Navigation is
        # anchored to the owning monitor's work area; normalizing against the
        # wrapper union can otherwise click another QMT command or monitor.
        $Monitor = [ProBigAQmtReleaseWindow]::MonitorFromWindow($Handle, 2)
        if ($Monitor -eq [IntPtr]::Zero) {
            throw "QMT navigation monitor is unavailable"
        }
        $MonitorInfo = New-Object ProBigAQmtReleaseWindow+MONITORINFO
        $MonitorInfo.cbSize = [Runtime.InteropServices.Marshal]::SizeOf(
            $MonitorInfo
        )
        if (
            ![ProBigAQmtReleaseWindow]::GetMonitorInfo(
                $Monitor,
                [ref]$MonitorInfo
            )
        ) {
            throw "QMT navigation monitor bounds are unavailable"
        }
        $ClickLeft = $MonitorInfo.rcWork.Left
        $ClickTop = $MonitorInfo.rcWork.Top
        $ClickRight = $MonitorInfo.rcWork.Right
        $ClickBottom = $MonitorInfo.rcWork.Bottom
        $BoundsTolerance = 8
        if (
            $ClickLeft -lt $Rect.Left - $BoundsTolerance -or
            $ClickTop -lt $Rect.Top - $BoundsTolerance -or
            $ClickRight -gt $Rect.Right + $BoundsTolerance -or
            $ClickBottom -gt $Rect.Bottom + $BoundsTolerance
        ) {
            throw "QMT main window does not cover its navigation monitor"
        }
    }
    $Width = $ClickRight - $ClickLeft
    $Height = $ClickBottom - $ClickTop
    if ($Width -lt 900 -or $Height -lt 500) {
        throw "QMT click target bounds are unsafe"
    }
    [ProBigAQmtReleaseWindow]::SetForegroundWindow($Handle) | Out-Null
    Start-Sleep -Milliseconds 200
    [ProBigAQmtReleaseWindow]::SetCursorPos(
        $ClickLeft + [int]($Width * $XRatio),
        $ClickTop + [int]($Height * $YRatio)
    ) | Out-Null
    [ProBigAQmtReleaseWindow]::mouse_event(
        0x0002, 0, 0, 0, [UIntPtr]::Zero
    )
    [ProBigAQmtReleaseWindow]::mouse_event(
        0x0004, 0, 0, 0, [UIntPtr]::Zero
    )
}

function Show-QmtMainWindow {
    [ProBigAQmtReleaseWindow]::ShowWindow($QmtMainHandle, 9) | Out-Null
    [ProBigAQmtReleaseWindow]::SetForegroundWindow($QmtMainHandle) | Out-Null
    Start-Sleep -Milliseconds 500
}

function Get-QmtMainWorkArea {
    $Rect = New-Object ProBigAQmtReleaseWindow+RECT
    if (
        ![ProBigAQmtReleaseWindow]::GetWindowRect(
            $QmtMainHandle,
            [ref]$Rect
        )
    ) {
        throw "QMT main window bounds are unavailable"
    }
    $Monitor = [ProBigAQmtReleaseWindow]::MonitorFromWindow(
        $QmtMainHandle,
        2
    )
    if ($Monitor -eq [IntPtr]::Zero) {
        throw "QMT navigation monitor is unavailable"
    }
    $MonitorInfo = New-Object ProBigAQmtReleaseWindow+MONITORINFO
    $MonitorInfo.cbSize = [Runtime.InteropServices.Marshal]::SizeOf(
        $MonitorInfo
    )
    if (
        ![ProBigAQmtReleaseWindow]::GetMonitorInfo(
            $Monitor,
            [ref]$MonitorInfo
        )
    ) {
        throw "QMT navigation monitor bounds are unavailable"
    }
    $BoundsTolerance = 8
    if (
        $MonitorInfo.rcWork.Left -lt $Rect.Left - $BoundsTolerance -or
        $MonitorInfo.rcWork.Top -lt $Rect.Top - $BoundsTolerance -or
        $MonitorInfo.rcWork.Right -gt $Rect.Right + $BoundsTolerance -or
        $MonitorInfo.rcWork.Bottom -gt $Rect.Bottom + $BoundsTolerance
    ) {
        throw "QMT main window does not cover its navigation monitor"
    }
    return [pscustomobject]@{
        Left = [int]$MonitorInfo.rcWork.Left
        Top = [int]$MonitorInfo.rcWork.Top
        Right = [int]$MonitorInfo.rcWork.Right
        Bottom = [int]$MonitorInfo.rcWork.Bottom
    }
}

function Invoke-ExactScreenPointClick(
    [IntPtr]$Handle,
    [string]$ExpectedTitle,
    [int]$X,
    [int]$Y
) {
    if (
        ![ProBigAQmtReleaseWindow]::IsWindow($Handle) -or
        [ProBigAQmtReleaseWindow]::Owner($Handle) -ne [uint32]$QmtClient.Id -or
        [ProBigAQmtReleaseWindow]::Title($Handle) -cne $ExpectedTitle
    ) {
        throw "QMT point-click target identity changed"
    }
    $Rect = New-Object ProBigAQmtReleaseWindow+RECT
    if (![ProBigAQmtReleaseWindow]::GetWindowRect($Handle, [ref]$Rect)) {
        throw "QMT point-click target bounds are unavailable"
    }
    if (
        $X -lt $Rect.Left -or
        $X -ge $Rect.Right -or
        $Y -lt $Rect.Top -or
        $Y -ge $Rect.Bottom
    ) {
        throw "QMT point click escapes the exact target window"
    }
    [ProBigAQmtReleaseWindow]::SetForegroundWindow($Handle) | Out-Null
    Start-Sleep -Milliseconds 200
    [ProBigAQmtReleaseWindow]::SetCursorPos($X, $Y) | Out-Null
    [ProBigAQmtReleaseWindow]::mouse_event(
        0x0002, 0, 0, 0, [UIntPtr]::Zero
    )
    [ProBigAQmtReleaseWindow]::mouse_event(
        0x0004, 0, 0, 0, [UIntPtr]::Zero
    )
}

function Get-QmtStrategyPaneLayout {
    Assert-NoUnexpectedVisibleQmtWindow
    $Work = Get-QmtMainWorkArea
    $PaneLeft = [ProBigAQmtReleaseWindow]::FindStrategyPaneLeft(
        $Work.Left,
        $Work.Top,
        $Work.Right,
        $Work.Bottom
    )
    $RelativeLeft = $PaneLeft - $Work.Left
    $FullWidthList = $RelativeLeft -ge 45 -and $RelativeLeft -le 60
    $EmbeddedList = $RelativeLeft -ge 400 -and $RelativeLeft -le 1000
    if (!$FullWidthList -and !$EmbeddedList) {
        Throw-NeedsUserAction (
            "the visible QMT model-research strategy pane is not unique"
        )
    }
    return [pscustomobject]@{
        SearchX = [int]($PaneLeft + 70)
        SearchY = [int]($Work.Top + 67)
        EditX = [int]($PaneLeft + 322)
        EditY = [int]($Work.Top + 137)
    }
}

function Open-ExactStrategyEditor {
    Assert-NoOtherStrategyEditors
    $Existing = @(Get-ExactEditorWindows)
    if ($Existing.Count -eq 1) {
        return $Existing[0]
    }

    Add-Type -AssemblyName System.Windows.Forms
    Show-QmtMainWindow
    # QMT 2.1.19 leaves the client on a model backtest page after an editor
    # closes.  This exact location is the read-only "return to home" link;
    # on the home page it is a harmless market panel label.
    Invoke-ExactWindowClick `
        $QmtMainHandle `
        $QmtMainTitle `
        0.056 `
        0.039 `
        -UseMonitorWorkArea
    Start-Sleep -Milliseconds 1200
    # The model-research header tracks the full QMT wrapper, while the search
    # pane is anchored to the owning monitor. This normalizes spanning layouts.
    Invoke-ExactWindowClick $QmtMainHandle $QmtMainTitle 0.470 0.015
    Start-Sleep -Milliseconds 1000
    $Layout = Get-QmtStrategyPaneLayout
    Invoke-ExactScreenPointClick `
        $QmtMainHandle `
        $QmtMainTitle `
        $Layout.SearchX `
        $Layout.SearchY
    [System.Windows.Forms.SendKeys]::SendWait("^a")
    [System.Windows.Forms.SendKeys]::SendWait($StrategyName)
    [System.Windows.Forms.SendKeys]::SendWait("{ENTER}")
    Start-Sleep -Milliseconds 1000
    Invoke-ExactScreenPointClick `
        $QmtMainHandle `
        $QmtMainTitle `
        $Layout.EditX `
        $Layout.EditY
    $Deadline = (Get-Date).AddSeconds(8)
    do {
        Start-Sleep -Milliseconds 250
        Assert-NoOtherStrategyEditors
        $Opened = @(Get-ExactEditorWindows)
    } while ($Opened.Count -ne 1 -and (Get-Date) -lt $Deadline)
    if ($Opened.Count -eq 1) {
        return $Opened[0]
    }
    Throw-NeedsUserAction (
        "the exact QMT strategy editor could not be opened safely"
    )
}

function Stop-ExactStrategy([IntPtr]$Editor) {
    $Before = Get-Heartbeat
    if ([string]$Before.status -eq "stopped") {
        return $Before
    }
    if (!(Test-RunningHeartbeat $Before)) {
        throw "target QMT strategy heartbeat is not running before stop"
    }
    for ($Attempt = 0; $Attempt -lt 2; $Attempt += 1) {
        Show-QmtMainWindow
        Invoke-ExactWindowClick $Editor $EditorTitle 0.477 0.154
        $Stopped = Wait-ForHeartbeat {
            param($Heartbeat)
            return (
                $Heartbeat -and
                [string]$Heartbeat.status -eq "stopped" -and
                [int]$Heartbeat.pid -eq [int]$QmtClient.Id -and
                [string]$Heartbeat.updated_at -cne [string]$Before.updated_at
            )
        } $StopTimeoutSeconds
        if ($Stopped) {
            return $Stopped
        }
    }
    throw "exact QMT strategy stop control did not produce a stopped heartbeat"
}

function Close-ExactStrategyEditor([IntPtr]$Editor) {
    if (
        [ProBigAQmtReleaseWindow]::Owner($Editor) -ne [uint32]$QmtClient.Id -or
        [ProBigAQmtReleaseWindow]::Title($Editor) -cne $EditorTitle
    ) {
        throw "QMT editor close target identity changed"
    }
    if (![ProBigAQmtReleaseWindow]::PostMessage(
        $Editor, 0x0010, [IntPtr]::Zero, [IntPtr]::Zero
    )) {
        throw "QMT editor close message was rejected"
    }
    $Deadline = (Get-Date).AddSeconds(10)
    do {
        Start-Sleep -Milliseconds 250
        $Remaining = @(Get-ExactEditorWindows)
    } while ($Remaining.Count -ne 0 -and (Get-Date) -lt $Deadline)
    if ($Remaining.Count -ne 0) {
        throw "exact QMT strategy editor did not close"
    }
    Assert-NoUnexpectedVisibleQmtWindow
}

function Start-ExactStrategy(
    [IntPtr]$Editor,
    [scriptblock]$HeartbeatPredicate
) {
    for ($Attempt = 0; $Attempt -lt 3; $Attempt += 1) {
        Show-QmtMainWindow
        # QMT 2.1.19's run action is the second toolbar command.  The click
        # is accepted only on the exact, uniquely titled target editor and is
        # subsequently proven by the model's own in-process heartbeat.
        Invoke-ExactWindowClick $Editor $EditorTitle 0.339 0.151
        $Running = Wait-ForHeartbeat $HeartbeatPredicate $StartTimeoutSeconds
        if ($Running) {
            return $Running
        }
    }
    throw "exact QMT strategy run control did not produce its expected heartbeat"
}

function Get-InstalledStrategyAliases {
    return @(
        Get-ChildItem -LiteralPath $QmtPythonRoot -File -Force |
            Where-Object {
                $_.Name.ToLowerInvariant() -eq `
                    "probiga_big_qmt_bridge.py"
            } |
            Sort-Object FullName
    )
}

function New-ArtifactBackup {
    $Aliases = @(Get-InstalledStrategyAliases)
    if ($Aliases.Count -eq 0) {
        throw "installed QMT bridge strategy is missing before release"
    }
    foreach ($Alias in $Aliases) {
        if (
            ($Alias.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
                -ne 0 -or
            !(Test-PathInside $Alias.FullName $QmtPythonRoot)
        ) {
            throw "installed QMT bridge strategy path is unsafe"
        }
    }
    $TransactionId = (
        $ExpectedBuild + "-" +
        [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssfffZ") + "-" + $PID
    )
    $Directory = Join-Path $ReloadStateRoot $TransactionId
    New-Item -ItemType Directory -Path $Directory | Out-Null
    Assert-OrdinaryDirectory $Directory "QMT reload transaction directory"
    $Entries = @()
    for ($Index = 0; $Index -lt $Aliases.Count; $Index += 1) {
        $BackupPath = Join-Path $Directory "original-strategy-$Index.bin"
        [System.IO.File]::WriteAllBytes(
            $BackupPath,
            [System.IO.File]::ReadAllBytes($Aliases[$Index].FullName)
        )
        $Entries += [ordered]@{
            target_path = $Aliases[$Index].FullName
            backup_path = $BackupPath
            sha256 = Get-FileSha256 $Aliases[$Index].FullName
        }
    }
    $ManifestPath = Join-Path $QmtPythonRoot $ReleaseManifestName
    $ManifestBackupPath = ""
    $ManifestHash = ""
    if (Test-Path -LiteralPath $ManifestPath -PathType Leaf) {
        Assert-OrdinaryFile $ManifestPath "existing QMT release manifest"
        $ManifestBackupPath = Join-Path $Directory "original-manifest.json"
        [System.IO.File]::WriteAllBytes(
            $ManifestBackupPath,
            [System.IO.File]::ReadAllBytes($ManifestPath)
        )
        $ManifestHash = Get-FileSha256 $ManifestPath
    }
    $Snapshot = [ordered]@{
        schema = "probiga.bigqmt-ui-reload-backup.v1"
        transaction_id = $TransactionId
        expected_build_sha = $ExpectedBuild
        created_at = [DateTime]::UtcNow.ToString("o")
        directory = $Directory
        aliases = $Entries
        manifest_path = $ManifestPath
        manifest_backup_path = $ManifestBackupPath
        manifest_sha256 = $ManifestHash
    }
    Write-AtomicJson (Join-Path $Directory "backup.json") $Snapshot
    return [pscustomobject]$Snapshot
}

function Invoke-ExactStrategyInstall {
    $PreviousBuildEnvironment = $env:PROBIGA_BUILD_COMMIT_SHA
    try {
        $env:PROBIGA_BUILD_COMMIT_SHA = $ExpectedBuild
        $RawOutput = & $PythonExe -P $Installer `
            --install-strategy --install-only `
            --expected-build-sha $ExpectedBuild --json 2>&1
        $InstallExit = $LASTEXITCODE
    }
    finally {
        $env:PROBIGA_BUILD_COMMIT_SHA = $PreviousBuildEnvironment
    }
    if ($InstallExit -ne 0) {
        throw "exact BigQMT strategy atomic install failed"
    }
    try {
        $Release = ($RawOutput -join "`n") | ConvertFrom-Json
    }
    catch {
        throw "exact BigQMT strategy installer returned invalid JSON"
    }
    if (
        [string]$Release.strategy_git_blob -notmatch `
            "^[0-9a-f]{40}$|^[0-9a-f]{64}$" -or
        [string]$Release.strategy_git_blob -cne $Blob
    ) {
        throw "exact BigQMT strategy Git blob identity differs"
    }
    foreach ($Name in @(
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256"
    )) {
        if ([string]$Release.$Name -notmatch "^[0-9a-f]{64}$") {
            throw "exact BigQMT strategy installer identity is malformed"
        }
    }
    if (
        [string]$Release.schema -ne "probiga.bigqmt-strategy-install.v1" -or
        [string]$Release.status -ne "installed" -or
        [string]$Release.build_sha -cne $ExpectedBuild -or
        $Release.database_writes -ne $false -or
        $Release.automatic_order_submission -ne $false
    ) {
        throw "exact BigQMT strategy installer contract differs"
    }
    $ManifestPath = [System.IO.Path]::GetFullPath(
        [string]$Release.strategy_release_manifest
    )
    if (
        !(Test-PathInside $ManifestPath $QmtPythonRoot) -or
        [System.IO.Path]::GetFileName($ManifestPath) -cne `
            $ReleaseManifestName
    ) {
        throw "BigQMT release manifest path differs"
    }
    Assert-OrdinaryFile $ManifestPath "BigQMT release manifest"
    $Manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
    if (
        [string]$Manifest.schema -ne $ReleaseManifestSchema -or
        [string]$Manifest.strategy_release_protocol -ne $ReleaseProtocol -or
        [string]$Manifest.strategy_identity_protocol -ne $IdentityProtocol -or
        [string]$Manifest.strategy_build_sha -cne $ExpectedBuild
    ) {
        throw "BigQMT release manifest contract differs"
    }
    foreach ($Name in @(
        "strategy_git_blob",
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256"
    )) {
        if ([string]$Manifest.$Name -cne [string]$Release.$Name) {
            throw "BigQMT release manifest identity differs: $Name"
        }
    }
    $InstalledPaths = @($Release.installed_paths)
    if ($InstalledPaths.Count -eq 0) {
        throw "BigQMT release installer returned no strategy aliases"
    }
    foreach ($InstalledPathValue in $InstalledPaths) {
        $InstalledPath = [System.IO.Path]::GetFullPath(
            [string]$InstalledPathValue
        )
        if (!(Test-PathInside $InstalledPath $QmtPythonRoot)) {
            throw "BigQMT installed strategy path escapes QMT Python root"
        }
        Assert-OrdinaryFile $InstalledPath "installed BigQMT strategy alias"
        if (
            (Get-FileSha256 $InstalledPath) -cne `
                [string]$Release.strategy_artifact_sha256
        ) {
            throw "BigQMT installed strategy alias hash differs"
        }
    }
    return $Release
}

function Restore-OriginalArtifact {
    if (!$Backup) {
        throw "QMT reload backup is unavailable"
    }
    $OriginalTargets = @{}
    foreach ($Entry in @($Backup.aliases)) {
        $TargetPath = [System.IO.Path]::GetFullPath(
            [string]$Entry.target_path
        )
        if (!(Test-PathInside $TargetPath $QmtPythonRoot)) {
            throw "QMT rollback target escapes QMT Python root"
        }
        $OriginalTargets[$TargetPath.ToLowerInvariant()] = $true
    }
    $FailedIndex = 0
    foreach ($Current in @(Get-InstalledStrategyAliases)) {
        $CurrentPath = [System.IO.Path]::GetFullPath($Current.FullName)
        if (!$OriginalTargets.ContainsKey($CurrentPath.ToLowerInvariant())) {
            $FailedPath = Join-Path (
                [string]$Backup.directory
            ) "failed-new-strategy-$FailedIndex.bin"
            Move-Item -LiteralPath $CurrentPath -Destination $FailedPath
            $FailedIndex += 1
        }
    }
    foreach ($Entry in @($Backup.aliases)) {
        $TargetPath = [System.IO.Path]::GetFullPath(
            [string]$Entry.target_path
        )
        $Temporary = "$TargetPath.rollback.$PID.tmp"
        [System.IO.File]::WriteAllBytes(
            $Temporary,
            [System.IO.File]::ReadAllBytes([string]$Entry.backup_path)
        )
        Move-Item -LiteralPath $Temporary -Destination $TargetPath -Force
        if ((Get-FileSha256 $TargetPath) -cne [string]$Entry.sha256) {
            throw "QMT rollback strategy hash differs"
        }
    }
    $ManifestPath = [System.IO.Path]::GetFullPath(
        [string]$Backup.manifest_path
    )
    if ([string]$Backup.manifest_backup_path) {
        $TemporaryManifest = "$ManifestPath.rollback.$PID.tmp"
        [System.IO.File]::WriteAllBytes(
            $TemporaryManifest,
            [System.IO.File]::ReadAllBytes(
                [string]$Backup.manifest_backup_path
            )
        )
        Move-Item -LiteralPath $TemporaryManifest `
            -Destination $ManifestPath -Force
        if ((Get-FileSha256 $ManifestPath) -cne [string]$Backup.manifest_sha256) {
            throw "QMT rollback manifest hash differs"
        }
    }
    elseif (Test-Path -LiteralPath $ManifestPath -PathType Leaf) {
        $FailedManifest = Join-Path (
            [string]$Backup.directory
        ) "failed-new-manifest.json"
        Move-Item -LiteralPath $ManifestPath -Destination $FailedManifest
    }
}

function Invoke-ModelRollback {
    Restore-OriginalArtifact
    $CurrentHeartbeat = Get-Heartbeat
    if (Test-OriginalReleaseHeartbeat $CurrentHeartbeat) {
        return "OLD_MODEL_RETAINED"
    }
    $Editors = @(Get-ExactEditorWindows)
    if ($Editors.Count -eq 1) {
        if ([string]$CurrentHeartbeat.status -in @("running", "busy")) {
            try {
                Stop-ExactStrategy $Editors[0] | Out-Null
            }
            catch {
                # A model whose heartbeat cannot be stopped is never closed.
                throw "QMT rollback could not safely stop the loaded model"
            }
        }
        $StoppedHeartbeat = Get-Heartbeat
        if ([string]$StoppedHeartbeat.status -eq "stopped") {
            Close-ExactStrategyEditor $Editors[0]
        }
    }
    $Editor = Open-ExactStrategyEditor
    Start-ExactStrategy $Editor {
        param($Heartbeat)
        return Test-OriginalReleaseHeartbeat $Heartbeat
    } | Out-Null
    return "OLD_MODEL_RESTORED"
}

try {
    if ($Root -ine $ExpectedRoot) {
        throw "QMT reload tool differs from its registered production root"
    }
    Assert-OrdinaryDirectory $ExpectedRoot "QMT registered production root"
    Assert-OrdinaryFile $PythonExe "QMT production Python"
    Assert-OrdinaryFile $Installer "BigQMT strategy installer"
    Assert-OrdinaryDirectory $ReloadStateRoot "QMT reload protected state root"
    if (!(Test-PathInside $ReloadStateRoot $ProgramDataRoot)) {
        throw "QMT reload protected state root escapes ProgramData"
    }

    $TopLevel = ((Invoke-Git @("rev-parse", "--show-toplevel")) -join "").Trim()
    $Origin = ((Invoke-Git @("remote", "get-url", "origin")) -join "").Trim()
    $Branch = ((Invoke-Git @("symbolic-ref", "--short", "HEAD")) -join "").Trim()
    $Head = ((Invoke-Git @("rev-parse", "HEAD")) -join "").Trim().ToLowerInvariant()
    $Blob = ((
        Invoke-Git @(
            "rev-parse",
            "$ExpectedBuild`:$StrategyRepositoryPath"
        )
    ) -join "").Trim().ToLowerInvariant()
    $Dirty = ((
        Invoke-Git @("status", "--porcelain", "--untracked-files=normal")
    ) -join "`n").Trim()
    if (
        [System.IO.Path]::GetFullPath($TopLevel) -ine $ExpectedRoot -or
        $Origin -ine $ExpectedOrigin -or
        $Branch -cne "main" -or
        $Head -cne $ExpectedBuild -or
        $Blob -notmatch "^[0-9a-f]{40}$|^[0-9a-f]{64}$" -or
        $Dirty
    ) {
        throw "QMT reload checkout is not clean exact-main"
    }

    $RecoveryMutexCreated = $false
    $RecoveryMutex = [System.Threading.Mutex]::new(
        $false,
        "Local\ProBigA.BigQmtStrategyRecovery",
        [ref]$RecoveryMutexCreated
    )
    try {
        $RecoveryMutexOwned = $RecoveryMutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $RecoveryMutexOwned = $true
    }
    if (!$RecoveryMutexOwned) {
        throw "QMT strategy recovery is active; release reload refused"
    }
    $ReleaseMutexCreated = $false
    $ReleaseMutex = [System.Threading.Mutex]::new(
        $false,
        "Local\ProBigA.BigQmtStrategyReleaseReload",
        [ref]$ReleaseMutexCreated
    )
    try {
        $ReleaseMutexOwned = $ReleaseMutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $ReleaseMutexOwned = $true
    }
    if (!$ReleaseMutexOwned) {
        throw "QMT strategy release reload is already active"
    }

    $QmtClients = @(
        Get-Process -Name "XtItClient" -ErrorAction SilentlyContinue
    )
    if ($QmtClients.Count -ne 1) {
        Throw-NeedsUserAction "exactly one QMT client must be running"
    }
    $QmtClient = $QmtClients[0]
    if (
        $QmtClient.MainWindowHandle -eq [IntPtr]::Zero -or
        [string]::IsNullOrWhiteSpace([string]$QmtClient.Path)
    ) {
        Throw-NeedsUserAction "the QMT interactive client window is unavailable"
    }
    $CurrentSession = (Get-Process -Id $PID).SessionId
    if ([int]$QmtClient.SessionId -ne [int]$CurrentSession) {
        Throw-NeedsUserAction "QMT is not in the updater interactive session"
    }
    $QmtMainTitle = [string]$QmtClient.MainWindowTitle
    if ($QmtMainTitle -notmatch "^\s*\d+\s*-\s*.+QMT") {
        Throw-NeedsUserAction "QMT login or broker authentication is required"
    }
    $QmtMainHandle = $QmtClient.MainWindowHandle
    $WasMinimized = [ProBigAQmtReleaseWindow]::IsIconic($QmtMainHandle)
    $PreviousForeground = [ProBigAQmtReleaseWindow]::GetForegroundWindow()
    $QmtRoot = [System.IO.Path]::GetFullPath(
        (Split-Path -Parent (Split-Path -Parent $QmtClient.Path))
    )
    Assert-OrdinaryDirectory $QmtRoot "QMT installation root"
    $ExpectedClientPath = Join-Path $QmtRoot "bin.x64\XtItClient.exe"
    if ([System.IO.Path]::GetFullPath($QmtClient.Path) -ine $ExpectedClientPath) {
        throw "QMT client executable path differs"
    }
    $QmtPythonRoot = [System.IO.Path]::GetFullPath(
        (Join-Path $QmtRoot "python")
    )
    Assert-OrdinaryDirectory $QmtPythonRoot "QMT strategy directory"
    $HeartbeatPath = Join-Path (
        $QmtRoot
    ) "userdata\probiga_bridge\heartbeat.json"

    Show-QmtMainWindow
    Assert-NoOtherStrategyEditors
    Assert-NoUnexpectedVisibleQmtWindow
    $Editors = @(Get-ExactEditorWindows)
    if ($Editors.Count -eq 0) {
        $Editor = Open-ExactStrategyEditor
    }
    else {
        $Editor = $Editors[0]
    }
    Assert-NoOtherStrategyEditors
    Assert-NoUnexpectedVisibleQmtWindow
    $PreviousHeartbeat = Get-Heartbeat
    if (!(Test-RunningHeartbeat $PreviousHeartbeat)) {
        Throw-NeedsUserAction (
            "the existing exact QMT strategy must be running before release"
        )
    }

    $Backup = New-ArtifactBackup
    $InstallAttempted = $true
    $Release = Invoke-ExactStrategyInstall

    # Installation is deliberately completed and fully hash-verified while
    # the old in-memory model remains running.  Only then may UI control stop
    # that model.  A stale editor cannot claim the new embedded identity.
    $StillOld = Get-Heartbeat
    if (!(Test-RunningHeartbeat $StillOld)) {
        throw "old QMT model changed state during atomic release install"
    }
    if (Test-ExpectedReleaseHeartbeat $StillOld $Release) {
        $NewModelStarted = $true
        $FinalPayload = [ordered]@{
            schema = "probiga.bigqmt-ui-release-reload.v1"
            status = "IDEMPOTENT"
            expected_build_sha = $ExpectedBuild
            strategy_git_blob = [string]$Release.strategy_git_blob
            strategy_source_sha256 = [string]$Release.strategy_source_sha256
            strategy_artifact_sha256 = [string]$Release.strategy_artifact_sha256
            strategy_loaded_identity_sha256 = `
                [string]$Release.strategy_loaded_identity_sha256
            qmt_client_count = 1
            target_editor_count = 1
            automatic_order_submission = $false
            direct_python_strategy_execution = $false
            rollback_snapshot = [string]$Backup.transaction_id
        }
        $FinalExitCode = 0
    }
    else {
        Stop-ExactStrategy $Editor | Out-Null
        $OldModelStopped = $true
        Close-ExactStrategyEditor $Editor
        $OldEditorClosed = $true
        $NewEditor = Open-ExactStrategyEditor
        $Loaded = Start-ExactStrategy $NewEditor {
            param($Heartbeat)
            return Test-ExpectedReleaseHeartbeat $Heartbeat $Release
        }
        $NewModelStarted = $true
        $Receipt = [ordered]@{
            schema = "probiga.bigqmt-ui-release-reload.v1"
            status = "COMPLETE"
            completed_at = [DateTime]::UtcNow.ToString("o")
            expected_build_sha = $ExpectedBuild
            strategy_git_blob = [string]$Release.strategy_git_blob
            strategy_source_sha256 = [string]$Release.strategy_source_sha256
            strategy_artifact_sha256 = [string]$Release.strategy_artifact_sha256
            strategy_loaded_identity_sha256 = `
                [string]$Release.strategy_loaded_identity_sha256
            qmt_client_pid = [int]$QmtClient.Id
            loaded_heartbeat_status = [string]$Loaded.status
            loaded_heartbeat_updated_at = [string]$Loaded.updated_at
            qmt_client_count = 1
            target_editor_count = 1
            automatic_order_submission = $false
            direct_python_strategy_execution = $false
            rollback_snapshot = [string]$Backup.transaction_id
        }
        Write-AtomicJson (
            Join-Path ([string]$Backup.directory) "complete.json"
        ) $Receipt
        $FinalPayload = $Receipt
        $FinalExitCode = 0
    }
}
catch {
    $FailureText = Get-SafeErrorText $_.Exception.Message
    $NeedsUser = $FailureText.StartsWith(
        "NEEDS_USER_ACTION:",
        [System.StringComparison]::Ordinal
    )
    if ($NeedsUser) {
        $FailureText = $FailureText.Substring(
            "NEEDS_USER_ACTION:".Length
        )
    }
    $RollbackStatus = "NOT_REQUIRED"
    $RollbackError = ""
    if ($InstallAttempted -and $Backup) {
        try {
            $RollbackStatus = Invoke-ModelRollback
        }
        catch {
            $RollbackStatus = "FILES_OR_MODEL_UNVERIFIED"
            $RollbackError = Get-SafeErrorText $_.Exception.Message
        }
    }
    $Status = if ($NeedsUser) {
        "NEEDS_USER_ACTION"
    }
    elseif ($RollbackStatus -in @(
        "OLD_MODEL_RETAINED", "OLD_MODEL_RESTORED"
    )) {
        "ROLLED_BACK"
    }
    else {
        "FAILED_CLOSED"
    }
    $FinalPayload = [ordered]@{
        schema = "probiga.bigqmt-ui-release-reload.v1"
        status = $Status
        expected_build_sha = $ExpectedBuild
        reason = $FailureText
        rollback_status = $RollbackStatus
        rollback_error = $RollbackError
        old_model_was_stopped = $OldModelStopped
        old_editor_was_closed = $OldEditorClosed
        new_model_was_started = $NewModelStarted
        automatic_order_submission = $false
        direct_python_strategy_execution = $false
    }
    $FinalExitCode = if ($NeedsUser) { 3 } else { 2 }
}
finally {
    if ($QmtMainHandle -ne [IntPtr]::Zero) {
        if ($WasMinimized) {
            [ProBigAQmtReleaseWindow]::ShowWindow(
                $QmtMainHandle, 6
            ) | Out-Null
        }
        elseif (
            $PreviousForeground -ne [IntPtr]::Zero -and
            [ProBigAQmtReleaseWindow]::IsWindow($PreviousForeground)
        ) {
            [ProBigAQmtReleaseWindow]::SetForegroundWindow(
                $PreviousForeground
            ) | Out-Null
        }
    }
    if ($ReleaseMutexOwned) {
        $ReleaseMutex.ReleaseMutex()
    }
    if ($ReleaseMutex) {
        $ReleaseMutex.Dispose()
    }
    if ($RecoveryMutexOwned) {
        $RecoveryMutex.ReleaseMutex()
    }
    if ($RecoveryMutex) {
        $RecoveryMutex.Dispose()
    }
}

if (!$FinalPayload) {
    $FinalPayload = [ordered]@{
        schema = "probiga.bigqmt-ui-release-reload.v1"
        status = "FAILED_CLOSED"
        expected_build_sha = $ExpectedBuild
        reason = "QMT reload ended without a release receipt"
    }
    $FinalExitCode = 2
}
[Console]::Out.WriteLine(
    ($FinalPayload | ConvertTo-Json -Depth 12 -Compress)
)
exit $FinalExitCode
