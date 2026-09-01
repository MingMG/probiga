param(
    [switch]$Force,
    [int]$HeartbeatMaxAgeSeconds = 30,
    [int]$FullSnapshotMaxAgeSeconds = 75,
    [int]$SyncReceiptMaxAgeSeconds = 75,
    [int]$Level1CallbackMaxAgeSeconds = 15,
    [int]$MinimumBackoffSeconds = 30,
    [int]$MaximumBackoffSeconds = 900
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"
if (!(Test-Path -LiteralPath $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

$StrategyName = "PROBIGA_BIGQMT_BRIDGE"
$StatePath = Join-Path $DataDir "big_qmt_strategy_autorecover.state.json"

function Test-RecoveryWindow {
    if ($Force) {
        return $true
    }
    $now = Get-Date
    if ($now.DayOfWeek -in @(
        [System.DayOfWeek]::Saturday,
        [System.DayOfWeek]::Sunday
    )) {
        return $false
    }
    return (
        $now.TimeOfDay -ge [TimeSpan]::FromHours(6.5) -and
        $now.TimeOfDay -le [TimeSpan]::FromHours(23)
    )
}

function Test-Level1CollectionWindow {
    $now = Get-Date
    if ($now.DayOfWeek -in @(
        [System.DayOfWeek]::Saturday,
        [System.DayOfWeek]::Sunday
    )) {
        return $false
    }
    $morning = (
        $now.TimeOfDay -ge [TimeSpan]::FromHours(9.5) -and
        $now.TimeOfDay -le [TimeSpan]::FromHours(11.5)
    )
    $afternoon = (
        $now.TimeOfDay -ge [TimeSpan]::FromHours(13) -and
        $now.TimeOfDay -le [TimeSpan]::FromHours(15)
    )
    return $morning -or $afternoon
}

function Get-Heartbeat {
    param([string]$Path)
    if (!(Test-Path -LiteralPath $Path)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Get-EndToEndHealth {
    param([string]$BridgeRoot)
    $heartbeatPath = Join-Path $BridgeRoot "heartbeat.json"
    $fullPath = Join-Path $BridgeRoot "full_quotes.json"
    $consumerPath = Join-Path $BridgeRoot "consumer_status.json"
    $trackedPath = Join-Path $BridgeRoot "tracked_quotes.json"
    $heartbeat = Get-Heartbeat $heartbeatPath
    $full = Get-Heartbeat $fullPath
    $consumer = Get-Heartbeat $consumerPath
    $tracked = Get-Heartbeat $trackedPath
    $heartbeatHealthy = Test-HeartbeatHealthy $heartbeat
    $fullHealthy = $false
    if (
        $full -and
        (Test-Path -LiteralPath $fullPath) -and
        [int]$full.quote_count -gt 0
    ) {
        $fullAge = (
            (Get-Date) -
            (Get-Item -LiteralPath $fullPath).LastWriteTime
        ).TotalSeconds
        $fullHealthy = $fullAge -le $FullSnapshotMaxAgeSeconds
    }
    $receiptHealthy = $false
    $receiptSourceAge = $null
    if (
        $consumer -and
        (Test-Path -LiteralPath $consumerPath) -and
        $consumer.full_sync_receipt
    ) {
        $receiptAge = (
            (Get-Date) -
            (Get-Item -LiteralPath $consumerPath).LastWriteTime
        ).TotalSeconds
        $sourceSnapshotTimestamp = 0.0
        $hasSourceSnapshotTimestamp = [double]::TryParse(
            [string]$consumer.full_sync_receipt.source_snapshot_token,
            [ref]$sourceSnapshotTimestamp
        )
        if ($hasSourceSnapshotTimestamp) {
            $nowUnix = [DateTimeOffset]::Now.ToUnixTimeMilliseconds() / 1000.0
            $receiptSourceAge = [Math]::Max(
                0.0,
                $nowUnix - $sourceSnapshotTimestamp
            )
        }
        $receiptMatchesCurrent = (
            [string]$consumer.full_sync_receipt.source_batch_id -eq
                [string]$full.batch_id
        )
        # The producer can publish its next file while the previous full
        # database replacement is committing. Accept that bounded overlap
        # only when the generation proven by the receipt is still fresh.
        $receiptAttestsFreshGeneration = (
            $receiptMatchesCurrent -or
            (
                $null -ne $receiptSourceAge -and
                $receiptSourceAge -le $SyncReceiptMaxAgeSeconds
            )
        )
        $receiptHealthy = (
            $receiptAge -le $SyncReceiptMaxAgeSeconds -and
            [string]$consumer.full_sync_receipt.quality_status -eq "PASS" -and
            $receiptAttestsFreshGeneration
        )
    }
    $level1Required = Test-Level1CollectionWindow
    $level1Healthy = !$level1Required
    $level1CallbackAge = $null
    if ($level1Required) {
        $lastCallbackTimestamp = 0.0
        $hasLastCallbackTimestamp = [double]::TryParse(
            [string]$heartbeat.last_callback_ts,
            [ref]$lastCallbackTimestamp
        ) -and $lastCallbackTimestamp -gt 0
        if (!$hasLastCallbackTimestamp -and $tracked) {
            $hasLastCallbackTimestamp = [double]::TryParse(
                [string]$tracked.last_callback_ts,
                [ref]$lastCallbackTimestamp
            ) -and $lastCallbackTimestamp -gt 0
        }
        if (!$hasLastCallbackTimestamp -and $tracked -and $tracked.quotes) {
            $latestCallbackAt = [DateTime]::MinValue
            foreach ($property in $tracked.quotes.PSObject.Properties) {
                $candidateAt = [DateTime]::MinValue
                if (
                    [DateTime]::TryParse(
                        [string]$property.Value._probiga_received_at,
                        [ref]$candidateAt
                    ) -and
                    $candidateAt -gt $latestCallbackAt
                ) {
                    $latestCallbackAt = $candidateAt
                }
            }
            if ($latestCallbackAt -gt [DateTime]::MinValue) {
                $lastCallbackTimestamp = (
                    [DateTimeOffset]$latestCallbackAt
                ).ToUnixTimeMilliseconds() / 1000.0
                $hasLastCallbackTimestamp = $true
            }
        }
        $trackedAge = [double]::PositiveInfinity
        if (Test-Path -LiteralPath $trackedPath) {
            $trackedAge = (
                (Get-Date) -
                (Get-Item -LiteralPath $trackedPath).LastWriteTime
            ).TotalSeconds
        }
        if ($hasLastCallbackTimestamp) {
            $nowUnix = [DateTimeOffset]::Now.ToUnixTimeMilliseconds() / 1000.0
            $level1CallbackAge = [Math]::Max(
                0.0,
                $nowUnix - $lastCallbackTimestamp
            )
        }
        $subscriptionHealthy = (
            $heartbeat -and
            $null -ne $heartbeat.subscription_id -and
            [string]$heartbeat.subscription_id -notin @("", "-1")
        )
        $level1Healthy = (
            $subscriptionHealthy -and
            $null -ne $level1CallbackAge -and
            $level1CallbackAge -le $Level1CallbackMaxAgeSeconds -and
            $trackedAge -le $Level1CallbackMaxAgeSeconds
        )
    }
    $failed = @()
    if (!$heartbeatHealthy) { $failed += "strategy_heartbeat" }
    if (!$fullHealthy) { $failed += "full_market_snapshot" }
    if (!$receiptHealthy) { $failed += "sync_receipt" }
    if (!$level1Healthy) { $failed += "level1_callback" }
    return [pscustomobject]@{
        Healthy = $failed.Count -eq 0
        HeartbeatHealthy = $heartbeatHealthy
        FullSnapshotHealthy = $fullHealthy
        SyncReceiptHealthy = $receiptHealthy
        Level1Required = $level1Required
        Level1CallbackHealthy = $level1Healthy
        Level1CallbackAgeSeconds = $level1CallbackAge
        ReceiptSourceAgeSeconds = $receiptSourceAge
        FailedChecks = $failed
        Heartbeat = $heartbeat
    }
}

function Get-RetryDelaySeconds {
    param([int]$ConsecutiveFailures)
    $power = [Math]::Min(
        10,
        [Math]::Max(0, $ConsecutiveFailures - 1)
    )
    return [int][Math]::Min(
        $MaximumBackoffSeconds,
        $MinimumBackoffSeconds * [Math]::Pow(2, $power)
    )
}

function Write-QmtAlert {
    param(
        [string]$Status,
        [string]$Message
    )
    $payload = [ordered]@{
        timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        component = "qmt_end_to_end_bridge"
        status = $Status
        message = $Message
    }
    $payload |
        ConvertTo-Json -Compress |
        Add-Content -LiteralPath (
            Join-Path $DataDir "qmt_health_alerts.jsonl"
        ) -Encoding UTF8
    $webhook = if ($env:QMT_ALERT_WEBHOOK_URL) {
        $env:QMT_ALERT_WEBHOOK_URL
    }
    else {
        $env:WECOM_WEBHOOK_URL
    }
    if ($webhook) {
        try {
            $body = @{
                msgtype = "markdown"
                markdown = @{
                    content = (
                        "### ProBigA QMT $Status`n" +
                        "> component: end-to-end quote bridge`n" +
                        "> time: $($payload.timestamp)`n" +
                        "> $Message"
                    )
                }
            } | ConvertTo-Json -Depth 5
            Invoke-RestMethod `
                -Uri $webhook `
                -Method Post `
                -ContentType "application/json" `
                -Body $body `
                -TimeoutSec 10 | Out-Null
        }
        catch {
            Write-Warning "QMT alert delivery failed: $($_.Exception.Message)"
        }
    }
}

function Test-HeartbeatHealthy {
    param($Heartbeat)
    $status = ([string]$Heartbeat.status).Trim().ToLowerInvariant()
    if (!$Heartbeat -or $status -notin @("running", "busy")) {
        return $false
    }
    $updatedAt = [DateTime]::MinValue
    if (![DateTime]::TryParse([string]$Heartbeat.updated_at, [ref]$updatedAt)) {
        return $false
    }
    return ((Get-Date) - $updatedAt).TotalSeconds -le $HeartbeatMaxAgeSeconds
}

function Get-RecoveryState {
    if (!(Test-Path -LiteralPath $StatePath)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Set-RecoveryState {
    param(
        [int]$Attempts,
        [string]$Status,
        [string]$Detail,
        $Client,
        [DateTime]$NextAttemptAt = [DateTime]::MinValue
    )
    $payload = @{
        consecutive_failures = $Attempts
        attempts = $Attempts
        last_attempt = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        status = $Status
        detail = $Detail
        next_attempt_at = if (
            $NextAttemptAt -gt [DateTime]::MinValue
        ) {
            $NextAttemptAt.ToString("yyyy-MM-dd HH:mm:ss")
        }
        else {
            $null
        }
    }
    if ($Client) {
        $payload.client_pid = [int]$Client.Id
        $payload.client_started_at = $Client.StartTime.ToString(
            "yyyy-MM-dd HH:mm:ss"
        )
    }
    $payload |
        ConvertTo-Json |
        Set-Content -LiteralPath $StatePath -Encoding UTF8
}

if (-not ("ProBigAQmtWindow" -as [type])) {
    Add-Type @'
using System;
using System.Runtime.InteropServices;
using System.Text;

public static class ProBigAQmtWindow
{
    public delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);

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
    public static extern bool EnumWindows(EnumWindowsProc callback, IntPtr lParam);

    [DllImport("user32.dll")]
    public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint processId);

    [DllImport("user32.dll", CharSet = CharSet.Unicode)]
    public static extern int GetWindowText(IntPtr hWnd, StringBuilder text, int count);

    [DllImport("user32.dll")]
    public static extern bool IsWindowVisible(IntPtr hWnd);

    [DllImport("user32.dll")]
    public static extern bool GetWindowRect(IntPtr hWnd, out RECT rect);

    [DllImport("user32.dll")]
    public static extern IntPtr MonitorFromWindow(
        IntPtr hWnd,
        uint flags
    );

    [DllImport("user32.dll")]
    public static extern bool GetMonitorInfo(
        IntPtr monitor,
        ref MONITORINFO info
    );

    [DllImport("user32.dll")]
    public static extern bool SetForegroundWindow(IntPtr hWnd);

    [DllImport("user32.dll")]
    public static extern IntPtr GetForegroundWindow();

    [DllImport("user32.dll")]
    public static extern bool ShowWindow(IntPtr hWnd, int command);

    [DllImport("user32.dll")]
    public static extern bool IsIconic(IntPtr hWnd);

    [DllImport("user32.dll")]
    public static extern bool SetCursorPos(int x, int y);

    [DllImport("user32.dll")]
    public static extern void mouse_event(
        uint flags,
        uint dx,
        uint dy,
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

    public static IntPtr FindVisibleWindow(uint processId, string titlePart)
    {
        IntPtr found = IntPtr.Zero;
        EnumWindows(delegate(IntPtr hWnd, IntPtr lParam)
        {
            uint owner;
            GetWindowThreadProcessId(hWnd, out owner);
            if (owner != processId || !IsWindowVisible(hWnd))
            {
                return true;
            }
            StringBuilder title = new StringBuilder(512);
            GetWindowText(hWnd, title, title.Capacity);
            if (title.ToString().IndexOf(
                titlePart,
                StringComparison.OrdinalIgnoreCase
            ) >= 0)
            {
                found = hWnd;
                return false;
            }
            return true;
        }, IntPtr.Zero);
        return found;
    }
}
'@
}

function Invoke-WindowClick {
    param(
        [IntPtr]$Handle,
        [double]$XRatio,
        [double]$YRatio,
        [switch]$UseMonitorWorkArea
    )
    $rect = New-Object ProBigAQmtWindow+RECT
    if (![ProBigAQmtWindow]::GetWindowRect($Handle, [ref]$rect)) {
        throw "cannot read QMT window bounds"
    }
    $clickLeft = $rect.Left
    $clickTop = $rect.Top
    $clickRight = $rect.Right
    $clickBottom = $rect.Bottom
    if ($UseMonitorWorkArea) {
        # QMT's main wrapper can span monitors with different resolutions.
        # Its actionable navigation pane is anchored to the work area of the
        # monitor that owns the main window, not to the wrapper's union rect.
        $monitor = [ProBigAQmtWindow]::MonitorFromWindow($Handle, 2)
        if ($monitor -eq [IntPtr]::Zero) {
            throw "cannot resolve QMT main monitor"
        }
        $monitorInfo = New-Object ProBigAQmtWindow+MONITORINFO
        $monitorInfo.cbSize = [Runtime.InteropServices.Marshal]::SizeOf(
            $monitorInfo
        )
        if (![ProBigAQmtWindow]::GetMonitorInfo($monitor, [ref]$monitorInfo)) {
            throw "cannot read QMT main monitor bounds"
        }
        $clickLeft = $monitorInfo.rcWork.Left
        $clickTop = $monitorInfo.rcWork.Top
        $clickRight = $monitorInfo.rcWork.Right
        $clickBottom = $monitorInfo.rcWork.Bottom
        $boundsTolerance = 8
        if (
            $clickLeft -lt $rect.Left - $boundsTolerance -or
            $clickTop -lt $rect.Top - $boundsTolerance -or
            $clickRight -gt $rect.Right + $boundsTolerance -or
            $clickBottom -gt $rect.Bottom + $boundsTolerance
        ) {
            throw "QMT main window does not cover its navigation monitor"
        }
    }
    $width = [Math]::Max(1, $clickRight - $clickLeft)
    $height = [Math]::Max(1, $clickBottom - $clickTop)
    $x = $clickLeft + [int]($width * $XRatio)
    $y = $clickTop + [int]($height * $YRatio)
    [ProBigAQmtWindow]::SetForegroundWindow($Handle) | Out-Null
    [ProBigAQmtWindow]::SetCursorPos($x, $y) | Out-Null
    [ProBigAQmtWindow]::mouse_event(
        0x0002,
        0,
        0,
        0,
        [UIntPtr]::Zero
    )
    [ProBigAQmtWindow]::mouse_event(
        0x0004,
        0,
        0,
        0,
        [UIntPtr]::Zero
    )
}

function Get-QmtMainWorkArea {
    param([IntPtr]$Handle)
    $rect = New-Object ProBigAQmtWindow+RECT
    if (![ProBigAQmtWindow]::GetWindowRect($Handle, [ref]$rect)) {
        throw "cannot read QMT main window bounds"
    }
    $monitor = [ProBigAQmtWindow]::MonitorFromWindow($Handle, 2)
    if ($monitor -eq [IntPtr]::Zero) {
        throw "cannot resolve QMT main monitor"
    }
    $monitorInfo = New-Object ProBigAQmtWindow+MONITORINFO
    $monitorInfo.cbSize = [Runtime.InteropServices.Marshal]::SizeOf(
        $monitorInfo
    )
    if (![ProBigAQmtWindow]::GetMonitorInfo($monitor, [ref]$monitorInfo)) {
        throw "cannot read QMT main monitor bounds"
    }
    $boundsTolerance = 8
    if (
        $monitorInfo.rcWork.Left -lt $rect.Left - $boundsTolerance -or
        $monitorInfo.rcWork.Top -lt $rect.Top - $boundsTolerance -or
        $monitorInfo.rcWork.Right -gt $rect.Right + $boundsTolerance -or
        $monitorInfo.rcWork.Bottom -gt $rect.Bottom + $boundsTolerance
    ) {
        throw "QMT main window does not cover its navigation monitor"
    }
    return [pscustomobject]@{
        Left = [int]$monitorInfo.rcWork.Left
        Top = [int]$monitorInfo.rcWork.Top
        Right = [int]$monitorInfo.rcWork.Right
        Bottom = [int]$monitorInfo.rcWork.Bottom
    }
}

function Invoke-WindowPointClick {
    param(
        [IntPtr]$Handle,
        [int]$X,
        [int]$Y
    )
    $rect = New-Object ProBigAQmtWindow+RECT
    if (![ProBigAQmtWindow]::GetWindowRect($Handle, [ref]$rect)) {
        throw "cannot read QMT point-click bounds"
    }
    if (
        $X -lt $rect.Left -or
        $X -ge $rect.Right -or
        $Y -lt $rect.Top -or
        $Y -ge $rect.Bottom
    ) {
        throw "QMT point click escapes the target window"
    }
    [ProBigAQmtWindow]::SetForegroundWindow($Handle) | Out-Null
    [ProBigAQmtWindow]::SetCursorPos($X, $Y) | Out-Null
    [ProBigAQmtWindow]::mouse_event(
        0x0002, 0, 0, 0, [UIntPtr]::Zero
    )
    [ProBigAQmtWindow]::mouse_event(
        0x0004, 0, 0, 0, [UIntPtr]::Zero
    )
}

function Get-QmtStrategyPaneLayout {
    param([IntPtr]$MainHandle)
    $work = Get-QmtMainWorkArea $MainHandle
    $paneLeft = [ProBigAQmtWindow]::FindStrategyPaneLeft(
        $work.Left,
        $work.Top,
        $work.Right,
        $work.Bottom
    )
    $relativeLeft = $paneLeft - $work.Left
    $fullWidthList = $relativeLeft -ge 45 -and $relativeLeft -le 60
    $embeddedList = $relativeLeft -ge 400 -and $relativeLeft -le 1000
    if (!$fullWidthList -and !$embeddedList) {
        throw "QMT model-research strategy pane is not visibly identifiable"
    }
    return [pscustomobject]@{
        SearchX = [int]($paneLeft + 70)
        SearchY = [int]($work.Top + 67)
        EditX = [int]($paneLeft + 322)
        EditY = [int]($work.Top + 137)
    }
}

$createdNew = $false
$recoveryMutex = [System.Threading.Mutex]::new(
    $false,
    "Local\ProBigA.BigQmtStrategyRecovery",
    [ref]$createdNew
)
$mutexOwned = $false
try {
    $mutexOwned = $recoveryMutex.WaitOne(0)
}
catch [System.Threading.AbandonedMutexException] {
    # The previous recovery process died while owning the lock.  The current
    # process owns the abandoned mutex now and may safely repair the bridge.
    $mutexOwned = $true
}
if (!$mutexOwned) {
    # ``createdNew`` only tells us whether the named kernel object already
    # existed; it cannot distinguish an active recovery from a process that
    # has held the mutex far beyond this script's bounded 55-second window.
    # A fresh RUNNING lease wins.  A stale lease may be taken over under a
    # second mutex, avoiding both permanent lockout and concurrent takeovers.
    $leaseState = Get-RecoveryState
    $leaseAttempt = [DateTime]::MinValue
    $leaseFresh = (
        $leaseState -and
        [string]$leaseState.status -eq "running" -and
        [DateTime]::TryParse(
            [string]$leaseState.last_attempt,
            [ref]$leaseAttempt
        ) -and
        ((Get-Date) - $leaseAttempt).TotalSeconds -lt 120
    )
    if ($leaseFresh) {
        $recoveryMutex.Dispose()
        Write-Output "Big QMT strategy recovery already running."
        exit 0
    }

    $recoveryMutex.Dispose()
    $staleTakeoverCreated = $false
    $recoveryMutex = [System.Threading.Mutex]::new(
        $false,
        "Local\ProBigA.BigQmtStrategyRecovery.StaleTakeover",
        [ref]$staleTakeoverCreated
    )
    try {
        $mutexOwned = $recoveryMutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $mutexOwned = $true
    }
    if (!$mutexOwned) {
        $recoveryMutex.Dispose()
        Write-Output "Big QMT stale-lock takeover already running."
        exit 0
    }
    Write-Warning (
        "Big QMT recovery mutex exceeded its lease; " +
        "continuing under stale-lock takeover protection."
    )
}

$mainHandle = [IntPtr]::Zero
$previousForeground = [IntPtr]::Zero
$wasMinimized = $false
try {
    $qmt = Get-Process -Name "XtItClient" -ErrorAction SilentlyContinue |
        Where-Object { $_.MainWindowHandle -ne [IntPtr]::Zero } |
        Select-Object -First 1
    if (!$qmt) {
        Write-Output "Big QMT strategy recovery skipped: client window unavailable."
        exit 0
    }

    $qmtHome = Split-Path -Parent (Split-Path -Parent $qmt.Path)
    $bridgeRoot = Join-Path $qmtHome "userdata\probiga_bridge"
    $heartbeatPath = Join-Path $bridgeRoot "heartbeat.json"
    $health = Get-EndToEndHealth $bridgeRoot
    if ($health.Healthy) {
        $healthyState = Get-RecoveryState
        if (
            $healthyState -and
            [string]$healthyState.status -ne "success"
        ) {
            Set-RecoveryState `
                0 `
                "success" `
                "heartbeat, full snapshot, sync receipt and Level1 callback healthy" `
                $qmt
            Write-QmtAlert `
                "RECOVERED" `
                "Strategy heartbeat, full snapshot, sync receipt and Level1 callback recovered."
        }
        Write-Output "Big QMT end-to-end health is healthy."
        exit 0
    }
    if (
        $health.HeartbeatHealthy -and
        $health.FullSnapshotHealthy -and
        !$health.SyncReceiptHealthy -and
        $health.Level1CallbackHealthy
    ) {
        Write-Output (
            "Big QMT consumer receipt is stale; " +
            "consumer restart is delegated to the local supervisor."
        )
        exit 0
    }
    if (
        [string]::IsNullOrWhiteSpace([string]$qmt.MainWindowTitle) -or
        ([string]$qmt.MainWindowTitle) -notmatch (
            "^\s*\d+\s*-\s*.+QMT"
        )
    ) {
        Write-Output "Big QMT strategy recovery skipped: client is not logged in."
        exit 0
    }
    if (!(Test-RecoveryWindow)) {
        Write-Output "Big QMT strategy recovery skipped outside guarded window."
        exit 0
    }

    $state = Get-RecoveryState
    $attempts = if ($state) {
        [int]$state.consecutive_failures
    }
    else {
        0
    }
    $lastAttempt = [DateTime]::MinValue
    $nextAttempt = [DateTime]::MinValue
    if ($state) {
        [DateTime]::TryParse(
            [string]$state.last_attempt,
            [ref]$lastAttempt
        ) | Out-Null
        [DateTime]::TryParse(
            [string]$state.next_attempt_at,
            [ref]$nextAttempt
        ) | Out-Null
        $sameClient = (
            ([int]$state.client_pid -eq [int]$qmt.Id) -and
            $lastAttempt -ge $qmt.StartTime
        )
        if (!$sameClient) {
            # A newly logged-in client starts a new consecutive-failure series.
            $attempts = 0
            $lastAttempt = [DateTime]::MinValue
            $nextAttempt = [DateTime]::MinValue
        }
    }
    if (!$Force -and (Get-Date) -lt $nextAttempt) {
        Write-Output (
            "Big QMT strategy recovery waiting for persistent backoff " +
            "until $($nextAttempt.ToString('HH:mm:ss'))."
        )
        exit 0
    }

    $attempts += 1
    $delaySeconds = Get-RetryDelaySeconds $attempts
    $nextAttempt = (Get-Date).AddSeconds($delaySeconds)
    Set-RecoveryState `
        $attempts `
        "running" `
        "opening read-only bridge strategy" `
        $qmt `
        $nextAttempt
    Write-QmtAlert `
        "RETRYING" `
        (
            "End-to-end health failed: $($health.FailedChecks -join ', '). " +
            "Recovery attempt $attempts started; next retry in " +
            "$delaySeconds seconds with no daily attempt limit."
        )

    Add-Type -AssemblyName System.Windows.Forms
    $mainHandle = $qmt.MainWindowHandle
    $previousForeground = [ProBigAQmtWindow]::GetForegroundWindow()
    $wasMinimized = [ProBigAQmtWindow]::IsIconic($mainHandle)
    [ProBigAQmtWindow]::ShowWindow($mainHandle, 9) | Out-Null
    [ProBigAQmtWindow]::SetForegroundWindow($mainHandle) | Out-Null
    Start-Sleep -Milliseconds 500

    $editorHandle = [ProBigAQmtWindow]::FindVisibleWindow(
        [uint32]$qmt.Id,
        "$StrategyName-"
    )
    if ($editorHandle -eq [IntPtr]::Zero) {
        # QMT 2.1.19: open model research, visually locate its strategy pane,
        # filter to the exact bridge strategy, then click that row's edit
        # action. The pane boundary is detected from the visible chart/pane
        # relationship so both single-screen and spanning layouts are safe.
        # Closing an editor may leave QMT on its model backtest/trading
        # subpage.  The same location is a harmless market-index tab on the
        # home page, so this safely normalizes both states before navigation.
        # QMT 2.1.19: 0.056/0.039 is the account badge and opens About QMT.
        # The read-only return-home link is centered at this work-area point.
        Invoke-WindowClick $mainHandle 0.107 0.077 -UseMonitorWorkArea
        Start-Sleep -Milliseconds 800
        Invoke-WindowClick $mainHandle 0.470 0.015
        Start-Sleep -Milliseconds 1000
        $layout = Get-QmtStrategyPaneLayout $mainHandle
        Invoke-WindowPointClick `
            $mainHandle `
            $layout.SearchX `
            $layout.SearchY
        [System.Windows.Forms.SendKeys]::SendWait("^a")
        [System.Windows.Forms.SendKeys]::SendWait($StrategyName)
        [System.Windows.Forms.SendKeys]::SendWait("{ENTER}")
        Start-Sleep -Milliseconds 1200
        Invoke-WindowPointClick `
            $mainHandle `
            $layout.EditX `
            $layout.EditY
        $editorDeadline = (Get-Date).AddSeconds(8)
        do {
            Start-Sleep -Milliseconds 500
            $heartbeat = Get-Heartbeat $heartbeatPath
            if (Test-HeartbeatHealthy $heartbeat) {
                break
            }
            $editorHandle = [ProBigAQmtWindow]::FindVisibleWindow(
                [uint32]$qmt.Id,
                "$StrategyName-"
            )
        } while (
            $editorHandle -eq [IntPtr]::Zero -and
            (Get-Date) -lt $editorDeadline
        )
    }
    $heartbeat = Get-Heartbeat $heartbeatPath
    $healthy = Test-HeartbeatHealthy $heartbeat
    if (!$healthy -and $editorHandle -eq [IntPtr]::Zero) {
        throw "QMT bridge strategy editor did not open"
    }

    if (!$healthy) {
        # QMT 2.1.19 full-screen editor: run is the second labeled action in
        # the compact toolbar immediately above the source pane.
        Invoke-WindowClick $editorHandle 0.339 0.151
    }

    $deadline = (Get-Date).AddSeconds(55)
    do {
        Start-Sleep -Seconds 1
        $health = Get-EndToEndHealth $bridgeRoot
    } while (!$health.Healthy -and (Get-Date) -lt $deadline)

    if (!$health.Healthy) {
        throw (
            "QMT bridge end-to-end health did not recover: " +
            ($health.FailedChecks -join ",")
        )
    }
    Set-RecoveryState `
        0 `
        "success" `
        "heartbeat, full snapshot and sync receipt healthy" `
        $qmt
    Write-QmtAlert `
        "RECOVERED" `
        "Strategy heartbeat, full snapshot and sync receipt recovered."
    Write-Output "Big QMT strategy recovered end-to-end."
}
catch {
    $state = Get-RecoveryState
    $attempts = if ($state) {
        [Math]::Max(
            1,
            [int]$state.consecutive_failures
        )
    }
    else {
        1
    }
    $delaySeconds = Get-RetryDelaySeconds $attempts
    $nextAttempt = (Get-Date).AddSeconds($delaySeconds)
    Set-RecoveryState `
        $attempts `
        "failed" `
        $_.Exception.Message `
        $qmt `
        $nextAttempt
    Write-QmtAlert `
        "FAILED" `
        (
            "QMT end-to-end recovery failed: $($_.Exception.Message). " +
            "Retrying in $delaySeconds seconds."
        )
    Write-Error "Big QMT strategy recovery failed: $($_.Exception.Message)"
    exit 1
}
finally {
    if ($mainHandle -ne [IntPtr]::Zero) {
        if ($wasMinimized) {
            [ProBigAQmtWindow]::ShowWindow($mainHandle, 6) | Out-Null
        }
        if (
            $previousForeground -ne [IntPtr]::Zero -and
            $previousForeground -ne $mainHandle
        ) {
            [ProBigAQmtWindow]::SetForegroundWindow(
                $previousForeground
            ) | Out-Null
        }
    }
    if ($mutexOwned) {
        $recoveryMutex.ReleaseMutex()
    }
    $recoveryMutex.Dispose()
}
