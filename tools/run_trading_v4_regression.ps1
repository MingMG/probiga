[CmdletBinding()]
param(
    [switch]$IncludeTradingCore,
    [switch]$IncludeStage3
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$gate = Join-Path $repoRoot "tools\run_trading_v4_ci_gate.py"

foreach ($requiredPath in @($python, $gate)) {
    if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
        throw "Required regression input is missing: $requiredPath"
    }
}

$suite = if ($IncludeTradingCore -and $IncludeStage3) {
    "all"
} elseif ($IncludeTradingCore) {
    "extended"
} elseif ($IncludeStage3) {
    "research"
} else {
    "base"
}
& $python $gate --suite $suite --repo-root $repoRoot
exit $LASTEXITCODE
