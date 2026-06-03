#Requires -Version 5.1
<#
.SYNOPSIS
  AkShare 新浪日线 -> sm_stock_kline，命令风格对齐 a_share_daily_import（--start-date YYYYMMDD、--offset/--limit、--skip-progress）。

.DESCRIPTION
  等价于在项目根执行 python -m biz.stock_market.sync_stock_market --only stock_kline --kline-source akshare ...
  若禁止运行 .ps1，请用 sync_stock_kline_akshare.cmd。

.PARAMETER StartDate
  起始日 YYYYMMDD 或 YYYY-MM-DD，默认 20200101

.PARAMETER EndDate
  结束日；留空则今天

.PARAMETER Offset
  si_all_code 排序后偏移，默认 0

.PARAMETER Limit
  本批股票数；0=从 Offset 到表尾（全市场分批时常用）；正整数=只拉 N 只

.PARAMETER SkipProgress
  不传进度文件、全量重跑（等同 --skip-progress）

.PARAMETER ProgressFile
  断点文件路径；留空则默认 项目根\stock_kline_akshare_progress.txt（仅当未 -SkipProgress 时传入 CLI）

.PARAMETER Adjust
  复权: 空 | qfq | hfq

.PARAMETER AkshareSleep
  每只股票请求后休眠秒数；留空则用 SM_REQUEST_SLEEP

.PARAMETER SkipDdl
  1 跳过 DDL

.PARAMETER TruncateAll
  1 时先 TRUNCATE 整张 sm_stock_kline

.EXAMPLE
  .\scripts\sync_stock_kline_akshare.ps1 -StartDate 20200101 -EndDate 20260417 -Offset 0 -Limit 0 -SkipProgress

.EXAMPLE
  .\scripts\sync_stock_kline_akshare.ps1 -StartDate 20200101 -EndDate 20260417 -Offset 0 -Limit 200
#>
param(
    [string]$StartDate = "20200101",
    [string]$EndDate = "",
    [int]$Offset = 0,
    [int]$Limit = 0,
    [switch]$SkipProgress,
    [string]$ProgressFile = "",
    [ValidateSet("", "qfq", "hfq")]
    [string]$Adjust = "",
    [string]$AkshareSleep = "",
    [ValidateSet("0", "1")]
    [string]$SkipDdl = "1",
    [ValidateSet("0", "1")]
    [string]$TruncateAll = "0",
    [string]$MysqlUrl = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $RepoRoot

if (-not $EndDate) {
    $EndDate = Get-Date -Format "yyyyMMdd"
}

if ($Offset -lt 0) { $Offset = 0 }

if ($SkipDdl -eq "1") {
    $env:SM_SKIP_DDL = "1"
} else {
    Remove-Item Env:SM_SKIP_DDL -ErrorAction SilentlyContinue
}
$env:SM_SKIP_GLOBAL_TRUNCATE = "1"
if ($TruncateAll -eq "1") {
    $env:SM_STOCK_KLINE_AKSHARE_TRUNCATE = "1"
} else {
    Remove-Item Env:SM_STOCK_KLINE_AKSHARE_TRUNCATE -ErrorAction SilentlyContinue
}

if ($AkshareSleep) {
    $env:SM_STOCK_KLINE_AKSHARE_SLEEP = $AkshareSleep
} else {
    Remove-Item Env:SM_STOCK_KLINE_AKSHARE_SLEEP -ErrorAction SilentlyContinue
}

if ($MysqlUrl) {
    $env:MYSQL_URL = $MysqlUrl
}

$adjArg = @()
if ($Adjust) {
    $adjArg = @("--kline-adjust", $Adjust)
}

$pyArgs = @(
    "-m", "biz.stock_market.sync_stock_market",
    "--only", "stock_kline",
    "--kline-source", "akshare",
    "--start-date", $StartDate,
    "--end-date", $EndDate,
    "--offset", "$Offset",
    "--limit", "$Limit"
)
if ($SkipProgress) {
    $pyArgs += "--skip-progress"
} elseif ($ProgressFile) {
    $pyArgs += "--progress-file", $ProgressFile
} else {
    $defaultPf = Join-Path $RepoRoot "stock_kline_akshare_progress.txt"
    $pyArgs += "--progress-file", $defaultPf
}

Write-Host "Repo: $RepoRoot"
Write-Host "Args: start=$StartDate end=$EndDate offset=$Offset limit=$Limit skipProgress=$SkipProgress"

& python @pyArgs @adjArg

exit $LASTEXITCODE
