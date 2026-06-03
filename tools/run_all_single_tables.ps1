# 在仓库根依次跑：新浪指数（可选）+ run_single_table --run-all
# 用法: 在 ProBigA 根目录执行:  powershell -ExecutionPolicy Bypass -File tools\run_all_single_tables.ps1

$ErrorActionPreference = "Continue"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "工作目录: $Root" -ForegroundColor Green

# 若已写入 si_all_index_code 可注释掉下一行
Write-Host "`n[1/2] 新浪指数 si_all_index_code ..." -ForegroundColor Cyan
python tools\fetch_si_all_index_code_sina.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "新浪指数脚本退出码 $LASTEXITCODE（若表已有数据可忽略）" -ForegroundColor Yellow
}

Write-Host "`n[2/2] 其余表 run_single_table --run-all（全市场耗时可数小时）..." -ForegroundColor Cyan
python tools\run_single_table.py --run-all
$code = $LASTEXITCODE
if ($code -ne 0) {
    Write-Host "`nrun_all 结束 exit=$code（有步骤失败时会为 1）" -ForegroundColor Yellow
    exit $code
}
Write-Host "`n全部完成。" -ForegroundColor Green
