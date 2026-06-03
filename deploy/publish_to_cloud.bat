@echo off
chcp 65001 >nul
echo ============================================
echo   ProBigA 一键发布到云服务器
echo   本地 → 47.113.123.190
echo ============================================

set SERVER=root@47.113.123.190
set PORT=22

echo.
echo [1/4] 打包项目（排除无关文件）...
:: 排除 .git __pycache__ .pyc 等
cd /d "E:\My Code\ProBigA"
tar -cf %TEMP%\probiga_publish.tar ^
    --exclude=".git" --exclude="__pycache__" --exclude="*.pyc" --exclude=".gitignore" ^
    adata/ biz/ server/ tools/ deploy/ requirements-platform.txt .env 2>nul

if %ERRORLEVEL% NEQ 0 (
    :: tar 不支持 --exclude 的话用简单模式
    tar -cf %TEMP%\probiga_publish.tar ^
        adata biz server tools deploy requirements-platform.txt .env 2>nul
)

echo   打包完成

echo.
echo [2/4] 上传到服务器...
scp -P %PORT% %TEMP%\probiga_publish.tar %SERVER%:/root/

echo.
echo [3/4] 在服务器解压并重启...
ssh -p %PORT% %SERVER% "cd /opt/ProBigA && tar -xf /root/probiga_publish.tar && systemctl restart probiga"

echo.
echo [4/4] 验证...
timeout /t 2 >nul
ssh -p %PORT% %SERVER% "systemctl status probiga --no-pager -l | head -5"

del %TEMP%\probiga_publish.tar 2>nul

echo.
echo ============================================
echo   ✅ 发布完成！
echo   访问: http://47.113.123.190
echo ============================================
pause
