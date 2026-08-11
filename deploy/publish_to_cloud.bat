@echo off
chcp 65001 >nul
if not "%PROBIGA_ALLOW_LEGACY_DEPLOY%"=="I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES" (
    echo Legacy deploy blocked. Set PROBIGA_ALLOW_LEGACY_DEPLOY=I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES to override. 1>&2
    exit /b 64
)
if "%PROBIGA_REMOTE_SSH_HOST%"=="" (echo Set PROBIGA_REMOTE_SSH_HOST first. & exit /b 1)
if "%PROBIGA_REMOTE_SSH_USER%"=="" (echo Set PROBIGA_REMOTE_SSH_USER to a named deploy account first. & exit /b 1)
if /I "%PROBIGA_REMOTE_SSH_USER%"=="root" (echo Root production deploy is forbidden. & exit /b 1)
if "%PROBIGA_SSH_KNOWN_HOSTS%"=="" (echo Set PROBIGA_SSH_KNOWN_HOSTS first. & exit /b 1)
if not exist "%PROBIGA_SSH_KNOWN_HOSTS%" (echo Pinned known-hosts file does not exist. & exit /b 1)
if "%PROBIGA_REMOTE_SSH_KEY_FILE%"=="" (echo Set PROBIGA_REMOTE_SSH_KEY_FILE first. & exit /b 1)
if not exist "%PROBIGA_REMOTE_SSH_KEY_FILE%" (echo Deploy key file does not exist. & exit /b 1)
set SSH_OPTIONS=-o BatchMode=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=yes -o UserKnownHostsFile="%PROBIGA_SSH_KNOWN_HOSTS%" -i "%PROBIGA_REMOTE_SSH_KEY_FILE%"
if "%PROBIGA_REMOTE_ROOT%"=="" (echo Set PROBIGA_REMOTE_ROOT first. & exit /b 1)
set "SERVER=%PROBIGA_REMOTE_SSH_USER%@%PROBIGA_REMOTE_SSH_HOST%"
set "REMOTE=%PROBIGA_REMOTE_ROOT%"
echo ============================================
echo   ProBigA 一键发布到云服务器
echo   本地 → %PROBIGA_REMOTE_SSH_HOST%
echo ============================================

set SERVER=%SERVER%
set PORT=22

echo.
echo [1/4] 打包项目（排除无关文件）...
:: 排除 .git __pycache__ .pyc 等
cd /d "%~dp0.."
tar -cf %TEMP%\probiga_publish.tar ^
    --exclude=".git" --exclude="__pycache__" --exclude="*.pyc" --exclude=".gitignore" ^
    adata/ biz/ server/ tools/ deploy/ requirements-platform.txt 2>nul

if %ERRORLEVEL% NEQ 0 (
    :: tar 不支持 --exclude 的话用简单模式
    tar -cf %TEMP%\probiga_publish.tar ^
        adata biz server tools deploy requirements-platform.txt 2>nul
)

echo   打包完成

echo.
echo [2/4] 上传到服务器...
scp %SSH_OPTIONS% -P %PORT% %TEMP%\probiga_publish.tar %SERVER%:/root/

echo.
echo [3/4] 在服务器解压并重启...
ssh %SSH_OPTIONS% -p %PORT% %SERVER% "cd %REMOTE% && tar -xf /root/probiga_publish.tar && systemctl restart probiga"

echo.
echo [4/4] 验证...
timeout /t 2 >nul
ssh %SSH_OPTIONS% -p %PORT% %SERVER% "systemctl status probiga --no-pager -l | head -5"

del %TEMP%\probiga_publish.tar 2>nul

echo.
echo ============================================
echo   ✅ 发布完成！
echo   访问: http://%PROBIGA_REMOTE_SSH_HOST%
echo ============================================
pause
