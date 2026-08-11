@echo off
chcp 65001 >nul
if not "%PROBIGA_ALLOW_LEGACY_DEPLOY%"=="I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES" (
    echo Legacy deploy blocked. Set PROBIGA_ALLOW_LEGACY_DEPLOY=I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES to override. 1>&2
    exit /b 64
)
powershell -ExecutionPolicy Bypass -File "%~dp0deploy.ps1"
