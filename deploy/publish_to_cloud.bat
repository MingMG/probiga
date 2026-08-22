@echo off
setlocal
if not defined PROBIGA_REMOTE_SSH_HOST goto missing
if not defined PROBIGA_REMOTE_SSH_USER goto missing
if not defined PROBIGA_REMOTE_SSH_KEY_FILE goto missing
if not defined PROBIGA_SSH_KNOWN_HOSTS goto missing
echo ERROR: Legacy mutable-file production upload is retired. Use the audited production deployment workflow. 1>&2
exit /b 2

:missing
echo ERROR: Missing required key-only SSH configuration. 1>&2
exit /b 2
