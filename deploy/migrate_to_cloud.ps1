# Legacy one-off migration entrypoint retained as an explicit safety fence.
$ErrorActionPreference = "Stop"

$requiredEnvironment = @(
    "PROBIGA_REMOTE_SSH_HOST",
    "PROBIGA_REMOTE_SSH_USER",
    "PROBIGA_REMOTE_SSH_KEY_FILE",
    "PROBIGA_SSH_KNOWN_HOSTS",
    "PROBIGA_LOCAL_MYSQL_OPTION_FILE",
    "PROBIGA_REMOTE_MYSQL_OPTION_FILE"
)

foreach ($name in $requiredEnvironment) {
    $value = [Environment]::GetEnvironmentVariable($name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Missing required credential-file or SSH identity configuration."
    }
}

throw "Legacy cloud migration is retired; use the audited backup/restore runbook and production deployment workflow."
