# Legacy direct-to-production upload entrypoint retained as a safety fence.
$ErrorActionPreference = "Stop"

$requiredEnvironment = @(
    "PROBIGA_REMOTE_SSH_HOST",
    "PROBIGA_REMOTE_SSH_USER",
    "PROBIGA_REMOTE_SSH_KEY_FILE",
    "PROBIGA_SSH_KNOWN_HOSTS"
)
foreach ($name in $requiredEnvironment) {
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name))) {
        throw "Missing required key-only SSH configuration."
    }
}

throw "Legacy mutable-file production upload is retired; use the audited root-owned deployment broker."
