#!/usr/bin/env bash
set -Eeuo pipefail

# This was the original mutable-checkout bootstrapper. It previously created
# reusable database passwords and printed them to the terminal. Keeping that
# behavior would bypass the audited release path, so the legacy entrypoint is
# deliberately fail closed.

readonly LEGACY_RUNTIME_ENV="${PROBIGA_ROOT_RUNTIME_ENV_FILE:-/etc/probiga/runtime.env}"
readonly LEGACY_MYSQL_OPTION_FILE="${PROBIGA_ROOT_MYSQL_OPTION_FILE:-/etc/probiga/mysql-bootstrap.ini}"

fail() {
    printf 'ERROR: %s\n' "$1" >&2
    exit 2
}

require_root_owned_secret_file() {
    local path="$1"
    local owner mode
    [[ "$path" = /* ]] || fail "credential file path must be absolute"
    [[ -f "$path" && ! -L "$path" ]] || fail "required root-owned credential file is missing"
    owner="$(stat -c '%U' -- "$path")"
    mode="$(stat -c '%a' -- "$path")"
    [[ "$owner" == "root" ]] || fail "credential file must be owned by root"
    (( (8#$mode & 8#077) == 0 )) || fail "credential file must not be accessible by group or others"
}

require_root_owned_secret_file "$LEGACY_RUNTIME_ENV"
require_root_owned_secret_file "$LEGACY_MYSQL_OPTION_FILE"

fail "legacy bootstrap deployment is retired; use the audited production deployment workflow"
