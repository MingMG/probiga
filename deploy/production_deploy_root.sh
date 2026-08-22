#!/usr/bin/env bash
# Root-owned production deployment broker invoked through restricted sudo.

set -Eeuo pipefail
umask 077

# Do not let sudo caller state influence Bash, Git, SSH, pip, or Python.  The
# one-time re-exec happens before argument parsing and before touching any
# production lock, cache, journal, unit, or receipt.
if [ "${PROBIGA_BROKER_CLEAN_ENV:-}" != 1 ]; then
  exec /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/root \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    SUDO_USER="${SUDO_USER:-}" \
    PROBIGA_BROKER_CLEAN_ENV=1 \
    /usr/bin/bash --noprofile --norc "$0" "$@"
fi
unset BASH_ENV ENV CDPATH GLOBIGNORE SHELLOPTS BASHOPTS 2>/dev/null || true
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
  GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM \
  GIT_CONFIG_NOSYSTEM GIT_CONFIG_COUNT GIT_SSH GIT_SSH_COMMAND \
  GIT_CEILING_DIRECTORIES GIT_DISCOVERY_ACROSS_FILESYSTEM GIT_NAMESPACE \
  GIT_ATTR_NOSYSTEM 2>/dev/null || true
unset PIP_CONFIG_FILE PIP_INDEX_URL PIP_EXTRA_INDEX_URL PIP_TRUSTED_HOST \
  PIP_REQUIRE_VIRTUALENV PIP_TARGET PIP_PREFIX PIP_USER PIP_CACHE_DIR \
  PYTHONPATH PYTHONHOME PYTHONSTARTUP PYTHONUSERBASE PYTHONINSPECT \
  PYTHONWARNINGS 2>/dev/null || true

readonly LEGACY_REPOSITORY=/opt/ProBigA
readonly RELEASE_SOURCE_ROOT=/var/lib/probiga/release-sources
readonly CODE_GIT_CACHE="$RELEASE_SOURCE_ROOT/probiga.git"
readonly BROKER_LOCK_ROOT=/run/probiga
readonly BROKER_LOCK_FILE="$BROKER_LOCK_ROOT/production-broker.lock"
readonly DEPLOY_PROTOCOL_VERSION=probiga-production-deploy-v4
readonly RECOVERY_PROTOCOL_VERSION=probiga-database-guard-recovery-v2
readonly CAPABILITY_SCHEMA=probiga.production-deploy.capabilities.v1
readonly TRUSTED_ARTIFACT_PROTOCOL=probiga-trusted-artifacts-v2
readonly BROKER_COMPILED_LOCK_STATUS=BLOCKED_CROSS_PLATFORM_REGEN_REQUIRED
readonly TRUSTED_REMOTE=git@github.com:MingMG/probiga.git
readonly DEPLOY_USER=probiga-deploy
readonly GITHUB_SSH_KEY=/etc/probiga/github-readonly-ed25519
readonly GITHUB_KNOWN_HOSTS=/etc/probiga/github_known_hosts
readonly REMOTE_GIT_SSH="/usr/bin/ssh -i $GITHUB_SSH_KEY -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$GITHUB_KNOWN_HOSTS -o GlobalKnownHostsFile=/dev/null -o PasswordAuthentication=no -o KbdInteractiveAuthentication=no"
readonly DATABASE_WRITER_GUARD_DIR=/var/lib/probiga/deploy-guards
readonly DATABASE_WRITER_GUARD_FILE="$DATABASE_WRITER_GUARD_DIR/database-migration-unverified"
readonly DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/database-writer-restore-pending"
readonly ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/activation-unit-transaction"
readonly ACTIVATION_UNIT_SNAPSHOT_MANIFEST="$ACTIVATION_UNIT_SNAPSHOT_DIR/manifest"
readonly ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST="$ACTIVATION_UNIT_SNAPSHOT_DIR/new-manifest"
readonly ACTIVATION_UNIT_SNAPSHOT_PHASE="$ACTIVATION_UNIT_SNAPSHOT_DIR/phase"
readonly ACTIVATION_UNIT_SNAPSHOT_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state"
readonly ACTIVATION_UNIT_SNAPSHOT_STATE_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state.sha256"
readonly ACTIVATION_GOVERNANCE_OLD_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/governance-task-old.json"
readonly ACTIVATION_GOVERNANCE_OLD_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/governance-task-old.sha256"
readonly ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/governance-task-new.json"
readonly ACTIVATION_GOVERNANCE_NEW_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/governance-task-new.sha256"
readonly ACTIVATION_RECEIPT_PENDING="$ACTIVATION_UNIT_SNAPSHOT_DIR/deployed-receipt-pending.json"
readonly ACTIVATION_RECEIPT_PENDING_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/deployed-receipt-pending.sha256"
readonly ACTIVATION_RELEASE_IDENTITY="$ACTIVATION_UNIT_SNAPSHOT_DIR/release-identity"
readonly ACTIVATION_RELEASE_IDENTITY_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/release-identity.sha256"
declare -ar ACTIVATION_UNIT_PATHS=(
  /etc/systemd/system/probiga.service.d/scheduler.conf
  /etc/systemd/system/probiga.service.d/release.conf
  /etc/systemd/system/probiga.service.d/release-path.conf
  /etc/systemd/system/probiga.service.d/release-revision.conf
  /etc/systemd/system/probiga.service.d/zz-probiga-env.conf
  /etc/systemd/system/probiga-scheduler.service
  /etc/systemd/system/probiga-scheduler.service.d/release.conf
  /etc/systemd/system/probiga-scheduler.service.d/release-path.conf
  /etc/systemd/system/probiga-scheduler.service.d/release-revision.conf
  /etc/systemd/system/probiga-scheduler.service.d/zz-probiga-env.conf
  /etc/systemd/system/probiga-ai-recommendation-worker.service.d/release-runtime.conf
  /opt/ProBigA-current
)

fail() {
  echo "production deploy broker: $*" >&2
  exit 2
}

clean_git() {
  /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/var/empty \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    GIT_CONFIG_NOSYSTEM=1 \
    GIT_CONFIG_GLOBAL=/dev/null \
    GIT_ATTR_NOSYSTEM=1 \
    GIT_OPTIONAL_LOCKS=0 \
    GIT_TERMINAL_PROMPT=0 \
    /usr/bin/git --no-replace-objects \
      -c core.hooksPath=/dev/null \
      -c core.fsmonitor=false \
      -c diff.external= \
      -c protocol.file.allow=never \
      "$@"
}

clean_git_ssh() {
  /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/var/empty \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    GIT_CONFIG_NOSYSTEM=1 \
    GIT_CONFIG_GLOBAL=/dev/null \
    GIT_ATTR_NOSYSTEM=1 \
    GIT_OPTIONAL_LOCKS=0 \
    GIT_TERMINAL_PROMPT=0 \
    GIT_SSH_COMMAND="$REMOTE_GIT_SSH" \
    /usr/bin/git --no-replace-objects \
      -c core.hooksPath=/dev/null \
      -c core.fsmonitor=false \
      -c diff.external= \
      -c protocol.file.allow=never \
      "$@"
}

assert_root_file() {
  local expected_mode="$2"
  local path="$1"
  test -f "$path" || fail "trusted file is missing: $path"
  test ! -L "$path" || fail "trusted file must not be a symlink: $path"
  test "$(stat -c '%U:%G' "$path")" = root:root || \
    fail "trusted file owner differs: $path"
  test "$(stat -c '%a' "$path")" = "$expected_mode" || \
    fail "trusted file mode differs: $path"
}

assert_secure_parent_chain() {
  local current="$1"
  while [ "$current" != / ]; do
    test -d "$current" || fail "trusted parent is missing: $current"
    test ! -L "$current" || fail "trusted parent is a symlink: $current"
    test "$(stat -c '%U:%G' "$current")" = root:root || \
      fail "trusted parent owner differs: $current"
    test $((8#$(stat -c '%a' "$current") & 8#022)) -eq 0 || \
      fail "trusted parent is group/other writable: $current"
    current="$(dirname "$current")"
  done
}

assert_git_cache_contract() {
  local cache="$1"
  local disallowed_config
  local unsafe_path
  test -d "$cache" || fail "release mirror is missing"
  test ! -L "$cache" || fail "release mirror must not be a symlink"
  test "$(readlink -f "$cache")" = "$cache" || \
    fail "release mirror resolves unexpectedly"
  test "$(clean_git --git-dir="$cache" rev-parse --is-bare-repository)" = true || \
    fail "release source is not a bare Git mirror"
  unsafe_path="$(find -P "$cache" -xdev \
    \( ! -user root -o ! -group root -o -perm /022 \) -print -quit)" || \
    fail "release mirror ownership scan failed"
  test -z "$unsafe_path" || \
    fail "release mirror is not recursively root-owned/read-only to non-root: $unsafe_path"
  test ! -e "$cache/objects/info/alternates" || \
    fail "release mirror alternates are forbidden"
  test ! -e "$cache/objects/info/http-alternates" || \
    fail "release mirror HTTP alternates are forbidden"
  test ! -e "$cache/info/attributes" || \
    fail "release mirror local attributes are forbidden"
  test ! -e "$cache/refs/replace" || \
    fail "release mirror replace refs directory is forbidden"
  test -z "$(clean_git --git-dir="$cache" for-each-ref \
    --format='%(refname)' refs/replace)" || \
    fail "release mirror replace refs are forbidden"
  if [ -d "$cache/hooks" ]; then
    unsafe_path="$(find -P "$cache/hooks" -mindepth 1 -print -quit)" || \
      fail "release mirror hook scan failed"
    test -z "$unsafe_path" || fail "release mirror local hooks are forbidden"
  fi
  disallowed_config="$(clean_git --git-dir="$cache" config --local \
    --name-only --get-regexp \
    '^(include|includeif|core\.hookspath|core\.fsmonitor|core\.attributesfile|core\.sshcommand|core\.gitproxy|diff\..*\.command|diff\.external|filter\.|credential\.|http\.|url\.|protocol\.|remote\..*\.(uploadpack|receivepack)|extensions\.)' \
    2>/dev/null || true)"
  test -z "$disallowed_config" || \
    fail "release mirror contains forbidden local Git configuration"
  test "$(clean_git --git-dir="$cache" remote get-url origin)" = \
    "$TRUSTED_REMOTE" || fail "release mirror remote differs"
}

assert_commit_attributes_safe() {
  local commit="$1"
  local attributes
  local paths
  local path
  local submodule
  paths="$(clean_git --git-dir="$CODE_GIT_CACHE" ls-tree -r \
    --name-only "$commit")" || fail "trusted commit path inventory cannot be read"
  while IFS= read -r path; do
    case "$path" in
      .gitattributes|*/.gitattributes) ;;
      *) continue ;;
    esac
    attributes="$(clean_git --git-dir="$CODE_GIT_CACHE" show \
      "$commit:$path")" || fail "trusted attributes cannot be read"
    if printf '%s\n' "$attributes" | /usr/bin/grep -E \
      '(^|[[:space:]])(filter|diff|merge|working-tree-encoding)=' >/dev/null; then
      fail "trusted commit contains executable or transform Git attributes"
    fi
  done <<< "$paths"
  submodule="$(clean_git --git-dir="$CODE_GIT_CACHE" ls-tree -r "$commit" | \
    /usr/bin/awk '$1 == "160000" {print; exit}')" || \
    fail "trusted commit tree inventory cannot be read"
  test -z "$submodule" || \
    fail "trusted commit contains unsupported submodules"
}

parse_broker_invocation() {
  BROKER_OPERATION=""
  EXPECTED_SHA=""
  EXPECTED_INPUT_LOCK_SHA256=""
  EXPECTED_ADATA_SHA=""
  EXPECTED_ADATA_TREE_SHA256=""
  EXPECTED_RECOVERY_GUARD_SHA=""
  case "$#" in
    1)
      if [ "$1" = --capabilities ]; then
        BROKER_OPERATION=capabilities
      else
        BROKER_OPERATION=deploy
        EXPECTED_SHA="$1"
      fi
      ;;
    2)
      test "$1" = --recover-database-guard || \
        fail "unsupported two-argument operation"
      BROKER_OPERATION=recover-database-guard
      EXPECTED_RECOVERY_GUARD_SHA="$2"
      ;;
    *) fail "expected one trusted-main SHA or an exact guard recovery request" ;;
  esac
  if [ "$BROKER_OPERATION" = capabilities ]; then
    return 0
  elif [ "$BROKER_OPERATION" = deploy ]; then
    [[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || fail "invalid release SHA"
  else
    [[ "$EXPECTED_RECOVERY_GUARD_SHA" =~ ^[0-9a-f]{40}$ ]] || \
      fail "invalid expected guard SHA"
  fi
}

test "${EUID:-$(id -u)}" -eq 0 || fail "must run as root"
test "${SUDO_USER:-}" = "$DEPLOY_USER" || fail "unexpected sudo caller"
parse_broker_invocation "$@"
if [ "$BROKER_OPERATION" = capabilities ]; then
  printf '%s\n' \
    "$CAPABILITY_SCHEMA" \
    "deploy_protocol=$DEPLOY_PROTOCOL_VERSION" \
    "recovery_protocol=$RECOVERY_PROTOCOL_VERSION" \
    "artifact_protocol=$TRUSTED_ARTIFACT_PROTOCOL" \
    'snapshot_only_recovery=true' \
    'input_and_freeze_digests=true' \
    'governance_task_snapshot=true' \
    'receipt_pending_recovery=true' \
    'activation_release_identity=true' \
    'release_tree_and_adapter_seal=true'
  exit 0
fi
if [ "$BROKER_OPERATION" = deploy ] && \
  [ "$BROKER_COMPILED_LOCK_STATUS" != READY ]; then
  fail "production dependency lock is not READY in the installed reviewed broker"
fi
test ! -L "$BROKER_LOCK_ROOT" || fail "broker lock root must not be a symlink"
install -d -o root -g root -m 0700 "$BROKER_LOCK_ROOT"
test "$(readlink -f "$BROKER_LOCK_ROOT")" = "$BROKER_LOCK_ROOT" || \
  fail "broker lock root resolves unexpectedly"
test ! -L "$BROKER_LOCK_FILE" || fail "broker lock file must not be a symlink"
touch "$BROKER_LOCK_FILE"
chown root:root "$BROKER_LOCK_FILE"
chmod 0600 "$BROKER_LOCK_FILE"
exec 8>"$BROKER_LOCK_FILE"
if ! flock -n 8; then
  fail "another production deploy broker is active"
fi
REPOSITORY_BUILD=""
REQUIREMENTS_FILE=""
RELEASE_MANIFEST_FILE=""
WHEEL_MANIFEST_FILE=""
BOOTSTRAP_FILE=""
cleanup() {
  [ -z "$REQUIREMENTS_FILE" ] || rm -f -- "$REQUIREMENTS_FILE"
  [ -z "$RELEASE_MANIFEST_FILE" ] || rm -f -- "$RELEASE_MANIFEST_FILE"
  [ -z "$WHEEL_MANIFEST_FILE" ] || rm -f -- "$WHEEL_MANIFEST_FILE"
  [ -z "$BOOTSTRAP_FILE" ] || rm -f -- "$BOOTSTRAP_FILE"
  if [ -n "$REPOSITORY_BUILD" ]; then
    case "$REPOSITORY_BUILD" in
      "$RELEASE_SOURCE_ROOT"/probiga-git.*) rm -rf -- "$REPOSITORY_BUILD" ;;
    esac
  fi
}
trap cleanup EXIT

assert_recovery_state_record() {
  local active_state
  local extra
  local load_state
  local record="$2"
  local unit_file_state
  local unit_kind="$1"
  IFS=, read -r load_state active_state unit_file_state extra <<< "$record"
  test -z "$extra" || fail "malformed recovery unit state"
  test "$record" = "$load_state,$active_state,$unit_file_state" || \
    fail "non-canonical recovery unit state"
  case "$unit_kind:$load_state:$active_state:$unit_file_state" in
    main:loaded:active:enabled|main:loaded:active:disabled|\
    main:loaded:inactive:enabled|main:loaded:inactive:disabled|\
    scheduler:loaded:active:enabled|scheduler:loaded:active:disabled|\
    scheduler:loaded:inactive:enabled|scheduler:loaded:inactive:disabled|\
    scheduler:not-found:not-found:not-found|\
    ai-service:loaded:active:enabled|ai-service:loaded:active:disabled|\
    ai-service:loaded:active:static|ai-service:loaded:inactive:enabled|\
    ai-service:loaded:inactive:disabled|ai-service:loaded:inactive:static|\
    ai-service:not-found:not-found:not-found|\
    ai-timer:loaded:active:enabled|ai-timer:loaded:active:disabled|\
    ai-timer:loaded:inactive:enabled|ai-timer:loaded:inactive:disabled|\
    ai-timer:not-found:not-found:not-found) ;;
    *) fail "unsupported recovery unit state" ;;
  esac
}

recovery_file_sha() {
  local expected_header="$2"
  local path="$1"
  local recovered_sha
  local -a lines=()
  test -f "$path" || fail "recovery state is not a regular file"
  test ! -L "$path" || fail "recovery state must not be a symlink"
  test "$(stat -c '%U:%G' "$path")" = root:root || \
    fail "recovery state owner differs"
  test "$(stat -c '%a' "$path")" = 600 || \
    fail "recovery state mode differs"
  mapfile -t lines < "$path"
  test "${#lines[@]}" -eq 6 || fail "malformed recovery state length"
  test "${lines[0]}" = "$expected_header" || \
    fail "unexpected recovery state schema"
  case "${lines[1]}" in
    release=*) recovered_sha="${lines[1]#release=}" ;;
    *) fail "recovery state release is missing" ;;
  esac
  [[ "$recovered_sha" =~ ^[0-9a-f]{40}$ ]] || \
    fail "recovery state release is invalid"
  case "${lines[2]}" in main_unit=*) ;; *) fail "main recovery state is missing" ;; esac
  case "${lines[3]}" in scheduler_unit=*) ;; *) fail "scheduler recovery state is missing" ;; esac
  case "${lines[4]}" in ai_service_unit=*) ;; *) fail "AI service recovery state is missing" ;; esac
  case "${lines[5]}" in ai_timer_unit=*) ;; *) fail "AI timer recovery state is missing" ;; esac
  assert_recovery_state_record main "${lines[2]#main_unit=}"
  assert_recovery_state_record scheduler "${lines[3]#scheduler_unit=}"
  assert_recovery_state_record ai-service "${lines[4]#ai_service_unit=}"
  assert_recovery_state_record ai-timer "${lines[5]#ai_timer_unit=}"
  test "${lines[4]#ai_service_unit=}" = not-found,not-found,not-found && \
    test "${lines[5]#ai_timer_unit=}" = not-found,not-found,not-found || \
    test "${lines[4]#ai_service_unit=}" != not-found,not-found,not-found && \
    test "${lines[5]#ai_timer_unit=}" != not-found,not-found,not-found || \
    fail "AI recovery unit inventory is inconsistent"
  printf '%s\n' "$recovered_sha"
}

activation_snapshot_release() {
  local expected_path
  local file_sha
  local index=0
  local kind
  local mode
  local path
  local payload
  local phase
  local release
  local -a lines=()
  local -a new_lines=()
  local -a identity_lines=()
  test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR" || \
    fail "activation transaction is missing"
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || \
    fail "activation transaction must not be a symlink"
  test "$(readlink -f "$ACTIVATION_UNIT_SNAPSHOT_DIR")" = \
    "$ACTIVATION_UNIT_SNAPSHOT_DIR" || fail "activation transaction resolves unexpectedly"
  test "$(stat -c '%U:%G' "$ACTIVATION_UNIT_SNAPSHOT_DIR")" = root:root || \
    fail "activation transaction owner differs"
  test "$(stat -c '%a' "$ACTIVATION_UNIT_SNAPSHOT_DIR")" = 700 || \
    fail "activation transaction mode differs"
  for path in "$ACTIVATION_UNIT_SNAPSHOT_MANIFEST" \
    "$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST" \
    "$ACTIVATION_UNIT_SNAPSHOT_STATE" \
    "$ACTIVATION_UNIT_SNAPSHOT_STATE_SHA" \
    "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" \
    "$ACTIVATION_GOVERNANCE_OLD_SHA" \
    "$ACTIVATION_RELEASE_IDENTITY" \
    "$ACTIVATION_RELEASE_IDENTITY_SHA" \
    "$ACTIVATION_UNIT_SNAPSHOT_PHASE"; do
    test -f "$path" || fail "activation transaction metadata is missing"
    test ! -L "$path" || fail "activation transaction metadata is a symlink"
    test "$(stat -c '%U:%G' "$path")" = root:root || \
      fail "activation transaction metadata owner differs"
    test "$(stat -c '%a' "$path")" = 600 || \
      fail "activation transaction metadata mode differs"
  done
  phase="$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")"
  case "$phase" in
    prepared|runtime-units-installing|runtime-units-installed|\
    restoring-old|old-set-restored|old-runtime-verified|\
    new-runtime-verified|finalized) ;;
    *) fail "activation transaction phase is invalid" ;;
  esac
  test "$(<"$ACTIVATION_UNIT_SNAPSHOT_STATE_SHA")" = \
    "$(sha256sum "$ACTIVATION_UNIT_SNAPSHOT_STATE" | cut -d' ' -f1)" || \
    fail "activation writer-state digest differs"
  test "$(<"$ACTIVATION_GOVERNANCE_OLD_SHA")" = \
    "$(sha256sum "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" | cut -d' ' -f1)" || \
    fail "activation old governance snapshot digest differs"
  test "$(<"$ACTIVATION_RELEASE_IDENTITY_SHA")" = \
    "$(sha256sum "$ACTIVATION_RELEASE_IDENTITY" | cut -d' ' -f1)" || \
    fail "activation release identity digest differs"
  case "$phase" in
    new-runtime-verified|finalized)
      for path in "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" \
        "$ACTIVATION_GOVERNANCE_NEW_SHA" "$ACTIVATION_RECEIPT_PENDING" \
        "$ACTIVATION_RECEIPT_PENDING_SHA"; do
        test -f "$path" || fail "verified activation metadata is missing"
        test ! -L "$path" || fail "verified activation metadata is a symlink"
        test "$(stat -c '%U:%G' "$path")" = root:root || \
          fail "verified activation metadata owner differs"
        test "$(stat -c '%a' "$path")" = 600 || \
          fail "verified activation metadata mode differs"
      done
      test "$(<"$ACTIVATION_GOVERNANCE_NEW_SHA")" = \
        "$(sha256sum "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" | cut -d' ' -f1)" || \
        fail "activation new governance snapshot digest differs"
      test "$(<"$ACTIVATION_RECEIPT_PENDING_SHA")" = \
        "$(sha256sum "$ACTIVATION_RECEIPT_PENDING" | cut -d' ' -f1)" || \
        fail "activation pending receipt digest differs"
      ;;
  esac
  mapfile -t lines < "$ACTIVATION_UNIT_SNAPSHOT_MANIFEST"
  test "${#lines[@]}" -eq "$((${#ACTIVATION_UNIT_PATHS[@]} + 2))" || \
    fail "activation transaction manifest length differs"
  test "${lines[0]}" = probiga.activation-unit-transaction.v1 || \
    fail "activation transaction schema differs"
  case "${lines[1]}" in
    release=*) release="${lines[1]#release=}" ;;
    *) fail "activation transaction release is missing" ;;
  esac
  [[ "$release" =~ ^[0-9a-f]{40}$ ]] || \
    fail "activation transaction release is invalid"
  mapfile -t identity_lines < "$ACTIVATION_RELEASE_IDENTITY"
  test "${#identity_lines[@]}" -eq 5 || \
    fail "activation release identity length differs"
  test "${identity_lines[0]}" = probiga.activation-release-identity.v1 || \
    fail "activation release identity schema differs"
  test "${identity_lines[1]}" = "new_release=$release" || \
    fail "activation new release identity differs"
  case "${identity_lines[2]}" in
    old_release=[0-9a-f][0-9a-f][0-9a-f][0-9a-f]*) ;;
    *) fail "activation old release identity is invalid" ;;
  esac
  [[ "${identity_lines[2]#old_release=}" =~ ^[0-9a-f]{40}$ ]] || \
    fail "activation old release identity is invalid"
  [[ "${identity_lines[3]#release_tree_sha256=}" =~ ^[0-9a-f]{64}$ ]] || \
    fail "activation release tree identity is invalid"
  test "${identity_lines[3]}" = \
    "release_tree_sha256=${identity_lines[3]#release_tree_sha256=}" || \
    fail "activation release tree field differs"
  [[ "${identity_lines[4]#adapter_registry_seal_sha256=}" =~ ^[0-9a-f]{64}$ ]] || \
    fail "activation adapter registry seal is invalid"
  test "${identity_lines[4]}" = \
    "adapter_registry_seal_sha256=${identity_lines[4]#adapter_registry_seal_sha256=}" || \
    fail "activation adapter registry seal field differs"
  test "$(recovery_file_sha "$ACTIVATION_UNIT_SNAPSHOT_STATE" \
    probiga.database-writer-restore.v1)" = "$release" || \
    fail "activation transaction writer state differs"
  for expected_path in "${ACTIVATION_UNIT_PATHS[@]}"; do
    IFS='|' read -r kind path mode file_sha payload \
      <<< "${lines[$((index + 2))]}"
    test "$path" = "$expected_path" || \
      fail "activation transaction path inventory differs"
    case "$kind" in
      missing)
        test "$mode:$file_sha:$payload" = '-:-:-' || \
          fail "activation transaction missing record is malformed"
        ;;
      file)
        [[ "$mode" =~ ^[0-7]{3,4}$ ]] || \
          fail "activation transaction file mode is invalid"
        [[ "$file_sha" =~ ^[0-9a-f]{64}$ ]] || \
          fail "activation transaction file digest is invalid"
        test "$payload" = "files/$index" || \
          fail "activation transaction backup name differs"
        path="$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload"
        test -f "$path" || fail "activation transaction backup is missing"
        test ! -L "$path" || fail "activation transaction backup is a symlink"
        test "$(stat -c '%U:%G' "$path")" = root:root || \
          fail "activation transaction backup owner differs"
        test "$(stat -c '%a' "$path")" = 600 || \
          fail "activation transaction backup mode differs"
        test "$(sha256sum "$path" | cut -d' ' -f1)" = "$file_sha" || \
          fail "activation transaction backup digest differs"
        ;;
      symlink)
        test "$expected_path" = /opt/ProBigA-current || \
          fail "activation transaction symlink path differs"
        test "$mode" = - || fail "activation transaction symlink mode differs"
        if [ "$payload" != /opt/ProBigA ] && \
          [[ ! "$payload" =~ ^/opt/ProBigA-releases/[0-9a-f]{40}$ ]]; then
          fail "activation transaction symlink target is invalid"
        fi
        test "$(printf '%s' "$payload" | sha256sum | cut -d' ' -f1)" = \
          "$file_sha" || fail "activation transaction symlink digest differs"
        ;;
      *) fail "activation transaction record type is invalid" ;;
    esac
    index=$((index + 1))
  done
  mapfile -t new_lines < "$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST"
  test "${#new_lines[@]}" -eq "$((${#ACTIVATION_UNIT_PATHS[@]} + 2))" || \
    fail "activation target manifest length differs"
  test "${new_lines[0]}" = probiga.activation-unit-target.v1 || \
    fail "activation target schema differs"
  test "${new_lines[1]}" = "release=$release" || \
    fail "activation target release differs"
  index=0
  for expected_path in "${ACTIVATION_UNIT_PATHS[@]}"; do
    IFS='|' read -r kind path mode file_sha payload \
      <<< "${new_lines[$((index + 2))]}"
    test "$path" = "$expected_path" || \
      fail "activation target path inventory differs"
    case "$kind" in
      missing)
        test "$mode:$file_sha:$payload" = '-:-:-' || \
          fail "activation target missing record is malformed"
        ;;
      file)
        test "$mode" = 644 || fail "activation target file mode differs"
        [[ "$file_sha" =~ ^[0-9a-f]{64}$ ]] || \
          fail "activation target file digest is invalid"
        test "$payload" = "new-files/$index" || \
          fail "activation target backup name differs"
        path="$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload"
        test -f "$path" || fail "activation target backup is missing"
        test ! -L "$path" || fail "activation target backup is a symlink"
        test "$(stat -c '%U:%G' "$path")" = root:root || \
          fail "activation target backup owner differs"
        test "$(stat -c '%a' "$path")" = 600 || \
          fail "activation target backup mode differs"
        test "$(sha256sum "$path" | cut -d' ' -f1)" = "$file_sha" || \
          fail "activation target backup digest differs"
        ;;
      symlink)
        test "$expected_path" = /opt/ProBigA-current || \
          fail "activation target symlink path differs"
        test "$payload" = "/opt/ProBigA-releases/$release" || \
          fail "activation target symlink differs"
        test "$(printf '%s' "$payload" | sha256sum | cut -d' ' -f1)" = \
          "$file_sha" || fail "activation target symlink digest differs"
        ;;
      *) fail "activation target record type is invalid" ;;
    esac
    index=$((index + 1))
  done
  printf '%s\n' "$release"
}

if [ "$BROKER_OPERATION" = recover-database-guard ]; then
  test -d "$DATABASE_WRITER_GUARD_DIR" || \
    fail "recovery state directory is missing"
  test ! -L "$DATABASE_WRITER_GUARD_DIR" || \
    fail "recovery state directory must not be a symlink"
  test "$(readlink -f "$DATABASE_WRITER_GUARD_DIR")" = \
    "$DATABASE_WRITER_GUARD_DIR" || fail "recovery state directory resolves unexpectedly"
  test "$(stat -c '%U:%G' "$DATABASE_WRITER_GUARD_DIR")" = root:root || \
    fail "recovery state directory owner differs"
  test "$(stat -c '%a' "$DATABASE_WRITER_GUARD_DIR")" = 700 || \
    fail "recovery state directory mode differs"
  RECORDED_RECOVERY_SHA=""
  SNAPSHOT_RECOVERY_SHA=""
  RECOVERY_STATE_COUNT=0
  if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ]; then
    RECORDED_RECOVERY_SHA="$(recovery_file_sha "$DATABASE_WRITER_GUARD_FILE" \
      probiga.database-writer-guard.v2)"
    RECOVERY_STATE_COUNT=$((RECOVERY_STATE_COUNT + 1))
  fi
  if [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] || \
    [ -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" ]; then
    SNAPSHOT_RECOVERY_SHA="$(activation_snapshot_release)"
    if [ -n "$RECORDED_RECOVERY_SHA" ]; then
      test "$SNAPSHOT_RECOVERY_SHA" = "$RECORDED_RECOVERY_SHA" || \
        fail "activation transaction and recovery state releases differ"
    elif [ -z "$RECORDED_RECOVERY_SHA" ]; then
      SNAPSHOT_RECOVERY_PHASE="$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")"
      case "$SNAPSHOT_RECOVERY_PHASE" in
        old-runtime-verified|new-runtime-verified|finalized)
          RECORDED_RECOVERY_SHA="$SNAPSHOT_RECOVERY_SHA"
          ;;
        *)
          fail "snapshot-only recovery requires a verified old or new runtime"
          ;;
      esac
    fi
    RECOVERY_STATE_COUNT=$((RECOVERY_STATE_COUNT + 1))
  elif [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ]; then
    fail "database guard exists without an activation transaction"
  fi
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    RESTORE_RECOVERY_SHA="$(recovery_file_sha "$DATABASE_WRITER_RESTORE_FILE" \
      probiga.database-writer-restore.v1)"
    if [ -n "$RECORDED_RECOVERY_SHA" ]; then
      test "$RESTORE_RECOVERY_SHA" = "$RECORDED_RECOVERY_SHA" || \
        fail "guard and restore journal releases differ"
    else
      RECORDED_RECOVERY_SHA="$RESTORE_RECOVERY_SHA"
    fi
    RECOVERY_STATE_COUNT=$((RECOVERY_STATE_COUNT + 1))
  fi
  if [ -n "$SNAPSHOT_RECOVERY_SHA" ] && [ -n "$RECORDED_RECOVERY_SHA" ]; then
    test "$SNAPSHOT_RECOVERY_SHA" = "$RECORDED_RECOVERY_SHA" || \
      fail "activation transaction and recovery state releases differ"
  fi
  test "$RECOVERY_STATE_COUNT" -ge 1 || fail "persistent recovery state is absent"
  test "$RECORDED_RECOVERY_SHA" = "$EXPECTED_RECOVERY_GUARD_SHA" || \
    fail "requested guard SHA differs from root-owned recovery state"
fi

test -d "$LEGACY_REPOSITORY" || fail "production working directory is missing"
if [ "$BROKER_OPERATION" = deploy ]; then
  assert_secure_parent_chain /etc/probiga
  assert_root_file "$GITHUB_SSH_KEY" 600
  assert_root_file "$GITHUB_KNOWN_HOSTS" 644
  test "$(wc -c < "$GITHUB_SSH_KEY")" -le 16384 || \
    fail "GitHub deploy key is unexpectedly large"
  test "$(wc -c < "$GITHUB_KNOWN_HOSTS")" -le 1048576 || \
    fail "GitHub known-hosts file is unexpectedly large"
  test -n "$(/usr/bin/ssh-keygen -l -f "$GITHUB_SSH_KEY" 2>/dev/null)" || \
    fail "GitHub deploy key cannot be parsed"
  test -n "$(/usr/bin/ssh-keygen -F github.com -f "$GITHUB_KNOWN_HOSTS" \
    2>/dev/null)" || fail "GitHub known-hosts has no github.com identity"
  REMOTE_SHA="$(clean_git_ssh ls-remote "$TRUSTED_REMOTE" refs/heads/main | \
    /usr/bin/awk 'NR == 1 {print $1}')"
  test "$REMOTE_SHA" = "$EXPECTED_SHA" || \
    fail "requested revision is not the current trusted main revision"
fi

test ! -L "$RELEASE_SOURCE_ROOT" || fail "release source root must not be a symlink"
if [ "$BROKER_OPERATION" = deploy ]; then
  install -d -o root -g root -m 0755 "$RELEASE_SOURCE_ROOT"
else
  test -d "$RELEASE_SOURCE_ROOT" || \
    fail "sealed offline release source root is missing for recovery"
  test "$(stat -c '%U:%G' "$RELEASE_SOURCE_ROOT")" = root:root || \
    fail "sealed offline release source root owner differs"
  test $((8#$(stat -c '%a' "$RELEASE_SOURCE_ROOT") & 8#022)) -eq 0 || \
    fail "sealed offline release source root is writable by non-root"
fi
test "$(readlink -f "$RELEASE_SOURCE_ROOT")" = "$RELEASE_SOURCE_ROOT" || \
  fail "release source root resolves unexpectedly"
assert_secure_parent_chain "$RELEASE_SOURCE_ROOT"
test ! -L "$CODE_GIT_CACHE" || fail "release mirror must not be a symlink"
if [ ! -d "$CODE_GIT_CACHE" ]; then
  test "$BROKER_OPERATION" = deploy || \
    fail "sealed offline release mirror is missing for recovery"
  REPOSITORY_BUILD="$(mktemp -d "$RELEASE_SOURCE_ROOT/probiga-git.XXXXXX")"
  if ! clean_git init --bare "$REPOSITORY_BUILD/repository.git" || \
    ! clean_git --git-dir="$REPOSITORY_BUILD/repository.git" remote add origin \
      "$TRUSTED_REMOTE" || \
    ! rm -rf -- "$REPOSITORY_BUILD/repository.git/hooks" || \
    ! install -d -o root -g root -m 0555 \
      "$REPOSITORY_BUILD/repository.git/hooks" || \
    ! mv "$REPOSITORY_BUILD/repository.git" "$CODE_GIT_CACHE" || \
    ! rmdir "$REPOSITORY_BUILD"; then
    rm -rf "$REPOSITORY_BUILD"
    fail "independent release mirror could not be initialized"
  fi
  REPOSITORY_BUILD=""
fi
assert_git_cache_contract "$CODE_GIT_CACHE"
GIT=(clean_git --git-dir="$CODE_GIT_CACHE")
if [ "$BROKER_OPERATION" = deploy ]; then
  clean_git_ssh --git-dir="$CODE_GIT_CACHE" fetch --no-tags origin \
    "+refs/heads/main:refs/remotes/origin/main"
fi
assert_git_cache_contract "$CODE_GIT_CACHE"
if [ "$BROKER_OPERATION" = deploy ]; then
  test "$("${GIT[@]}" rev-parse refs/remotes/origin/main)" = "$EXPECTED_SHA" || \
    fail "fetched release mirror tip differs"
  "${GIT[@]}" cat-file -e "${EXPECTED_SHA}^{commit}" || \
    fail "requested revision is absent from the trusted release mirror"
else
  "${GIT[@]}" cat-file -e "${EXPECTED_RECOVERY_GUARD_SHA}^{commit}" || \
    fail "recovery revision is absent from the trusted release mirror"
fi
if [ "$BROKER_OPERATION" = deploy ]; then
  assert_commit_attributes_safe "$EXPECTED_SHA"
else
  assert_commit_attributes_safe "$EXPECTED_RECOVERY_GUARD_SHA"
fi

BOOTSTRAP_FILE="$(mktemp /root/probiga-production-deploy.XXXXXX)"
if [ "$BROKER_OPERATION" = deploy ]; then
  "${GIT[@]}" show "${EXPECTED_SHA}:deploy/production_deploy.sh" > \
    "$BOOTSTRAP_FILE"
else
  "${GIT[@]}" show \
    "${EXPECTED_RECOVERY_GUARD_SHA}:deploy/production_deploy.sh" > \
    "$BOOTSTRAP_FILE"
fi
chmod 0700 "$BOOTSTRAP_FILE"
chown root:root "$BOOTSTRAP_FILE"
test "$(sha256sum "$BOOTSTRAP_FILE" | cut -d' ' -f1)" = \
  "$("${GIT[@]}" show \
    "$([ "$BROKER_OPERATION" = deploy ] && printf '%s' "$EXPECTED_SHA" || \
      printf '%s' "$EXPECTED_RECOVERY_GUARD_SHA"):deploy/production_deploy.sh" | \
      sha256sum | cut -d' ' -f1)" || fail "trusted deploy engine digest differs"

if [ "$BROKER_OPERATION" = deploy ]; then
  REQUIREMENTS_FILE="$(mktemp /root/probiga-requirements.XXXXXX)"
  RELEASE_MANIFEST_FILE="$(mktemp /root/probiga-production-release.XXXXXX)"
  WHEEL_MANIFEST_FILE="$(mktemp /root/probiga-production-wheels.XXXXXX)"
  "${GIT[@]}" show "${EXPECTED_SHA}:deploy/production_requirements.lock" > \
    "$REQUIREMENTS_FILE" || fail "trusted release requirements are missing"
  "${GIT[@]}" show "${EXPECTED_SHA}:deploy/production_release.env" > \
    "$RELEASE_MANIFEST_FILE" || fail "trusted production release manifest is missing"
  "${GIT[@]}" show "${EXPECTED_SHA}:deploy/production_wheel_manifest.lock" > \
    "$WHEEL_MANIFEST_FILE" || fail "trusted production wheel manifest is missing"
  test -s "$REQUIREMENTS_FILE" || fail "trusted release requirements are empty"
  test "$(wc -c < "$REQUIREMENTS_FILE")" -le 2097152 || \
    fail "trusted release requirements are too large"
  mapfile -t RELEASE_MANIFEST_LINES < "$RELEASE_MANIFEST_FILE" || \
    fail "trusted production release manifest is unreadable"
  test "${#RELEASE_MANIFEST_LINES[@]}" -eq 10 || \
    fail "trusted production release manifest has unexpected fields"
  test "${RELEASE_MANIFEST_LINES[0]}" = \
    PROBIGA_PRODUCTION_RELEASE_MANIFEST_VERSION=2 || \
    fail "trusted production release manifest version differs"
  test "${RELEASE_MANIFEST_LINES[1]}" = \
    PROBIGA_PRODUCTION_LOCK_TARGET=cp314-manylinux_2_17_x86_64 || \
    fail "trusted production lock target differs"
  case "${RELEASE_MANIFEST_LINES[2]}" in
    PROBIGA_PRODUCTION_LOCK_STATUS=*) \
      PRODUCTION_LOCK_STATUS="${RELEASE_MANIFEST_LINES[2]#PROBIGA_PRODUCTION_LOCK_STATUS=}" ;;
    *) fail "trusted production lock status is missing" ;;
  esac
  case "${RELEASE_MANIFEST_LINES[3]}" in
    INPUT_LOCK_SHA256=*) \
      MANIFEST_INPUT_LOCK_SHA256="${RELEASE_MANIFEST_LINES[3]#INPUT_LOCK_SHA256=}" ;;
    *) fail "trusted input lock digest is missing" ;;
  esac
  case "${RELEASE_MANIFEST_LINES[4]}" in
    WHEEL_MANIFEST_SHA256=*) \
      MANIFEST_WHEEL_SHA256="${RELEASE_MANIFEST_LINES[4]#WHEEL_MANIFEST_SHA256=}" ;;
    *) fail "trusted wheel manifest digest is missing" ;;
  esac
  case "${RELEASE_MANIFEST_LINES[5]}" in
    ADATA_RELEASE_SHA=*) \
      EXPECTED_ADATA_SHA="${RELEASE_MANIFEST_LINES[5]#ADATA_RELEASE_SHA=}" ;;
    *) fail "trusted production release adata identity is missing" ;;
  esac
  case "${RELEASE_MANIFEST_LINES[6]}" in
    ADATA_TREE_SHA256=*) \
      EXPECTED_ADATA_TREE_SHA256="${RELEASE_MANIFEST_LINES[6]#ADATA_TREE_SHA256=}" ;;
    *) fail "trusted production release adata tree identity is missing" ;;
  esac
  test "${RELEASE_MANIFEST_LINES[7]}" = \
    TRUSTED_WHEEL_MANIFEST_VERSION=1 || \
    fail "trusted wheel manifest protocol differs"
  test "${RELEASE_MANIFEST_LINES[8]}" = \
    TRUSTED_ARTIFACT_PROTOCOL="$TRUSTED_ARTIFACT_PROTOCOL" || \
    fail "trusted artifact protocol differs"
  case "${RELEASE_MANIFEST_LINES[9]}" in
    ADAPTER_REGISTRY_SEAL_SHA256=*) \
      EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="${RELEASE_MANIFEST_LINES[9]#ADAPTER_REGISTRY_SEAL_SHA256=}" ;;
    *) fail "trusted adapter registry seal is missing" ;;
  esac
  [[ "$EXPECTED_ADATA_SHA" =~ ^[0-9a-f]{40}$ ]] || \
    fail "trusted production release adata identity is invalid"
  [[ "$EXPECTED_ADATA_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]] || \
    fail "trusted production release adata tree identity is invalid"
  [[ "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" =~ ^[0-9a-f]{64}$ ]] || \
    fail "trusted adapter registry seal is invalid"
  RELEASE_TREE_OID="$("${GIT[@]}" rev-parse "${EXPECTED_SHA}^{tree}")" || \
    fail "trusted release tree cannot be derived"
  [[ "$RELEASE_TREE_OID" =~ ^[0-9a-f]{40,64}$ ]] || \
    fail "trusted release tree object identity is invalid"
  EXPECTED_RELEASE_TREE_SHA256="$(printf \
    '{"kind":"git-tree","tree":"%s"}' "$RELEASE_TREE_OID" | \
    sha256sum | cut -d' ' -f1)"
  [[ "$EXPECTED_RELEASE_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]] || \
    fail "trusted release tree SHA-256 is invalid"
  EXPECTED_INPUT_LOCK_SHA256="$(sha256sum "$REQUIREMENTS_FILE" | cut -d' ' -f1)"
  [[ "$EXPECTED_INPUT_LOCK_SHA256" =~ ^[0-9a-f]{64}$ ]] || \
    fail "root-derived input lock digest is invalid"
  test "$EXPECTED_INPUT_LOCK_SHA256" = "$MANIFEST_INPUT_LOCK_SHA256" || \
    fail "trusted input lock digest differs from release manifest"
  EXPECTED_WHEEL_MANIFEST_SHA256="$(sha256sum "$WHEEL_MANIFEST_FILE" | \
    cut -d' ' -f1)"
  test "$EXPECTED_WHEEL_MANIFEST_SHA256" = "$MANIFEST_WHEEL_SHA256" || \
    fail "trusted wheel manifest digest differs from release manifest"
  if [ "$PRODUCTION_LOCK_STATUS" != READY ]; then
    fail "production dependency lock is not READY for cp314 Linux; regenerate and review the complete hashed lock and wheel manifest"
  fi
  grep -Fx 'PROBIGA_TRUSTED_WHEEL_MANIFEST_VERSION=1' \
    "$WHEEL_MANIFEST_FILE" >/dev/null || fail "trusted wheel manifest header differs"
  grep -Fx 'TARGET=cp314-manylinux_2_17_x86_64' \
    "$WHEEL_MANIFEST_FILE" >/dev/null || fail "trusted wheel manifest target differs"
  grep -Fx 'STATUS=READY' "$WHEEL_MANIFEST_FILE" >/dev/null || \
    fail "trusted wheel manifest is not READY"
  grep -Eq '^[0-9a-f]{64}  [A-Za-z0-9_.+-]+\.whl$' \
    "$WHEEL_MANIFEST_FILE" || fail "trusted wheel manifest has no exact wheel entries"
  if grep -Ev '^(PROBIGA_TRUSTED_WHEEL_MANIFEST_VERSION=1|TARGET=cp314-manylinux_2_17_x86_64|STATUS=READY|[0-9a-f]{64}  [A-Za-z0-9_.+-]+\.whl|#.*|[[:space:]]*)$' \
      "$WHEEL_MANIFEST_FILE" >/dev/null; then
    fail "trusted wheel manifest contains malformed entries"
  fi
  RESOLVED_REQUIREMENTS_B64="$(base64 -w0 "$REQUIREMENTS_FILE")"
  test -n "$RESOLVED_REQUIREMENTS_B64" || \
    fail "root-derived requirements payload is empty"
  TRUSTED_WHEEL_MANIFEST_B64="$(base64 -w0 "$WHEEL_MANIFEST_FILE")"
  test -n "$TRUSTED_WHEEL_MANIFEST_B64" || \
    fail "root-derived wheel manifest payload is empty"
  rm -f -- "$RELEASE_MANIFEST_FILE" "$WHEEL_MANIFEST_FILE"
  RELEASE_MANIFEST_FILE=""
  WHEEL_MANIFEST_FILE=""
  unset PROBIGA_RECOVERY_GUARD_SHA
  /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/root LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    PROBIGA_DEPLOY_PROTOCOL_VERSION="$DEPLOY_PROTOCOL_VERSION" \
    PROBIGA_RECOVERY_PROTOCOL_VERSION="$RECOVERY_PROTOCOL_VERSION" \
    EXPECTED_SHA="$EXPECTED_SHA" \
    RESOLVED_REQUIREMENTS_B64="$RESOLVED_REQUIREMENTS_B64" \
    EXPECTED_INPUT_LOCK_SHA256="$EXPECTED_INPUT_LOCK_SHA256" \
    TRUSTED_WHEEL_MANIFEST_B64="$TRUSTED_WHEEL_MANIFEST_B64" \
    EXPECTED_WHEEL_MANIFEST_SHA256="$EXPECTED_WHEEL_MANIFEST_SHA256" \
    EXPECTED_ADATA_SHA="$EXPECTED_ADATA_SHA" \
    EXPECTED_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256" \
    EXPECTED_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256" \
    EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
    /usr/bin/bash --noprofile --norc "$BOOTSTRAP_FILE"
else
  /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/root LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    PROBIGA_DEPLOY_PROTOCOL_VERSION="$DEPLOY_PROTOCOL_VERSION" \
    PROBIGA_RECOVERY_PROTOCOL_VERSION="$RECOVERY_PROTOCOL_VERSION" \
    PROBIGA_RECOVERY_GUARD_SHA="$EXPECTED_RECOVERY_GUARD_SHA" \
    /usr/bin/bash --noprofile --norc "$BOOTSTRAP_FILE" \
      --recover-database-guard
fi
