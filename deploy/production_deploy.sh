#!/usr/bin/env bash
# Production deployment logic invoked by the pinned GitHub SSH action.
# Inputs are passed explicitly through the action env allowlist.
# ERR inheritance catches failures inside preparation helpers. rollback()
# fences child shells by BASHPID so a subshell cannot perform system rollback.
set -Eeuo pipefail
# The SSH transport can close before its remote process group receives HUP or
# TERM.  Keep shell builtins from dying with 141 so ERR can enter the durable
# failure handler, which immediately detaches all further output.
trap '' PIPE
umask 022
unset BASH_ENV ENV CDPATH GLOBIGNORE 2>/dev/null || true
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
  GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM \
  GIT_CONFIG_COUNT GIT_SSH GIT_SSH_COMMAND GIT_NAMESPACE \
  GIT_CEILING_DIRECTORIES GIT_DISCOVERY_ACROSS_FILESYSTEM 2>/dev/null || true
unset PIP_CONFIG_FILE PIP_INDEX_URL PIP_EXTRA_INDEX_URL PIP_TRUSTED_HOST \
  PIP_TARGET PIP_PREFIX PIP_USER PIP_CACHE_DIR PYTHONHOME PYTHONSTARTUP \
  PYTHONUSERBASE PYTHONINSPECT 2>/dev/null || true

REPOSITORY_ROOT=/opt/ProBigA

# Every Git call in this engine resolves to the absolute executable under a
# new, fixed environment.  This applies even when helpers run from an `if` or
# rollback context and prevents replace refs, alternate object databases,
# caller config, hooks, fsmonitor helpers, and external diff drivers.  The
# legacy live checkout predates the root broker and may be owned by its service
# account, so trust that one exact path for this process without mutating any
# global or system Git configuration.
git() {
  /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/var/empty \
    LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    GIT_CONFIG_NOSYSTEM=1 \
    GIT_CONFIG_GLOBAL=/dev/null \
    GIT_ATTR_NOSYSTEM=1 \
    GIT_OPTIONAL_LOCKS=0 \
    GIT_TERMINAL_PROMPT=0 \
    /usr/bin/git --no-replace-objects \
      -c "safe.directory=$REPOSITORY_ROOT" \
      -c core.hooksPath=/dev/null \
      -c core.fsmonitor=false \
      -c diff.external= \
      -c protocol.file.allow=never \
      "$@"
}

assert_root_owned_bare_cache() {
  local cache="$1"
  local expected_remote="$2"
  local unsafe
  test -d "$cache" || return 1
  test ! -L "$cache" || return 1
  test "$(readlink -f "$cache")" = "$cache" || return 1
  test "$(git --git-dir="$cache" rev-parse --is-bare-repository)" = true || \
    return 1
  test "$(git --git-dir="$cache" remote get-url origin)" = \
    "$expected_remote" || return 1
  unsafe="$(find -P "$cache" -xdev \
    \( ! -user root -o ! -group root -o -perm /022 \) -print -quit)" || \
    return 1
  test -z "$unsafe" || return 1
  test ! -e "$cache/objects/info/alternates" || return 1
  test ! -e "$cache/objects/info/http-alternates" || return 1
  test ! -e "$cache/info/attributes" || return 1
  test ! -e "$cache/refs/replace" || return 1
  test -z "$(git --git-dir="$cache" for-each-ref \
    --format='%(refname)' refs/replace)" || return 1
  if [ -d "$cache/hooks" ]; then
    # The installed v2 broker initialized this root-owned mirror with Git's
    # stock `*.sample` hook templates.  They are not hook entrypoints and every
    # Git call in this engine also pins core.hooksPath=/dev/null.  Permit only
    # those inert templates; any real hook, nested path, foreign owner, or
    # group/other-writable file still fails closed.
    unsafe="$(find -P "$cache/hooks" -mindepth 1 -maxdepth 1 \
      \( ! -type f -o ! -name '*.sample' -o ! -user root -o ! -group root \
         -o -perm /022 \) -print -quit)" || return 1
    test -z "$unsafe" || return 1
  fi
  test -z "$(git --git-dir="$cache" config --local --name-only \
    --get-regexp \
    '^(include|includeif|core\.hookspath|core\.fsmonitor|core\.attributesfile|core\.sshcommand|diff\..*\.command|diff\.external|filter\.|credential\.|http\.|url\.|protocol\.|remote\..*\.(uploadpack|receivepack)|extensions\.)' \
    2>/dev/null || true)" || return 1
  return 0
}
CODE_GIT_CACHE=/var/lib/probiga/release-sources/probiga.git
CODE_RELEASE_ROOT=/opt/ProBigA-releases
RELEASE_VENV_ROOT=/var/lib/probiga/release-venvs
ADATA_RUNTIME_ROOT=/var/lib/probiga/release-sources/adata
LEGACY_RELEASE_VENV_ROOT=/opt/ProBigA/.release_venvs
DEPLOY_LOCK_ROOT=/run/probiga
DEPLOY_LOCK_FILE="$DEPLOY_LOCK_ROOT/production-deploy.lock"
REQUIRED_DEPLOY_PROTOCOL_V4=probiga-production-deploy-v4
COMPATIBLE_DEPLOY_PROTOCOL_V2=probiga-production-deploy-v2
REQUIRED_RECOVERY_PROTOCOL=probiga-database-guard-recovery-v2
DEPLOY_ARTIFACT_MODE=""
if [ "${EUID:-$(id -u)}" -ne 0 ]; then
  echo "production deploy engine must run through the root broker" >&2
  exit 2
fi
case "${PROBIGA_DEPLOY_PROTOCOL_VERSION:-}" in
  "$REQUIRED_DEPLOY_PROTOCOL_V4")
    DEPLOY_ARTIFACT_MODE=static-wheel-lock-v2
    if [ "${PROBIGA_RECOVERY_PROTOCOL_VERSION:-}" != \
      "$REQUIRED_RECOVERY_PROTOCOL" ]; then
      echo "production deploy recovery broker capability mismatch; install the new root broker out of band" >&2
      exit 2
    fi
    ;;
  "$COMPATIBLE_DEPLOY_PROTOCOL_V2")
    DEPLOY_ARTIFACT_MODE=ci-resolved-freeze-v1
    if [ -n "${PROBIGA_RECOVERY_PROTOCOL_VERSION:-}" ] || \
      [ -n "${PROBIGA_RECOVERY_GUARD_SHA:-}" ]; then
      echo "v2 production deploy broker cannot authorize recovery" >&2
      exit 2
    fi
    ;;
  *)
    echo "production deploy broker protocol mismatch" >&2
    exit 2
    ;;
esac
DEPLOY_OPERATION=deploy
if [ "$#" -eq 1 ] && [ "$1" = --recover-database-guard ]; then
  if [ "$DEPLOY_ARTIFACT_MODE" != static-wheel-lock-v2 ]; then
    echo "v2 production deploy broker cannot authorize recovery" >&2
    exit 2
  fi
  DEPLOY_OPERATION=recover-database-guard
elif [ "$#" -ne 0 ]; then
  echo "production deploy engine rejected unsupported arguments" >&2
  exit 2
fi
if [ "$DEPLOY_OPERATION" = recover-database-guard ]; then
  : "${PROBIGA_RECOVERY_GUARD_SHA:?broker-validated recovery SHA is required}"
  [[ "$PROBIGA_RECOVERY_GUARD_SHA" =~ ^[0-9a-f]{40}$ ]]
elif [ -n "${PROBIGA_RECOVERY_GUARD_SHA:-}" ]; then
  echo "production deploy engine rejected recovery state during normal deploy" >&2
  exit 2
fi
cd "$REPOSITORY_ROOT"
test ! -L "$DEPLOY_LOCK_ROOT"
install -d -o root -g root -m 0700 "$DEPLOY_LOCK_ROOT"
test "$(readlink -f "$DEPLOY_LOCK_ROOT")" = "$DEPLOY_LOCK_ROOT"
test ! -L "$DEPLOY_LOCK_FILE"
touch "$DEPLOY_LOCK_FILE"
chown root:root "$DEPLOY_LOCK_FILE"
chmod 0600 "$DEPLOY_LOCK_FILE"
exec 9>"$DEPLOY_LOCK_FILE"
if ! flock -n 9; then
  echo "Another production deployment holds the remote lock" >&2
  exit 2
fi
release_lock() {
  if declare -F cleanup_prepare_artifacts >/dev/null 2>&1; then
    cleanup_prepare_artifacts || true
  fi
}
trap release_lock EXIT
DATABASE_WRITER_GUARD_DIR=/var/lib/probiga/deploy-guards
DATABASE_WRITER_GUARD_FILE="$DATABASE_WRITER_GUARD_DIR/database-migration-unverified"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/database-writer-restore-pending"
ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/activation-unit-transaction"
ACTIVATION_UNIT_SNAPSHOT_MANIFEST="$ACTIVATION_UNIT_SNAPSHOT_DIR/manifest"
ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST="$ACTIVATION_UNIT_SNAPSHOT_DIR/new-manifest"
ACTIVATION_UNIT_SNAPSHOT_PHASE="$ACTIVATION_UNIT_SNAPSHOT_DIR/phase"
ACTIVATION_UNIT_SNAPSHOT_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state"
ACTIVATION_UNIT_SNAPSHOT_STATE_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state.sha256"
ACTIVATION_GOVERNANCE_OLD_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/governance-task-old.json"
ACTIVATION_GOVERNANCE_OLD_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/governance-task-old.sha256"
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/governance-task-new.json"
ACTIVATION_GOVERNANCE_NEW_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/governance-task-new.sha256"
ACTIVATION_RECEIPT_PENDING="$ACTIVATION_UNIT_SNAPSHOT_DIR/deployed-receipt-pending.json"
ACTIVATION_RECEIPT_PENDING_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/deployed-receipt-pending.sha256"
V2_FORWARD_FINALIZED_SHA=""
V2_FORWARD_FINALIZED_REQUEST_MATCH=0
V2_RECOVERY_STEP=not-started
# A completed production health scan has taken about 21 minutes.  Bound each
# direct database gate at 30 minutes so a wedged query cannot leave every
# writer fenced forever, while preserving measured headroom for a healthy run.
CONTROLLED_DATABASE_GATE_TIMEOUT=30m
CONTROLLED_DATABASE_GATE_KILL_AFTER=30s
readonly CONTROLLED_DATABASE_GATE_TIMEOUT CONTROLLED_DATABASE_GATE_KILL_AFTER
ACTIVATION_RELEASE_IDENTITY="$ACTIVATION_UNIT_SNAPSHOT_DIR/release-identity"
ACTIVATION_RELEASE_IDENTITY_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/release-identity.sha256"
RECEIPT_DIR=/var/lib/probiga/deploy-receipts
DATABASE_WRITER_GUARD_DROPIN_NAME=database-writer-guard.conf
MAIN_RELEASE_DROPIN=/etc/systemd/system/probiga.service.d/scheduler.conf
SCHEDULER_UNIT=/etc/systemd/system/probiga-scheduler.service
AI_WORKER_DROPIN=/etc/systemd/system/probiga-ai-recommendation-worker.service.d/release-runtime.conf
STATIC_RELEASE_LINK=/opt/ProBigA-current
MAIN_DATABASE_WRITER_GUARD_DROPIN="/etc/systemd/system/probiga.service.d/$DATABASE_WRITER_GUARD_DROPIN_NAME"
SCHEDULER_DATABASE_WRITER_GUARD_DROPIN="/etc/systemd/system/probiga-scheduler.service.d/$DATABASE_WRITER_GUARD_DROPIN_NAME"
AI_SERVICE_DATABASE_WRITER_GUARD_DROPIN="/etc/systemd/system/probiga-ai-recommendation-worker.service.d/$DATABASE_WRITER_GUARD_DROPIN_NAME"
AI_TIMER_DATABASE_WRITER_GUARD_DROPIN="/etc/systemd/system/probiga-ai-recommendation-worker.timer.d/$DATABASE_WRITER_GUARD_DROPIN_NAME"
declare -a DATABASE_WRITER_GUARD_DROPINS=(
  "$MAIN_DATABASE_WRITER_GUARD_DROPIN"
  "$SCHEDULER_DATABASE_WRITER_GUARD_DROPIN"
  "$AI_SERVICE_DATABASE_WRITER_GUARD_DROPIN"
  "$AI_TIMER_DATABASE_WRITER_GUARD_DROPIN"
)
declare -a ACTIVATION_UNIT_PATHS=(
  "$MAIN_RELEASE_DROPIN"
  /etc/systemd/system/probiga.service.d/release.conf
  /etc/systemd/system/probiga.service.d/release-path.conf
  /etc/systemd/system/probiga.service.d/release-revision.conf
  /etc/systemd/system/probiga.service.d/zz-probiga-env.conf
  "$SCHEDULER_UNIT"
  /etc/systemd/system/probiga-scheduler.service.d/release.conf
  /etc/systemd/system/probiga-scheduler.service.d/release-path.conf
  /etc/systemd/system/probiga-scheduler.service.d/release-revision.conf
  /etc/systemd/system/probiga-scheduler.service.d/zz-probiga-env.conf
  "$AI_WORKER_DROPIN"
  "$STATIC_RELEASE_LINK"
)
activation_snapshot_assert_container() {
  test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test "$(readlink -f "$ACTIVATION_UNIT_SNAPSHOT_DIR")" = \
    "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test "$(stat -c '%U:%G' "$ACTIVATION_UNIT_SNAPSHOT_DIR")" = root:root || \
    return 1
  test "$(stat -c '%a' "$ACTIVATION_UNIT_SNAPSHOT_DIR")" = 700 || return 1
  controlled_guard_assert_file "$ACTIVATION_UNIT_SNAPSHOT_MANIFEST" 600 || \
    return 1
  controlled_guard_assert_file "$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST" 600 || \
    return 1
  controlled_guard_assert_file "$ACTIVATION_UNIT_SNAPSHOT_PHASE" 600 || return 1
  controlled_guard_assert_file "$ACTIVATION_UNIT_SNAPSHOT_STATE" 600 || return 1
  controlled_guard_assert_file "$ACTIVATION_UNIT_SNAPSHOT_STATE_SHA" 600 || \
    return 1
  controlled_guard_assert_file "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" 600 || \
    return 1
  controlled_guard_assert_file "$ACTIVATION_GOVERNANCE_OLD_SHA" 600 || return 1
  controlled_guard_assert_file "$ACTIVATION_RELEASE_IDENTITY" 600 || return 1
  controlled_guard_assert_file "$ACTIVATION_RELEASE_IDENTITY_SHA" 600 || \
    return 1
  test "$(<"$ACTIVATION_UNIT_SNAPSHOT_STATE_SHA")" = \
    "$(sha256sum "$ACTIVATION_UNIT_SNAPSHOT_STATE" | cut -d' ' -f1)" || return 1
  test "$(<"$ACTIVATION_GOVERNANCE_OLD_SHA")" = \
    "$(sha256sum "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" | cut -d' ' -f1)" || \
    return 1
  test "$(<"$ACTIVATION_RELEASE_IDENTITY_SHA")" = \
    "$(sha256sum "$ACTIVATION_RELEASE_IDENTITY" | cut -d' ' -f1)" || \
    return 1
  test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR/files" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR/files" || return 1
  test "$(stat -c '%U:%G' "$ACTIVATION_UNIT_SNAPSHOT_DIR/files")" = \
    root:root || return 1
  test "$(stat -c '%a' "$ACTIVATION_UNIT_SNAPSHOT_DIR/files")" = 700 || \
    return 1
  test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR/new-files" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR/new-files" || return 1
  test "$(stat -c '%U:%G' "$ACTIVATION_UNIT_SNAPSHOT_DIR/new-files")" = \
    root:root || return 1
  test "$(stat -c '%a' "$ACTIVATION_UNIT_SNAPSHOT_DIR/new-files")" = 700 || \
    return 1
  return 0
}
activation_snapshot_validate_release_identity() {
  local expected_release="$1"
  local adapter_registry_seal_sha
  local new_release
  local old_release
  local release_tree_sha
  local -a lines=()
  activation_snapshot_assert_container || return 1
  mapfile -t lines < "$ACTIVATION_RELEASE_IDENTITY" || return 1
  test "${#lines[@]}" -eq 5 || return 1
  test "${lines[0]}" = probiga.activation-release-identity.v1 || return 1
  case "${lines[1]}" in new_release=*) new_release="${lines[1]#new_release=}" ;; *) return 1 ;; esac
  case "${lines[2]}" in old_release=*) old_release="${lines[2]#old_release=}" ;; *) return 1 ;; esac
  case "${lines[3]}" in release_tree_sha256=*) release_tree_sha="${lines[3]#release_tree_sha256=}" ;; *) return 1 ;; esac
  case "${lines[4]}" in adapter_registry_seal_sha256=*) adapter_registry_seal_sha="${lines[4]#adapter_registry_seal_sha256=}" ;; *) return 1 ;; esac
  test "$new_release" = "$expected_release" || return 1
  [[ "$new_release" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$old_release" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ "$adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  return 0
}
activation_snapshot_old_release() {
  local expected_release="$1"
  local -a lines=()
  activation_snapshot_validate_release_identity "$expected_release" || return 1
  mapfile -t lines < "$ACTIVATION_RELEASE_IDENTITY" || return 1
  printf '%s\n' "${lines[2]#old_release=}"
}
activation_snapshot_validate_governance_new() {
  controlled_guard_assert_file "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" 600 || \
    return 1
  if [ -e "$ACTIVATION_GOVERNANCE_NEW_SHA" ] || \
    [ -L "$ACTIVATION_GOVERNANCE_NEW_SHA" ]; then
    controlled_guard_assert_file "$ACTIVATION_GOVERNANCE_NEW_SHA" 600 || \
      return 1
    test "$(<"$ACTIVATION_GOVERNANCE_NEW_SHA")" = \
      "$(sha256sum "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" | cut -d' ' -f1)" || \
      return 1
  else
    # The canonical snapshot is fsynced and atomically renamed before its
    # redundant checksum, so a crash in between remains safely recoverable.
    test ! -e "$ACTIVATION_GOVERNANCE_NEW_SHA" || return 1
    test ! -L "$ACTIVATION_GOVERNANCE_NEW_SHA" || return 1
  fi
  return 0
}
activation_snapshot_install_governance_new() {
  local source="$1"
  local snapshot_tmp
  local sha_tmp
  activation_snapshot_validate "$EXPECTED_SHA" >/dev/null || return 1
  test -f "$source" || return 1
  test ! -L "$source" || return 1
  snapshot_tmp="$(mktemp "$ACTIVATION_UNIT_SNAPSHOT_DIR/.governance-new.XXXXXX")" || \
    return 1
  sha_tmp="$(mktemp "$ACTIVATION_UNIT_SNAPSHOT_DIR/.governance-new-sha.XXXXXX")" || {
    rm -f -- "$snapshot_tmp"
    return 1
  }
  if ! install -o root -g root -m 0600 "$source" "$snapshot_tmp" || \
    ! printf '%s\n' "$(sha256sum "$snapshot_tmp" | cut -d' ' -f1)" \
      > "$sha_tmp" || ! chown root:root "$sha_tmp" || ! chmod 0600 "$sha_tmp" || \
    ! sync -f "$snapshot_tmp" || ! sync -f "$sha_tmp" || \
    ! mv -fT "$snapshot_tmp" "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || \
    ! mv -fT "$sha_tmp" "$ACTIVATION_GOVERNANCE_NEW_SHA" || \
    ! sync -f "$ACTIVATION_UNIT_SNAPSHOT_DIR"; then
    rm -f -- "$snapshot_tmp" "$sha_tmp"
    return 1
  fi
  activation_snapshot_validate_governance_new || return 1
  return 0
}
activation_snapshot_validate_receipt_pending() {
  local expected_release="$1"
  controlled_guard_assert_file "$ACTIVATION_RECEIPT_PENDING" 600 || return 1
  if [ -e "$ACTIVATION_RECEIPT_PENDING_SHA" ] || \
    [ -L "$ACTIVATION_RECEIPT_PENDING_SHA" ]; then
    controlled_guard_assert_file "$ACTIVATION_RECEIPT_PENDING_SHA" 600 || \
      return 1
    test "$(<"$ACTIVATION_RECEIPT_PENDING_SHA")" = \
      "$(sha256sum "$ACTIVATION_RECEIPT_PENDING" | cut -d' ' -f1)" || return 1
  else
    # The canonical JSON is independently fsynced and atomically renamed first.
    # A crash before the redundant checksum rename is therefore recoverable.
    test ! -e "$ACTIVATION_RECEIPT_PENDING_SHA" || return 1
    test ! -L "$ACTIVATION_RECEIPT_PENDING_SHA" || return 1
  fi
  /usr/bin/python3.14 -I - "$ACTIVATION_RECEIPT_PENDING" \
    "$expected_release" <<'PY'
import json
import re
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
sha = sys.argv[2]
valid = (
    isinstance(payload, dict)
    and payload.get("schema_version") == "probiga.deploy-receipt.v4"
    and payload.get("status") == "DEPLOYED"
    and payload.get("expected_sha") == sha
    and payload.get("active_sha") == sha
    and re.fullmatch(r"[0-9a-f]{40}", sha) is not None
)
raise SystemExit(0 if valid else 2)
PY
}
activation_snapshot_assert_pending_receipt_absent() {
  test ! -e "$ACTIVATION_RECEIPT_PENDING" || return 1
  test ! -L "$ACTIVATION_RECEIPT_PENDING" || return 1
  test ! -e "$ACTIVATION_RECEIPT_PENDING_SHA" || return 1
  test ! -L "$ACTIVATION_RECEIPT_PENDING_SHA" || return 1
  return 0
}
activation_snapshot_receipt_matches_current_v2_request() {
  local expected_release="$1"
  activation_snapshot_validate_receipt_pending "$expected_release" || return 1
  /usr/bin/python3.14 -I - "$ACTIVATION_RECEIPT_PENDING" \
    "$expected_release" "$EXPECTED_INPUT_LOCK_SHA256" "$EXPECTED_ADATA_SHA" \
    "$EXPECTED_ADATA_TREE_SHA256" <<'PY'
import json
import re
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
release_sha, input_lock_sha256, adata_sha, adata_tree_sha256 = sys.argv[2:]
expected_identity = (
    ("expected_sha", release_sha),
    ("active_sha", release_sha),
    ("expected_input_lock_sha256", input_lock_sha256),
    ("active_input_lock_sha256", input_lock_sha256),
    # V2 builds install the CI-resolved freeze verbatim, so its digest must
    # equal the request's input-lock digest even before a new build is made.
    ("expected_resolved_freeze_sha256", input_lock_sha256),
    ("active_resolved_freeze_sha256", input_lock_sha256),
    ("expected_adata_sha", adata_sha),
    ("active_adata_sha", adata_sha),
    ("expected_adata_tree_sha256", adata_tree_sha256),
    ("active_adata_tree_sha256", adata_tree_sha256),
)
valid = (
    isinstance(payload, dict)
    and re.fullmatch(r"[0-9a-f]{40}", release_sha) is not None
    and re.fullmatch(r"[0-9a-f]{64}", input_lock_sha256) is not None
    and re.fullmatch(r"[0-9a-f]{40}", adata_sha) is not None
    and re.fullmatch(r"[0-9a-f]{64}", adata_tree_sha256) is not None
    and all(payload.get(key) == value for key, value in expected_identity)
)
raise SystemExit(0 if valid else 2)
PY
}
finalized_receipt_matches_current_v2_request() {
  local candidate
  local receipt_hash
  test -d "$RECEIPT_DIR" || return 1
  test ! -L "$RECEIPT_DIR" || return 1
  test "$(readlink -f "$RECEIPT_DIR")" = "$RECEIPT_DIR" || return 1
  test "$(stat -c '%U:%G' "$RECEIPT_DIR")" = root:root || return 1
  test "$(stat -c '%a' "$RECEIPT_DIR")" = 700 || return 1
  while IFS= read -r -d '' candidate; do
    case "$candidate" in
      "$RECEIPT_DIR/$EXPECTED_SHA-finalized-"*.json) ;;
      *) continue ;;
    esac
    controlled_guard_assert_file "$candidate" 600 || continue
    receipt_hash="${candidate##*-finalized-}"
    receipt_hash="${receipt_hash%.json}"
    [[ "$receipt_hash" =~ ^[0-9a-f]{64}$ ]] || continue
    test "$(sha256sum "$candidate" | cut -d' ' -f1)" = "$receipt_hash" || \
      continue
    if /usr/bin/python3.14 -I - "$candidate" "$EXPECTED_SHA" \
      "$EXPECTED_INPUT_LOCK_SHA256" "$EXPECTED_RESOLVED_FREEZE_SHA256" \
      "$EXPECTED_WHEEL_MANIFEST_SHA256" "$EXPECTED_ADATA_SHA" \
      "$EXPECTED_ADATA_TREE_SHA256" <<'PY'
import json
import re
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
(
    release_sha,
    input_lock_sha256,
    resolved_freeze_sha256,
    wheel_manifest_sha256,
    adata_sha,
    adata_tree_sha256,
) = sys.argv[2:]
expected_identity = (
    ("expected_sha", release_sha),
    ("active_sha", release_sha),
    ("expected_input_lock_sha256", input_lock_sha256),
    ("active_input_lock_sha256", input_lock_sha256),
    ("expected_resolved_freeze_sha256", resolved_freeze_sha256),
    ("active_resolved_freeze_sha256", resolved_freeze_sha256),
    ("expected_wheel_manifest_sha256", wheel_manifest_sha256),
    ("expected_adata_sha", adata_sha),
    ("active_adata_sha", adata_sha),
    ("expected_adata_tree_sha256", adata_tree_sha256),
    ("active_adata_tree_sha256", adata_tree_sha256),
)
valid = (
    isinstance(payload, dict)
    and payload.get("schema_version") == "probiga.deploy-receipt.v4"
    and payload.get("status") == "DEPLOYED"
    and re.fullmatch(r"[0-9a-f]{40}", release_sha) is not None
    and all(
        re.fullmatch(r"[0-9a-f]{64}", value) is not None
        for value in (
            input_lock_sha256,
            resolved_freeze_sha256,
            wheel_manifest_sha256,
            adata_tree_sha256,
        )
    )
    and re.fullmatch(r"[0-9a-f]{40}", adata_sha) is not None
    and all(payload.get(key) == value for key, value in expected_identity)
)
raise SystemExit(0 if valid else 2)
PY
    then
      return 0
    fi
  done < <(find -P "$RECEIPT_DIR" -mindepth 1 -maxdepth 1 -type f \
    -name "$EXPECTED_SHA-finalized-*.json" -print0)
  return 1
}
activation_snapshot_validate_rollback_receipt_state() {
  local expected_release="$1"
  local phase="$2"
  if [ -e "$ACTIVATION_RECEIPT_PENDING" ] || \
    [ -L "$ACTIVATION_RECEIPT_PENDING" ] || \
    [ -e "$ACTIVATION_RECEIPT_PENDING_SHA" ] || \
    [ -L "$ACTIVATION_RECEIPT_PENDING_SHA" ]; then
    # A disconnected caller can leave a sealed forward receipt after the
    # forward runtime started or while its same-process rollback was running.
    # The receipt is not authority to keep that runtime: accept only the
    # complete, hash-verified pair in a phase reachable after forward startup.
    # The journal is removed only after the old runtime is verified again.
    case "$phase" in
      runtime-units-installed|restoring-old|old-set-restored|\
      old-runtime-verified) ;;
      *) return 1 ;;
    esac
    activation_snapshot_validate_receipt_pending "$expected_release" || return 1
  fi
  return 0
}
publish_deployed_receipt_pending() {
  local guarded_sha="$1"
  local pending_sha
  local receipt_target
  local receipt_tmp
  activation_snapshot_validate_receipt_pending "$guarded_sha" || return 1
  if [ -e "$ACTIVATION_RECEIPT_PENDING_SHA" ] || \
    [ -L "$ACTIVATION_RECEIPT_PENDING_SHA" ]; then
    pending_sha="$(<"$ACTIVATION_RECEIPT_PENDING_SHA")" || return 1
  else
    pending_sha="$(sha256sum "$ACTIVATION_RECEIPT_PENDING" | cut -d' ' -f1)" || \
      return 1
  fi
  [[ "$pending_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  install -d -o root -g root -m 0700 "$RECEIPT_DIR" || return 1
  test ! -L "$RECEIPT_DIR" || return 1
  test "$(readlink -f "$RECEIPT_DIR")" = "$RECEIPT_DIR" || return 1
  receipt_target="$RECEIPT_DIR/$guarded_sha-finalized-$pending_sha.json"
  if [ -e "$receipt_target" ] || [ -L "$receipt_target" ]; then
    controlled_guard_assert_file "$receipt_target" 600 || return 1
    cmp --silent "$ACTIVATION_RECEIPT_PENDING" "$receipt_target" || return 1
    sync -f "$RECEIPT_DIR" || return 1
    return 0
  fi
  receipt_tmp="$(mktemp "$RECEIPT_DIR/.finalized-receipt.XXXXXX")" || return 1
  if ! install -o root -g root -m 0600 "$ACTIVATION_RECEIPT_PENDING" \
      "$receipt_tmp" || ! sync -f "$receipt_tmp" || \
    ! mv -fT "$receipt_tmp" "$receipt_target" || \
    ! sync -f "$RECEIPT_DIR"; then
    rm -f -- "$receipt_tmp"
    return 1
  fi
  controlled_guard_assert_file "$receipt_target" 600 || return 1
  cmp --silent "$ACTIVATION_RECEIPT_PENDING" "$receipt_target" || return 1
  return 0
}
activation_snapshot_phase() {
  local phase
  activation_snapshot_assert_container || return 1
  phase="$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" || return 1
  case "$phase" in
    prepared|runtime-units-installing|runtime-units-installed|\
    restoring-old|old-set-restored|old-runtime-verified|\
    restoring-new-no-receipt|new-runtime-preserved-no-receipt|\
    new-runtime-verified|finalized) ;;
    *) return 1 ;;
  esac
  printf '%s\n' "$phase"
}
activation_snapshot_set_phase() {
  local expected_release="$1"
  local next_phase="$2"
  local phase_tmp
  case "$next_phase" in
    prepared|runtime-units-installing|runtime-units-installed|\
    restoring-old|old-set-restored|old-runtime-verified|\
    restoring-new-no-receipt|new-runtime-preserved-no-receipt|\
    new-runtime-verified|finalized) ;;
    *) return 1 ;;
  esac
  activation_snapshot_validate "$expected_release" >/dev/null || return 1
  case "$next_phase" in
    restoring-new-no-receipt|new-runtime-preserved-no-receipt)
      activation_snapshot_validate_new "$expected_release" || return 1
      activation_snapshot_validate_governance_new || return 1
      activation_snapshot_assert_pending_receipt_absent || return 1
      ;;
    new-runtime-verified|finalized)
      activation_snapshot_validate_governance_new || return 1
      activation_snapshot_validate_receipt_pending "$expected_release" || return 1
      ;;
  esac
  phase_tmp="$(mktemp "$ACTIVATION_UNIT_SNAPSHOT_DIR/.phase.XXXXXX")" || \
    return 1
  if ! printf '%s\n' "$next_phase" > "$phase_tmp" || \
    ! chown root:root "$phase_tmp" || ! chmod 0600 "$phase_tmp" || \
    ! mv -fT "$phase_tmp" "$ACTIVATION_UNIT_SNAPSHOT_PHASE"; then
    rm -f -- "$phase_tmp"
    return 1
  fi
  sync -f "$ACTIVATION_UNIT_SNAPSHOT_PHASE" || return 1
  sync -f "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = "$next_phase" || return 1
  return 0
}
activation_snapshot_validate() {
  local expected_release="$1"
  local expected_path
  local file_sha
  local index=0
  local kind
  local manifest_release
  local mode
  local path
  local payload
  local phase
  local -a lines=()
  activation_snapshot_assert_container || return 1
  mapfile -t lines < "$ACTIVATION_UNIT_SNAPSHOT_MANIFEST" || return 1
  test "${#lines[@]}" -eq "$((${#ACTIVATION_UNIT_PATHS[@]} + 2))" || \
    return 1
  test "${lines[0]}" = probiga.activation-unit-transaction.v1 || return 1
  case "${lines[1]}" in
    release=*) manifest_release="${lines[1]#release=}" ;;
    *) return 1 ;;
  esac
  test "$manifest_release" = "$expected_release" || return 1
  [[ "$manifest_release" =~ ^[0-9a-f]{40}$ ]] || return 1
  activation_snapshot_validate_release_identity "$expected_release" || return 1
  for expected_path in "${ACTIVATION_UNIT_PATHS[@]}"; do
    IFS='|' read -r kind path mode file_sha payload \
      <<< "${lines[$((index + 2))]}" || return 1
    test "$path" = "$expected_path" || return 1
    case "$kind" in
      missing)
        test "$mode:$file_sha:$payload" = '-:-:-' || return 1
        ;;
      file)
        [[ "$mode" =~ ^[0-7]{3,4}$ ]] || return 1
        [[ "$file_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
        test "$payload" = "files/$index" || return 1
        controlled_guard_assert_file \
          "$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload" 600 || return 1
        test "$(sha256sum "$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload" | \
          cut -d' ' -f1)" = "$file_sha" || return 1
        ;;
      symlink)
        test "$expected_path" = "$STATIC_RELEASE_LINK" || return 1
        test "$mode" = - || return 1
        [[ "$file_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
        if [ "$payload" != /opt/ProBigA ] && \
          [[ ! "$payload" =~ ^/opt/ProBigA-releases/[0-9a-f]{40}$ ]]; then
          return 1
        fi
        test "$(printf '%s' "$payload" | sha256sum | cut -d' ' -f1)" = \
          "$file_sha" || return 1
        ;;
      *) return 1 ;;
    esac
    index=$((index + 1))
  done
  activation_snapshot_phase_unchecked >/dev/null || return 1
  phase="$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" || return 1
  case "$phase" in
    restoring-new-no-receipt|new-runtime-preserved-no-receipt)
      activation_snapshot_validate_new "$expected_release" || return 1
      activation_snapshot_validate_governance_new || return 1
      activation_snapshot_assert_pending_receipt_absent || return 1
      ;;
    new-runtime-verified|finalized)
      activation_snapshot_validate_governance_new || return 1
      activation_snapshot_validate_receipt_pending "$expected_release" || return 1
      ;;
  esac
  return 0
}
activation_snapshot_phase_unchecked() {
  local phase
  phase="$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" || return 1
  case "$phase" in
    prepared|runtime-units-installing|runtime-units-installed|\
    restoring-old|old-set-restored|old-runtime-verified|\
    restoring-new-no-receipt|new-runtime-preserved-no-receipt|\
    new-runtime-verified|finalized) return 0 ;;
    *) return 1 ;;
  esac
}
activation_snapshot_committed_phase_for_release() {
  local expected_release="$1"
  local phase
  local recorded_release
  recorded_release="$(activation_snapshot_recorded_release)" || return 1
  test "$recorded_release" = "$expected_release" || return 1
  phase="$(activation_snapshot_phase)" || return 1
  case "$phase" in
    old-runtime-verified|new-runtime-preserved-no-receipt|\
    new-runtime-verified|finalized) ;;
    *) return 1 ;;
  esac
  printf '%s\n' "$phase"
}
activation_snapshot_recorded_release() {
  local release
  local -a lines=()
  activation_snapshot_assert_container || return 1
  mapfile -t lines < "$ACTIVATION_UNIT_SNAPSHOT_MANIFEST" || return 1
  test "${#lines[@]}" -ge 2 || return 1
  case "${lines[1]}" in
    release=*) release="${lines[1]#release=}" ;;
    *) return 1 ;;
  esac
  [[ "$release" =~ ^[0-9a-f]{40}$ ]] || return 1
  activation_snapshot_validate "$release" >/dev/null || return 1
  printf '%s\n' "$release"
}
activation_snapshot_retire_verified_transaction() {
  local retired_dir
  test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  retired_dir="$(mktemp -d \
    "$DATABASE_WRITER_GUARD_DIR/.activation-unit-transaction.retired.XXXXXX")" || \
    return 1
  case "$retired_dir" in
    "$DATABASE_WRITER_GUARD_DIR"/.activation-unit-transaction.retired.*) ;;
    *) return 1 ;;
  esac
  test -d "$retired_dir" || return 1
  test ! -L "$retired_dir" || return 1
  if ! rmdir -- "$retired_dir" || \
    ! mv -T -- "$ACTIVATION_UNIT_SNAPSHOT_DIR" "$retired_dir"; then
    rm -rf -- "$retired_dir" 2>/dev/null || true
    return 1
  fi
  # The same-filesystem rename is the logical commit.  Everything below is
  # best-effort physical cleanup: faults or SIGKILL can leave only an inert
  # tombstone, never a half-deleted active transaction that wedges recovery.
  sync -f "$DATABASE_WRITER_GUARD_DIR" || {
    echo "Warning: activation transaction retire fsync failed" >&2 || true
  }
  rm -rf -- "$retired_dir" || {
    echo "Warning: retired activation transaction cleanup failed: $retired_dir" \
      >&2 || true
  }
  sync -f "$DATABASE_WRITER_GUARD_DIR" || {
    echo "Warning: retired activation transaction cleanup fsync failed" >&2 || true
  }
  test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  return 0
}
activation_snapshot_remove_finalized_before_deploy() {
  local recorded_release
  local phase
  recorded_release="$(activation_snapshot_recorded_release)" || return 1
  activation_snapshot_validate_new "$recorded_release" || return 1
  phase="$(activation_snapshot_phase)" || return 1
  test "$phase" = finalized || return 1
  activation_snapshot_assert_new_set "$recorded_release" || return 1
  # The pending receipt lives inside the transaction directory.  Publish its
  # sealed, content-addressed copy before removing the only durable source.
  publish_deployed_receipt_pending "$recorded_release" || return 1
  activation_snapshot_retire_verified_transaction || return 1
  test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  return 0
}
activation_snapshot_remove_old_runtime_verified() {
  local recorded_release
  local phase
  recorded_release="$(activation_snapshot_recorded_release)" || return 1
  phase="$(activation_snapshot_phase)" || return 1
  test "$phase" = old-runtime-verified || return 1
  activation_snapshot_assert_old_set "$recorded_release" || return 1
  activation_snapshot_retire_verified_transaction || return 1
  test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  return 0
}
activation_snapshot_remove_new_runtime_preserved_no_receipt() {
  local recorded_release
  local phase
  recorded_release="$(activation_snapshot_recorded_release)" || return 1
  activation_snapshot_validate_new "$recorded_release" || return 1
  activation_snapshot_validate_governance_new || return 1
  activation_snapshot_assert_pending_receipt_absent || return 1
  phase="$(activation_snapshot_phase)" || return 1
  test "$phase" = new-runtime-preserved-no-receipt || return 1
  activation_snapshot_assert_new_set "$recorded_release" || return 1
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test ! -L "$DATABASE_WRITER_RESTORE_FILE" || return 1
  activation_snapshot_retire_verified_transaction || return 1
  test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  return 0
}
activation_snapshot_append_new_record() {
  local build_dir="$1"
  local manifest="$2"
  local index="$3"
  local path="$4"
  local source=""
  local target=""
  case "$path" in
    "$MAIN_RELEASE_DROPIN") source="$PREPARED_MAIN_DROPIN" ;;
    "$SCHEDULER_UNIT") source="$PREPARED_SCHEDULER_DROPIN" ;;
    "$AI_WORKER_DROPIN")
      if [ "${AI_WORKER_UNIT_PRESENT:-0}" -eq 1 ]; then
        source="$PREPARED_AI_WORKER_DROPIN"
      elif [ -f "$path" ] && [ ! -L "$path" ]; then
        source="$path"
      elif [ -e "$path" ] || [ -L "$path" ]; then
        return 1
      fi
      ;;
    "$STATIC_RELEASE_LINK")
      target="$CODE_RELEASE_ROOT/$EXPECTED_SHA"
      printf 'symlink|%s|-|%s|%s\n' "$path" \
        "$(printf '%s' "$target" | sha256sum | cut -d' ' -f1)" "$target" \
        >> "$manifest" || return 1
      return 0
      ;;
    *)
      printf 'missing|%s|-|-|-\n' "$path" >> "$manifest" || return 1
      return 0
      ;;
  esac
  if [ -z "$source" ]; then
    printf 'missing|%s|-|-|-\n' "$path" >> "$manifest" || return 1
    return 0
  fi
  test -f "$source" || return 1
  test ! -L "$source" || return 1
  install -o root -g root -m 0600 "$source" \
    "$build_dir/new-files/$index" || return 1
  printf 'file|%s|644|%s|new-files/%s\n' "$path" \
    "$(sha256sum "$build_dir/new-files/$index" | cut -d' ' -f1)" "$index" \
    >> "$manifest" || return 1
  return 0
}

activation_snapshot_validate_new() {
  local expected_release="$1"
  local expected_path
  local file_sha
  local index=0
  local kind
  local manifest_release
  local mode
  local path
  local payload
  local -a lines=()
  activation_snapshot_assert_container || return 1
  mapfile -t lines < "$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST" || return 1
  test "${#lines[@]}" -eq "$((${#ACTIVATION_UNIT_PATHS[@]} + 2))" || \
    return 1
  test "${lines[0]}" = probiga.activation-unit-target.v1 || return 1
  case "${lines[1]}" in
    release=*) manifest_release="${lines[1]#release=}" ;;
    *) return 1 ;;
  esac
  test "$manifest_release" = "$expected_release" || return 1
  for expected_path in "${ACTIVATION_UNIT_PATHS[@]}"; do
    IFS='|' read -r kind path mode file_sha payload \
      <<< "${lines[$((index + 2))]}" || return 1
    test "$path" = "$expected_path" || return 1
    case "$kind" in
      missing) test "$mode:$file_sha:$payload" = '-:-:-' || return 1 ;;
      file)
        test "$mode" = 644 || return 1
        [[ "$file_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
        test "$payload" = "new-files/$index" || return 1
        controlled_guard_assert_file \
          "$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload" 600 || return 1
        test "$(sha256sum "$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload" | \
          cut -d' ' -f1)" = "$file_sha" || return 1
        ;;
      symlink)
        test "$expected_path" = "$STATIC_RELEASE_LINK" || return 1
        test "$mode" = - || return 1
        test "$payload" = "$CODE_RELEASE_ROOT/$expected_release" || return 1
        test "$(printf '%s' "$payload" | sha256sum | cut -d' ' -f1)" = \
          "$file_sha" || return 1
        ;;
      *) return 1 ;;
    esac
    index=$((index + 1))
  done
  return 0
}

activation_snapshot_assert_new_set() {
  local expected_release="$1"
  local index=0
  local kind
  local mode
  local path
  local recorded_sha
  local payload
  local -a lines=()
  activation_snapshot_validate_new "$expected_release" || return 1
  mapfile -t lines < "$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST" || return 1
  for path in "${ACTIVATION_UNIT_PATHS[@]}"; do
    IFS='|' read -r kind path mode recorded_sha payload \
      <<< "${lines[$((index + 2))]}" || return 1
    case "$kind" in
      missing) test ! -e "$path" && test ! -L "$path" || return 1 ;;
      file)
        controlled_guard_assert_file "$path" "$mode" || return 1
        test "$(sha256sum "$path" | cut -d' ' -f1)" = "$recorded_sha" || \
          return 1
        ;;
      symlink)
        test -L "$path" || return 1
        test "$(readlink "$path")" = "$payload" || return 1
        ;;
      *) return 1 ;;
    esac
    index=$((index + 1))
  done
  return 0
}

activation_snapshot_restore_new_set() {
  local expected_release="$1"
  local index=0
  local kind
  local mode
  local path
  local recorded_sha
  local payload
  local link_tmp
  local -a lines=()
  activation_snapshot_validate_new "$expected_release" || return 1
  mapfile -t lines < "$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST" || return 1
  for path in "${ACTIVATION_UNIT_PATHS[@]}"; do
    IFS='|' read -r kind path mode recorded_sha payload \
      <<< "${lines[$((index + 2))]}" || return 1
    case "$kind" in
      missing) rm -f -- "$path" || return 1 ;;
      file)
        install -d -o root -g root -m 0755 "$(dirname "$path")" || return 1
        install -o root -g root -m "$mode" \
          "$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload" "$path" || return 1
        ;;
      symlink)
        link_tmp="$(dirname "$path")/.probiga-static-new-$$"
        rm -f -- "$link_tmp" || return 1
        ln -s "$payload" "$link_tmp" || return 1
        mv -fT "$link_tmp" "$path" || return 1
        ;;
      *) return 1 ;;
    esac
    index=$((index + 1))
  done
  sync -f /etc/systemd/system || return 1
  sync -f /opt || return 1
  activation_snapshot_assert_new_set "$expected_release" || return 1
  return 0
}

activation_snapshot_create() {
  local build_dir
  local file_sha
  local index=0
  local manifest_tmp
  local new_manifest_tmp
  local mode
  local path
  local target
  local release_identity_tmp
  test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  [[ "${PREVIOUS_RELEASE_REVISION:-}" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "${EXPECTED_RELEASE_TREE_SHA256:-}" =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ "${EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256:-}" =~ ^[0-9a-f]{64}$ ]] || \
    return 1
  build_dir="$(mktemp -d \
    "$DATABASE_WRITER_GUARD_DIR/.activation-unit-transaction.XXXXXX")" || \
    return 1
  if ! chown root:root "$build_dir" || ! chmod 0700 "$build_dir" || \
    ! install -d -o root -g root -m 0700 "$build_dir/files" || \
    ! install -d -o root -g root -m 0700 "$build_dir/new-files"; then
    rm -rf -- "$build_dir"
    return 1
  fi
  manifest_tmp="$build_dir/manifest"
  if ! printf '%s\n' probiga.activation-unit-transaction.v1 \
    "release=$EXPECTED_SHA" > "$manifest_tmp"; then
    rm -rf -- "$build_dir"
    return 1
  fi
  for path in "${ACTIVATION_UNIT_PATHS[@]}"; do
    if [ -L "$path" ]; then
      if [ "$path" != "$STATIC_RELEASE_LINK" ]; then
        rm -rf -- "$build_dir"
        return 1
      fi
      target="$(readlink "$path")" || { rm -rf -- "$build_dir"; return 1; }
      if [ "$target" != /opt/ProBigA ] && \
        [[ ! "$target" =~ ^/opt/ProBigA-releases/[0-9a-f]{40}$ ]]; then
        rm -rf -- "$build_dir"
        return 1
      fi
      file_sha="$(printf '%s' "$target" | sha256sum | cut -d' ' -f1)"
      printf 'symlink|%s|-|%s|%s\n' "$path" "$file_sha" "$target" \
        >> "$manifest_tmp" || { rm -rf -- "$build_dir"; return 1; }
    elif [ -e "$path" ]; then
      if ! test -f "$path" || ! test "$(stat -c '%U:%G' "$path")" = \
        root:root; then
        rm -rf -- "$build_dir"
        return 1
      fi
      mode="$(stat -c '%a' "$path")" || { rm -rf -- "$build_dir"; return 1; }
      case "$mode" in
        400|440|444|600|640|644) ;;
        *) rm -rf -- "$build_dir"; return 1 ;;
      esac
      if ! install -o root -g root -m 0600 "$path" "$build_dir/files/$index"; then
        rm -rf -- "$build_dir"
        return 1
      fi
      file_sha="$(sha256sum "$build_dir/files/$index" | cut -d' ' -f1)"
      printf 'file|%s|%s|%s|files/%s\n' \
        "$path" "$mode" "$file_sha" "$index" >> "$manifest_tmp" || \
        { rm -rf -- "$build_dir"; return 1; }
    else
      printf 'missing|%s|-|-|-\n' "$path" >> "$manifest_tmp" || \
        { rm -rf -- "$build_dir"; return 1; }
    fi
    index=$((index + 1))
  done
  new_manifest_tmp="$build_dir/new-manifest"
  if ! printf '%s\n' probiga.activation-unit-target.v1 \
    "release=$EXPECTED_SHA" > "$new_manifest_tmp"; then
    rm -rf -- "$build_dir"
    return 1
  fi
  index=0
  for path in "${ACTIVATION_UNIT_PATHS[@]}"; do
    activation_snapshot_append_new_record "$build_dir" "$new_manifest_tmp" \
      "$index" "$path" || { rm -rf -- "$build_dir"; return 1; }
    index=$((index + 1))
  done
  if ! chown root:root "$manifest_tmp" || ! chmod 0600 "$manifest_tmp" || \
    ! chown root:root "$new_manifest_tmp" || \
    ! chmod 0600 "$new_manifest_tmp" || \
    ! printf '%s\n' prepared > "$build_dir/phase" || \
    ! chown root:root "$build_dir/phase" || ! chmod 0600 "$build_dir/phase"; then
    rm -rf -- "$build_dir"
    return 1
  fi
  controlled_guard_assert_file "$DATABASE_WRITER_RESTORE_FILE" 600 || {
    rm -rf -- "$build_dir"
    return 1
  }
  install -o root -g root -m 0600 "$DATABASE_WRITER_RESTORE_FILE" \
    "$build_dir/writer-state" || { rm -rf -- "$build_dir"; return 1; }
  test -n "${GOVERNANCE_TASK_OLD_SOURCE:-}" || {
    rm -rf -- "$build_dir"
    return 1
  }
  test -f "$GOVERNANCE_TASK_OLD_SOURCE" || {
    rm -rf -- "$build_dir"
    return 1
  }
  test ! -L "$GOVERNANCE_TASK_OLD_SOURCE" || {
    rm -rf -- "$build_dir"
    return 1
  }
  install -o root -g root -m 0600 "$GOVERNANCE_TASK_OLD_SOURCE" \
    "$build_dir/governance-task-old.json" || {
      rm -rf -- "$build_dir"
      return 1
    }
  release_identity_tmp="$build_dir/release-identity"
  printf '%s\n' \
    probiga.activation-release-identity.v1 \
    "new_release=$EXPECTED_SHA" \
    "old_release=$PREVIOUS_RELEASE_REVISION" \
    "release_tree_sha256=$EXPECTED_RELEASE_TREE_SHA256" \
    "adapter_registry_seal_sha256=$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
    > "$release_identity_tmp" || { rm -rf -- "$build_dir"; return 1; }
  printf '%s\n' "$(sha256sum "$build_dir/writer-state" | cut -d' ' -f1)" \
    > "$build_dir/writer-state.sha256" || { rm -rf -- "$build_dir"; return 1; }
  printf '%s\n' \
    "$(sha256sum "$build_dir/governance-task-old.json" | cut -d' ' -f1)" \
    > "$build_dir/governance-task-old.sha256" || {
      rm -rf -- "$build_dir"
      return 1
    }
  printf '%s\n' "$(sha256sum "$release_identity_tmp" | cut -d' ' -f1)" \
    > "$build_dir/release-identity.sha256" || {
      rm -rf -- "$build_dir"
      return 1
    }
  chown root:root "$build_dir/writer-state.sha256" \
    "$build_dir/governance-task-old.sha256" "$release_identity_tmp" \
    "$build_dir/release-identity.sha256" || {
      rm -rf -- "$build_dir"
      return 1
    }
  chmod 0600 "$build_dir/writer-state.sha256" \
    "$build_dir/governance-task-old.sha256" "$release_identity_tmp" \
    "$build_dir/release-identity.sha256" || {
      rm -rf -- "$build_dir"
      return 1
    }
  find "$build_dir/files" -type f -exec sync -f {} \; || \
    { rm -rf -- "$build_dir"; return 1; }
  find "$build_dir/new-files" -type f -exec sync -f {} \; || \
    { rm -rf -- "$build_dir"; return 1; }
  sync -f "$manifest_tmp" || { rm -rf -- "$build_dir"; return 1; }
  sync -f "$new_manifest_tmp" || { rm -rf -- "$build_dir"; return 1; }
  sync -f "$build_dir/phase" || { rm -rf -- "$build_dir"; return 1; }
  sync -f "$build_dir/writer-state" || { rm -rf -- "$build_dir"; return 1; }
  sync -f "$build_dir/writer-state.sha256" || {
    rm -rf -- "$build_dir"
    return 1
  }
  sync -f "$build_dir/governance-task-old.json" || {
    rm -rf -- "$build_dir"
    return 1
  }
  sync -f "$build_dir/governance-task-old.sha256" || {
    rm -rf -- "$build_dir"
    return 1
  }
  sync -f "$release_identity_tmp" || { rm -rf -- "$build_dir"; return 1; }
  sync -f "$build_dir/release-identity.sha256" || {
    rm -rf -- "$build_dir"
    return 1
  }
  sync -f "$build_dir" || { rm -rf -- "$build_dir"; return 1; }
  mv -T "$build_dir" "$ACTIVATION_UNIT_SNAPSHOT_DIR" || \
    { rm -rf -- "$build_dir"; return 1; }
  sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
  activation_snapshot_validate "$EXPECTED_SHA" >/dev/null || return 1
  activation_snapshot_validate_new "$EXPECTED_SHA" >/dev/null || return 1
  return 0
}
activation_snapshot_assert_old_set() {
  local expected_path
  local index=0
  local kind
  local mode
  local path
  local recorded_sha
  local payload
  local -a lines=()
  activation_snapshot_validate "$1" >/dev/null || return 1
  mapfile -t lines < "$ACTIVATION_UNIT_SNAPSHOT_MANIFEST" || return 1
  for expected_path in "${ACTIVATION_UNIT_PATHS[@]}"; do
    IFS='|' read -r kind path mode recorded_sha payload \
      <<< "${lines[$((index + 2))]}" || return 1
    case "$kind" in
      missing) test ! -e "$path" && test ! -L "$path" || return 1 ;;
      file)
        test -f "$path" || return 1
        test ! -L "$path" || return 1
        test "$(stat -c '%U:%G' "$path")" = root:root || return 1
        test "$(stat -c '%a' "$path")" = "$mode" || return 1
        test "$(sha256sum "$path" | cut -d' ' -f1)" = "$recorded_sha" || \
          return 1
        ;;
      symlink)
        test -L "$path" || return 1
        test "$(readlink "$path")" = "$payload" || return 1
        ;;
      *) return 1 ;;
    esac
    index=$((index + 1))
  done
  return 0
}
activation_snapshot_restore_old_set() {
  local expected_release="$1"
  local index=0
  local kind
  local mode
  local path
  local recorded_sha
  local payload
  local link_tmp
  local -a lines=()
  activation_snapshot_validate "$expected_release" >/dev/null || return 1
  activation_snapshot_set_phase "$expected_release" restoring-old || return 1
  mapfile -t lines < "$ACTIVATION_UNIT_SNAPSHOT_MANIFEST" || return 1
  for path in "${ACTIVATION_UNIT_PATHS[@]}"; do
    IFS='|' read -r kind path mode recorded_sha payload \
      <<< "${lines[$((index + 2))]}" || return 1
    case "$kind" in
      missing) rm -f -- "$path" || return 1 ;;
      file)
        install -d -o root -g root -m 0755 "$(dirname "$path")" || return 1
        install -o root -g root -m "$mode" \
          "$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload" "$path" || return 1
        ;;
      symlink)
        link_tmp="$(dirname "$path")/.probiga-static-restore.$$"
        rm -f -- "$link_tmp" || return 1
        ln -s "$payload" "$link_tmp" || return 1
        mv -fT "$link_tmp" "$path" || return 1
        ;;
      *) return 1 ;;
    esac
    index=$((index + 1))
  done
  sync -f /etc/systemd/system || return 1
  sync -f /opt || return 1
  activation_snapshot_assert_old_set "$expected_release" || return 1
  activation_snapshot_set_phase "$expected_release" old-set-restored || return 1
  return 0
}
controlled_guard_assert_file() {
  local expected_mode="$2"
  local path="$1"
  test -f "$path" || return 1
  test ! -L "$path" || return 1
  test "$(stat -c '%U:%G' "$path")" = root:root || return 1
  test "$(stat -c '%a' "$path")" = "$expected_mode" || return 1
  return 0
}
controlled_guard_assert_directory() {
  test -d "$DATABASE_WRITER_GUARD_DIR" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_DIR" || return 1
  test "$(readlink -f "$DATABASE_WRITER_GUARD_DIR")" = \
    "$DATABASE_WRITER_GUARD_DIR" || return 1
  test "$(stat -c '%U:%G' "$DATABASE_WRITER_GUARD_DIR")" = root:root || \
    return 1
  test "$(stat -c '%a' "$DATABASE_WRITER_GUARD_DIR")" = 700 || return 1
  return 0
}
controlled_guard_assert_storage() {
  controlled_guard_assert_directory || return 1
  controlled_guard_assert_file "$DATABASE_WRITER_GUARD_FILE" 600 || return 1
  return 0
}
controlled_guard_assert_state_record() {
  local active_state
  local extra
  local load_state
  local record="$2"
  local unit_file_state
  local unit_kind="$1"
  IFS=, read -r load_state active_state unit_file_state extra <<< "$record" || \
    return 1
  test -z "$extra" || return 1
  test "$record" = "$load_state,$active_state,$unit_file_state" || return 1
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
    *) return 1 ;;
  esac
  return 0
}
controlled_guard_assert_marker() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local guarded_sha="$1"
  local main_record="$2"
  local scheduler_record="$3"
  local -a guard_lines=()
  controlled_guard_assert_state_record main "$main_record" || return 1
  controlled_guard_assert_state_record scheduler "$scheduler_record" || return 1
  controlled_guard_assert_state_record ai-service "$ai_service_record" || return 1
  controlled_guard_assert_state_record ai-timer "$ai_timer_record" || return 1
  test "${ai_service_record%%,*}" = "${ai_timer_record%%,*}" || return 1
  controlled_guard_assert_storage || return 1
  mapfile -t guard_lines < "$DATABASE_WRITER_GUARD_FILE" || return 1
  test "${#guard_lines[@]}" -eq 6 || return 1
  test "${guard_lines[0]}" = probiga.database-writer-guard.v2 || return 1
  test "${guard_lines[1]}" = "release=$guarded_sha" || return 1
  test "${guard_lines[2]}" = "main_unit=$main_record" || return 1
  test "${guard_lines[3]}" = "scheduler_unit=$scheduler_record" || return 1
  test "${guard_lines[4]}" = "ai_service_unit=$ai_service_record" || return 1
  test "${guard_lines[5]}" = "ai_timer_unit=$ai_timer_record" || return 1
  return 0
}
controlled_guard_assert_restore_file() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local guarded_sha="$1"
  local main_record="$2"
  local scheduler_record="$3"
  local -a restore_lines=()
  controlled_guard_assert_state_record main "$main_record" || return 1
  controlled_guard_assert_state_record scheduler "$scheduler_record" || return 1
  controlled_guard_assert_state_record ai-service "$ai_service_record" || return 1
  controlled_guard_assert_state_record ai-timer "$ai_timer_record" || return 1
  test "${ai_service_record%%,*}" = "${ai_timer_record%%,*}" || return 1
  controlled_guard_assert_directory || return 1
  controlled_guard_assert_file "$DATABASE_WRITER_RESTORE_FILE" 600 || return 1
  mapfile -t restore_lines < "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test "${#restore_lines[@]}" -eq 6 || return 1
  test "${restore_lines[0]}" = probiga.database-writer-restore.v1 || return 1
  test "${restore_lines[1]}" = "release=$guarded_sha" || return 1
  test "${restore_lines[2]}" = "main_unit=$main_record" || return 1
  test "${restore_lines[3]}" = "scheduler_unit=$scheduler_record" || return 1
  test "${restore_lines[4]}" = "ai_service_unit=$ai_service_record" || return 1
  test "${restore_lines[5]}" = "ai_timer_unit=$ai_timer_record" || return 1
  return 0
}
controlled_guard_write_restore_file() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local guarded_sha="$1"
  local main_record="$2"
  local restore_tmp
  local scheduler_record="$3"
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
    return 0
  fi
  restore_tmp="$(mktemp \
    "$DATABASE_WRITER_GUARD_DIR/.database-writer-restore.XXXXXX")" || return 1
  if ! printf '%s\n' \
    probiga.database-writer-restore.v1 \
    "release=$guarded_sha" \
    "main_unit=$main_record" \
    "scheduler_unit=$scheduler_record" \
    "ai_service_unit=$ai_service_record" \
    "ai_timer_unit=$ai_timer_record" \
    > "$restore_tmp" || \
    ! chown root:root "$restore_tmp" || \
    ! chmod 0600 "$restore_tmp" || \
    ! mv -fT "$restore_tmp" "$DATABASE_WRITER_RESTORE_FILE"; then
    rm -f -- "$restore_tmp"
    return 1
  fi
  sync -f "$DATABASE_WRITER_RESTORE_FILE" || return 1
  sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
  controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  return 0
}
controlled_guard_assert_dropin() {
  local parent
  local path="$1"
  parent="$(dirname "$path")" || return 1
  test -d "$parent" || return 1
  test ! -L "$parent" || return 1
  test "$(readlink -f "$parent")" = "$parent" || return 1
  test "$(stat -c '%U:%G' "$parent")" = root:root || return 1
  test "$(stat -c '%a' "$parent")" = 755 || return 1
  controlled_guard_assert_file "$path" 644 || return 1
  test "$(wc -l < "$path")" -eq 2 || return 1
  grep -Fx '[Unit]' "$path" >/dev/null || return 1
  grep -Fx "ConditionPathExists=!$DATABASE_WRITER_GUARD_FILE" \
    "$path" >/dev/null || return 1
  return 0
}
controlled_guard_assert_unit_dropin_loaded() {
  local dropin="$2"
  local dropin_paths
  local unit="$1"
  test "$(systemctl show -p LoadState --value "$unit")" = loaded || return 1
  dropin_paths="$(systemctl show -p DropInPaths --value "$unit")" || return 1
  case " $dropin_paths " in
    *" $dropin "*) ;;
    *) return 1 ;;
  esac
  return 0
}
controlled_guard_assert_unit_fenced() {
  local allowed_unit_file_states="$2"
  local active_state
  local exec_main_pid
  local main_pid
  local unit="$1"
  local unit_file_state
  test "$(systemctl show -p LoadState --value "$unit")" = loaded || return 1
  active_state="$(systemctl show -p ActiveState --value "$unit")" || return 1
  unit_file_state="$(systemctl show -p UnitFileState --value "$unit")" || return 1
  test "$active_state" = inactive || return 1
  main_pid="$(systemctl show -p MainPID --value "$unit")" || return 1
  exec_main_pid="$(systemctl show -p ExecMainPID --value "$unit")" || return 1
  test "${main_pid:-0}" = 0 || return 1
  test "${exec_main_pid:-0}" = 0 || return 1
  case ":$allowed_unit_file_states:" in
    *":$unit_file_state:"*) ;;
    *) return 1 ;;
  esac
  return 0
}
controlled_guard_normalize_unit_fenced() {
  local active_state
  local allowed_unit_file_states="$2"
  local exec_main_pid
  local main_pid
  local unit="$1"
  local unit_file_state
  test "$(systemctl show -p LoadState --value "$unit")" = loaded || return 1
  unit_file_state="$(systemctl show -p UnitFileState --value "$unit")" || return 1
  case ":$allowed_unit_file_states:" in
    *":$unit_file_state:"*) ;;
    *) return 1 ;;
  esac
  active_state="$(systemctl show -p ActiveState --value "$unit")" || return 1
  case "$active_state" in
    inactive) ;;
    failed)
      main_pid="$(systemctl show -p MainPID --value "$unit")" || return 1
      exec_main_pid="$(systemctl show -p ExecMainPID --value "$unit")" || return 1
      test "${main_pid:-0}" = 0 || return 1
      test "${exec_main_pid:-0}" = 0 || return 1
      systemctl stop "$unit" || return 1
      systemctl reset-failed "$unit" || return 1
      ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_unit_fenced "$unit" "$allowed_unit_file_states" || \
    return 1
  return 0
}
controlled_guard_assert_unit_inventory_fenced() {
  local allowed_unit_file_states="$2"
  local expected_load="$3"
  local unit="$1"
  case "$expected_load" in
    loaded)
      controlled_guard_normalize_unit_fenced "$unit" \
        "$allowed_unit_file_states" || return 1
      ;;
    not-found)
      test "$(systemctl show -p LoadState --value "$unit")" = not-found || \
        return 1
      ;;
    *) return 1 ;;
  esac
  return 0
}
controlled_guard_assert_all_writers_fenced() {
  local ai_service_load="$2"
  local ai_timer_load="$3"
  local scheduler_load="$1"
  local trigger_unit
  controlled_guard_normalize_unit_fenced probiga disabled || return 1
  controlled_guard_assert_unit_inventory_fenced \
    probiga-scheduler disabled "$scheduler_load" || return 1
  controlled_guard_assert_unit_inventory_fenced \
    probiga-ai-recommendation-worker.service disabled:static \
    "$ai_service_load" || return 1
  controlled_guard_assert_unit_inventory_fenced \
    probiga-ai-recommendation-worker.timer disabled "$ai_timer_load" || return 1
  for trigger_unit in \
    probiga-scheduler.timer \
    probiga-scheduler.path \
    probiga-scheduler.socket; do
    case "$(systemctl show -p LoadState --value "$trigger_unit")" in
      not-found) ;;
      loaded) controlled_guard_normalize_unit_fenced \
        "$trigger_unit" disabled || return 1 ;;
      *) return 1 ;;
    esac
  done
  return 0
}
controlled_guard_assert_dropin_contract() {
  local ai_service_load="$2"
  local ai_timer_load="$3"
  local dropin
  local scheduler_load="$1"
  for dropin in "${DATABASE_WRITER_GUARD_DROPINS[@]}"; do
    controlled_guard_assert_dropin "$dropin" || return 1
  done
  controlled_guard_assert_unit_dropin_loaded \
    probiga "$MAIN_DATABASE_WRITER_GUARD_DROPIN" || return 1
  if [ "$scheduler_load" = loaded ]; then
    controlled_guard_assert_unit_dropin_loaded \
      probiga-scheduler "$SCHEDULER_DATABASE_WRITER_GUARD_DROPIN" || return 1
  fi
  if [ "$ai_service_load" = loaded ]; then
    controlled_guard_assert_unit_dropin_loaded \
      probiga-ai-recommendation-worker.service \
      "$AI_SERVICE_DATABASE_WRITER_GUARD_DROPIN" || return 1
  fi
  if [ "$ai_timer_load" = loaded ]; then
    controlled_guard_assert_unit_dropin_loaded \
      probiga-ai-recommendation-worker.timer \
      "$AI_TIMER_DATABASE_WRITER_GUARD_DROPIN" || return 1
  fi
  return 0
}
controlled_guard_assert_dropin_boundary() {
  local ai_service_load="$2"
  local ai_timer_load="$3"
  local scheduler_load="$1"
  controlled_guard_assert_dropin_contract "$scheduler_load" \
    "$ai_service_load" "$ai_timer_load" || return 1
  controlled_guard_assert_all_writers_fenced \
    "$scheduler_load" "$ai_service_load" "$ai_timer_load" || return 1
  return 0
}
controlled_guard_assert_boundary() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local guarded_sha="$1"
  local main_record="$2"
  local scheduler_record="$3"
  controlled_guard_assert_marker "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  local scheduler_load="${scheduler_record%%,*}"
  local ai_service_load="${ai_service_record%%,*}"
  local ai_timer_load="${ai_timer_record%%,*}"
  controlled_guard_assert_dropin_boundary "$scheduler_load" \
    "$ai_service_load" "$ai_timer_load" || return 1
  return 0
}
controlled_guard_install_dropins() {
  local dropin
  local parent
  local prepared_dropin
  prepared_dropin="$(mktemp)" || return 1
  if ! printf '%s\n' \
    '[Unit]' \
    "ConditionPathExists=!$DATABASE_WRITER_GUARD_FILE" \
    > "$prepared_dropin"; then
    rm -f -- "$prepared_dropin"
    return 1
  fi
  for dropin in "${DATABASE_WRITER_GUARD_DROPINS[@]}"; do
    parent="$(dirname "$dropin")"
    if [ -e "$parent" ] || [ -L "$parent" ]; then
      if ! test -d "$parent" || ! test ! -L "$parent" || \
        ! test "$(readlink -f "$parent")" = "$parent" || \
        ! test "$(stat -c '%U:%G' "$parent")" = root:root || \
        ! test "$(stat -c '%a' "$parent")" = 755; then
        rm -f -- "$prepared_dropin"
        return 1
      fi
    elif ! install -d -o root -g root -m 0755 "$parent"; then
      rm -f -- "$prepared_dropin"
      return 1
    fi
    if [ -e "$dropin" ] || [ -L "$dropin" ]; then
      if ! controlled_guard_assert_dropin "$dropin"; then
        rm -f -- "$prepared_dropin"
        return 1
      fi
    elif ! install -o root -g root -m 0644 "$prepared_dropin" "$dropin" || \
      ! controlled_guard_assert_dropin "$dropin"; then
      rm -f -- "$prepared_dropin"
      return 1
    fi
  done
  rm -f -- "$prepared_dropin" || return 1
  return 0
}
controlled_guard_recreate_file() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local guarded_sha="$1"
  local guard_tmp
  local main_record="$2"
  local scheduler_record="$3"
  if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ]; then
    controlled_guard_assert_marker "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
    return 0
  fi
  guard_tmp="$(mktemp \
    "$DATABASE_WRITER_GUARD_DIR/.database-migration-unverified.XXXXXX")" || \
    return 1
  if ! printf '%s\n' \
    probiga.database-writer-guard.v2 \
    "release=$guarded_sha" \
    "main_unit=$main_record" \
    "scheduler_unit=$scheduler_record" \
    "ai_service_unit=$ai_service_record" \
    "ai_timer_unit=$ai_timer_record" \
    > "$guard_tmp" || \
    ! chown root:root "$guard_tmp" || \
    ! chmod 0600 "$guard_tmp" || \
    ! mv -fT "$guard_tmp" "$DATABASE_WRITER_GUARD_FILE"; then
    rm -f -- "$guard_tmp"
    return 1
  fi
  sync -f "$DATABASE_WRITER_GUARD_FILE" || return 1
  sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
  controlled_guard_assert_marker "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  return 0
}
controlled_guard_restore_after_cleanup_failure() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local failed=0
  local guarded_sha="$1"
  local main_record="$2"
  local scheduler_record="$3"
  controlled_guard_recreate_file "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || failed=1
  controlled_guard_install_dropins || failed=1
  systemctl daemon-reload || failed=1
  controlled_guard_force_all_writers_fenced "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || failed=1
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || failed=1
  test "$failed" -eq 0 || return 1
  return 0
}
controlled_guard_cleanup() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local guarded_sha="$1"
  local main_record="$2"
  local scheduler_record="$3"
  local scheduler_load="${scheduler_record%%,*}"
  local ai_service_load="${ai_service_record%%,*}"
  local ai_timer_load="${ai_timer_record%%,*}"
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  if ! rm -f -- "$DATABASE_WRITER_GUARD_FILE" || \
    ! sync -f "$DATABASE_WRITER_GUARD_DIR"; then
    controlled_guard_restore_after_cleanup_failure \
      "$guarded_sha" "$main_record" "$scheduler_record" \
      "$ai_service_record" "$ai_timer_record" || true
    return 1
  fi
  if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ] || \
    ! controlled_guard_assert_dropin_boundary "$scheduler_load" \
      "$ai_service_load" "$ai_timer_load"; then
    controlled_guard_restore_after_cleanup_failure \
      "$guarded_sha" "$main_record" "$scheduler_record" \
      "$ai_service_record" "$ai_timer_record" || true
    return 1
  fi
  return 0
}
controlled_guard_apply_unit_state() {
  local active_state
  local expected_active
  local expected_load
  local expected_unit_file
  local record="$2"
  local unit="$1"
  IFS=, read -r expected_load expected_active expected_unit_file <<< "$record" || \
    return 1
  if [ "$expected_load" = not-found ]; then
    test "$(systemctl show -p LoadState --value "$unit")" = not-found || \
      return 1
    return 0
  fi
  test "$expected_load" = loaded || return 1
  test "$(systemctl show -p LoadState --value "$unit")" = loaded || return 1
  case "$expected_unit_file" in
    enabled) systemctl enable "$unit" || return 1 ;;
    disabled) systemctl disable "$unit" || return 1 ;;
    static)
      test "$(systemctl show -p UnitFileState --value "$unit")" = static || \
        return 1
      ;;
    *) return 1 ;;
  esac
  case "$expected_active" in
    active) systemctl start "$unit" || return 1 ;;
    inactive) systemctl stop "$unit" || return 1 ;;
    *) return 1 ;;
  esac
  test "$(systemctl show -p LoadState --value "$unit")" = loaded || return 1
  test "$(systemctl show -p UnitFileState --value "$unit")" = \
    "$expected_unit_file" || return 1
  active_state="$(systemctl show -p ActiveState --value "$unit")" || return 1
  test "$active_state" = "$expected_active" || return 1
  if [ "$expected_active" = inactive ]; then
    test "$(systemctl show -p MainPID --value "$unit")" = 0 || return 1
    test "$(systemctl show -p ExecMainPID --value "$unit")" = 0 || return 1
  fi
  return 0
}
controlled_guard_restore_previous_writer_states() {
  local ai_service_record="$3"
  local ai_timer_record="$4"
  local main_active
  local main_load
  local main_record="$1"
  local main_unit_file
  local scheduler_record="$2"
  controlled_guard_apply_unit_state probiga "$main_record" || return 1
  controlled_guard_apply_unit_state probiga-scheduler "$scheduler_record" || \
    return 1
  controlled_guard_apply_unit_state \
    probiga-ai-recommendation-worker.service "$ai_service_record" || return 1
  controlled_guard_apply_unit_state \
    probiga-ai-recommendation-worker.timer "$ai_timer_record" || return 1
  IFS=, read -r main_load main_active main_unit_file <<< "$main_record" || \
    return 1
  if [ "$main_active" = active ]; then
    curl --fail --silent --show-error --retry 15 --retry-all-errors \
      --retry-delay 2 --retry-connrefused \
      http://127.0.0.1/api/health >/dev/null || return 1
    curl --fail --silent --show-error --retry 15 --retry-all-errors \
      --retry-delay 2 --retry-connrefused \
      http://127.0.0.1/api/health/runtime >/dev/null || return 1
  fi
  return 0
}
controlled_guard_run_gate_with_deadline() {
  local deadline="$1"
  shift
  [[ "$deadline" =~ ^[1-9][0-9]*[smh]$ ]] || return 1
  [[ "$CONTROLLED_DATABASE_GATE_KILL_AFTER" =~ ^[1-9][0-9]*[smh]$ ]] || \
    return 1
  test "$#" -gt 0 || return 1
  test -x /usr/bin/timeout || return 1
  # GNU timeout's default process-group mode is intentional: after TERM and the
  # grace period, KILL must cover sudo and every Python/DB descendant.
  /usr/bin/timeout --signal=TERM \
    "--kill-after=$CONTROLLED_DATABASE_GATE_KILL_AFTER" "$deadline" "$@"
}
controlled_guard_verify_restored_runtime() {
  local main_record="$1"
  local scheduler_record="$2"
  local expected_sha="$3"
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local verification_mode="${6:-full}"
  local main_load main_active main_unit_file
  local scheduler_load scheduler_active scheduler_unit_file
  local ai_service_load ai_service_active ai_service_unit_file
  local ai_timer_load ai_timer_active ai_timer_unit_file
  local code_root="$CODE_RELEASE_ROOT/$expected_sha"
  local release_venv="$RELEASE_VENV_ROOT/$expected_sha"
  local python_path="$release_venv/bin/python"
  local adata_sha adata_tree_sha adata_source service_user pid
  local release_tree_sha adapter_registry_seal_sha
  local require_attested_identity=0
  local has_attested_identity=0
  local snapshot_release=""
  local -a cmdline=()
  local -a attested_env=()
  case "$verification_mode" in
    full|rollback-only) ;;
    *) return 1 ;;
  esac
  [[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  IFS=, read -r main_load main_active main_unit_file <<< "$main_record" || return 1
  IFS=, read -r scheduler_load scheduler_active scheduler_unit_file \
    <<< "$scheduler_record" || return 1
  IFS=, read -r ai_service_load ai_service_active ai_service_unit_file \
    <<< "$ai_service_record" || return 1
  IFS=, read -r ai_timer_load ai_timer_active ai_timer_unit_file \
    <<< "$ai_timer_record" || return 1
  controlled_guard_apply_unit_state probiga "$main_record" || return 1
  controlled_guard_apply_unit_state probiga-scheduler "$scheduler_record" || return 1
  controlled_guard_apply_unit_state probiga-ai-recommendation-worker.service \
    "$ai_service_record" || return 1
  controlled_guard_apply_unit_state probiga-ai-recommendation-worker.timer \
    "$ai_timer_record" || return 1
  test -d "$code_root" || return 1
  test ! -L "$code_root" || return 1
  test "$(git -C "$code_root" rev-parse HEAD)" = "$expected_sha" || return 1
  test -L "$release_venv" || return 1
  test -x "$python_path" || return 1
  adata_sha="$(<"$release_venv/.adata.gitsha")" || return 1
  adata_tree_sha="$(<"$release_venv/.adata.tree.sha256")" || return 1
  [[ "$adata_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$adata_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  if [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] && \
    [ ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" ]; then
    snapshot_release="$(activation_snapshot_recorded_release)" || return 1
    if [ "$snapshot_release" = "$expected_sha" ]; then
      require_attested_identity=1
    fi
  fi
  if [ -e "$release_venv/.release-tree.sha256" ] || \
    [ -e "$release_venv/.adapter-registry-seal.sha256" ]; then
    require_attested_identity=1
  fi
  if [ "$require_attested_identity" -eq 1 ]; then
    test -f "$release_venv/.release-tree.sha256" || return 1
    test -f "$release_venv/.adapter-registry-seal.sha256" || return 1
    release_tree_sha="$(<"$release_venv/.release-tree.sha256")" || return 1
    adapter_registry_seal_sha="$(
      <"$release_venv/.adapter-registry-seal.sha256"
    )" || return 1
    [[ "$release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    [[ "$adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    has_attested_identity=1
    attested_env+=(
      "PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha"
      "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha"
    )
  fi
  adata_source="$ADATA_RUNTIME_ROOT/$adata_sha-$adata_tree_sha"
  test -d "$adata_source" || return 1
  test ! -L "$adata_source" || return 1
  test "$(<"$adata_source/.probiga-adata.gitsha")" = "$adata_sha" || return 1
  test "$(<"$adata_source/.probiga-adata.tree.sha256")" = \
    "$adata_tree_sha" || return 1
  service_user="$(systemctl show -p User --value probiga)" || return 1
  test -n "$service_user" || return 1
  test "$service_user" != root || return 1
  if [ "$main_active" = active ]; then
    pid="$(systemctl show -p MainPID --value probiga)" || return 1
    case "$pid" in ''|0|*[!0-9]*) return 1 ;; esac
    grep -zFx -- "PROBIGA_EXPECTED_GIT_SHA=$expected_sha" "/proc/$pid/environ" \
      >/dev/null || return 1
    grep -zFx -- "PROBIGA_CODE_ROOT=$code_root" "/proc/$pid/environ" \
      >/dev/null || return 1
    grep -zFx -- "PROBIGA_EXPECTED_ADATA_SHA=$adata_sha" "/proc/$pid/environ" \
      >/dev/null || return 1
    grep -zFx -- "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha" \
      "/proc/$pid/environ" >/dev/null || return 1
    if [ "$has_attested_identity" -eq 1 ]; then
      grep -zFx -- "PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha" \
        "/proc/$pid/environ" >/dev/null || return 1
      grep -zFx -- \
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha" \
        "/proc/$pid/environ" >/dev/null || return 1
    fi
    mapfile -d '' -t cmdline < "/proc/$pid/cmdline" || return 1
    test "${#cmdline[@]}" -ge 7 || return 1
    test "${cmdline[0]}" = "$python_path" || return 1
    test "${cmdline[1]}" = -P || return 1
    test "${cmdline[2]}" = -m || return 1
    test "${cmdline[3]}" = uvicorn || return 1
    test "${cmdline[4]}" = server.api.main:app || return 1
    test "${cmdline[5]}" = --app-dir || return 1
    test "${cmdline[6]}" = "$code_root" || return 1
    curl --fail --silent --show-error --retry 15 --retry-all-errors \
      --retry-delay 2 --retry-connrefused \
      http://127.0.0.1/api/health >/dev/null || return 1
    curl --fail --silent --show-error --retry 15 --retry-all-errors \
      --retry-delay 2 --retry-connrefused \
      http://127.0.0.1/api/health/runtime >/dev/null || return 1
  else
    test "$main_active" = inactive || return 1
  fi
  if [ "$scheduler_load" = loaded ] && [ "$scheduler_active" = active ]; then
    pid="$(systemctl show -p MainPID --value probiga-scheduler)" || return 1
    case "$pid" in ''|0|*[!0-9]*) return 1 ;; esac
    grep -zFx -- "PROBIGA_EXPECTED_GIT_SHA=$expected_sha" "/proc/$pid/environ" \
      >/dev/null || return 1
    grep -zFx -- "PROBIGA_CODE_ROOT=$code_root" "/proc/$pid/environ" \
      >/dev/null || return 1
    grep -zFx -- "PROBIGA_EXPECTED_ADATA_SHA=$adata_sha" "/proc/$pid/environ" \
      >/dev/null || return 1
    grep -zFx -- "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha" \
      "/proc/$pid/environ" >/dev/null || return 1
    if [ "$has_attested_identity" -eq 1 ]; then
      grep -zFx -- "PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha" \
        "/proc/$pid/environ" >/dev/null || return 1
      grep -zFx -- \
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha" \
        "/proc/$pid/environ" >/dev/null || return 1
    fi
    mapfile -d '' -t cmdline < "/proc/$pid/cmdline" || return 1
    test "${#cmdline[@]}" -ge 3 || return 1
    test "${cmdline[0]}" = "$python_path" || return 1
    test "${cmdline[1]}" = -P || return 1
    test "${cmdline[2]}" = "$code_root/tools/run_scheduler_daemon.py" || return 1
  elif [ "$scheduler_load" = loaded ]; then
    test "$scheduler_active" = inactive || return 1
  else
    test "$scheduler_load:$scheduler_active:$scheduler_unit_file" = \
      not-found:not-found:not-found || return 1
  fi
  if [ "$ai_service_load" = loaded ]; then
    systemctl show -p ExecStart --value probiga-ai-recommendation-worker.service \
      | grep -F -- "$python_path -P $code_root/tools/run_ai_recommendation_worker.py --once" \
        >/dev/null || return 1
    if [ "$ai_service_active" = active ]; then
      pid="$(systemctl show -p MainPID --value \
        probiga-ai-recommendation-worker.service)" || return 1
      case "$pid" in ''|0|*[!0-9]*) return 1 ;; esac
      grep -zFx -- "PROBIGA_EXPECTED_GIT_SHA=$expected_sha" "/proc/$pid/environ" \
        >/dev/null || return 1
      grep -zFx -- "PROBIGA_CODE_ROOT=$code_root" "/proc/$pid/environ" \
        >/dev/null || return 1
      grep -zFx -- "PROBIGA_EXPECTED_ADATA_SHA=$adata_sha" \
        "/proc/$pid/environ" >/dev/null || return 1
      grep -zFx -- "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha" \
        "/proc/$pid/environ" >/dev/null || return 1
      if [ "$has_attested_identity" -eq 1 ]; then
        grep -zFx -- "PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha" \
          "/proc/$pid/environ" >/dev/null || return 1
        grep -zFx -- \
          "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha" \
          "/proc/$pid/environ" >/dev/null || return 1
      fi
      mapfile -d '' -t cmdline < "/proc/$pid/cmdline" || return 1
      test "${cmdline[0]}" = "$python_path" || return 1
      test "${cmdline[1]}" = -P || return 1
      test "${cmdline[2]}" = \
        "$code_root/tools/run_ai_recommendation_worker.py" || return 1
      test "${cmdline[3]}" = --once || return 1
    else
      test "$ai_service_active" = inactive || return 1
    fi
  else
    test "$ai_service_load:$ai_service_active:$ai_service_unit_file" = \
      not-found:not-found:not-found || return 1
  fi
  case "$ai_timer_load:$ai_timer_active" in
    loaded:active|loaded:inactive|not-found:not-found) ;;
    *) return 1 ;;
  esac
  for activation_unit in probiga-scheduler.timer probiga-scheduler.path \
    probiga-scheduler.socket; do
    case "$(systemctl show -p LoadState --value "$activation_unit")" in
      not-found) ;;
      loaded)
        test "$(systemctl show -p ActiveState --value "$activation_unit")" = \
          inactive || return 1
        test "$(systemctl show -p UnitFileState --value "$activation_unit")" = \
          disabled || return 1
        ;;
      *) return 1 ;;
    esac
  done
  if [ "$verification_mode" = rollback-only ]; then
    # The rollback-only path deliberately proves the sealed old runtime and
    # its two HTTP health boundaries without re-running the failed forward
    # schema/governance validator.  Normal deploy and privileged recovery keep
    # the default full verification below.
    return 0
  fi
  controlled_guard_run_gate_with_deadline \
    "$CONTROLLED_DATABASE_GATE_TIMEOUT" \
    /usr/bin/sudo -u "$service_user" /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
    PROBIGA_DEPLOYMENT_MODE=production \
    PROBIGA_EXPECTED_GIT_SHA="$expected_sha" \
    PROBIGA_BUILD_COMMIT_SHA="$expected_sha" \
    PROBIGA_CODE_ROOT="$code_root" \
    PROBIGA_EXPECTED_ADATA_SHA="$adata_sha" \
    PROBIGA_EXPECTED_ADATA_TREE_SHA256="$adata_tree_sha" \
    PROBIGA_ADATA_SOURCE_DIR="$adata_source" \
    "${attested_env[@]}" \
    "PYTHONPATH=$adata_source:$code_root" "$python_path" -P \
    "$code_root/tools/check_strategy_governance_health.py" --compact \
    --expected-build-sha "$expected_sha" || return 1
  controlled_guard_run_gate_with_deadline \
    "$CONTROLLED_DATABASE_GATE_TIMEOUT" \
    /usr/bin/sudo -u "$service_user" /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
    PROBIGA_DEPLOYMENT_MODE=production \
    PROBIGA_EXPECTED_GIT_SHA="$expected_sha" \
    PROBIGA_BUILD_COMMIT_SHA="$expected_sha" \
    PROBIGA_CODE_ROOT="$code_root" \
    PROBIGA_EXPECTED_ADATA_SHA="$adata_sha" \
    PROBIGA_EXPECTED_ADATA_TREE_SHA256="$adata_tree_sha" \
    PROBIGA_ADATA_SOURCE_DIR="$adata_source" \
    "${attested_env[@]}" \
    "PYTHONPATH=$adata_source:$code_root" "$python_path" -P \
    "$code_root/tools/ensure_quality_gate.py" \
    --task-type analysis_premarket_external || return 1
  return 0
}
controlled_guard_force_unit_fenced() {
  local actual_load
  local allowed_unit_file_states="$3"
  local expected_load="$2"
  local failed=0
  local unit="$1"
  local unit_file_state
  actual_load="$(systemctl show -p LoadState --value "$unit")" || return 1
  case "$actual_load" in
    not-found)
      test "$expected_load" = not-found || return 1
      return 0
      ;;
    loaded) ;;
    *) return 1 ;;
  esac
  unit_file_state="$(systemctl show -p UnitFileState --value "$unit")" || \
    failed=1
  if [ "$unit_file_state" != static ]; then
    systemctl disable "$unit" || failed=1
  fi
  systemctl stop "$unit" || failed=1
  if [ "$(systemctl show -p ActiveState --value "$unit")" = failed ]; then
    systemctl reset-failed "$unit" || failed=1
  fi
  controlled_guard_assert_unit_fenced "$unit" \
    "$allowed_unit_file_states" || failed=1
  test "$expected_load" = loaded || failed=1
  test "$failed" -eq 0 || return 1
  return 0
}
controlled_guard_force_all_writers_fenced() {
  local ai_service_load="${3%%,*}"
  local ai_timer_load="${4%%,*}"
  local failed=0
  local scheduler_load="${2%%,*}"
  local trigger_unit
  controlled_guard_force_unit_fenced probiga loaded disabled || failed=1
  controlled_guard_force_unit_fenced probiga-scheduler \
    "$scheduler_load" disabled || failed=1
  controlled_guard_force_unit_fenced \
    probiga-ai-recommendation-worker.service "$ai_service_load" \
    disabled:static || failed=1
  controlled_guard_force_unit_fenced \
    probiga-ai-recommendation-worker.timer "$ai_timer_load" disabled || \
    failed=1
  for trigger_unit in \
    probiga-scheduler.timer \
    probiga-scheduler.path \
    probiga-scheduler.socket; do
    case "$(systemctl show -p LoadState --value "$trigger_unit")" in
      not-found) ;;
      loaded)
        controlled_guard_force_unit_fenced \
          "$trigger_unit" loaded disabled || failed=1
        ;;
      *) failed=1 ;;
    esac
  done
  test "$failed" -eq 0 || return 1
  return 0
}
controlled_guard_refence_after_restore_failure() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local failed=0
  local guarded_sha="$1"
  local main_record="$2"
  local scheduler_record="$3"
  controlled_guard_recreate_file "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || failed=1
  controlled_guard_install_dropins || failed=1
  systemctl daemon-reload || failed=1
  controlled_guard_force_all_writers_fenced "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || failed=1
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || failed=1
  test "$failed" -eq 0 || return 1
  return 0
}
controlled_guard_governance_snapshot() {
  local action="$1"
  local guarded_sha="$2"
  local snapshot="$3"
  local code_root="$CODE_RELEASE_ROOT/$guarded_sha"
  local release_venv="$RELEASE_VENV_ROOT/$guarded_sha"
  local service_user adata_sha adata_tree_sha adata_source
  local release_tree_sha adapter_registry_seal_sha
  case "$snapshot" in
    "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT")
      controlled_guard_assert_file "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" 600 || \
        return 1
      test "$(<"$ACTIVATION_GOVERNANCE_OLD_SHA")" = \
        "$(sha256sum "$snapshot" | cut -d' ' -f1)" || return 1
      ;;
    "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT")
      activation_snapshot_validate_governance_new || return 1
      ;;
    *) return 1 ;;
  esac
  case "$action" in restore|verify) ;; *) return 1 ;; esac
  test -d "$code_root" || return 1
  test ! -L "$code_root" || return 1
  test -L "$release_venv" || return 1
  test -x "$release_venv/bin/python" || return 1
  adata_sha="$(<"$release_venv/.adata.gitsha")" || return 1
  adata_tree_sha="$(<"$release_venv/.adata.tree.sha256")" || return 1
  release_tree_sha="$(<"$release_venv/.release-tree.sha256")" || return 1
  adapter_registry_seal_sha="$(
    <"$release_venv/.adapter-registry-seal.sha256"
  )" || return 1
  [[ "$adata_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$adata_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ "$release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ "$adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  adata_source="$ADATA_RUNTIME_ROOT/$adata_sha-$adata_tree_sha"
  test -d "$adata_source" || return 1
  test ! -L "$adata_source" || return 1
  service_user="$(systemctl show -p User --value probiga)" || return 1
  test -n "$service_user" || return 1
  test "$service_user" != root || return 1
  sudo -u "$service_user" /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
    PROBIGA_DEPLOYMENT_MODE=production \
    PROBIGA_EXPECTED_GIT_SHA="$guarded_sha" \
    PROBIGA_BUILD_COMMIT_SHA="$guarded_sha" \
    PROBIGA_CODE_ROOT="$code_root" \
    PROBIGA_EXPECTED_ADATA_SHA="$adata_sha" \
    PROBIGA_EXPECTED_ADATA_TREE_SHA256="$adata_tree_sha" \
    PROBIGA_ADATA_SOURCE_DIR="$adata_source" \
    PROBIGA_RELEASE_TREE_SHA256="$release_tree_sha" \
    PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$adapter_registry_seal_sha" \
    PYTHONPATH="$adata_source:$code_root" \
    "$release_venv/bin/python" -P \
    "$code_root/tools/add_strategy_governance_task.py" \
    "--${action}-snapshot" - < "$snapshot" || return 1
  return 0
}
controlled_guard_restore_and_verify_governance_snapshot() {
  # Most rollback attempts fail before the scheduler row is changed.  Prove
  # that case first and avoid unnecessary database writes.  A mismatch falls
  # through to the exact restore, whose own failure remains visible.
  if controlled_guard_governance_snapshot verify "$1" "$2" \
      >/dev/null 2>&1; then
    return 0
  fi
  controlled_guard_governance_snapshot restore "$1" "$2" || return 1
  controlled_guard_governance_snapshot verify "$1" "$2" || return 1
  return 0
}
controlled_guard_assert_immutable_venv_tree() {
  local bootstrap_entry=/usr/bin/python3.14
  local expected_owner=root
  local expected_owner_group=root:root
  local tree_root="$1"
  local trusted_bootstrap_python
  local unsafe_path
  test -d "$tree_root" || return 1
  test ! -L "$tree_root" || return 1
  test -x "$bootstrap_entry" || return 1
  test "$(stat -c '%U:%G' "$bootstrap_entry")" = \
    "$expected_owner_group" || return 1
  trusted_bootstrap_python="$(readlink -f -- "$bootstrap_entry")" || return 1
  test -n "$trusted_bootstrap_python" || return 1
  unsafe_path="$(find -P "$tree_root" -xdev ! -user "$expected_owner" \
    -print -quit)" || return 1
  test -z "$unsafe_path" || return 1
  # Symlink permission bits are conventionally 0777 and do not make their
  # targets writable.  Check write bits only on concrete nodes, then validate
  # every root-owned link and its resolved target separately.
  unsafe_path="$(find -P "$tree_root" -xdev ! -type l -perm /022 \
    -print -quit)" || return 1
  test -z "$unsafe_path" || return 1
  VENV_TREE_ROOT="$tree_root" find -P "$tree_root" -xdev -type l \
    -exec /usr/bin/env -i \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      VENV_BOOTSTRAP_ENTRY="$bootstrap_entry" \
      VENV_EXPECTED_OWNER_GROUP="$expected_owner_group" \
      VENV_TREE_ROOT="$tree_root" \
      VENV_TRUSTED_BOOTSTRAP_PYTHON="$trusted_bootstrap_python" \
      /usr/bin/bash --noprofile --norc -c '
        set -u
        for link_path in "$@"; do
          test -L "$link_path" || exit 1
          test "$(stat -c "%U:%G" "$link_path")" = \
            "$VENV_EXPECTED_OWNER_GROUP" || exit 1
          raw_target="$(readlink -- "$link_path")" || exit 1
          test -n "$raw_target" || exit 1
          case "$raw_target" in
            /*)
              case "$raw_target" in
                "$VENV_BOOTSTRAP_ENTRY"|"$VENV_TRUSTED_BOOTSTRAP_PYTHON") ;;
                *) exit 1 ;;
              esac
              ;;
            *)
              lexical_target="$(/usr/bin/realpath -ms -- \
                "$(dirname -- "$link_path")/$raw_target")" || exit 1
              case "$lexical_target" in
                "$VENV_TREE_ROOT"|"$VENV_TREE_ROOT"/*) ;;
                *) exit 1 ;;
              esac
              ;;
          esac
          resolved="$(readlink -f -- "$link_path")" || exit 1
          test -e "$resolved" || exit 1
          case "$resolved" in
            "$VENV_TREE_ROOT"|"$VENV_TREE_ROOT"/*) ;;
            "$VENV_TRUSTED_BOOTSTRAP_PYTHON")
              trusted_path="$resolved"
              while :; do
                test ! -L "$trusted_path" || exit 1
                test "$(stat -c "%U:%G" "$trusted_path")" = \
                  "$VENV_EXPECTED_OWNER_GROUP" || exit 1
                trusted_mode="$(stat -c "%a" "$trusted_path")" || exit 1
                test $((8#$trusted_mode & 8#022)) -eq 0 || exit 1
                test "$trusted_path" != / || break
                trusted_path="$(dirname "$trusted_path")" || exit 1
              done
              ;;
            *) exit 1 ;;
          esac
        done
      ' _ {} + || return 1
  return 0
}
controlled_guard_assert_governance_restore_runtime() {
  local guarded_sha="$1"
  local code_root="$CODE_RELEASE_ROOT/$guarded_sha"
  local release_venv="$RELEASE_VENV_ROOT/$guarded_sha"
  local release_venv_target
  local service_user
  local adata_sha
  local adata_tree_sha
  local adata_source
  local release_tree_sha
  local adapter_registry_seal_sha
  local -a release_identity_lines=()
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  activation_snapshot_validate "$guarded_sha" >/dev/null || return 1
  mapfile -t release_identity_lines < "$ACTIVATION_RELEASE_IDENTITY" || return 1
  test "${#release_identity_lines[@]}" -eq 5 || return 1
  release_tree_sha="${release_identity_lines[3]#release_tree_sha256=}" || return 1
  adapter_registry_seal_sha="${release_identity_lines[4]#adapter_registry_seal_sha256=}" || \
    return 1
  test "${release_identity_lines[3]}" = \
    "release_tree_sha256=$release_tree_sha" || return 1
  test "${release_identity_lines[4]}" = \
    "adapter_registry_seal_sha256=$adapter_registry_seal_sha" || return 1
  [[ "$release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ "$adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  test -d "$code_root" || return 1
  test ! -L "$code_root" || return 1
  test "$(readlink -f "$code_root")" = "$code_root" || return 1
  test "$(stat -c '%U:%G' "$code_root")" = root:root || return 1
  test -z "$(find -P "$code_root" -xdev \
    \( ! -user root -o -perm /022 \) -print -quit)" || return 1
  test "$(git -C "$code_root" rev-parse HEAD)" = "$guarded_sha" || return 1
  test -z "$(git -C "$code_root" \
    status --porcelain=v1 --untracked-files=all)" || return 1
  controlled_guard_assert_file \
    "$code_root/tools/add_strategy_governance_task.py" 444 || return 1
  test -L "$release_venv" || return 1
  test -x "$release_venv/bin/python" || return 1
  test "$(<"$release_venv/.probiga.gitsha")" = "$guarded_sha" || return 1
  test "$(<"$release_venv/.release-tree.sha256")" = \
    "$release_tree_sha" || return 1
  test "$(<"$release_venv/.adapter-registry-seal.sha256")" = \
    "$adapter_registry_seal_sha" || return 1
  release_venv_target="$(readlink -f "$release_venv")" || return 1
  case "$release_venv_target" in
    "$RELEASE_VENV_ROOT"/build-*) ;;
    *) return 1 ;;
  esac
  test "$(dirname "$release_venv_target")" = "$RELEASE_VENV_ROOT" || return 1
  test "$(stat -c '%U' "$release_venv_target")" = root || return 1
  controlled_guard_assert_immutable_venv_tree "$release_venv_target" || return 1
  adata_sha="$(<"$release_venv/.adata.gitsha")" || return 1
  adata_tree_sha="$(<"$release_venv/.adata.tree.sha256")" || return 1
  [[ "$adata_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$adata_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  adata_source="$ADATA_RUNTIME_ROOT/$adata_sha-$adata_tree_sha"
  test -d "$adata_source" || return 1
  test ! -L "$adata_source" || return 1
  test "$(readlink -f "$adata_source")" = "$adata_source" || return 1
  test "$(stat -c '%U:%G' "$adata_source")" = root:root || return 1
  test "$(<"$adata_source/.probiga-adata.gitsha")" = "$adata_sha" || return 1
  test "$(<"$adata_source/.probiga-adata.tree.sha256")" = \
    "$adata_tree_sha" || return 1
  test -z "$(find -P "$adata_source" -xdev \
    \( ! -user root -o -perm /022 \) -print -quit)" || return 1
  service_user="$(systemctl show -p User --value probiga)" || return 1
  test -n "$service_user" || return 1
  test "$service_user" != root || return 1
  sudo -u "$service_user" test ! -w "$code_root" || return 1
  sudo -u "$service_user" test ! -w "$release_venv_target" || return 1
  sudo -u "$service_user" test ! -w "$adata_source" || return 1
  return 0
}
controlled_guard_capture_current_governance_snapshot() {
  local guarded_sha="$1"
  local old_runtime_sha="$2"
  local code_root="$CODE_RELEASE_ROOT/$guarded_sha"
  local release_venv="$RELEASE_VENV_ROOT/$old_runtime_sha"
  local release_venv_target
  local service_user
  local adata_sha
  local adata_tree_sha
  local adata_source
  local release_tree_sha
  local adapter_registry_seal_sha
  local runtime_release_tree_sha
  local runtime_adapter_registry_seal_sha
  local capture_root=/tmp
  local current_snapshot
  local capture_valid=1
  local -a release_identity_lines=()
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$old_runtime_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  test "$old_runtime_sha" != "$guarded_sha" || return 1
  activation_snapshot_validate "$guarded_sha" >/dev/null || return 1
  test "$(activation_snapshot_old_release "$guarded_sha")" = \
    "$old_runtime_sha" || return 1
  mapfile -t release_identity_lines < "$ACTIVATION_RELEASE_IDENTITY" || return 1
  test "${#release_identity_lines[@]}" -eq 5 || return 1
  case "${release_identity_lines[3]}" in
    release_tree_sha256=*)
      release_tree_sha="${release_identity_lines[3]#release_tree_sha256=}"
      ;;
    *) return 1 ;;
  esac
  case "${release_identity_lines[4]}" in
    adapter_registry_seal_sha256=*)
      adapter_registry_seal_sha="${release_identity_lines[4]#adapter_registry_seal_sha256=}"
      ;;
    *) return 1 ;;
  esac
  [[ "$release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ "$adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  controlled_guard_assert_file "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" 600 || \
    return 1
  controlled_guard_assert_file "$ACTIVATION_GOVERNANCE_OLD_SHA" 600 || \
    return 1
  test "$(<"$ACTIVATION_GOVERNANCE_OLD_SHA")" = \
    "$(sha256sum "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" | cut -d' ' -f1)" || \
    return 1
  test -d "$code_root" || return 1
  test ! -L "$code_root" || return 1
  test "$(readlink -f "$code_root")" = "$code_root" || return 1
  test "$(stat -c '%U:%G' "$code_root")" = root:root || return 1
  test -z "$(find -P "$code_root" -xdev \
    \( ! -user root -o -perm /022 \) -print -quit)" || return 1
  test "$(git -C "$code_root" rev-parse HEAD)" = "$guarded_sha" || return 1
  test -z "$(git -C "$code_root" \
    status --porcelain=v1 --untracked-files=all)" || return 1
  test -L "$release_venv" || return 1
  test -x "$release_venv/bin/python" || return 1
  test "$(<"$release_venv/.probiga.gitsha")" = "$old_runtime_sha" || return 1
  adata_sha="$(<"$release_venv/.adata.gitsha")" || return 1
  adata_tree_sha="$(<"$release_venv/.adata.tree.sha256")" || return 1
  [[ "$adata_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$adata_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  # The sealed old runtime may predate the paired tree/adapter attestations.
  # If either marker exists, both must be regular, non-symlink, valid markers;
  # a legacy runtime is accepted only when both are completely absent.
  if [ -e "$release_venv/.release-tree.sha256" ] || \
    [ -L "$release_venv/.release-tree.sha256" ] || \
    [ -e "$release_venv/.adapter-registry-seal.sha256" ] || \
    [ -L "$release_venv/.adapter-registry-seal.sha256" ]; then
    test -f "$release_venv/.release-tree.sha256" || return 1
    test ! -L "$release_venv/.release-tree.sha256" || return 1
    test -f "$release_venv/.adapter-registry-seal.sha256" || return 1
    test ! -L "$release_venv/.adapter-registry-seal.sha256" || return 1
    runtime_release_tree_sha="$(
      <"$release_venv/.release-tree.sha256"
    )" || return 1
    runtime_adapter_registry_seal_sha="$(
      <"$release_venv/.adapter-registry-seal.sha256"
    )" || return 1
    [[ "$runtime_release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    [[ "$runtime_adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  fi
  adata_source="$ADATA_RUNTIME_ROOT/$adata_sha-$adata_tree_sha"
  test -d "$adata_source" || return 1
  test ! -L "$adata_source" || return 1
  service_user="$(systemctl show -p User --value probiga)" || return 1
  test -n "$service_user" || return 1
  test "$service_user" != root || return 1
  sudo -u "$service_user" test ! -w "$code_root" || return 1
  release_venv_target="$(readlink -f "$release_venv")" || return 1
  case "$release_venv_target" in
    "$RELEASE_VENV_ROOT"/build-*) ;;
    *) return 1 ;;
  esac
  test "$(dirname "$release_venv_target")" = "$RELEASE_VENV_ROOT" || return 1
  test "$(stat -c '%U' "$release_venv_target")" = root || return 1
  controlled_guard_assert_immutable_venv_tree "$release_venv_target" || return 1
  sudo -u "$service_user" test ! -w "$release_venv_target" || return 1
  test "$(readlink -f "$adata_source")" = "$adata_source" || return 1
  test "$(stat -c '%U' "$adata_source")" = root || return 1
  test "$(<"$adata_source/.probiga-adata.gitsha")" = "$adata_sha" || return 1
  test "$(<"$adata_source/.probiga-adata.tree.sha256")" = \
    "$adata_tree_sha" || return 1
  test -z "$(find -P "$adata_source" -xdev \
    \( ! -user root -o -perm /022 \) -print -quit)" || return 1
  sudo -u "$service_user" test ! -w "$adata_source" || return 1
  test -d "$capture_root" || return 1
  test ! -L "$capture_root" || return 1
  test "$(readlink -f "$capture_root")" = "$capture_root" || return 1
  test "$(stat -c '%U:%G' "$capture_root")" = root:root || return 1
  test "$(stat -c '%a' "$capture_root")" = 1777 || return 1
  current_snapshot="$(mktemp \
    "$capture_root/.probiga-governance-capture.XXXXXX")" || return 1
  case "$current_snapshot" in
    "$capture_root"/.probiga-governance-capture.*) ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_file "$current_snapshot" 600 || return 1
  test "$(dirname "$current_snapshot")" = "$capture_root" || return 1
  if chown "$service_user:$service_user" "$current_snapshot" && \
    chmod 0600 "$current_snapshot" && \
    sudo -u "$service_user" /usr/bin/env -i \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
      PROBIGA_DEPLOYMENT_MODE=production \
      PROBIGA_EXPECTED_GIT_SHA="$guarded_sha" \
      PROBIGA_BUILD_COMMIT_SHA="$guarded_sha" \
      PROBIGA_CODE_ROOT="$code_root" \
      PROBIGA_EXPECTED_ADATA_SHA="$adata_sha" \
      PROBIGA_EXPECTED_ADATA_TREE_SHA256="$adata_tree_sha" \
      PROBIGA_ADATA_SOURCE_DIR="$adata_source" \
      PROBIGA_RELEASE_TREE_SHA256="$release_tree_sha" \
      PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$adapter_registry_seal_sha" \
      PYTHONPATH="$adata_source:$code_root" \
      "$release_venv/bin/python" -P \
      "$code_root/tools/add_strategy_governance_task.py" \
      --capture-snapshot "$current_snapshot" && \
    test -f "$current_snapshot" && test ! -L "$current_snapshot" && \
    test "$(stat -c '%U:%G' "$current_snapshot")" = \
      "$service_user:$service_user" && \
    test "$(stat -c '%a' "$current_snapshot")" = 600 && \
    chown root:root "$current_snapshot" && chmod 0600 "$current_snapshot" && \
    controlled_guard_assert_file "$current_snapshot" 600 && \
    cmp --silent "$current_snapshot" \
      "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"; then
    capture_valid=0
  fi
  if ! rm -f -- "$current_snapshot" || [ -e "$current_snapshot" ] || \
    [ -L "$current_snapshot" ]; then
    return 1
  fi
  test "$capture_valid" -eq 0 || return 1
  return 0
}
controlled_guard_restore_and_finalize() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local guarded_sha="$1"
  local governance_runtime="${6:-controlled}"
  local main_record="$2"
  local old_runtime_sha="$guarded_sha"
  local scheduler_record="$3"
  case "$governance_runtime" in
    controlled) ;;
    prepared)
      test "$DEPLOY_OPERATION" = deploy || return 1
      test "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 || return 1
      test "$guarded_sha" = "$EXPECTED_SHA" || return 1
      ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  if [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] || \
    [ -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" ]; then
    old_runtime_sha="$(activation_snapshot_old_release "$guarded_sha")" || \
      return 1
    activation_snapshot_restore_old_set "$guarded_sha" || return 1
    systemctl daemon-reload || return 1
    activation_snapshot_assert_old_set "$guarded_sha" || return 1
    case "$governance_runtime" in
      controlled)
        controlled_guard_restore_and_verify_governance_snapshot "$guarded_sha" \
          "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" || return 1
        ;;
      prepared)
        # The same-process rollback restored and cross-runtime verified this
        # state before the guard was removed.  Once old writers are running,
        # any drift must re-fence rather than trigger another database write.
        prepared_governance_snapshot verify \
          "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" || return 1
        ;;
    esac
  fi
  if ! controlled_guard_restore_previous_writer_states "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
    ! controlled_guard_verify_restored_runtime "$main_record" \
      "$scheduler_record" "$old_runtime_sha" "$ai_service_record" \
      "$ai_timer_record"; then
    controlled_guard_refence_after_restore_failure \
      "$guarded_sha" "$main_record" "$scheduler_record" \
      "$ai_service_record" "$ai_timer_record" || true
    return 1
  fi
  if [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] || \
    [ -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" ]; then
    activation_snapshot_set_phase "$guarded_sha" old-runtime-verified || \
      return 1
  fi
  if ! rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || \
    ! sync -f "$DATABASE_WRITER_GUARD_DIR" || \
    [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    # old-runtime-verified is the durable rollback commit.  A cleanup-only
    # failure must leave the already verified old writers online so the next
    # invocation can revalidate and finish the journal without another outage.
    return 1
  fi
  return 0
}
activation_snapshot_allows_missing_guard_for_recovery() {
  case "$1" in
    prepared|runtime-units-installed|restoring-old|old-set-restored|\
    old-runtime-verified) return 0 ;;
    *) return 1 ;;
  esac
}
controlled_v2_forward_preserve_no_receipt_recovery() {
  local guarded_sha
  local old_runtime_sha
  local phase
  local main_record
  local scheduler_record
  local forward_main_record
  local forward_scheduler_record=loaded,active,enabled
  local fenced_main_record=loaded,inactive,disabled
  local fenced_scheduler_record=loaded,inactive,disabled
  local fenced_ai_service_record
  local fenced_ai_timer_record
  local ai_service_load ai_service_active ai_service_unit_file
  local ai_timer_load ai_timer_active ai_timer_unit_file
  local ai_service_record
  local ai_timer_record
  local main_load main_active main_unit_file
  local scheduler_load scheduler_active scheduler_unit_file
  local fence_status=0
  local -a state_lines=()
  V2_RECOVERY_STEP=forward-validate-request
  case "$DEPLOY_OPERATION:$DEPLOY_ARTIFACT_MODE" in
    deploy:ci-resolved-freeze-v1)
      [[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || return 1
      ;;
    recover-database-guard:static-wheel-lock-v2) ;;
    *) return 1 ;;
  esac
  V2_RECOVERY_STEP=forward-validate-transaction
  guarded_sha="$(activation_snapshot_recorded_release)" || return 1
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  old_runtime_sha="$(activation_snapshot_old_release "$guarded_sha")" || \
    return 1
  [[ "$old_runtime_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  test "$old_runtime_sha" != "$guarded_sha" || return 1
  phase="$(activation_snapshot_phase)" || return 1
  case "$phase" in
    runtime-units-installed|restoring-old|restoring-new-no-receipt|\
    new-runtime-preserved-no-receipt) ;;
    *) return 1 ;;
  esac
  activation_snapshot_validate_new "$guarded_sha" || return 1
  activation_snapshot_validate_governance_new || return 1
  activation_snapshot_assert_pending_receipt_absent || return 1
  controlled_guard_assert_governance_restore_runtime "$guarded_sha" || return 1
  controlled_guard_assert_directory || return 1
  controlled_guard_assert_file "$ACTIVATION_UNIT_SNAPSHOT_STATE" 600 || \
    return 1
  mapfile -t state_lines < "$ACTIVATION_UNIT_SNAPSHOT_STATE" || return 1
  test "${#state_lines[@]}" -eq 6 || return 1
  test "${state_lines[0]}" = probiga.database-writer-restore.v1 || return 1
  test "${state_lines[1]}" = "release=$guarded_sha" || return 1
  case "${state_lines[2]}" in
    main_unit=*) main_record="${state_lines[2]#main_unit=}" ;;
    *) return 1 ;;
  esac
  case "${state_lines[3]}" in
    scheduler_unit=*) scheduler_record="${state_lines[3]#scheduler_unit=}" ;;
    *) return 1 ;;
  esac
  case "${state_lines[4]}" in
    ai_service_unit=*) ai_service_record="${state_lines[4]#ai_service_unit=}" ;;
    *) return 1 ;;
  esac
  case "${state_lines[5]}" in
    ai_timer_unit=*) ai_timer_record="${state_lines[5]#ai_timer_unit=}" ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_state_record main "$main_record" || return 1
  controlled_guard_assert_state_record scheduler "$scheduler_record" || return 1
  controlled_guard_assert_state_record ai-service "$ai_service_record" || return 1
  controlled_guard_assert_state_record ai-timer "$ai_timer_record" || return 1
  IFS=, read -r main_load main_active main_unit_file <<< "$main_record" || \
    return 1
  IFS=, read -r scheduler_load scheduler_active scheduler_unit_file \
    <<< "$scheduler_record" || return 1
  test "$main_load" = loaded || return 1
  case "$main_active" in active|inactive) ;; *) return 1 ;; esac
  case "$main_unit_file" in enabled|disabled) ;; *) return 1 ;; esac
  forward_main_record="loaded,active,$main_unit_file"
  test "$scheduler_load" = loaded || return 1
  case "$scheduler_active" in active|inactive) ;; *) return 1 ;; esac
  case "$scheduler_unit_file" in enabled|disabled) ;; *) return 1 ;; esac
  IFS=, read -r ai_service_load ai_service_active ai_service_unit_file \
    <<< "$ai_service_record" || return 1
  IFS=, read -r ai_timer_load ai_timer_active ai_timer_unit_file \
    <<< "$ai_timer_record" || return 1
  case "$ai_service_load" in
    loaded)
      case "$ai_service_unit_file" in
        static) fenced_ai_service_record=loaded,inactive,static ;;
        enabled|disabled) fenced_ai_service_record=loaded,inactive,disabled ;;
        *) return 1 ;;
      esac
      ;;
    not-found) fenced_ai_service_record=not-found,not-found,not-found ;;
    *) return 1 ;;
  esac
  case "$ai_timer_load" in
    loaded) fenced_ai_timer_record=loaded,inactive,disabled ;;
    not-found) fenced_ai_timer_record=not-found,not-found,not-found ;;
    *) return 1 ;;
  esac

  if [ "$phase" = new-runtime-preserved-no-receipt ]; then
    # This phase is the durable commit point.  Never fence or stop a runtime
    # which was already fully re-attested; only revalidate it and finish the
    # atomic journal retirement after an interrupted cleanup.
    V2_RECOVERY_STEP=forward-commit-revalidate
    test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
    test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
    if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
      [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
      controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
        "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
        return 1
    fi
    activation_snapshot_assert_new_set "$guarded_sha" || return 1
    controlled_guard_assert_dropin_contract loaded \
      "${ai_service_record%%,*}" "${ai_timer_record%%,*}" || return 1
    controlled_guard_verify_restored_runtime "$forward_main_record" \
      "$forward_scheduler_record" "$guarded_sha" "$ai_service_record" \
      "$ai_timer_record" rollback-only || return 1
    controlled_guard_governance_snapshot verify "$guarded_sha" \
      "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
    activation_snapshot_assert_pending_receipt_absent || return 1
    if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
      [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
      V2_RECOVERY_STEP=forward-commit-remove-restore
      rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || return 1
      sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
      test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
      test ! -L "$DATABASE_WRITER_RESTORE_FILE" || return 1
    fi
    V2_RECOVERY_STEP=forward-commit-retire
    activation_snapshot_remove_new_runtime_preserved_no_receipt || return 1
    echo "v2 recovery finalized preserved runtime $guarded_sha without receipt" \
      >&2
    V2_RECOVERY_STEP=complete
    return 0
  fi

  V2_RECOVERY_STEP=forward-validate-restore-journal
  controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ]; then
    controlled_guard_assert_marker "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  else
    controlled_guard_recreate_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  fi
  V2_RECOVERY_STEP=forward-fence
  controlled_guard_install_dropins || fence_status=$?
  if [ "$fence_status" -eq 0 ]; then
    systemctl daemon-reload || fence_status=$?
  fi
  if [ "$fence_status" -eq 0 ]; then
    controlled_guard_force_all_writers_fenced "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
      fence_status=$?
  fi
  if [ "$fence_status" -eq 0 ]; then
    controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
      fence_status=$?
  fi
  if [ "$fence_status" -ne 0 ]; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  if [ "$phase" = runtime-units-installed ] || [ "$phase" = restoring-old ]; then
    V2_RECOVERY_STEP=forward-probe-governance
    if ! controlled_guard_governance_snapshot verify "$guarded_sha" \
        "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" >/dev/null 2>&1 || \
      ! activation_snapshot_set_phase "$guarded_sha" \
        restoring-new-no-receipt; then
      controlled_guard_refence_after_restore_failure "$guarded_sha" \
        "$main_record" "$scheduler_record" "$ai_service_record" \
        "$ai_timer_record" || true
      return 1
    fi
  fi
  V2_RECOVERY_STEP=forward-restore-units
  if ! activation_snapshot_restore_new_set "$guarded_sha" || \
    ! systemctl daemon-reload || \
    ! activation_snapshot_assert_new_set "$guarded_sha" || \
    ! controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-verify-governance-fenced
  if ! controlled_guard_governance_snapshot verify "$guarded_sha" \
      "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-verify-gates-fenced
  if ! controlled_guard_verify_restored_runtime "$fenced_main_record" \
      "$fenced_scheduler_record" "$guarded_sha" \
      "$fenced_ai_service_record" "$fenced_ai_timer_record" full || \
    ! controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-remove-fence
  if ! controlled_guard_cleanup "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-verify-runtime
  # The full governance and quality gates passed while every writer remained
  # fenced above.  Start the exact forward processes only now, then verify their
  # PID/environment/command line and both HTTP boundaries without repeating the
  # long database scans.  A committed cleanup retry follows the same rule.
  if ! controlled_guard_verify_restored_runtime "$forward_main_record" \
      "$forward_scheduler_record" "$guarded_sha" "$ai_service_record" \
      "$ai_timer_record" rollback-only || \
    ! controlled_guard_governance_snapshot verify "$guarded_sha" \
      "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || \
    ! activation_snapshot_assert_pending_receipt_absent; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-commit-phase
  if ! activation_snapshot_set_phase "$guarded_sha" \
      new-runtime-preserved-no-receipt; then
    # The phase rename is the logical commit and precedes its fsync checks.  If
    # one of those checks reports a fault after the rename, revalidate the exact
    # committed state and preserve the online runtime instead of re-fencing it.
    if ! activation_snapshot_validate "$guarded_sha" >/dev/null || \
      [ "$(activation_snapshot_phase)" != \
        new-runtime-preserved-no-receipt ]; then
      controlled_guard_refence_after_restore_failure "$guarded_sha" \
        "$main_record" "$scheduler_record" "$ai_service_record" \
        "$ai_timer_record" || true
      return 1
    fi
  fi
  # The phase above is the durable commit.  Cleanup faults from this point must
  # preserve the fully verified forward runtime online for the next read-mostly
  # retry and must never enter the rollback/refence path.
  V2_RECOVERY_STEP=forward-remove-restore
  if ! rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || \
    ! sync -f "$DATABASE_WRITER_GUARD_DIR" || \
    [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    return 1
  fi
  V2_RECOVERY_STEP=forward-retire
  activation_snapshot_remove_new_runtime_preserved_no_receipt || return 1
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test ! -L "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  echo "v2 recovery preserved verified runtime $guarded_sha without receipt" >&2
  V2_RECOVERY_STEP=complete
  return 0
}
controlled_v2_rollback_only_recovery() {
  local guarded_sha
  local old_runtime_sha
  local phase
  local main_record
  local scheduler_record
  local ai_service_record
  local ai_timer_record
  local fence_status=0
  local restore_forward_governance=0
  local -a state_lines=()
  V2_RECOVERY_STEP=rollback-validate-request
  test "$DEPLOY_OPERATION" = deploy || return 1
  test "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 || return 1
  [[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || return 1
  V2_RECOVERY_STEP=rollback-validate-transaction
  guarded_sha="$(activation_snapshot_recorded_release)" || return 1
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  old_runtime_sha="$(activation_snapshot_old_release "$guarded_sha")" || \
    return 1
  [[ "$old_runtime_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  test "$old_runtime_sha" != "$guarded_sha" || return 1
  phase="$(activation_snapshot_phase)" || return 1
  case "$phase" in
    restoring-new-no-receipt|new-runtime-preserved-no-receipt)
      controlled_v2_forward_preserve_no_receipt_recovery || return 1
      return 0
      ;;
  esac
  case "$phase" in
    prepared|runtime-units-installing|runtime-units-installed|\
    restoring-old|old-set-restored|old-runtime-verified) ;;
    *) return 1 ;;
  esac
  case "$phase" in
    prepared|runtime-units-installing|runtime-units-installed)
      # These phases all span a possible governance write.  Prove the sealed
      # forward restore runtime before any additional service mutation.  A
      # previous EXIT cleanup may have removed it only when the database never
      # changed; in that case the sealed old runtime must independently capture
      # an exact OLD match before recovery is allowed to continue.
      if [ -e "$RELEASE_VENV_ROOT/$guarded_sha" ] || \
        [ -L "$RELEASE_VENV_ROOT/$guarded_sha" ]; then
        V2_RECOVERY_STEP=rollback-validate-forward-runtime
        controlled_guard_assert_governance_restore_runtime "$guarded_sha" || \
          return 1
        restore_forward_governance=1
      else
        controlled_guard_capture_current_governance_snapshot "$guarded_sha" \
          "$old_runtime_sha" || return 1
      fi
      ;;
  esac
  case "$phase" in
    runtime-units-installed|restoring-old|old-set-restored|old-runtime-verified)
      if [ -e "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" ] || \
        [ -L "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" ] || \
        [ -e "$ACTIVATION_GOVERNANCE_NEW_SHA" ] || \
        [ -L "$ACTIVATION_GOVERNANCE_NEW_SHA" ]; then
        # A same-process rollback retains the sealed forward snapshot after it
        # advances the journal to restoring-old/old-set-restored.  Accept only
        # the complete, hash-verified pair; partial or changed evidence stays
        # fenced.
        activation_snapshot_validate_governance_new || return 1
      fi
      ;;
    *)
      test ! -e "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
      test ! -L "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
      test ! -e "$ACTIVATION_GOVERNANCE_NEW_SHA" || return 1
      test ! -L "$ACTIVATION_GOVERNANCE_NEW_SHA" || return 1
      ;;
  esac
  activation_snapshot_validate_rollback_receipt_state \
    "$guarded_sha" "$phase" || return 1
  V2_RECOVERY_STEP=rollback-validate-writer-state
  controlled_guard_assert_directory || return 1
  controlled_guard_assert_file "$ACTIVATION_UNIT_SNAPSHOT_STATE" 600 || \
    return 1
  mapfile -t state_lines < "$ACTIVATION_UNIT_SNAPSHOT_STATE" || return 1
  test "${#state_lines[@]}" -eq 6 || return 1
  test "${state_lines[0]}" = probiga.database-writer-restore.v1 || return 1
  test "${state_lines[1]}" = "release=$guarded_sha" || return 1
  case "${state_lines[2]}" in
    main_unit=*) main_record="${state_lines[2]#main_unit=}" ;;
    *) return 1 ;;
  esac
  case "${state_lines[3]}" in
    scheduler_unit=*) scheduler_record="${state_lines[3]#scheduler_unit=}" ;;
    *) return 1 ;;
  esac
  case "${state_lines[4]}" in
    ai_service_unit=*) ai_service_record="${state_lines[4]#ai_service_unit=}" ;;
    *) return 1 ;;
  esac
  case "${state_lines[5]}" in
    ai_timer_unit=*) ai_timer_record="${state_lines[5]#ai_timer_unit=}" ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_state_record main "$main_record" || return 1
  controlled_guard_assert_state_record scheduler "$scheduler_record" || return 1
  controlled_guard_assert_state_record ai-service "$ai_service_record" || return 1
  controlled_guard_assert_state_record ai-timer "$ai_timer_record" || return 1
  if [ "$phase" = old-runtime-verified ] && \
    [ ! -e "$DATABASE_WRITER_GUARD_FILE" ] && \
    [ ! -L "$DATABASE_WRITER_GUARD_FILE" ]; then
    # The old runtime is already the committed safe state.  Revalidate it
    # read-only and finish cleanup without recreating the guard or stopping
    # writers again.
    if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
      [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
      controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
        "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
        return 1
    fi
    activation_snapshot_assert_old_set "$guarded_sha" || return 1
    controlled_guard_capture_current_governance_snapshot "$guarded_sha" \
      "$old_runtime_sha" || return 1
    controlled_guard_verify_restored_runtime "$main_record" \
      "$scheduler_record" "$old_runtime_sha" "$ai_service_record" \
      "$ai_timer_record" rollback-only || return 1
    if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
      [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
      rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || return 1
      sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
      test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
      test ! -L "$DATABASE_WRITER_RESTORE_FILE" || return 1
    fi
    activation_snapshot_remove_old_runtime_verified || return 1
    test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
    test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
    echo "v2 rollback-only recovery finalized verified runtime $old_runtime_sha" \
      >&2
    return 0
  fi
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  else
    test "$phase" = old-runtime-verified || return 1
    controlled_guard_write_restore_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  fi
  if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ]; then
    controlled_guard_assert_marker "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  else
    # Normal activation removes the marker only after the complete pre-start
    # checks, while the durable restore journal deliberately remains until the
    # post-start boundary is finalized.  A disconnect in that window therefore
    # has runtime-units-installed plus no marker and must be re-fenced here.
    activation_snapshot_allows_missing_guard_for_recovery "$phase" || return 1
    controlled_guard_recreate_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  fi
  # Reapply the complete fence even when a prior interrupted attempt already
  # recreated the marker.  Marker creation, drop-in loading and writer stops
  # are separate durable steps, so this sequence must itself be idempotent.
  V2_RECOVERY_STEP=rollback-fence
  controlled_guard_install_dropins || fence_status=$?
  if [ "$fence_status" -eq 0 ]; then
    systemctl daemon-reload || fence_status=$?
  fi
  if [ "$fence_status" -eq 0 ]; then
    controlled_guard_force_all_writers_fenced "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
      fence_status=$?
  fi
  if [ "$fence_status" -eq 0 ]; then
    controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
      fence_status=$?
  fi
  if [ "$fence_status" -ne 0 ]; then
    controlled_guard_refence_after_restore_failure \
      "$guarded_sha" "$main_record" "$scheduler_record" \
      "$ai_service_record" "$ai_timer_record" || true
    return 1
  fi
  if { [ "$phase" = runtime-units-installed ] || \
      [ "$phase" = restoring-old ]; } && \
    activation_snapshot_assert_pending_receipt_absent && \
    activation_snapshot_validate_new "$guarded_sha" && \
    controlled_guard_assert_governance_restore_runtime "$guarded_sha" && \
    controlled_guard_governance_snapshot verify "$guarded_sha" \
      "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" >/dev/null 2>&1; then
    # Prefer the exact sealed forward state even when OLD and NEW governance
    # happen to be identical.  The durable phase proves that forward unit
    # installation began, while this probe proves the live database is exactly
    # the captured NEW state.  The dedicated recovery restores any partially
    # changed unit set before re-attesting the runtime.
    V2_RECOVERY_STEP=forward-preserve
    controlled_v2_forward_preserve_no_receipt_recovery || return 1
    return 0
  fi
  if [ "$restore_forward_governance" -eq 1 ]; then
    V2_RECOVERY_STEP=rollback-probe-old-governance
    if controlled_guard_governance_snapshot verify "$guarded_sha" \
        "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" >/dev/null 2>&1; then
      :
    else
      V2_RECOVERY_STEP=rollback-restore-old-governance
      if ! controlled_guard_restore_and_verify_governance_snapshot \
          "$guarded_sha" "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"; then
        controlled_guard_refence_after_restore_failure \
          "$guarded_sha" "$main_record" "$scheduler_record" \
          "$ai_service_record" "$ai_timer_record" || true
        return 1
      fi
    fi
  fi
  V2_RECOVERY_STEP=rollback-restore-old-units
  if ! activation_snapshot_restore_old_set "$guarded_sha" || \
    ! systemctl daemon-reload || \
    ! activation_snapshot_assert_old_set "$guarded_sha" || \
    ! controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    controlled_guard_refence_after_restore_failure \
      "$guarded_sha" "$main_record" "$scheduler_record" \
      "$ai_service_record" "$ai_timer_record" || true
    return 1
  fi
  # A failed release venv is deliberately removed during rollback.  Reuse the
  # sealed old runtime with the guarded release's read-only capture tool and
  # require an exact match; any governance drift remains fenced for explicit
  # recovery instead of depending on the removed forward venv or writing here.
  V2_RECOVERY_STEP=rollback-verify-old-governance
  if ! controlled_guard_capture_current_governance_snapshot "$guarded_sha" \
      "$old_runtime_sha" || \
    ! controlled_guard_cleanup "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    controlled_guard_refence_after_restore_failure \
      "$guarded_sha" "$main_record" "$scheduler_record" \
      "$ai_service_record" "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=rollback-verify-old-runtime
  if ! controlled_guard_restore_previous_writer_states "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
    ! controlled_guard_verify_restored_runtime "$main_record" \
      "$scheduler_record" "$old_runtime_sha" "$ai_service_record" \
      "$ai_timer_record" rollback-only; then
    controlled_guard_refence_after_restore_failure \
      "$guarded_sha" "$main_record" "$scheduler_record" \
      "$ai_service_record" "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=rollback-commit-phase
  if ! activation_snapshot_set_phase "$guarded_sha" old-runtime-verified; then
    if ! activation_snapshot_validate "$guarded_sha" >/dev/null || \
      [ "$(activation_snapshot_phase)" != old-runtime-verified ]; then
      controlled_guard_refence_after_restore_failure \
        "$guarded_sha" "$main_record" "$scheduler_record" \
        "$ai_service_record" "$ai_timer_record" || true
      return 1
    fi
  fi
  V2_RECOVERY_STEP=rollback-retire
  if ! rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || \
    ! sync -f "$DATABASE_WRITER_GUARD_DIR" || \
    [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ] || \
    ! activation_snapshot_remove_old_runtime_verified; then
    # The durable phase above proves the old runtime is healthy.  Preserve it
    # on cleanup faults and let the next recovery use the read-only fast path.
    return 1
  fi
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test ! -L "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  echo "v2 rollback-only recovery restored sealed runtime $old_runtime_sha" >&2
  V2_RECOVERY_STEP=complete
  return 0
}
controlled_v2_forward_finalize_recovery() {
  local guarded_sha
  local phase
  local main_record
  local main_load
  local main_active
  local main_unit_file
  local request_matches=0
  local scheduler_record
  local ai_service_record
  local ai_timer_record
  local -a state_lines=()
  test "$DEPLOY_OPERATION" = deploy || return 1
  test "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 || return 1
  [[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || return 1
  guarded_sha="$(activation_snapshot_recorded_release)" || return 1
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  phase="$(activation_snapshot_phase)" || return 1
  case "$phase" in
    new-runtime-verified|finalized) ;;
    *) return 1 ;;
  esac
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
  activation_snapshot_validate "$guarded_sha" >/dev/null || return 1
  activation_snapshot_validate_new "$guarded_sha" >/dev/null || return 1
  activation_snapshot_validate_governance_new || return 1
  activation_snapshot_validate_receipt_pending "$guarded_sha" || return 1
  if [ "$guarded_sha" = "$EXPECTED_SHA" ]; then
    # A workflow re-run may reuse the commit while supplying a different
    # dependency or Adata artifact.  Only report the old transaction as this
    # request's success when the complete artifact identity also matches.
    if activation_snapshot_receipt_matches_current_v2_request "$guarded_sha"; then
      request_matches=1
    fi
  fi
  activation_snapshot_assert_new_set "$guarded_sha" || return 1
  controlled_guard_assert_file "$ACTIVATION_UNIT_SNAPSHOT_STATE" 600 || \
    return 1
  mapfile -t state_lines < "$ACTIVATION_UNIT_SNAPSHOT_STATE" || return 1
  test "${#state_lines[@]}" -eq 6 || return 1
  test "${state_lines[0]}" = probiga.database-writer-restore.v1 || return 1
  test "${state_lines[1]}" = "release=$guarded_sha" || return 1
  case "${state_lines[2]}" in
    main_unit=*) main_record="${state_lines[2]#main_unit=}" ;;
    *) return 1 ;;
  esac
  case "${state_lines[3]}" in
    scheduler_unit=*) scheduler_record="${state_lines[3]#scheduler_unit=}" ;;
    *) return 1 ;;
  esac
  case "${state_lines[4]}" in
    ai_service_unit=*) ai_service_record="${state_lines[4]#ai_service_unit=}" ;;
    *) return 1 ;;
  esac
  case "${state_lines[5]}" in
    ai_timer_unit=*) ai_timer_record="${state_lines[5]#ai_timer_unit=}" ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_state_record main "$main_record" || return 1
  controlled_guard_assert_state_record scheduler "$scheduler_record" || return 1
  controlled_guard_assert_state_record ai-service "$ai_service_record" || \
    return 1
  controlled_guard_assert_state_record ai-timer "$ai_timer_record" || return 1
  IFS=, read -r main_load main_active main_unit_file <<< "$main_record" || \
    return 1
  test "$main_load" = loaded || return 1
  case "$main_unit_file" in enabled|disabled) ;; *) return 1 ;; esac
  controlled_guard_assert_dropin_contract loaded \
    "${ai_service_record%%,*}" "${ai_timer_record%%,*}" || return 1
  controlled_guard_verify_restored_runtime \
    "loaded,active,$main_unit_file" loaded,active,enabled "$guarded_sha" \
    "$ai_service_record" "$ai_timer_record" rollback-only || return 1
  controlled_guard_governance_snapshot verify "$guarded_sha" \
    "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    test "$phase" = new-runtime-verified || return 1
    controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
    rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || return 1
    sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
    test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
    test ! -L "$DATABASE_WRITER_RESTORE_FILE" || return 1
  fi
  if [ "$phase" = new-runtime-verified ]; then
    activation_snapshot_set_phase "$guarded_sha" finalized || return 1
  fi
  activation_snapshot_remove_finalized_before_deploy || return 1
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test ! -L "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  V2_FORWARD_FINALIZED_SHA="$guarded_sha"
  V2_FORWARD_FINALIZED_REQUEST_MATCH="$request_matches"
  echo "v2 forward-finalize recovery preserved verified runtime $guarded_sha" >&2
  return 0
}
controlled_guard_sync_activation_journal() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local guarded_sha="$1"
  local main_record="$2"
  local scheduler_record="$3"
  controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  sync -f "$DATABASE_WRITER_RESTORE_FILE" || return 1
  sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
  controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  return 0
}
controlled_guard_finalize_successful_activation() {
  local ai_service_record="$4"
  local ai_timer_record="$5"
  local expected_main_unit_file
  local guarded_sha="$1"
  local main_active
  local main_load
  local main_record="$2"
  local scheduler_record="$3"
  IFS=, read -r main_load main_active expected_main_unit_file <<< "$main_record"
  test "$main_load" = loaded || return 1
  controlled_guard_sync_activation_journal "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
  controlled_guard_assert_dropin_contract "${scheduler_record%%,*}" \
    "${ai_service_record%%,*}" "${ai_timer_record%%,*}" || return 1
  test "$(systemctl show -p LoadState --value probiga)" = loaded || return 1
  test "$(systemctl show -p ActiveState --value probiga)" = active || return 1
  test "$(systemctl show -p UnitFileState --value probiga)" = \
    "$expected_main_unit_file" || return 1
  test "$(systemctl show -p LoadState --value probiga-scheduler)" = loaded || \
    return 1
  test "$(systemctl show -p ActiveState --value probiga-scheduler)" = active || \
    return 1
  test "$(systemctl show -p UnitFileState --value probiga-scheduler)" = enabled || \
    return 1
  controlled_guard_apply_unit_state \
    probiga-ai-recommendation-worker.service "$ai_service_record" || return 1
  controlled_guard_apply_unit_state \
    probiga-ai-recommendation-worker.timer "$ai_timer_record" || return 1
  curl --fail --silent --show-error --retry 15 --retry-all-errors \
    --retry-delay 2 --retry-connrefused \
    http://127.0.0.1/api/health >/dev/null || return 1
  curl --fail --silent --show-error --retry 15 --retry-all-errors \
    --retry-delay 2 --retry-connrefused \
    http://127.0.0.1/api/health/runtime >/dev/null || return 1
  activation_snapshot_validate "$guarded_sha" >/dev/null || return 1
  activation_snapshot_validate_new "$guarded_sha" >/dev/null || return 1
  activation_snapshot_assert_new_set "$guarded_sha" || return 1
  activation_snapshot_set_phase "$guarded_sha" new-runtime-verified || return 1
  if ! rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || \
    ! sync -f "$DATABASE_WRITER_GUARD_DIR" || \
    [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    # new-runtime-verified is a durable commit point reached only after every
    # runtime and receipt boundary passed.  A journal-cleanup fault must leave
    # that verified runtime online; startup forward-finalize recovery retries
    # the exact removal without fencing or rolling the release back.
    return 1
  fi
  activation_snapshot_set_phase "$guarded_sha" finalized || return 1
  activation_snapshot_assert_new_set "$guarded_sha" || return 1
  return 0
}
controlled_guard_run_schema_tool() {
  local code_root="$1"
  local guarded_sha="$4"
  local phase="$3"
  local release_venv="$2"
  local adata_sha
  local adata_tree_sha
  local release_tree_sha=""
  local adapter_registry_seal_sha=""
  local -a phase_args=(--phase "$phase")
  local -a attested_env=()
  case "$phase" in
    resume) phase_args+=(--writers-fenced) ;;
    preflight|recover) ;;
    *) return 2 ;;
  esac
  adata_sha="$(cat "$release_venv/.adata.gitsha")" || return 1
  adata_tree_sha="$(cat "$release_venv/.adata.tree.sha256")" || return 1
  [[ "$adata_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$adata_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  if [ -e "$release_venv/.release-tree.sha256" ] || \
    [ -e "$release_venv/.adapter-registry-seal.sha256" ]; then
    test -f "$release_venv/.release-tree.sha256" || return 1
    test -f "$release_venv/.adapter-registry-seal.sha256" || return 1
    release_tree_sha="$(<"$release_venv/.release-tree.sha256")" || return 1
    adapter_registry_seal_sha="$(
      <"$release_venv/.adapter-registry-seal.sha256"
    )" || return 1
    [[ "$release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    [[ "$adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    attested_env+=(
      "PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha"
      "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha"
    )
  fi
  (
    cd "$code_root"
    /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      GIT_OPTIONAL_LOCKS=0 \
      PYTHONDONTWRITEBYTECODE=1 \
      PYTHONSAFEPATH=1 \
      PROBIGA_DEPLOYMENT_MODE=production \
      PROBIGA_EXPECTED_GIT_SHA="$guarded_sha" \
      PROBIGA_BUILD_COMMIT_SHA="$guarded_sha" \
      PROBIGA_CODE_ROOT="$code_root" \
      PROBIGA_EXPECTED_ADATA_SHA="$adata_sha" \
      PROBIGA_EXPECTED_ADATA_TREE_SHA256="$adata_tree_sha" \
      "${attested_env[@]}" \
      "PYTHONPATH=$code_root" \
      "$release_venv/bin/python" -P \
      "$code_root/tools/prepare_strategy_governance_schema.py" \
      "${phase_args[@]}"
  )
}
controlled_guard_run_writer_fence() {
  local adata_sha="$5"
  local adata_source="$3"
  local adata_tree_sha="$6"
  local code_root="$1"
  local guarded_sha="$4"
  local release_venv="$2"
  local service_user="$7"
  local release_tree_sha=""
  local adapter_registry_seal_sha=""
  local -a attested_env=()
  if [ -e "$release_venv/.release-tree.sha256" ] || \
    [ -e "$release_venv/.adapter-registry-seal.sha256" ]; then
    test -f "$release_venv/.release-tree.sha256" || return 1
    test -f "$release_venv/.adapter-registry-seal.sha256" || return 1
    release_tree_sha="$(<"$release_venv/.release-tree.sha256")" || return 1
    adapter_registry_seal_sha="$(
      <"$release_venv/.adapter-registry-seal.sha256"
    )" || return 1
    [[ "$release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    [[ "$adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    attested_env+=(
      "PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha"
      "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha"
    )
  fi
  (
    cd "$code_root"
    sudo -u "$service_user" /usr/bin/env -i \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      -u MYSQL_URL -u DATABASE_URL -u MYSQL_PWD \
      -u MYSQL_UNIX_PORT -u MYSQL_TEST_LOGIN_FILE \
      API_EMBEDDED_SCHEDULER_ENABLED=false \
      GIT_OPTIONAL_LOCKS=0 \
      PYTHONDONTWRITEBYTECODE=1 \
      PYTHONSAFEPATH=1 \
      PROBIGA_DEPLOYMENT_MODE=production \
      PROBIGA_EXPECTED_GIT_SHA="$guarded_sha" \
      PROBIGA_BUILD_COMMIT_SHA="$guarded_sha" \
      PROBIGA_CODE_ROOT="$code_root" \
      PROBIGA_EXPECTED_ADATA_SHA="$adata_sha" \
      PROBIGA_EXPECTED_ADATA_TREE_SHA256="$adata_tree_sha" \
      PROBIGA_ADATA_SOURCE_DIR="$adata_source" \
      "${attested_env[@]}" \
      "PYTHONPATH=$adata_source:$code_root" \
      "$release_venv/bin/python" -P \
      "$code_root/tools/add_trading_v3_tasks.py" \
      --writer-fence \
      --require-no-live-scheduler-writers \
      --writer-drain-timeout-seconds 150 \
      --writer-drain-poll-seconds 5
  )
}
controlled_guard_validate_writer_fence_json() {
  local python_bin="$1"
  "$python_bin" -I -c \
    'import json,sys; p=json.load(sys.stdin); q=p.get("writer_quiescence") if isinstance(p,dict) else None; ok=isinstance(p,dict) and p.get("status")=="ok" and p.get("mode")=="writer-fence" and p.get("writer_fence_active") is True and p.get("layer4_writers_enabled") is False and isinstance(p.get("fenced_row_count"),int) and p["fenced_row_count"]>=0 and isinstance(q,dict) and q.get("checked") is True and q.get("ready") is True and q.get("reason_codes")==[] and q.get("live_writers")==[] and isinstance(p.get("tasks"),list); raise SystemExit(0 if ok else 2)'
}
controlled_guard_validate_recover_json() {
  local python_bin="$1"
  "$python_bin" -I -c \
    'import json,sys; p=json.load(sys.stdin); ok=isinstance(p,dict) and p.get("status")=="ok" and p.get("phase")=="recover" and p.get("trust_restoration_verified") is True and p.get("restore_primary_verified") is True and p.get("restore_secondary_verified") is True and p.get("runtime_trust_off_verified") is True and p.get("automatic_real_order_submission") is False; raise SystemExit(0 if ok else 2)'
}
controlled_guard_validate_resume_json() {
  local python_bin="$1"
  "$python_bin" -I -c \
    'import json,sys; p=json.load(sys.stdin); migrations=p.get("v3_migrations") if isinstance(p,dict) else None; trigger=p.get("trigger_contract") if isinstance(p,dict) else None; repair=p.get("legacy_trigger_repair") if isinstance(p,dict) else None; candidates=repair.get("candidate_names") if isinstance(repair,dict) else None; repaired=repair.get("repaired_names") if isinstance(repair,dict) else None; windows=p.get("trigger_trust_window_names") if isinstance(p,dict) else None; security=p.get("runtime_grant_summary") if isinstance(p,dict) else None; binding=p.get("legacy_binding_plan") if isinstance(p,dict) else None; expected_schema={"BIGA.*":["SELECT"],"PROBIGA.*":["ALTER","CREATE","CREATE TEMPORARY TABLES","DELETE","DROP","INDEX","INSERT","REFERENCES","SELECT","UPDATE"],"PROBIGA_QMT_HISTORY.*":["SELECT"]}; allowed={"trg_trade_account_v2_real_disabled_bi","trg_trade_account_v2_real_disabled_bu"}; least=isinstance(security,dict) and security.get("global_privileges")==["USAGE"] and security.get("schema_privileges")==expected_schema and security.get("require_ssl") is True and security.get("roles")==[] and security.get("grant_option") is False and p.get("runtime_definer_routine_count")==0 and p.get("runtime_definer_routine_inventory_verified") is True; legacy=isinstance(binding,dict) and isinstance(binding.get("legacy_run_count"),int) and binding["legacy_run_count"]>=0 and isinstance(binding.get("legacy_binding_plan_hash"),str) and bool(binding["legacy_binding_plan_hash"]) and binding.get("legacy_binding_pending") is False and binding.get("legacy_binding_marker_present") is bool(binding["legacy_run_count"]); ok=isinstance(p,dict) and p.get("status")=="ok" and p.get("phase")=="resume" and p.get("runtime_least_privilege_verified") is True and least and legacy and p.get("trust_restoration_verified") is True and p.get("restore_primary_verified") is True and p.get("restore_secondary_verified") is True and p.get("runtime_trust_off_verified") is True and isinstance(repair,dict) and repair.get("post_validation_verified") is True and isinstance(candidates,list) and candidates==sorted(set(candidates)) and set(candidates)<=allowed and repaired==candidates and isinstance(windows,list) and all(isinstance(x,str) for x in windows) and windows==list(dict.fromkeys(windows)) and p.get("trigger_trust_window_count")==len(windows) and p.get("global_trust_changed") is bool(windows) and isinstance(migrations,list) and bool(migrations) and all(isinstance(x,dict) and x.get("status") in {"applied","exists"} for x in migrations) and isinstance(trigger,dict) and trigger.get("metadata_frozen") is True and trigger.get("legacy_rehome_names")==[] and trigger.get("definer")=="probiga_migrator@127.0.0.1" and trigger.get("observed_count")==trigger.get("required_count",-1)+trigger.get("optional_count",-1) and isinstance(p.get("seeded_strategy_count"),int) and p["seeded_strategy_count"]>0 and isinstance(p.get("governance_trigger_count"),int) and p["governance_trigger_count"]>0 and p.get("automatic_real_order_submission") is False; raise SystemExit(0 if ok else 2)'
}
controlled_guard_validate_preflight_json() {
  local python_bin="$1"
  "$python_bin" -I -c \
    'import json,sys; p=json.load(sys.stdin); migrations=p.get("v3_migrations") if isinstance(p,dict) else None; trigger=p.get("trigger_contract") if isinstance(p,dict) else None; security=p.get("runtime_grant_summary") if isinstance(p,dict) else None; binding=p.get("legacy_binding_plan") if isinstance(p,dict) else None; expected_schema={"BIGA.*":["SELECT"],"PROBIGA.*":["ALTER","CREATE","CREATE TEMPORARY TABLES","DELETE","DROP","INDEX","INSERT","REFERENCES","SELECT","UPDATE"],"PROBIGA_QMT_HISTORY.*":["SELECT"]}; least=isinstance(security,dict) and security.get("global_privileges")==["USAGE"] and security.get("schema_privileges")==expected_schema and security.get("require_ssl") is True and security.get("roles")==[] and security.get("grant_option") is False and p.get("runtime_definer_routine_count")==0 and p.get("runtime_definer_routine_inventory_verified") is True; legacy=isinstance(binding,dict) and isinstance(binding.get("legacy_run_count"),int) and binding["legacy_run_count"]>=0 and isinstance(binding.get("legacy_binding_plan_hash"),str) and bool(binding["legacy_binding_plan_hash"]) and binding.get("legacy_binding_pending") is False and binding.get("legacy_binding_marker_present") is bool(binding["legacy_run_count"]); ok=isinstance(p,dict) and p.get("status")=="ok" and p.get("phase")=="preflight" and p.get("runtime_least_privilege_verified") is True and least and legacy and p.get("global_trust_changed") is False and p.get("trust_restoration_verified") is True and p.get("pending_v3_versions")==[] and isinstance(migrations,list) and bool(migrations) and all(isinstance(x,dict) and x.get("status")=="exists" for x in migrations) and isinstance(trigger,dict) and trigger.get("metadata_frozen") is True and trigger.get("legacy_rehome_names")==[] and trigger.get("definer")=="probiga_migrator@127.0.0.1" and trigger.get("observed_count")==trigger.get("required_count",-1)+trigger.get("optional_count",-1) and isinstance(p.get("qmt_table_count"),int) and p["qmt_table_count"]>0 and isinstance(p.get("governance_table_count"),int) and p["governance_table_count"]>0 and p.get("automatic_real_order_submission") is False; raise SystemExit(0 if ok else 2)'
}
controlled_database_guard_recovery() {
  local adata_sha
  local adata_source
  local adata_tree_sha
  local ai_service_record
  local ai_service_load
  local ai_timer_record
  local ai_timer_load
  local code_root
  local resume_output
  local guarded_sha
  local main_record
  local preflight_output
  local initial_recover_output
  local final_recover_output
  local release_venv
  local release_venv_target
  local scheduler_record
  local scheduler_load
  local service_user
  local writer_fence_output
  local -a guard_lines=()
  controlled_guard_assert_storage
  mapfile -t guard_lines < "$DATABASE_WRITER_GUARD_FILE"
  test "${#guard_lines[@]}" -eq 6
  test "${guard_lines[0]}" = probiga.database-writer-guard.v2
  case "${guard_lines[1]}" in
    release=*) guarded_sha="${guard_lines[1]#release=}" ;;
    *) return 1 ;;
  esac
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]]
  test "$guarded_sha" = "$PROBIGA_RECOVERY_GUARD_SHA"
  case "${guard_lines[2]}" in
    main_unit=*) main_record="${guard_lines[2]#main_unit=}" ;;
    *) return 1 ;;
  esac
  case "${guard_lines[3]}" in
    scheduler_unit=*) scheduler_record="${guard_lines[3]#scheduler_unit=}" ;;
    *) return 1 ;;
  esac
  case "${guard_lines[4]}" in
    ai_service_unit=*) ai_service_record="${guard_lines[4]#ai_service_unit=}" ;;
    *) return 1 ;;
  esac
  case "${guard_lines[5]}" in
    ai_timer_unit=*) ai_timer_record="${guard_lines[5]#ai_timer_unit=}" ;;
    *) return 1 ;;
  esac
  scheduler_load="${scheduler_record%%,*}"
  ai_service_load="${ai_service_record%%,*}"
  ai_timer_load="${ai_timer_record%%,*}"
  case "$scheduler_load:$ai_service_load:$ai_timer_load" in
    loaded:loaded:loaded|loaded:not-found:not-found|\
    not-found:loaded:loaded|not-found:not-found:not-found) ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_marker "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  fi
  service_user="$(systemctl show -p User --value probiga)"
  test -n "$service_user"
  test "$service_user" != root
  controlled_guard_install_dropins
  systemctl daemon-reload
  controlled_guard_force_all_writers_fenced "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  code_root="$CODE_RELEASE_ROOT/$guarded_sha"
  release_venv="$RELEASE_VENV_ROOT/$guarded_sha"
  test -d "$code_root"
  test ! -L "$code_root"
  test "$(readlink -f "$code_root")" = "$code_root"
  test "$(stat -c '%U' "$code_root")" = root
  test -z "$(find -P "$code_root" -xdev \
    \( ! -user root -o -perm /022 \) -print -quit)"
  test "$(git -C "$code_root" rev-parse HEAD)" = \
    "$guarded_sha"
  test -z "$(git -C "$code_root" \
    status --porcelain=v1 --untracked-files=all)"
  sudo -u "$service_user" test ! -w "$code_root"
  test -L "$release_venv"
  release_venv_target="$(readlink -f "$release_venv")"
  case "$release_venv_target" in
    "$RELEASE_VENV_ROOT"/build-*) ;;
    *) return 1 ;;
  esac
  test "$(dirname "$release_venv_target")" = "$RELEASE_VENV_ROOT"
  test "$(cat "$release_venv/.probiga.gitsha")" = "$guarded_sha"
  test -x "$release_venv/bin/python"
  test "$(stat -c '%U' "$release_venv_target")" = root
  test -z "$(find -P "$release_venv_target" -xdev \
    \( ! -user root -o -perm /022 \) -print -quit)"
  sudo -u "$service_user" test ! -w "$release_venv_target"
  adata_sha="$(cat "$release_venv/.adata.gitsha")"
  adata_tree_sha="$(cat "$release_venv/.adata.tree.sha256")"
  [[ "$adata_sha" =~ ^[0-9a-f]{40}$ ]]
  [[ "$adata_tree_sha" =~ ^[0-9a-f]{64}$ ]]
  adata_source="$ADATA_RUNTIME_ROOT/$adata_sha-$adata_tree_sha"
  test -d "$adata_source"
  test ! -L "$adata_source"
  test "$(readlink -f "$adata_source")" = "$adata_source"
  test "$(stat -c '%U' "$adata_source")" = root
  test "$(cat "$adata_source/.probiga-adata.gitsha")" = "$adata_sha"
  test "$(cat "$adata_source/.probiga-adata.tree.sha256")" = \
    "$adata_tree_sha"
  test -z "$(find -P "$adata_source" -xdev \
    \( ! -user root -o -perm /022 \) -print -quit)"
  sudo -u "$service_user" test ! -w "$adata_source"
  controlled_guard_assert_file \
    "$code_root/tools/prepare_strategy_governance_schema.py" 444
  controlled_guard_assert_file \
    "$code_root/tools/add_trading_v3_tasks.py" 444
  initial_recover_output="$(controlled_guard_run_schema_tool \
    "$code_root" "$release_venv" recover "$guarded_sha")"
  printf '%s\n' "$initial_recover_output"
  printf '%s' "$initial_recover_output" \
    | controlled_guard_validate_recover_json "$release_venv/bin/python"
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  writer_fence_output="$(controlled_guard_run_writer_fence \
    "$code_root" "$release_venv" "$adata_source" "$guarded_sha" \
    "$adata_sha" "$adata_tree_sha" "$service_user")"
  printf '%s\n' "$writer_fence_output"
  printf '%s' "$writer_fence_output" \
    | controlled_guard_validate_writer_fence_json "$release_venv/bin/python"
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  resume_output="$(controlled_guard_run_schema_tool \
    "$code_root" "$release_venv" resume "$guarded_sha")"
  printf '%s\n' "$resume_output"
  printf '%s' "$resume_output" \
    | controlled_guard_validate_resume_json "$release_venv/bin/python"
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  final_recover_output="$(controlled_guard_run_schema_tool \
    "$code_root" "$release_venv" recover "$guarded_sha")"
  printf '%s\n' "$final_recover_output"
  printf '%s' "$final_recover_output" \
    | controlled_guard_validate_recover_json "$release_venv/bin/python"
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  preflight_output="$(controlled_guard_run_schema_tool \
    "$code_root" "$release_venv" preflight "$guarded_sha")"
  printf '%s\n' "$preflight_output"
  printf '%s' "$preflight_output" \
    | controlled_guard_validate_preflight_json "$release_venv/bin/python"
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  echo "controlled database guard recovery validated for $guarded_sha" >&2
  controlled_guard_write_restore_file "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  controlled_guard_cleanup "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  controlled_guard_restore_and_finalize "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
}
controlled_database_writer_restore_recovery() {
  local ai_service_load
  local ai_service_record
  local ai_timer_load
  local ai_timer_record
  local guarded_sha
  local main_record
  local scheduler_load
  local scheduler_record
  local -a restore_lines=()
  controlled_guard_assert_directory
  controlled_guard_assert_file "$DATABASE_WRITER_RESTORE_FILE" 600
  mapfile -t restore_lines < "$DATABASE_WRITER_RESTORE_FILE"
  test "${#restore_lines[@]}" -eq 6
  test "${restore_lines[0]}" = probiga.database-writer-restore.v1
  case "${restore_lines[1]}" in
    release=*) guarded_sha="${restore_lines[1]#release=}" ;;
    *) return 1 ;;
  esac
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]]
  test "$guarded_sha" = "$PROBIGA_RECOVERY_GUARD_SHA"
  case "${restore_lines[2]}" in
    main_unit=*) main_record="${restore_lines[2]#main_unit=}" ;;
    *) return 1 ;;
  esac
  case "${restore_lines[3]}" in
    scheduler_unit=*) scheduler_record="${restore_lines[3]#scheduler_unit=}" ;;
    *) return 1 ;;
  esac
  case "${restore_lines[4]}" in
    ai_service_unit=*) ai_service_record="${restore_lines[4]#ai_service_unit=}" ;;
    *) return 1 ;;
  esac
  case "${restore_lines[5]}" in
    ai_timer_unit=*) ai_timer_record="${restore_lines[5]#ai_timer_unit=}" ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  test ! -e "$DATABASE_WRITER_GUARD_FILE"
  test ! -L "$DATABASE_WRITER_GUARD_FILE"
  scheduler_load="${scheduler_record%%,*}"
  ai_service_load="${ai_service_record%%,*}"
  ai_timer_load="${ai_timer_record%%,*}"
  if ! controlled_guard_recreate_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
    ! controlled_guard_install_dropins || ! systemctl daemon-reload || \
    ! controlled_guard_force_all_writers_fenced "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
    ! controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    controlled_guard_refence_after_restore_failure \
      "$guarded_sha" "$main_record" "$scheduler_record" \
      "$ai_service_record" "$ai_timer_record" || true
    return 1
  fi
  controlled_guard_cleanup "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
  controlled_guard_restore_and_finalize "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record"
}
controlled_activation_snapshot_only_recovery() {
  local ai_service_record
  local ai_timer_record
  local guarded_sha
  local old_runtime_sha
  local main_active
  local main_record
  local main_unit_file
  local phase
  local scheduler_record
  local -a state_lines=()
  guarded_sha="$(activation_snapshot_recorded_release)" || return 1
  test "$guarded_sha" = "$PROBIGA_RECOVERY_GUARD_SHA" || return 1
  old_runtime_sha="$(activation_snapshot_old_release "$guarded_sha")" || \
    return 1
  phase="$(activation_snapshot_phase)" || return 1
  case "$phase" in
    old-runtime-verified|new-runtime-preserved-no-receipt|\
    new-runtime-verified|finalized) ;;
    *) return 1 ;;
  esac
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
  activation_snapshot_validate_new "$guarded_sha" || return 1
  controlled_guard_assert_file "$ACTIVATION_UNIT_SNAPSHOT_STATE" 600 || return 1
  mapfile -t state_lines < "$ACTIVATION_UNIT_SNAPSHOT_STATE" || return 1
  test "${#state_lines[@]}" -eq 6 || return 1
  test "${state_lines[0]}" = probiga.database-writer-restore.v1 || return 1
  test "${state_lines[1]}" = "release=$guarded_sha" || return 1
  case "${state_lines[2]}" in main_unit=*) main_record="${state_lines[2]#main_unit=}" ;; *) return 1 ;; esac
  case "${state_lines[3]}" in scheduler_unit=*) scheduler_record="${state_lines[3]#scheduler_unit=}" ;; *) return 1 ;; esac
  case "${state_lines[4]}" in ai_service_unit=*) ai_service_record="${state_lines[4]#ai_service_unit=}" ;; *) return 1 ;; esac
  case "${state_lines[5]}" in ai_timer_unit=*) ai_timer_record="${state_lines[5]#ai_timer_unit=}" ;; *) return 1 ;; esac
  controlled_guard_assert_state_record main "$main_record" || return 1
  controlled_guard_assert_state_record scheduler "$scheduler_record" || return 1
  controlled_guard_assert_state_record ai-service "$ai_service_record" || return 1
  controlled_guard_assert_state_record ai-timer "$ai_timer_record" || return 1
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  fi
  if [ "$phase" = new-runtime-preserved-no-receipt ]; then
    activation_snapshot_validate_governance_new || return 1
    activation_snapshot_assert_pending_receipt_absent || return 1
    activation_snapshot_assert_new_set "$guarded_sha" || return 1
    IFS=, read -r _main_load main_active main_unit_file <<< "$main_record" || \
      return 1
    case "$main_unit_file" in enabled|disabled) ;; *) return 1 ;; esac
    controlled_guard_assert_dropin_contract loaded \
      "${ai_service_record%%,*}" "${ai_timer_record%%,*}" || return 1
    controlled_guard_verify_restored_runtime \
      "loaded,active,$main_unit_file" loaded,active,enabled "$guarded_sha" \
      "$ai_service_record" "$ai_timer_record" rollback-only || return 1
    controlled_guard_governance_snapshot verify "$guarded_sha" \
      "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
    if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
      [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
      rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || return 1
      sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
    fi
    activation_snapshot_remove_new_runtime_preserved_no_receipt || return 1
    return 0
  fi
  if [ "$phase" = old-runtime-verified ]; then
    activation_snapshot_restore_old_set "$guarded_sha" || return 1
    systemctl daemon-reload || return 1
    activation_snapshot_assert_old_set "$guarded_sha" || return 1
    controlled_guard_restore_and_verify_governance_snapshot "$guarded_sha" \
      "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" || return 1
    controlled_guard_restore_previous_writer_states "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
    controlled_guard_verify_restored_runtime "$main_record" \
      "$scheduler_record" "$old_runtime_sha" "$ai_service_record" \
      "$ai_timer_record" || return 1
    activation_snapshot_set_phase "$guarded_sha" old-runtime-verified || \
      return 1
    if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
      [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
      rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || return 1
      sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
    fi
    activation_snapshot_remove_old_runtime_verified || return 1
    return 0
  fi
  activation_snapshot_restore_new_set "$guarded_sha" || return 1
  systemctl daemon-reload || return 1
  activation_snapshot_assert_new_set "$guarded_sha" || return 1
  controlled_guard_restore_and_verify_governance_snapshot "$guarded_sha" \
    "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
  IFS=, read -r _main_load main_active main_unit_file <<< "$main_record" || \
    return 1
  case "$main_unit_file" in
    enabled) systemctl enable probiga || return 1 ;;
    disabled) systemctl disable probiga || return 1 ;;
    *) return 1 ;;
  esac
  systemctl start probiga || return 1
  systemctl enable probiga-scheduler || return 1
  systemctl start probiga-scheduler || return 1
  controlled_guard_apply_unit_state \
    probiga-ai-recommendation-worker.service "$ai_service_record" || return 1
  controlled_guard_apply_unit_state \
    probiga-ai-recommendation-worker.timer "$ai_timer_record" || return 1
  controlled_guard_verify_restored_runtime \
    "loaded,active,$main_unit_file" loaded,active,enabled "$guarded_sha" \
    "$ai_service_record" "$ai_timer_record" || return 1
  if [ "${ai_service_record%%,*}" = loaded ]; then
    test "$(systemctl show -p User --value \
      probiga-ai-recommendation-worker.service)" != root || return 1
    systemctl show -p ExecStart --value \
      probiga-ai-recommendation-worker.service | \
      grep -F -- "/var/lib/probiga/release-venvs/$guarded_sha/bin/python" \
      >/dev/null || return 1
    systemctl show -p ExecStart --value \
      probiga-ai-recommendation-worker.service | \
      grep -F -- "/opt/ProBigA-releases/$guarded_sha/tools/run_ai_recommendation_worker.py --once" \
      >/dev/null || return 1
  fi
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || return 1
    sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
  fi
  activation_snapshot_set_phase "$guarded_sha" finalized || return 1
  activation_snapshot_assert_new_set "$guarded_sha" || return 1
  publish_deployed_receipt_pending "$guarded_sha" || return 1
  activation_snapshot_remove_finalized_before_deploy || return 1
  return 0
}
if [ "$DEPLOY_OPERATION" = recover-database-guard ]; then
  if [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] && \
    { [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = \
        restoring-new-no-receipt ] || \
      [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = \
        new-runtime-preserved-no-receipt ]; }; then
    controlled_v2_forward_preserve_no_receipt_recovery
  elif [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] && \
    [ ! -e "$DATABASE_WRITER_GUARD_FILE" ] && \
    { [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = old-runtime-verified ] || \
      [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = \
        new-runtime-preserved-no-receipt ] || \
      [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = new-runtime-verified ] || \
      [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = finalized ]; }; then
    controlled_activation_snapshot_only_recovery
  elif [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ]; then
    controlled_database_guard_recovery
  elif [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    controlled_database_writer_restore_recovery
  else
    echo "controlled recovery found no persistent guard or restore state" >&2
    exit 2
  fi
  exit 0
fi
: "${EXPECTED_SHA:?EXPECTED_SHA is required}"
: "${RESOLVED_REQUIREMENTS_B64:?RESOLVED_REQUIREMENTS_B64 is required}"
: "${EXPECTED_ADATA_SHA:?EXPECTED_ADATA_SHA is required}"
: "${EXPECTED_ADATA_TREE_SHA256:?EXPECTED_ADATA_TREE_SHA256 is required}"
[[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]]
if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]; then
  : "${EXPECTED_REQUIREMENTS_SHA256:?EXPECTED_REQUIREMENTS_SHA256 is required}"
  [[ "$EXPECTED_REQUIREMENTS_SHA256" =~ ^[0-9a-f]{64}$ ]]
  if [ -n "${EXPECTED_INPUT_LOCK_SHA256:-}" ] && \
    [ "$EXPECTED_INPUT_LOCK_SHA256" != "$EXPECTED_REQUIREMENTS_SHA256" ]; then
    echo "v2 dependency digest aliases differ" >&2
    exit 2
  fi
  EXPECTED_INPUT_LOCK_SHA256="$EXPECTED_REQUIREMENTS_SHA256"
  test -z "${TRUSTED_WHEEL_MANIFEST_B64:-}"
  EXPECTED_WHEEL_MANIFEST_SHA256=""
else
  : "${EXPECTED_INPUT_LOCK_SHA256:?EXPECTED_INPUT_LOCK_SHA256 is required}"
  : "${TRUSTED_WHEEL_MANIFEST_B64:?TRUSTED_WHEEL_MANIFEST_B64 is required}"
  : "${EXPECTED_WHEEL_MANIFEST_SHA256:?EXPECTED_WHEEL_MANIFEST_SHA256 is required}"
  : "${EXPECTED_RELEASE_TREE_SHA256:?EXPECTED_RELEASE_TREE_SHA256 is required}"
  : "${EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256:?EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256 is required}"
  [[ "$EXPECTED_WHEEL_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ ]]
  [[ "$EXPECTED_RELEASE_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]]
  [[ "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" =~ ^[0-9a-f]{64}$ ]]
fi
[[ "$EXPECTED_ADATA_SHA" =~ ^[0-9a-f]{40}$ ]]
[[ "$EXPECTED_ADATA_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]]
[[ "$EXPECTED_INPUT_LOCK_SHA256" =~ ^[0-9a-f]{64}$ ]]
ENGINE_RELEASE_TREE_OID="$(git --git-dir="$CODE_GIT_CACHE" rev-parse \
  "${EXPECTED_SHA}^{tree}")"
[[ "$ENGINE_RELEASE_TREE_OID" =~ ^[0-9a-f]{40,64}$ ]]
ENGINE_RELEASE_TREE_SHA256="$(printf '{"kind":"git-tree","tree":"%s"}' \
  "$ENGINE_RELEASE_TREE_OID" | sha256sum | cut -d' ' -f1)"
[[ "$ENGINE_RELEASE_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]]
if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]; then
  EXPECTED_RELEASE_TREE_SHA256="$ENGINE_RELEASE_TREE_SHA256"
  ENGINE_RELEASE_MANIFEST="$(git --git-dir="$CODE_GIT_CACHE" show \
    "${EXPECTED_SHA}:deploy/production_release.env")"
  test "$(printf '%s\n' "$ENGINE_RELEASE_MANIFEST" | \
    grep -c '^ADAPTER_REGISTRY_SEAL_SHA256=[0-9a-f]\{64\}$')" -eq 1
  EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$(printf '%s\n' \
    "$ENGINE_RELEASE_MANIFEST" | sed -n \
    's/^ADAPTER_REGISTRY_SEAL_SHA256=//p')"
  [[ "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" =~ ^[0-9a-f]{64}$ ]]
  unset ENGINE_RELEASE_MANIFEST
else
  test "$ENGINE_RELEASE_TREE_SHA256" = "$EXPECTED_RELEASE_TREE_SHA256"
fi
LEGACY_LIVE_SHA="$(git rev-parse HEAD)"
PREVIOUS_SHA="$LEGACY_LIVE_SHA"
DEPLOY_MAIN_BASHPID="$BASHPID"
DEPLOY_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RECEIPT_ID="${EXPECTED_SHA}-$(date -u +%Y%m%dT%H%M%SZ)"
sudo mkdir -p "$RECEIPT_DIR"
sudo chown root:root "$RECEIPT_DIR"
sudo chmod 0700 "$RECEIPT_DIR"
render_receipt_file() {
  local status="$1"
  local active_sha="$2"
  local output_file="$3"
  local ended_at
  ended_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '{"schema_version":"probiga.deploy-receipt.v4","status":"%s","expected_sha":"%s","previous_sha":"%s","active_sha":"%s","expected_input_lock_sha256":"%s","previous_input_lock_sha256":"%s","active_input_lock_sha256":"%s","expected_resolved_freeze_sha256":"%s","previous_resolved_freeze_sha256":"%s","active_resolved_freeze_sha256":"%s","expected_wheel_manifest_sha256":"%s","expected_adata_sha":"%s","expected_adata_tree_sha256":"%s","previous_adata_sha":"%s","previous_adata_tree_sha256":"%s","active_adata_sha":"%s","active_adata_tree_sha256":"%s","started_at":"%s","ended_at":"%s"}\n' \
    "$status" "$EXPECTED_SHA" "$PREVIOUS_SHA" "$active_sha" \
    "$EXPECTED_INPUT_LOCK_SHA256" \
    "${PREVIOUS_INPUT_LOCK_SHA256:-}" \
    "${ACTIVE_INPUT_LOCK_SHA256:-}" \
    "${EXPECTED_RESOLVED_FREEZE_SHA256:-}" \
    "${PREVIOUS_RESOLVED_FREEZE_SHA256:-}" \
    "${ACTIVE_RESOLVED_FREEZE_SHA256:-}" \
    "${EXPECTED_WHEEL_MANIFEST_SHA256:-}" "$EXPECTED_ADATA_SHA" \
    "$EXPECTED_ADATA_TREE_SHA256" "${PREVIOUS_ADATA_SHA:-}" \
    "${PREVIOUS_ADATA_TREE_SHA256:-}" "${ACTIVE_ADATA_SHA:-}" \
    "${ACTIVE_ADATA_TREE_SHA256:-}" \
    "$DEPLOY_STARTED_AT" "$ended_at" \
    > "$output_file" || return 1
  return 0
}
write_receipt() {
  local status="$1"
  local active_sha="$2"
  local receipt_tmp
  receipt_tmp="$(sudo mktemp \
    "$RECEIPT_DIR/.${RECEIPT_ID}.XXXXXX")" || return 1
  if ! render_receipt_file "$status" "$active_sha" "$receipt_tmp"; then
    sudo rm -f "$receipt_tmp"
    return 1
  fi
  if ! sudo chmod 0600 "$receipt_tmp" || \
    ! sudo mv -f "$receipt_tmp" "$RECEIPT_DIR/$RECEIPT_ID.json"; then
    sudo rm -f "$receipt_tmp"
    return 1
  fi
}
persist_deployed_receipt_pending() {
  local pending_tmp
  local sha_tmp
  activation_snapshot_validate "$EXPECTED_SHA" >/dev/null || return 1
  activation_snapshot_validate_governance_new || return 1
  pending_tmp="$(mktemp "$ACTIVATION_UNIT_SNAPSHOT_DIR/.receipt.XXXXXX")" || \
    return 1
  sha_tmp="$(mktemp "$ACTIVATION_UNIT_SNAPSHOT_DIR/.receipt-sha.XXXXXX")" || {
    rm -f -- "$pending_tmp"
    return 1
  }
  if ! render_receipt_file DEPLOYED "$EXPECTED_SHA" "$pending_tmp" || \
    ! chown root:root "$pending_tmp" || ! chmod 0600 "$pending_tmp" || \
    ! printf '%s\n' "$(sha256sum "$pending_tmp" | cut -d' ' -f1)" \
      > "$sha_tmp" || ! chown root:root "$sha_tmp" || ! chmod 0600 "$sha_tmp" || \
    ! sync -f "$pending_tmp" || ! sync -f "$sha_tmp" || \
    ! mv -fT "$pending_tmp" "$ACTIVATION_RECEIPT_PENDING" || \
    ! mv -fT "$sha_tmp" "$ACTIVATION_RECEIPT_PENDING_SHA" || \
    ! sync -f "$ACTIVATION_UNIT_SNAPSHOT_DIR"; then
    rm -f -- "$pending_tmp" "$sha_tmp"
    return 1
  fi
  activation_snapshot_validate_receipt_pending "$EXPECTED_SHA" || return 1
  return 0
}
detach_failure_handler_from_transport() {
  # Failure handling must converge even after GitHub closes the SSH transport.
  # Ignore any repeated termination signal and detach output before a builtin or
  # external command can die on SIGPIPE or report a false mutation failure.
  trap '' PIPE TERM INT HUP
  trap - ERR
  set +e
  exec >/dev/null 2>&1
}
precutover_failure() {
  local failed_status="$1"
  local failed_line="$2"
  if [ "$BASHPID" != "$DEPLOY_MAIN_BASHPID" ]; then
    trap - ERR TERM INT HUP
    exit "$failed_status"
  fi
  detach_failure_handler_from_transport
  set +e
  if [ "${DEPLOY_SUCCEEDED:-0}" -eq 1 ]; then
    exit "$failed_status"
  fi
  printf 'deploy_failure phase=preflight line=%s status=%s\n' \
    "$failed_line" "$failed_status" >&2
  write_receipt "PREFLIGHT_FAILED" "$PREVIOUS_SHA" || true
  exit "$failed_status"
}
v2_recovery_failure() {
  local failed_status="$1"
  local failed_line="$2"
  printf 'v2 recovery failed step=%s\n' "${V2_RECOVERY_STEP:-unknown}" >&7 || true
  precutover_failure "$failed_status" "$failed_line"
}
trap 'precutover_failure "$?" "$LINENO"' ERR
trap 'precutover_failure 143 "$LINENO"' TERM
trap 'precutover_failure 130 "$LINENO"' INT
trap 'precutover_failure 129 "$LINENO"' HUP
MAIN_SERVICE=probiga
SERVICE_USER="$(systemctl show -p User --value "$MAIN_SERVICE")"
test -n "$SERVICE_USER"
test "$SERVICE_USER" != root
if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ] && \
  [ -d "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] && \
  [ ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] && \
  [ -f "$ACTIVATION_UNIT_SNAPSHOT_PHASE" ] && \
  [ ! -L "$ACTIVATION_UNIT_SNAPSHOT_PHASE" ] && \
  { [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = new-runtime-verified ] || \
    [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = finalized ]; }; then
  CUTOVER_STEP=v2_forward_finalize_recovery
  trap '' TERM INT HUP
  controlled_v2_forward_finalize_recovery >/dev/null 2>&1
  if [ "$V2_FORWARD_FINALIZED_SHA" = "$EXPECTED_SHA" ] && \
    [ "$V2_FORWARD_FINALIZED_REQUEST_MATCH" -eq 1 ]; then
    trap - ERR TERM INT HUP
    exit 0
  fi
  trap 'precutover_failure 143 "$LINENO"' TERM
  trap 'precutover_failure 130 "$LINENO"' INT
  trap 'precutover_failure 129 "$LINENO"' HUP
fi
if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ] && \
  { [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] || \
    [ -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" ]; } && \
  { [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ] || \
    { [ -f "$ACTIVATION_UNIT_SNAPSHOT_PHASE" ] && \
      [ ! -L "$ACTIVATION_UNIT_SNAPSHOT_PHASE" ] && \
      { [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = old-runtime-verified ] || \
        [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = \
          restoring-new-no-receipt ] || \
        [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = \
          new-runtime-preserved-no-receipt ]; }; }; }; then
  CUTOVER_STEP=v2_rollback_only_recovery
  # This bounded recovery owns a durable journal and must converge after the
  # caller disconnects.  Do not let transport signals interrupt it between
  # fencing and old-runtime verification; ERR still fails closed.
  trap '' TERM INT HUP
  V2_RECOVERY_STEP=dispatch
  # Keep the recovery call out of an if/!/|| condition: Bash disables errexit
  # throughout every nested helper in those contexts.  A dedicated descriptor
  # preserves one sanitized checkpoint while ordinary recovery output remains
  # detached from a transport which may close at any time.
  exec 7>&2
  trap 'v2_recovery_failure "$?" "$LINENO"' ERR
  controlled_v2_rollback_only_recovery >/dev/null 2>&1
  exec 7>&-
  trap 'precutover_failure "$?" "$LINENO"' ERR
  trap 'precutover_failure 143 "$LINENO"' TERM
  trap 'precutover_failure 130 "$LINENO"' INT
  trap 'precutover_failure 129 "$LINENO"' HUP
fi
BUILD_USER=probiga-build
test "$BUILD_USER" != "$SERVICE_USER"
test "$BUILD_USER" != root
if ! id "$BUILD_USER" >/dev/null 2>&1 && \
  [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]; then
  # The stable v2 broker predates the dedicated package-build identity.  It is
  # safe for the root engine to provision this non-login account once; no
  # credential, home directory, role, or service permission is granted.
  /usr/sbin/useradd --system --no-create-home --home-dir /var/empty \
    --shell /usr/sbin/nologin "$BUILD_USER"
fi
id "$BUILD_USER" >/dev/null 2>&1 || {
  echo "dedicated non-login build account probiga-build is required" >&2
  exit 2
}
test "$(id -u "$BUILD_USER")" -ne 0
BUILD_SHELL="$(getent passwd "$BUILD_USER" | cut -d: -f7)"
case "$BUILD_SHELL" in
  /usr/sbin/nologin|/sbin/nologin|/bin/false) ;;
  *) echo "probiga-build must use a non-login shell" >&2; exit 2 ;;
esac
sudo -u "$SERVICE_USER" test ! -w /opt/ProBigA
test "$(systemctl show -p LoadState --value "$MAIN_SERVICE")" = loaded
PREVIOUS_MAIN_ACTIVE_STATE="$(systemctl show \
  -p ActiveState --value "$MAIN_SERVICE")"
PREVIOUS_MAIN_UNIT_FILE_STATE="$(systemctl show \
  -p UnitFileState --value "$MAIN_SERVICE")"
case "$PREVIOUS_MAIN_ACTIVE_STATE" in
  active|inactive) ;;
  *)
    echo "probiga service has unsupported active state" >&2
    exit 2
    ;;
esac
PREVIOUS_MAIN_ENABLED=0
  case "$PREVIOUS_MAIN_UNIT_FILE_STATE" in
    enabled) PREVIOUS_MAIN_ENABLED=1 ;;
    disabled) ;;
    masked|masked-runtime|static|linked|linked-runtime|alias|indirect|generated)
      echo "probiga service unit-file state is intentionally blocked" >&2
      exit 2
      ;;
  *)
    echo "probiga service has unsupported unit-file state" >&2
    exit 2
    ;;
esac
AI_WORKER_SERVICE=probiga-ai-recommendation-worker.service
AI_WORKER_TIMER=probiga-ai-recommendation-worker.timer
AI_WORKER_DROPIN=/etc/systemd/system/probiga-ai-recommendation-worker.service.d/release-runtime.conf
SCHEDULER_UNIT=/etc/systemd/system/probiga-scheduler.service
MAIN_RELEASE_DROPIN=/etc/systemd/system/probiga.service.d/scheduler.conf
if [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] || \
  [ -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" ]; then
  if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    echo "persistent activation transaction requires controlled recovery" >&2
    false
  fi
  activation_snapshot_remove_finalized_before_deploy
fi
if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
  [ -L "$DATABASE_WRITER_GUARD_FILE" ] || \
  [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
  [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
  echo "persistent database writer guard/restore state requires controlled recovery" >&2
  false
fi
LEGACY_MAIN_OVERRIDE_DROPINS=(
  /etc/systemd/system/probiga.service.d/release.conf
  /etc/systemd/system/probiga.service.d/release-path.conf
  /etc/systemd/system/probiga.service.d/release-revision.conf
  /etc/systemd/system/probiga.service.d/zz-probiga-env.conf
)
MAIN_LIMITS_DROPIN=/etc/systemd/system/probiga.service.d/limits.conf
MAIN_MARKET_RADAR_DROPIN=/etc/systemd/system/probiga.service.d/market-radar.conf
MAIN_SERVICE_USER_DROPIN=/etc/systemd/system/probiga.service.d/service-user.conf
LEGACY_SCHEDULER_OVERRIDE_DROPINS=(
  /etc/systemd/system/probiga-scheduler.service.d/release.conf
  /etc/systemd/system/probiga-scheduler.service.d/release-path.conf
  /etc/systemd/system/probiga-scheduler.service.d/release-revision.conf
  /etc/systemd/system/probiga-scheduler.service.d/zz-probiga-env.conf
)
SCHEDULER_LIMITS_DROPIN=/etc/systemd/system/probiga-scheduler.service.d/limits.conf
STATIC_RELEASE_LINK=/opt/ProBigA-current
LEGACY_ADATA_REPOSITORY=/opt/ProBigA/adata
LEGACY_STATE_DIR="$RECEIPT_DIR/legacy-state-$RECEIPT_ID"
quarantine_unsafe_untracked_release_files() {
  declare -A unsafe_paths=()
  local candidate
  local code_root
  local manifest_file
  local relative_path
  local source_path
  local target_path
  local target_parent
  local digest
  local metadata

  collect_untracked_candidate() {
    candidate="$1"
    relative_path="${candidate#./}"
    case "$relative_path" in
      ''|.|/*|..|../*|*/..|*/../*)
        echo "unsafe quarantine path: $relative_path" >&2
        return 2
        ;;
    esac
    if git ls-files --error-unmatch -- "$relative_path" >/dev/null 2>&1; then
      return 0
    fi
    unsafe_paths["$relative_path"]=1
  }

  # Production code/config/evidence roots must contain exactly the Git release.
  # Preserve every non-ignored legacy file from those roots outside the
  # checkout, including harmless-looking .bak files which still make the
  # release identity unverifiable.
  while IFS= read -r -d '' candidate; do
    collect_untracked_candidate "$candidate"
  done < <(git ls-files --others --exclude-standard -z -- \
    server biz integrations tools scripts strategies versions \
    artifacts/trading_v4 artifacts/trading_v5 artifacts/trading_v6 \
    .github deploy requirements-platform.txt .gitattributes .gitignore \
    sitecustomize.py usercustomize.py \
    ':(top,glob)*.py' ':(top,glob)*.pyw' ':(top,glob)*.pyd' \
    ':(top,glob)*.so' ':(top,glob)*/__init__.py' \
    ':(top,glob)*/__init__*.pyc' ':(top,glob)*/__init__*.pyd' \
    ':(top,glob)*/__init__*.so')

  # Git ignores bytecode and some local helpers, but Python can still import
  # them. Include those executable shadows even though status omits them.
  while IFS= read -r -d '' candidate; do
    collect_untracked_candidate "$candidate"
  done < <(find . -maxdepth 1 \( -type f -o -type l \) \
    \( -name '*.py' -o -name '*.pyw' -o -name '*.pyc' \
    -o -name '*.pyo' -o -name '*.pyd' -o -name '*.so' \) -print0)

  for code_root in server biz integrations tools scripts strategies versions; do
    if [ ! -d "$code_root" ]; then
      continue
    fi
    while IFS= read -r -d '' candidate; do
      collect_untracked_candidate "$candidate"
    done < <(find "$code_root" \( -type f -o -type l \) \
      \( -name '*.py' -o -name '*.pyw' -o -name '*.pyc' \
      -o -name '*.pyo' -o -name '*.pyd' -o -name '*.so' \) -print0)
  done

  while IFS= read -r -d '' candidate; do
    collect_untracked_candidate "$candidate"
  done < <(find . -mindepth 2 -maxdepth 2 \( -type f -o -type l \) \
    \( -name '__init__.py' -o -name '*.pyc' -o -name '*.pyo' \
    -o -name '__init__*.pyd' -o -name '__init__*.so' \) -print0)

  if [ "${#unsafe_paths[@]}" -eq 0 ]; then
    return 0
  fi

  mkdir -p "$LEGACY_STATE_DIR/untracked-release-files"
  chown root:root "$LEGACY_STATE_DIR" \
    "$LEGACY_STATE_DIR/untracked-release-files"
  chmod 0700 "$LEGACY_STATE_DIR" \
    "$LEGACY_STATE_DIR/untracked-release-files"
  manifest_file="$LEGACY_STATE_DIR/untracked-release-files.manifest"
  : > "$manifest_file"
  chmod 0600 "$manifest_file"
  for relative_path in "${!unsafe_paths[@]}"; do
    source_path="$REPOSITORY_ROOT/$relative_path"
    target_path="$LEGACY_STATE_DIR/untracked-release-files/$relative_path"
    target_parent="$(dirname "$target_path")"
    mkdir -p "$target_parent"
    chmod 0700 "$target_parent"
    if [ -L "$source_path" ]; then
      digest="$(readlink -- "$source_path" | sha256sum | cut -d' ' -f1)"
    else
      digest="$(sha256sum -- "$source_path" | cut -d' ' -f1)"
    fi
    metadata="$(stat -c '%F|%a|%U:%G|%s|%y' -- "$source_path")"
    printf 'path=%q\tsha256=%s\tstat=%q\n' \
      "$relative_path" "$digest" "$metadata" >> "$manifest_file"
    mv -- "$source_path" "$target_path"
    chown -h root:root -- "$target_path"
    if [ ! -L "$target_path" ]; then
      chmod 0600 -- "$target_path"
    fi
    echo "Quarantined unsafe untracked release file: $relative_path" >&2
  done
  chown -R root:root "$LEGACY_STATE_DIR/untracked-release-files"
  chmod 0600 "$manifest_file"
}
RELEASE_VENV_RETENTION=2
CODE_RELEASE_RETENTION=2

path_is_runtime_referenced() {
  local candidate="$1"
  local proc_dir
  local resolved
  for proc_dir in /proc/[0-9]*; do
    [ -d "$proc_dir" ] || continue
    if [ -r "$proc_dir/cmdline" ] && \
      grep -aFq -- "$candidate" "$proc_dir/cmdline"; then
      return 0
    fi
    if [ -r "$proc_dir/environ" ] && \
      grep -aFq -- "$candidate" "$proc_dir/environ"; then
      return 0
    fi
    resolved="$(readlink -f -- "$proc_dir/exe" 2>/dev/null || true)"
    case "$resolved" in
      "$candidate"|"$candidate"/*) return 0 ;;
    esac
    resolved="$(readlink -f -- "$proc_dir/cwd" 2>/dev/null || true)"
    case "$resolved" in
      "$candidate"|"$candidate"/*) return 0 ;;
    esac
  done
  return 1
}

path_is_opt_link_target() {
  local candidate="$1"
  local link
  local resolved
  for link in /opt/*; do
    [ -L "$link" ] || continue
    resolved="$(readlink -f -- "$link" 2>/dev/null || true)"
    case "$resolved" in
      "$candidate"|"$candidate"/*) return 0 ;;
    esac
  done
  return 1
}

prune_release_venvs() {
  local protected_sha="$1"
  local rollback_sha="${2:-}"
  local release_root_real
  local sha
  local link
  local target
  local build_dir
  local build_real
  local release_scan
  local kept=0
  local removed_bytes=0
  local candidate_bytes=0
  local -a release_shas=()
  declare -A keep_shas=()
  declare -A keep_targets=()

  release_root_real="$(readlink -f -- "$RELEASE_VENV_ROOT")" || return 2
  test "$release_root_real" = "$RELEASE_VENV_ROOT" || return 2
  [[ "$protected_sha" =~ ^[0-9a-f]{40}$ ]] || return 2
  release_scan="$(find "$RELEASE_VENV_ROOT" -mindepth 1 -maxdepth 1 \
    -type l -printf '%T@ %f\n')" || return 2
  mapfile -t release_shas < <(
    printf '%s\n' "$release_scan" | LC_ALL=C sort -nr | awk 'NF {print $2}'
  )

  # The running release is always retained, even if its link timestamp is old.
  link="$RELEASE_VENV_ROOT/$protected_sha"
  test -L "$link" || return 2
  target="$(readlink -f -- "$link")" || return 2
  case "$target" in
    "$RELEASE_VENV_ROOT"/build-*) ;;
    *) echo "protected release venv escaped its immutable root" >&2; return 2 ;;
  esac
  test "$(dirname -- "$target")" = "$RELEASE_VENV_ROOT" || return 2
  keep_shas["$protected_sha"]=1
  keep_targets["$target"]=1
  kept=1

  # Preserve the exact previous immutable venv when it already lives in the
  # external release root. Legacy /opt venvs are outside this cleanup scope.
  if [ -n "$rollback_sha" ] && [ "$rollback_sha" != "$protected_sha" ]; then
    [[ "$rollback_sha" =~ ^[0-9a-f]{40}$ ]] || return 2
  fi
  if [ -n "$rollback_sha" ] && [ "$rollback_sha" != "$protected_sha" ] && \
    [ -L "$RELEASE_VENV_ROOT/$rollback_sha" ]; then
    target="$(readlink -f -- "$RELEASE_VENV_ROOT/$rollback_sha")" || return 2
    case "$target" in
      "$RELEASE_VENV_ROOT"/build-*) ;;
      *) echo "rollback release venv escaped its immutable root" >&2; return 2 ;;
    esac
    test "$(dirname -- "$target")" = "$RELEASE_VENV_ROOT" || return 2
    keep_shas["$rollback_sha"]=1
    keep_targets["$target"]=1
    kept=$((kept + 1))
  fi

  # Keep the newest successful releases until the rollback retention is full.
  for sha in "${release_shas[@]}"; do
    [[ "$sha" =~ ^[0-9a-f]{40}$ ]] || continue
    [ -n "${keep_shas[$sha]:-}" ] && continue
    link="$RELEASE_VENV_ROOT/$sha"
    target="$(readlink -f -- "$link" 2>/dev/null || true)"
    case "$target" in
      "$RELEASE_VENV_ROOT"/build-*) ;;
      *) continue ;;
    esac
    test "$(dirname -- "$target")" = "$RELEASE_VENV_ROOT" || return 2
    if [ "$kept" -lt "$RELEASE_VENV_RETENTION" ]; then
      keep_shas["$sha"]=1
      keep_targets["$target"]=1
      kept=$((kept + 1))
    fi
  done

  # Remove stale SHA links first. Their build directories are handled below.
  for sha in "${release_shas[@]}"; do
    [[ "$sha" =~ ^[0-9a-f]{40}$ ]] || continue
    [ -n "${keep_shas[$sha]:-}" ] && continue
    link="$RELEASE_VENV_ROOT/$sha"
    target="$(readlink -f -- "$link" 2>/dev/null || true)"
    if path_is_runtime_referenced "$link" || \
      { [ -n "$target" ] && path_is_runtime_referenced "$target"; }; then
      echo "Retained runtime-referenced release venv: $sha" >&2
      [ -n "$target" ] && keep_targets["$target"]=1
      continue
    fi
    rm -f -- "$link" || return 2
    echo "Removed stale release venv link: $sha" >&2
  done

  # Every removable target must be a direct build-* child of the immutable root.
  while IFS= read -r -d '' build_dir; do
    build_real="$(readlink -f -- "$build_dir")" || return 2
    case "$build_real" in
      "$RELEASE_VENV_ROOT"/build-*) ;;
      *) echo "refusing unsafe release venv target: $build_real" >&2; return 2 ;;
    esac
    test "$(dirname -- "$build_real")" = "$RELEASE_VENV_ROOT" || return 2
    [ -n "${keep_targets[$build_real]:-}" ] && continue
    if path_is_runtime_referenced "$build_real" || \
      path_is_opt_link_target "$build_real"; then
      echo "Retained referenced release venv target: $build_real" >&2
      continue
    fi
    candidate_bytes="$(du -sb -- "$build_real" | awk '{print $1}')" || return 2
    rm -rf -- "$build_real" || return 2
    removed_bytes=$((removed_bytes + candidate_bytes))
    echo "Removed stale release venv target: $build_real ($candidate_bytes bytes)" >&2
  done < <(find "$RELEASE_VENV_ROOT" -mindepth 1 -maxdepth 1 \
    -type d -name 'build-*' -print0)
  echo "Release venv cleanup reclaimed $removed_bytes bytes" >&2
}

prune_code_releases() {
  local active_root="$1"
  local rollback_root="$2"
  local entry
  local entry_real
  local entry_name
  local removed_bytes=0
  local candidate_bytes=0
  local release_root_real
  local active_name
  local rollback_name
  local unsafe_link

  test ! -L "$CODE_RELEASE_ROOT" || return 2
  release_root_real="$(readlink -f -- "$CODE_RELEASE_ROOT")" || return 2
  test "$release_root_real" = "$CODE_RELEASE_ROOT" || return 2
  active_name="${active_root#"$CODE_RELEASE_ROOT"/}"
  [[ "$active_name" =~ ^[0-9a-f]{40}$ ]] || return 2
  test "$active_root" = "$CODE_RELEASE_ROOT/$active_name" || return 2
  if [ "$rollback_root" != "$REPOSITORY_ROOT" ]; then
    rollback_name="${rollback_root#"$CODE_RELEASE_ROOT"/}"
    [[ "$rollback_name" =~ ^[0-9a-f]{40}$ ]] || return 2
    test "$rollback_root" = "$CODE_RELEASE_ROOT/$rollback_name" || return 2
  fi
  unsafe_link="$(find "$CODE_RELEASE_ROOT" -mindepth 1 -maxdepth 1 \
    -type l -print -quit)" || return 2
  if [ -n "$unsafe_link" ]; then
    echo "refusing symlink inside immutable code release root" >&2
    return 2
  fi
  while IFS= read -r -d '' entry; do
    entry_name="$(basename -- "$entry")"
    if [[ ! "$entry_name" =~ ^[0-9a-f]{40}$ ]] && \
      [[ ! "$entry_name" =~ ^\.build-[0-9a-f]{40}-[0-9]+$ ]]; then
      continue
    fi
    entry_real="$(readlink -f -- "$entry")" || return 2
    test "$(dirname -- "$entry_real")" = "$CODE_RELEASE_ROOT" || return 2
    case "$entry_real" in
      "$active_root"|"$rollback_root") continue ;;
    esac
    if path_is_runtime_referenced "$entry_real" || \
      path_is_opt_link_target "$entry_real"; then
      echo "Retained referenced immutable code release: $entry_real" >&2
      continue
    fi
    candidate_bytes="$(du -sb -- "$entry_real" | awk '{print $1}')" || return 2
    chmod -R u+rwX "$entry_real" 2>/dev/null || true
    if [ -f "$entry_real/.git" ]; then
      git --git-dir="$CODE_GIT_CACHE" worktree remove --force "$entry_real" || \
        return 2
    fi
    if [ -e "$entry_real" ]; then
      rm -rf -- "$entry_real" || return 2
    fi
    removed_bytes=$((removed_bytes + candidate_bytes))
    echo "Removed stale immutable code release: $entry_real ($candidate_bytes bytes)" >&2
  done < <(find "$CODE_RELEASE_ROOT" -mindepth 1 -maxdepth 1 \
    -type d -print0)
  git --git-dir="$CODE_GIT_CACHE" worktree prune || return 2
  echo "Code release cleanup reclaimed $removed_bytes bytes" >&2
}

prune_release_temp_files() {
  local temp_file
  local removed_bytes=0
  local candidate_bytes=0
  while IFS= read -r -d '' temp_file; do
    case "$temp_file" in
      /tmp/probiga-release-*.tar.gz|/tmp/probiga-*.bundle) ;;
      *) echo "refusing unsafe release temp path: $temp_file" >&2; return 2 ;;
    esac
    candidate_bytes="$(stat -c '%s' -- "$temp_file")" || return 2
    rm -f -- "$temp_file" || return 2
    removed_bytes=$((removed_bytes + candidate_bytes))
    echo "Removed stale release temp file: $temp_file ($candidate_bytes bytes)" >&2
  done < <(find /tmp -mindepth 1 -maxdepth 1 -type f \
    \( -name 'probiga-release-*.tar.gz' -o -name 'probiga-*.bundle' \) \
    -mtime +0 -print0)
  echo "Release temp cleanup reclaimed $removed_bytes bytes" >&2
}
assert_service_cannot_write_tree() {
  local tree_root="$1"
  local label="$2"
  local writable_path
  # POSIX symlinks commonly report mode 0777; their effective writability is
  # governed by the resolved target, which is scanned separately by find.
  writable_path="$(find "$tree_root" ! -type l -perm /0222 -print -quit)"
  if [ -n "$writable_path" ]; then
    echo "$label retains a write permission bit: $writable_path" >&2
    return 2
  fi
}
assert_scheduler_triggers_quiescent() {
  local active_state
  local load_state
  local trigger_unit
  local unit_file_state
  for trigger_unit in \
    probiga-scheduler.timer \
    probiga-scheduler.path \
    probiga-scheduler.socket; do
    load_state="$(systemctl show -p LoadState --value "$trigger_unit")" || \
      return 2
    if [ "$load_state" = not-found ]; then
      continue
    fi
    active_state="$(systemctl show -p ActiveState --value "$trigger_unit")" || \
      return 2
    if [ "$active_state" != inactive ]; then
      echo "scheduler activation unit is not quiescent: $trigger_unit ($active_state)" >&2
      return 2
    fi
    unit_file_state="$(systemctl show -p UnitFileState --value "$trigger_unit")" || \
      return 2
    case "$unit_file_state" in
      disabled) ;;
      *)
        echo "scheduler activation unit is enabled: $trigger_unit ($unit_file_state)" >&2
        return 2
        ;;
    esac
  done
}
assert_service_cannot_write_release_paths() {
  local checkout_root="${1:-$REPOSITORY_ROOT}"
  local writable_path
  local writable_root_file
  writable_path="$(sudo -u "$SERVICE_USER" find \
    "$checkout_root/.git" "$checkout_root/.github" \
    "$checkout_root/deploy" "$checkout_root/server" \
    "$checkout_root/biz" "$checkout_root/integrations" \
    "$checkout_root/tools" "$checkout_root/scripts" \
    "$checkout_root/strategies" "$checkout_root/versions" \
    "$checkout_root/artifacts/trading_v4" \
    "$checkout_root/artifacts/trading_v5" \
    "$checkout_root/artifacts/trading_v6" \
    "$checkout_root/requirements-platform.txt" \
    "$checkout_root/.gitattributes" "$checkout_root/.gitignore" \
    -writable -print -quit 2>/dev/null || true)"
  if [ -n "$writable_path" ]; then
    echo "service account can modify protected release paths: $writable_path" >&2
    return 2
  fi
  writable_root_file="$(sudo -u "$SERVICE_USER" find "$checkout_root" -maxdepth 1 \
    -type f \( -name '*.py' -o -name '*.pyw' -o -name '*.pyc' \
    -o -name '*.pyd' -o -name '*.so' \) -writable -print -quit \
    2>/dev/null || true)"
  if [ -n "$writable_root_file" ]; then
    echo "service account can modify root executable code: $writable_root_file" >&2
    return 2
  fi
}
seal_release_checkout() {
  local checkout_root="${1:-$REPOSITORY_ROOT}"
  declare -A tracked_directories=()
  local directory
  local entry
  local git_mode
  local tracked_path
  (
  cd "$checkout_root"
  while IFS= read -r -d '' entry; do
    git_mode="${entry%% *}"
    tracked_path="${entry#*$'\t'}"
    if [ -L "$tracked_path" ]; then
      chown -h root:root -- "$tracked_path"
    else
      chown root:root -- "$tracked_path"
      if [ "$git_mode" = 100755 ]; then
        chmod 0555 -- "$tracked_path"
      else
        chmod 0444 -- "$tracked_path"
      fi
    fi
    directory="$(dirname "$tracked_path")"
    while [ "$directory" != . ]; do
      tracked_directories["$directory"]=1
      directory="$(dirname "$directory")"
    done
  done < <(git ls-files --stage -z)
  for directory in "${!tracked_directories[@]}"; do
    chown root:root -- "$directory"
    chmod 0555 -- "$directory"
  done
  if [ -d .git ]; then
    find .git -type f -exec chown root:root -- {} + \
      -exec chmod 0444 -- {} +
    find .git -type d -exec chown root:root -- {} + \
      -exec chmod 0555 -- {} +
  else
    test -f .git
    chown root:root .git
    chmod 0444 .git
  fi
  )
}
assert_scheduler_triggers_quiescent
write_dropin() {
  local revision="$1"
  local code_root="$2"
  local adata_sha="$3"
  local adata_tree_sha="$4"
  local adata_source="$5"
  local release_tree_sha="$6"
  local adapter_registry_seal_sha="$7"
  local output_file="$8"
  printf '%s\n' \
    '[Service]' \
    'WorkingDirectory=/opt/ProBigA' \
    'ExecStart=' \
    "ExecStart=/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin API_EMBEDDED_SCHEDULER_ENABLED=false PROBIGA_IN_APP_DEPLOY_ENABLED=0 PROBIGA_DEPLOYMENT_MODE=production PROBIGA_ADMIN_AUTH_ENABLED=true GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PROBIGA_EXPECTED_GIT_SHA=$revision PROBIGA_BUILD_COMMIT_SHA=$revision PROBIGA_CODE_ROOT=$code_root PROBIGA_EXPECTED_ADATA_SHA=$adata_sha PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha PROBIGA_ADATA_SOURCE_DIR=$adata_source PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha PYTHONPATH=$adata_source:$code_root $RELEASE_VENV_ROOT/$revision/bin/python -P -m uvicorn server.api.main:app --app-dir $code_root --host 127.0.0.1 --port 8000" \
    'Environment=API_EMBEDDED_SCHEDULER_ENABLED=false' \
    'Environment=PROBIGA_IN_APP_DEPLOY_ENABLED=0' \
    'Environment=PROBIGA_DEPLOYMENT_MODE=production' \
    'Environment=PROBIGA_ADMIN_AUTH_ENABLED=true' \
    'Environment=GIT_OPTIONAL_LOCKS=0' \
    'Environment=PYTHONDONTWRITEBYTECODE=1' \
    'Environment=PYTHONSAFEPATH=1' \
    "Environment=PROBIGA_EXPECTED_GIT_SHA=$revision" \
    "Environment=PROBIGA_BUILD_COMMIT_SHA=$revision" \
    "Environment=PROBIGA_CODE_ROOT=$code_root" \
    "Environment=PROBIGA_EXPECTED_ADATA_SHA=$adata_sha" \
    "Environment=PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha" \
    "Environment=PROBIGA_ADATA_SOURCE_DIR=$adata_source" \
    "Environment=PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha" \
    "Environment=PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha" \
    "Environment=PYTHONPATH=$adata_source:$code_root" \
    > "$output_file"
}
write_scheduler_dropin() {
  local revision="$1"
  local code_root="$2"
  local adata_sha="$3"
  local adata_tree_sha="$4"
  local adata_source="$5"
  local release_tree_sha="$6"
  local adapter_registry_seal_sha="$7"
  local output_file="$8"
  printf '%s\n' \
    '[Unit]' \
    'Description=ProBigA standalone scheduler' \
    'Wants=network-online.target' \
    'After=network-online.target' \
    '' \
    '[Service]' \
    'Type=simple' \
    "User=$SERVICE_USER" \
    "Group=$SERVICE_USER" \
    'WorkingDirectory=/opt/ProBigA' \
    "ExecStart=/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin API_EMBEDDED_SCHEDULER_ENABLED=false PROBIGA_DEPLOYMENT_MODE=production GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PROBIGA_EXPECTED_GIT_SHA=$revision PROBIGA_BUILD_COMMIT_SHA=$revision PROBIGA_CODE_ROOT=$code_root PROBIGA_EXPECTED_ADATA_SHA=$adata_sha PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha PROBIGA_ADATA_SOURCE_DIR=$adata_source PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha PYTHONPATH=$adata_source:$code_root $RELEASE_VENV_ROOT/$revision/bin/python -P $code_root/tools/run_scheduler_daemon.py" \
    'Restart=on-failure' \
    'RestartSec=5s' \
    'Environment=API_EMBEDDED_SCHEDULER_ENABLED=false' \
    'Environment=PROBIGA_DEPLOYMENT_MODE=production' \
    'Environment=GIT_OPTIONAL_LOCKS=0' \
    'Environment=PYTHONDONTWRITEBYTECODE=1' \
    'Environment=PYTHONSAFEPATH=1' \
    "Environment=PROBIGA_EXPECTED_GIT_SHA=$revision" \
    "Environment=PROBIGA_BUILD_COMMIT_SHA=$revision" \
    "Environment=PROBIGA_CODE_ROOT=$code_root" \
    "Environment=PROBIGA_EXPECTED_ADATA_SHA=$adata_sha" \
    "Environment=PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha" \
    "Environment=PROBIGA_ADATA_SOURCE_DIR=$adata_source" \
    "Environment=PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha" \
    "Environment=PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha" \
    "Environment=PYTHONPATH=$adata_source:$code_root" \
    '' \
    '[Install]' \
    'WantedBy=multi-user.target' \
    > "$output_file"
}
write_ai_worker_dropin() {
  local revision="$1"
  local code_root="$2"
  local adata_sha="$3"
  local adata_tree_sha="$4"
  local adata_source="$5"
  local release_tree_sha="$6"
  local adapter_registry_seal_sha="$7"
  local output_file="$8"
  printf '%s\n' \
    '[Service]' \
    "User=$SERVICE_USER" \
    "Group=$SERVICE_USER" \
    'WorkingDirectory=/opt/ProBigA' \
    'ExecStart=' \
    "ExecStart=/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PROBIGA_DEPLOYMENT_MODE=production PROBIGA_EXPECTED_GIT_SHA=$revision PROBIGA_CODE_ROOT=$code_root PROBIGA_EXPECTED_ADATA_SHA=$adata_sha PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha PROBIGA_ADATA_SOURCE_DIR=$adata_source PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha PYTHONPATH=$adata_source:$code_root $RELEASE_VENV_ROOT/$revision/bin/python -P $code_root/tools/run_ai_recommendation_worker.py --once" \
    'Environment=GIT_OPTIONAL_LOCKS=0' \
    'Environment=PYTHONDONTWRITEBYTECODE=1' \
    'Environment=PYTHONSAFEPATH=1' \
    'Environment=PROBIGA_DEPLOYMENT_MODE=production' \
    "Environment=PROBIGA_EXPECTED_GIT_SHA=$revision" \
    "Environment=PROBIGA_CODE_ROOT=$code_root" \
    "Environment=PROBIGA_EXPECTED_ADATA_SHA=$adata_sha" \
    "Environment=PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha" \
    "Environment=PROBIGA_ADATA_SOURCE_DIR=$adata_source" \
    "Environment=PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha" \
    "Environment=PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha" \
    "Environment=PYTHONPATH=$adata_source:$code_root" \
    > "$output_file"
}
assert_ai_worker_runtime() {
  local revision="$1"
  local venv_path="${2:-$RELEASE_VENV_ROOT/$revision}"
  local code_root="${3:-$CODE_RELEASE_ROOT/$revision}"
  local verification_mode="${4:-strict}"
  local release_tree_sha adapter_registry_seal_sha
  local has_attested_identity=0
  case "$verification_mode" in
    strict|legacy-rollback) ;;
    *) return 1 ;;
  esac
  if [ "$revision" = "${EXPECTED_SHA:-}" ] || \
    [ -e "$venv_path/.release-tree.sha256" ] || \
    [ -e "$venv_path/.adapter-registry-seal.sha256" ]; then
    test -f "$venv_path/.release-tree.sha256" || return 1
    test -f "$venv_path/.adapter-registry-seal.sha256" || return 1
    release_tree_sha="$(<"$venv_path/.release-tree.sha256")" || return 1
    adapter_registry_seal_sha="$(
      <"$venv_path/.adapter-registry-seal.sha256"
    )" || return 1
    [[ "$release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    [[ "$adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    has_attested_identity=1
  fi
  test "$(systemctl show -p User --value "$AI_WORKER_SERVICE")" = \
    "$SERVICE_USER" || return 1
  test "$(systemctl show -p Group --value "$AI_WORKER_SERVICE")" = \
    "$SERVICE_USER" || return 1
  test "$(systemctl show -p WorkingDirectory --value "$AI_WORKER_SERVICE")" = \
    /opt/ProBigA || return 1
  systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
    | grep -F -- 'PYTHONDONTWRITEBYTECODE=1' >/dev/null || return 1
  systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
    | grep -F -- 'PYTHONSAFEPATH=1' >/dev/null || return 1
  if ! systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
      | grep -F -- '/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin' \
        >/dev/null; then
    test "$verification_mode" = legacy-rollback || return 1
    test "$revision" != "${EXPECTED_SHA:-}" || return 1
    systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
      | grep -F -- '/usr/bin/env ' >/dev/null || return 1
  fi
  systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
    | grep -F -- "$venv_path/bin/python" >/dev/null || return 1
  systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
    | grep -F -- ' -P ' >/dev/null || return 1
  systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
    | grep -F -- "$code_root/tools/run_ai_recommendation_worker.py --once" \
      >/dev/null || return 1
  if [ "$has_attested_identity" -eq 1 ]; then
    systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
      | grep -F -- "PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha" \
        >/dev/null || return 1
    systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
      | grep -F -- \
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha" \
        >/dev/null || return 1
  fi
  return 0
}
assert_ai_worker_writer_fence() {
  local service_active_state
  local service_unit_file_state
  local timer_active_state
  local timer_unit_file_state
  service_active_state="$(systemctl show -p ActiveState --value \
    "$AI_WORKER_SERVICE")" || return 1
  timer_active_state="$(systemctl show -p ActiveState --value \
    "$AI_WORKER_TIMER")" || return 1
  service_unit_file_state="$(systemctl show -p UnitFileState --value \
    "$AI_WORKER_SERVICE")" || return 1
  timer_unit_file_state="$(systemctl show -p UnitFileState --value \
    "$AI_WORKER_TIMER")" || return 1
  test "$service_active_state" = inactive || return 1
  test "$timer_active_state" = inactive || return 1
  case "$service_unit_file_state" in
    disabled|static) ;;
    *) return 1 ;;
  esac
  test "$timer_unit_file_state" = disabled || return 1
  return 0
}
restore_ai_worker_previous_state() {
  if [ "$PREVIOUS_AI_WORKER_SERVICE_ENABLED" -eq 1 ]; then
    sudo systemctl enable "$AI_WORKER_SERVICE" || return 1
  else
    sudo systemctl disable "$AI_WORKER_SERVICE" || return 1
  fi
  if [ "$PREVIOUS_AI_WORKER_TIMER_ENABLED" -eq 1 ]; then
    sudo systemctl enable "$AI_WORKER_TIMER" || return 1
  else
    sudo systemctl disable "$AI_WORKER_TIMER" || return 1
  fi
  if [ "$PREVIOUS_AI_WORKER_SERVICE_ACTIVE" -eq 1 ]; then
    sudo systemctl start "$AI_WORKER_SERVICE" || return 1
  else
    sudo systemctl stop "$AI_WORKER_SERVICE" || return 1
  fi
  if [ "$PREVIOUS_AI_WORKER_TIMER_ACTIVE" -eq 1 ]; then
    sudo systemctl start "$AI_WORKER_TIMER" || return 1
  else
    sudo systemctl stop "$AI_WORKER_TIMER" || return 1
  fi
  return 0
}
assert_ai_worker_previous_state_restored() {
  local service_active_state
  local service_unit_file_state
  local timer_active_state
  local timer_unit_file_state
  service_active_state="$(systemctl show -p ActiveState --value \
    "$AI_WORKER_SERVICE")" || return 1
  timer_active_state="$(systemctl show -p ActiveState --value \
    "$AI_WORKER_TIMER")" || return 1
  service_unit_file_state="$(systemctl show -p UnitFileState --value \
    "$AI_WORKER_SERVICE")" || return 1
  timer_unit_file_state="$(systemctl show -p UnitFileState --value \
    "$AI_WORKER_TIMER")" || return 1
  test "$service_unit_file_state" = \
    "$PREVIOUS_AI_WORKER_SERVICE_UNIT_FILE_STATE" || return 1
  test "$timer_unit_file_state" = \
    "$PREVIOUS_AI_WORKER_TIMER_UNIT_FILE_STATE" || return 1
  if [ "$PREVIOUS_AI_WORKER_SERVICE_ACTIVE" -eq 1 ]; then
    test "$service_active_state" = active || return 1
  else
    test "$service_active_state" = inactive || return 1
  fi
  if [ "$PREVIOUS_AI_WORKER_TIMER_ACTIVE" -eq 1 ]; then
    test "$timer_active_state" = active || return 1
  else
    test "$timer_active_state" = inactive || return 1
  fi
  return 0
}
assert_database_writer_guard_dropin_file() {
  local dropin="$1"
  test -f "$dropin" || return 1
  test ! -L "$dropin" || return 1
  test "$(stat -c '%U:%G' "$dropin")" = root:root || return 1
  test "$(stat -c '%a' "$dropin")" = 644 || return 1
  test "$(wc -l < "$dropin")" -eq 2 || return 1
  grep -Fx '[Unit]' "$dropin" >/dev/null || return 1
  grep -Fx "ConditionPathExists=!$DATABASE_WRITER_GUARD_FILE" \
    "$dropin" >/dev/null
}
install_database_writer_guard_dropins() {
  controlled_guard_install_dropins
}
assert_unit_has_database_writer_guard() {
  local dropin="$2"
  local unit="$1"
  local dropin_paths
  dropin_paths="$(systemctl show -p DropInPaths --value "$unit")" || return 1
  case " $dropin_paths " in
    *" $dropin "*) ;;
    *) return 1 ;;
  esac
}
assert_database_writer_guard_dropins_loaded() {
  local dropin
  for dropin in "${DATABASE_WRITER_GUARD_DROPINS[@]}"; do
    assert_database_writer_guard_dropin_file "$dropin" || return 1
  done
  assert_unit_has_database_writer_guard \
    "$MAIN_SERVICE" "$MAIN_DATABASE_WRITER_GUARD_DROPIN" || return 1
  if [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
    assert_unit_has_database_writer_guard probiga-scheduler \
      "$SCHEDULER_DATABASE_WRITER_GUARD_DROPIN" || return 1
  fi
  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    assert_unit_has_database_writer_guard "$AI_WORKER_SERVICE" \
      "$AI_SERVICE_DATABASE_WRITER_GUARD_DROPIN" || return 1
    assert_unit_has_database_writer_guard "$AI_WORKER_TIMER" \
      "$AI_TIMER_DATABASE_WRITER_GUARD_DROPIN" || return 1
  fi
}
database_writer_guard_inventory() {
  local ai_service_record=not-found,not-found,not-found
  local ai_timer_record=not-found,not-found,not-found
  local main_record="loaded,$PREVIOUS_MAIN_ACTIVE_STATE,$PREVIOUS_MAIN_UNIT_FILE_STATE"
  local scheduler_record=not-found,not-found,not-found
  case "$SCHEDULER_UNIT_PRESENT" in
    0) ;;
    1)
      scheduler_record="loaded,$PREVIOUS_SCHEDULER_ACTIVE_STATE,$PREVIOUS_SCHEDULER_UNIT_FILE_STATE"
      ;;
    *) return 1 ;;
  esac
  case "$AI_WORKER_UNIT_PRESENT" in
    0) ;;
    1)
      ai_service_record="loaded,$PREVIOUS_AI_WORKER_SERVICE_ACTIVE_STATE,$PREVIOUS_AI_WORKER_SERVICE_UNIT_FILE_STATE"
      ai_timer_record="loaded,$PREVIOUS_AI_WORKER_TIMER_ACTIVE_STATE,$PREVIOUS_AI_WORKER_TIMER_UNIT_FILE_STATE"
      ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_state_record main "$main_record" || return 1
  controlled_guard_assert_state_record scheduler "$scheduler_record" || return 1
  controlled_guard_assert_state_record ai-service "$ai_service_record" || \
    return 1
  controlled_guard_assert_state_record ai-timer "$ai_timer_record" || return 1
  printf '%s %s %s %s\n' \
    "$main_record" "$scheduler_record" "$ai_service_record" "$ai_timer_record"
}
persist_database_writer_restore_journal() {
  local ai_service_record
  local ai_timer_record
  local main_record
  local scheduler_record
  read -r main_record scheduler_record ai_service_record ai_timer_record \
    < <(database_writer_guard_inventory) || return 1
  test ! -L "$DATABASE_WRITER_GUARD_DIR" || return 1
  sudo install -d -o root -g root -m 0700 \
    "$DATABASE_WRITER_GUARD_DIR" || return 1
  controlled_guard_assert_directory || return 1
  controlled_guard_write_restore_file "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  activation_snapshot_create || return 1
  controlled_guard_sync_activation_journal "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  activation_snapshot_validate "$EXPECTED_SHA" >/dev/null || return 1
  return 0
}
persist_database_writer_guard() {
  local ai_service_record
  local ai_timer_record
  local guard_tmp
  local main_record
  local scheduler_record
  read -r main_record scheduler_record ai_service_record ai_timer_record \
    < <(database_writer_guard_inventory) || return 1
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_DIR" || return 1
  sudo install -d -o root -g root -m 0700 \
    "$DATABASE_WRITER_GUARD_DIR" || return 1
  test "$(readlink -f "$DATABASE_WRITER_GUARD_DIR")" = \
    "$DATABASE_WRITER_GUARD_DIR" || return 1
  test "$(stat -c '%U:%G' "$DATABASE_WRITER_GUARD_DIR")" = \
    root:root || return 1
  test "$(stat -c '%a' "$DATABASE_WRITER_GUARD_DIR")" = 700 || return 1
  controlled_guard_assert_restore_file "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  guard_tmp="$(sudo mktemp \
    "$DATABASE_WRITER_GUARD_DIR/.database-migration-unverified.XXXXXX")" || \
    return 1
  if ! printf '%s\n' \
    probiga.database-writer-guard.v2 \
    "release=$EXPECTED_SHA" \
    "main_unit=$main_record" \
    "scheduler_unit=$scheduler_record" \
    "ai_service_unit=$ai_service_record" \
    "ai_timer_unit=$ai_timer_record" \
    | sudo tee "$guard_tmp" >/dev/null || \
    ! sudo chown root:root "$guard_tmp" || \
    ! sudo chmod 0600 "$guard_tmp" || \
    ! sudo mv -fT "$guard_tmp" "$DATABASE_WRITER_GUARD_FILE"; then
    sudo rm -f -- "$guard_tmp"
    return 1
  fi
  test -f "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
  test "$(stat -c '%U:%G' "$DATABASE_WRITER_GUARD_FILE")" = \
    root:root || return 1
  test "$(stat -c '%a' "$DATABASE_WRITER_GUARD_FILE")" = 600 || return 1
  controlled_guard_assert_marker "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  sudo sync -f "$DATABASE_WRITER_GUARD_FILE" || return 1
  sudo sync -f "$DATABASE_WRITER_GUARD_DIR"
}
restore_database_writer_guard_after_cleanup_failure() {
  local ai_service_record
  local ai_timer_record
  local failed=0
  local main_record
  local scheduler_record
  read -r main_record scheduler_record ai_service_record ai_timer_record \
    < <(database_writer_guard_inventory) || return 1
  controlled_guard_recreate_file "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || failed=1
  controlled_guard_install_dropins || failed=1
  sudo systemctl daemon-reload || failed=1
  controlled_guard_force_all_writers_fenced "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || failed=1
  controlled_guard_assert_boundary "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || failed=1
  test "$failed" -eq 0
}
remove_database_writer_guard_after_recovery() {
  local ai_service_record
  local ai_timer_record
  local dropin
  local main_record
  local scheduler_record
  read -r main_record scheduler_record ai_service_record ai_timer_record \
    < <(database_writer_guard_inventory) || return 1
  controlled_guard_assert_marker "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  controlled_guard_sync_activation_journal "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  for dropin in "${DATABASE_WRITER_GUARD_DROPINS[@]}"; do
    assert_database_writer_guard_dropin_file "$dropin" || return 1
  done
  assert_database_writer_guard_dropins_loaded || return 1
  if ! sudo rm -f -- "$DATABASE_WRITER_GUARD_FILE" || \
    ! sudo sync -f "$DATABASE_WRITER_GUARD_DIR"; then
    restore_database_writer_guard_after_cleanup_failure || true
    return 1
  fi
  if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ] || \
    ! controlled_guard_assert_restore_file "$EXPECTED_SHA" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    restore_database_writer_guard_after_cleanup_failure || true
    return 1
  fi
  for dropin in "${DATABASE_WRITER_GUARD_DROPINS[@]}"; do
    if ! assert_database_writer_guard_dropin_file "$dropin"; then
      restore_database_writer_guard_after_cleanup_failure || true
      return 1
    fi
  done
  if ! assert_database_writer_guard_dropins_loaded; then
    restore_database_writer_guard_after_cleanup_failure || true
    return 1
  fi
  if ! controlled_guard_assert_dropin_boundary \
    "${scheduler_record%%,*}" "${ai_service_record%%,*}" \
    "${ai_timer_record%%,*}"; then
    restore_database_writer_guard_after_cleanup_failure || true
    return 1
  fi
}
point_static_release_to_checkout() {
  local checkout_root="${1:-$REPOSITORY_ROOT}"
  local link_build
  link_build="$(sudo mktemp -d /opt/.probiga-static-link.XXXXXX)"
  sudo ln -s "$checkout_root" "$link_build/current"
  sudo mv -Tf "$link_build/current" "$STATIC_RELEASE_LINK"
  sudo rmdir "$link_build"
  test -L "$STATIC_RELEASE_LINK"
  test "$(readlink -f "$STATIC_RELEASE_LINK")" = "$checkout_root"
}
assert_nginx_static_matches_checkout() {
  local checkout_root="${1:-$REPOSITORY_ROOT}"
  local asset
  local response
  test -L "$STATIC_RELEASE_LINK" || return 1
  test "$(readlink -f "$STATIC_RELEASE_LINK")" = "$checkout_root" || \
    return 1
  for asset in js/app.js css/style.css; do
    response="$(mktemp)" || return 1
    if ! curl --fail --silent --show-error \
      -H 'Cache-Control: no-cache' \
      "http://127.0.0.1/static/$asset" > "$response"; then
      rm -f "$response" || true
      return 1
    fi
    if ! cmp --silent "$checkout_root/server/static/$asset" "$response"; then
      rm -f "$response" || true
      echo "Nginx served stale static asset: $asset" >&2
      return 1
    fi
    rm -f "$response" || return 1
  done
  return 0
}
release_identity_check() {
  local require_clean="$1"
  local checkout_root="${2:-$REPOSITORY_ROOT}"
  (
  cd "$checkout_root"
  sudo -u "$SERVICE_USER" /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    GIT_OPTIONAL_LOCKS=0 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONSAFEPATH=1 \
    PROBIGA_DEPLOYMENT_MODE=production \
    PROBIGA_EXPECTED_GIT_SHA="$EXPECTED_SHA" \
    PROBIGA_CODE_ROOT="$checkout_root" \
    PROBIGA_EXPECTED_ADATA_SHA="$EXPECTED_ADATA_SHA" \
    PROBIGA_EXPECTED_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256" \
    PROBIGA_ADATA_SOURCE_DIR="$ADATA_SOURCE" \
    PROBIGA_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256" \
    PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
    PYTHONPATH="$ADATA_SOURCE:$checkout_root" \
    PROBIGA_RELEASE_IDENTITY_REQUIRE_CLEAN="$require_clean" \
    "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P -c \
    'import json, os; from server.api.routers.health import _deployed_git_revision; info = _deployed_git_revision(); print(json.dumps(info, ensure_ascii=True, sort_keys=True)); raise SystemExit(2 if os.environ["PROBIGA_RELEASE_IDENTITY_REQUIRE_CLEAN"] == "1" and (info.get("matches_expected") is not True or info.get("code_worktree_clean") is not True) else 0)'
  )
}
BOOTSTRAP_PYTHON=/usr/bin/python3.14
test -x "$BOOTSTRAP_PYTHON"
test "$(stat -c '%U' "$BOOTSTRAP_PYTHON")" = root
sudo -u "$SERVICE_USER" test ! -w "$BOOTSTRAP_PYTHON"
test "$($BOOTSTRAP_PYTHON -I -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" = "3.14"
CUTOVER_STARTED=0
CUTOVER_STEP=preparation
API_STOPPED=0
DEPLOY_SUCCEEDED=0
NEW_VENV_LINK=0
STAGING_WORKTREE=""
PREPARED_CODE_ROOT="$CODE_RELEASE_ROOT/$EXPECTED_SHA"
CODE_VALIDATION_ROOT=""
NEW_CODE_RELEASE=0
SCHEDULER_UNIT_TOUCHED=0
ADATA_CACHE_BUILD=""
ADATA_SOURCE_BUILD=""
ADATA_BUILD_SOURCE=""
ADATA_WHEEL_DIR=""
EXPECTED_BUILD=""
EXPECTED_VENV_TARGET=""
RESOLVED_LOCK=""
TRUSTED_WHEEL_MANIFEST=""
TRUSTED_WHEELHOUSE=""
HEALTH_RESPONSE=""
PREPARED_MAIN_DROPIN=""
PREPARED_SCHEDULER_DROPIN=""
PREPARED_AI_WORKER_DROPIN=""
PREVIOUS_DROPIN=""
PREVIOUS_LEGACY_MAIN_DROPIN_DIR=""
declare -a PREVIOUS_LEGACY_MAIN_DROPINS=()
PREVIOUS_SCHEDULER_DROPIN=""
PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR=""
declare -a PREVIOUS_LEGACY_SCHEDULER_DROPINS=()
PREVIOUS_AI_WORKER_DROPIN=""
PREVIOUS_LOCK_SNAPSHOT=""
GOVERNANCE_TASK_OLD_SOURCE=""
GOVERNANCE_TASK_NEW_SOURCE=""
GOVERNANCE_TASK_TOUCHED=0
GOVERNANCE_INPUT_NOT_READY=0
QMT_HISTORY_PREFLIGHT_OUTPUT=""
QMT_HISTORY_WINDOW=""
DATABASE_FORWARD_MIGRATION_STARTED=0
cleanup_prepare_artifacts() {
  [ -z "$PREVIOUS_DROPIN" ] || rm -f -- "$PREVIOUS_DROPIN"
  [ -z "$PREVIOUS_LEGACY_MAIN_DROPIN_DIR" ] || \
    rm -rf -- "$PREVIOUS_LEGACY_MAIN_DROPIN_DIR"
  [ -z "$PREVIOUS_SCHEDULER_DROPIN" ] || \
    rm -f -- "$PREVIOUS_SCHEDULER_DROPIN"
  [ -z "$PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR" ] || \
    rm -rf -- "$PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR"
  [ -z "$PREVIOUS_AI_WORKER_DROPIN" ] || \
    rm -f -- "$PREVIOUS_AI_WORKER_DROPIN"
  [ -z "$PREVIOUS_LOCK_SNAPSHOT" ] || rm -f -- "$PREVIOUS_LOCK_SNAPSHOT"
  [ -z "$GOVERNANCE_TASK_OLD_SOURCE" ] || \
    rm -f -- "$GOVERNANCE_TASK_OLD_SOURCE"
  [ -z "$GOVERNANCE_TASK_NEW_SOURCE" ] || \
    rm -f -- "$GOVERNANCE_TASK_NEW_SOURCE"
  [ -z "$PREPARED_MAIN_DROPIN" ] || rm -f -- "$PREPARED_MAIN_DROPIN"
  [ -z "$PREPARED_SCHEDULER_DROPIN" ] || \
    rm -f -- "$PREPARED_SCHEDULER_DROPIN"
  [ -z "$PREPARED_AI_WORKER_DROPIN" ] || \
    rm -f -- "$PREPARED_AI_WORKER_DROPIN"
}
PREVIOUS_DROPIN="$(mktemp)"
PREVIOUS_DROPIN_PRESENT=0
if sudo test -f "$MAIN_RELEASE_DROPIN"; then
  sudo cat "$MAIN_RELEASE_DROPIN" > "$PREVIOUS_DROPIN"
  PREVIOUS_DROPIN_PRESENT=1
fi
PREVIOUS_LEGACY_MAIN_DROPIN_DIR="$(mktemp -d)"
for legacy_main_dropin in "${LEGACY_MAIN_OVERRIDE_DROPINS[@]}"; do
  if sudo test -f "$legacy_main_dropin"; then
    sudo cat "$legacy_main_dropin" > \
      "$PREVIOUS_LEGACY_MAIN_DROPIN_DIR/$(basename "$legacy_main_dropin")"
    PREVIOUS_LEGACY_MAIN_DROPINS+=("$legacy_main_dropin")
  fi
done
PREVIOUS_SCHEDULER_DROPIN="$(mktemp)"
PREVIOUS_SCHEDULER_DROPIN_PRESENT=0
if sudo test -f "$SCHEDULER_UNIT"; then
  sudo cat "$SCHEDULER_UNIT" > "$PREVIOUS_SCHEDULER_DROPIN"
  PREVIOUS_SCHEDULER_DROPIN_PRESENT=1
fi
PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR="$(mktemp -d)"
for legacy_scheduler_dropin in "${LEGACY_SCHEDULER_OVERRIDE_DROPINS[@]}"; do
  if sudo test -f "$legacy_scheduler_dropin"; then
    sudo cat "$legacy_scheduler_dropin" > \
      "$PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR/$(basename "$legacy_scheduler_dropin")"
    PREVIOUS_LEGACY_SCHEDULER_DROPINS+=("$legacy_scheduler_dropin")
  fi
done
PREVIOUS_AI_WORKER_DROPIN="$(mktemp)"
PREVIOUS_AI_WORKER_DROPIN_PRESENT=0
if sudo test -f "$AI_WORKER_DROPIN"; then
  sudo cat "$AI_WORKER_DROPIN" > "$PREVIOUS_AI_WORKER_DROPIN"
  PREVIOUS_AI_WORKER_DROPIN_PRESENT=1
fi
dropin_environment_value() {
  local name="$1"
  sed -n "s|^Environment=$name=||p" "$PREVIOUS_DROPIN" | tail -n 1
}
PREVIOUS_MAIN_PID="$(systemctl show "$MAIN_SERVICE" --property=MainPID --value)"
case "$PREVIOUS_MAIN_PID" in
  ''|0|*[!0-9]*)
    PREVIOUS_MAIN_STATE="$(systemctl show "$MAIN_SERVICE" \
      --property=ActiveState --value)"
    case "$PREVIOUS_MAIN_STATE" in
      inactive|failed) ;;
      *)
        echo "probiga service is $PREVIOUS_MAIN_STATE without a valid main PID" >&2
        exit 2
        ;;
    esac
    if [ "$PREVIOUS_DROPIN_PRESENT" -ne 1 ] || \
      ! grep -Fq 'API_EMBEDDED_SCHEDULER_ENABLED=false' \
        "$PREVIOUS_DROPIN"; then
      echo "stopped probiga service has no safe immutable runtime definition" >&2
      exit 2
    fi
    if systemctl is-active --quiet probiga-scheduler || \
      systemctl is-enabled --quiet probiga-scheduler; then
      echo "refusing API recovery while probiga-scheduler is active or enabled" >&2
      exit 2
    fi
    echo "Recovering stopped probiga API before deployment preflight" >&2
    sudo systemctl start "$MAIN_SERVICE"
    systemctl is-active --quiet "$MAIN_SERVICE"
    curl --fail --silent --show-error --retry 15 --retry-all-errors \
      --retry-delay 2 --retry-connrefused \
      http://127.0.0.1/api/health/runtime >/dev/null
    PREVIOUS_MAIN_PID="$(systemctl show "$MAIN_SERVICE" \
      --property=MainPID --value)"
    case "$PREVIOUS_MAIN_PID" in
      ''|0|*[!0-9]*)
        echo "recovered probiga service did not expose a valid main PID" >&2
        exit 2
        ;;
    esac
    ;;
esac
runtime_environment_value() {
  local name="$1"
  tr '\0' '\n' < "/proc/$PREVIOUS_MAIN_PID/environ" \
    | sed -n "s|^$name=||p" \
    | tail -n 1
}
PREVIOUS_RELEASE_REVISION="$(runtime_environment_value PROBIGA_EXPECTED_GIT_SHA)"
if [ -n "$PREVIOUS_RELEASE_REVISION" ]; then
  [[ "$PREVIOUS_RELEASE_REVISION" =~ ^[0-9a-f]{40}$ ]]
fi
PREVIOUS_VENV=""
if [ -z "$PREVIOUS_RELEASE_REVISION" ]; then
  for candidate_root in "$RELEASE_VENV_ROOT" "$LEGACY_RELEASE_VENV_ROOT"; do
    candidate_revision="$(sed -n \
      "s|^ExecStart=.*$candidate_root/\([0-9a-f]\{40\}\)/bin/python .*|\1|p" \
      "$PREVIOUS_DROPIN" | tail -n 1)"
    if [ -n "$candidate_revision" ]; then
      test -z "$PREVIOUS_RELEASE_REVISION"
      PREVIOUS_RELEASE_REVISION="$candidate_revision"
      PREVIOUS_VENV="$candidate_root/$candidate_revision"
    fi
  done
else
  for candidate_root in "$RELEASE_VENV_ROOT" "$LEGACY_RELEASE_VENV_ROOT"; do
    if [ -L "$candidate_root/$PREVIOUS_RELEASE_REVISION" ]; then
      PREVIOUS_VENV="$candidate_root/$PREVIOUS_RELEASE_REVISION"
      break
    fi
  done
  if [ -z "$PREVIOUS_VENV" ]; then
    runtime_python_argv0="$(tr '\0' '\n' \
      < "/proc/$PREVIOUS_MAIN_PID/cmdline" | head -n 1)"
    case "$runtime_python_argv0" in
      "$RELEASE_VENV_ROOT"/[0-9a-f][0-9a-f]*/bin/python|\
      "$LEGACY_RELEASE_VENV_ROOT"/[0-9a-f][0-9a-f]*/bin/python)
        runtime_argv0_venv="$(dirname "$(dirname "$runtime_python_argv0")")"
        runtime_argv0_revision="$(basename "$runtime_argv0_venv")"
        [[ "$runtime_argv0_revision" =~ ^[0-9a-f]{40}$ ]]
        if [ -L "$runtime_argv0_venv" ] && \
          [ -x "$runtime_argv0_venv/bin/python" ]; then
          PREVIOUS_VENV="$runtime_argv0_venv"
        fi
        ;;
    esac
  fi
  if [ -z "$PREVIOUS_VENV" ]; then
    running_venv=""
    mapfile -t matching_venvs < <(
      find "$RELEASE_VENV_ROOT" "$LEGACY_RELEASE_VENV_ROOT" \
        -mindepth 1 -maxdepth 1 -type d \
        -name "build-$PREVIOUS_RELEASE_REVISION-*" -print
    )
    if [ "${#matching_venvs[@]}" -eq 1 ]; then
      running_venv="${matching_venvs[0]}"
    else
      for candidate_venv in "${matching_venvs[@]}"; do
        if grep -aFq -- "$candidate_venv" \
          "/proc/$PREVIOUS_MAIN_PID/maps"; then
          test -z "$running_venv"
          running_venv="$candidate_venv"
        fi
      done
    fi
    test -n "$running_venv"
    test -x "$running_venv/bin/python"
    test "$(stat -c '%U' "$running_venv")" = root
    candidate_root="$(dirname "$running_venv")"
    recovered_link="$candidate_root/.recover-$PREVIOUS_RELEASE_REVISION-$$"
    test ! -e "$candidate_root/$PREVIOUS_RELEASE_REVISION"
    test ! -L "$candidate_root/$PREVIOUS_RELEASE_REVISION"
    ln -s "$running_venv" "$recovered_link"
    mv -T "$recovered_link" "$candidate_root/$PREVIOUS_RELEASE_REVISION"
    PREVIOUS_VENV="$candidate_root/$PREVIOUS_RELEASE_REVISION"
    echo "Recovered active release venv link for $PREVIOUS_RELEASE_REVISION" >&2
  fi
  test -n "$PREVIOUS_VENV"
fi
PREVIOUS_INPUT_LOCK_SHA256=""
PREVIOUS_RESOLVED_FREEZE_SHA256=""
if [ -n "$PREVIOUS_RELEASE_REVISION" ]; then
  PREVIOUS_SHA="$PREVIOUS_RELEASE_REVISION"
  test -L "$PREVIOUS_VENV"
  PREVIOUS_VENV_TARGET="$(readlink -f "$PREVIOUS_VENV")"
  case "$PREVIOUS_VENV_TARGET" in
    "$RELEASE_VENV_ROOT"/build-*|"$LEGACY_RELEASE_VENV_ROOT"/build-*) ;;
    *) echo "previous release venv escaped its immutable root" >&2; exit 2 ;;
  esac
  test "$(dirname "$PREVIOUS_VENV_TARGET")" = "$(dirname "$PREVIOUS_VENV")"
  test -x "$PREVIOUS_VENV/bin/python"
  if [ -f "$PREVIOUS_VENV/.requirements.input.sha256" ]; then
    PREVIOUS_INPUT_LOCK_SHA256="$(cat \
      "$PREVIOUS_VENV/.requirements.input.sha256")"
    [[ "$PREVIOUS_INPUT_LOCK_SHA256" =~ ^[0-9a-f]{64}$ ]]
  fi
  if [ -f "$PREVIOUS_VENV/.requirements.freeze.sha256" ]; then
    PREVIOUS_RESOLVED_FREEZE_SHA256="$(cat \
      "$PREVIOUS_VENV/.requirements.freeze.sha256")"
    [[ "$PREVIOUS_RESOLVED_FREEZE_SHA256" =~ ^[0-9a-f]{64}$ ]]
  fi
  PREVIOUS_LOCK_SNAPSHOT="$(mktemp)"
  PYTHONDONTWRITEBYTECODE=1 "$PREVIOUS_VENV/bin/python" \
    -m pip freeze --all --exclude-editable \
    | awk 'tolower($0) !~ /^adata([[:space:]]|==|@)/' \
    | LC_ALL=C sort > "$PREVIOUS_LOCK_SNAPSHOT"
  if [ -n "$PREVIOUS_RESOLVED_FREEZE_SHA256" ]; then
    test "$(sha256sum "$PREVIOUS_LOCK_SNAPSHOT" | cut -d' ' -f1)" = \
      "$PREVIOUS_RESOLVED_FREEZE_SHA256"
  fi
  rm -f "$PREVIOUS_LOCK_SNAPSHOT"
  assert_service_cannot_write_tree "$PREVIOUS_VENV_TARGET" \
    "previous release virtual environment"
fi
PREVIOUS_CODE_ROOT="$(dropin_environment_value PROBIGA_CODE_ROOT)"
if [ -z "$PREVIOUS_CODE_ROOT" ]; then
  PREVIOUS_CODE_ROOT="$(runtime_environment_value PROBIGA_CODE_ROOT)"
fi
if [ -z "$PREVIOUS_CODE_ROOT" ] && \
  [ "$PREVIOUS_SHA" = "$LEGACY_LIVE_SHA" ]; then
  PREVIOUS_CODE_ROOT="$REPOSITORY_ROOT"
fi
case "$PREVIOUS_CODE_ROOT" in
  "$REPOSITORY_ROOT")
    echo "legacy mutable code checkout cannot be used as a rollback seed; migrate the active runtime to a sealed /opt/ProBigA-releases/<sha> release out of band" >&2
    exit 2
    ;;
  "$CODE_RELEASE_ROOT/$PREVIOUS_SHA")
    test ! -L "$PREVIOUS_CODE_ROOT"
    test -d "$PREVIOUS_CODE_ROOT"
    test "$(git -C "$PREVIOUS_CODE_ROOT" rev-parse HEAD)" = "$PREVIOUS_SHA"
    assert_service_cannot_write_release_paths "$PREVIOUS_CODE_ROOT"
    ;;
  *)
    echo "previous code root escaped immutable release storage" >&2
    exit 2
    ;;
esac
PREVIOUS_ADATA_SHA="$(dropin_environment_value PROBIGA_EXPECTED_ADATA_SHA)"
PREVIOUS_ADATA_TREE_SHA256="$(dropin_environment_value PROBIGA_EXPECTED_ADATA_TREE_SHA256)"
PREVIOUS_ADATA_SOURCE="$(dropin_environment_value PROBIGA_ADATA_SOURCE_DIR)"
if [ -z "$PREVIOUS_ADATA_SHA" ]; then
  PREVIOUS_ADATA_SHA="$(runtime_environment_value PROBIGA_EXPECTED_ADATA_SHA)"
fi
if [ -z "$PREVIOUS_ADATA_TREE_SHA256" ]; then
  PREVIOUS_ADATA_TREE_SHA256="$(runtime_environment_value PROBIGA_EXPECTED_ADATA_TREE_SHA256)"
fi
if [ -z "$PREVIOUS_ADATA_SOURCE" ]; then
  PREVIOUS_ADATA_SOURCE="$(runtime_environment_value PROBIGA_ADATA_SOURCE_DIR)"
fi
if [ -n "$PREVIOUS_ADATA_SHA$PREVIOUS_ADATA_TREE_SHA256$PREVIOUS_ADATA_SOURCE" ]; then
  [[ "$PREVIOUS_ADATA_SHA" =~ ^[0-9a-f]{40}$ ]]
  [[ "$PREVIOUS_ADATA_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]]
  test -d "$PREVIOUS_ADATA_SOURCE"
  test "$(cat "$PREVIOUS_ADATA_SOURCE/.probiga-adata.gitsha")" = \
    "$PREVIOUS_ADATA_SHA"
  test "$(cat "$PREVIOUS_ADATA_SOURCE/.probiga-adata.tree.sha256")" = \
    "$PREVIOUS_ADATA_TREE_SHA256"
  sudo -u "$SERVICE_USER" test ! -w "$PREVIOUS_ADATA_SOURCE"
  sudo -u "$SERVICE_USER" test ! -w "$(dirname "$PREVIOUS_ADATA_SOURCE")"
  sudo -u "$SERVICE_USER" test ! -w "$(dirname "$(dirname "$PREVIOUS_ADATA_SOURCE")")"
  sudo -u "$SERVICE_USER" test ! -w \
    "$(dirname "$(dirname "$(dirname "$PREVIOUS_ADATA_SOURCE")")")"
else
  echo "legacy mutable adata checkout cannot be used as a rollback seed" >&2
  exit 2
fi
SCHEDULER_UNIT_PRESENT=0
PREVIOUS_SCHEDULER_ACTIVE=0
PREVIOUS_SCHEDULER_ENABLED=0
PREVIOUS_SCHEDULER_ACTIVE_STATE=not-found
PREVIOUS_SCHEDULER_UNIT_FILE_STATE=not-found
if systemctl list-unit-files probiga-scheduler.service --no-legend \
  | grep -q '^probiga-scheduler.service'; then
  SCHEDULER_UNIT_PRESENT=1
  test "$(systemctl show -p LoadState --value probiga-scheduler)" = loaded
  PREVIOUS_SCHEDULER_ACTIVE_STATE="$(systemctl show \
    -p ActiveState --value probiga-scheduler)"
  PREVIOUS_SCHEDULER_UNIT_FILE_STATE="$(systemctl show \
    -p UnitFileState --value probiga-scheduler)"
  case "$PREVIOUS_SCHEDULER_ACTIVE_STATE" in
    active) PREVIOUS_SCHEDULER_ACTIVE=1 ;;
    inactive) ;;
    *)
      echo "probiga-scheduler has unsupported active state" >&2
      exit 2
      ;;
  esac
  case "$PREVIOUS_SCHEDULER_UNIT_FILE_STATE" in
    enabled) PREVIOUS_SCHEDULER_ENABLED=1 ;;
    disabled) ;;
    masked|masked-runtime|static|linked|linked-runtime|alias|indirect|generated)
      echo "probiga-scheduler unit-file state is intentionally blocked" >&2
      exit 2
      ;;
    *)
      echo "probiga-scheduler has unsupported unit-file state" >&2
      exit 2
      ;;
  esac
fi
EXTERNAL_WRITER_BLOCKED=0
DATABASE_GUARD_MIGRATION_UNVERIFIED=0
DATABASE_WRITER_GUARD_PERSISTED=0
DATABASE_WRITER_RESTORE_PERSISTED=0
AI_WORKER_UNIT_PRESENT=0
PREVIOUS_AI_WORKER_SERVICE_ACTIVE=0
PREVIOUS_AI_WORKER_SERVICE_ENABLED=0
PREVIOUS_AI_WORKER_SERVICE_ACTIVE_STATE=not-found
PREVIOUS_AI_WORKER_SERVICE_UNIT_FILE_STATE=not-found
PREVIOUS_AI_WORKER_TIMER_ACTIVE=0
PREVIOUS_AI_WORKER_TIMER_ENABLED=0
PREVIOUS_AI_WORKER_TIMER_ACTIVE_STATE=not-found
PREVIOUS_AI_WORKER_TIMER_UNIT_FILE_STATE=not-found
AI_WORKER_SERVICE_LOAD="$(systemctl show -p LoadState --value \
  "$AI_WORKER_SERVICE")" || {
  echo "AI worker service inventory failed" >&2
  exit 2
}
AI_WORKER_TIMER_LOAD="$(systemctl show -p LoadState --value \
  "$AI_WORKER_TIMER")" || {
  echo "AI worker timer inventory failed" >&2
  exit 2
}
case "$AI_WORKER_SERVICE_LOAD:$AI_WORKER_TIMER_LOAD" in
  not-found:not-found) ;;
  loaded:loaded)
  AI_WORKER_UNIT_PRESENT=1
  PREVIOUS_AI_WORKER_SERVICE_ACTIVE_STATE="$(systemctl show \
    -p ActiveState --value "$AI_WORKER_SERVICE")"
  PREVIOUS_AI_WORKER_TIMER_ACTIVE_STATE="$(systemctl show \
    -p ActiveState --value "$AI_WORKER_TIMER")"
  PREVIOUS_AI_WORKER_SERVICE_UNIT_FILE_STATE="$(systemctl show \
    -p UnitFileState --value "$AI_WORKER_SERVICE")"
  PREVIOUS_AI_WORKER_TIMER_UNIT_FILE_STATE="$(systemctl show \
    -p UnitFileState --value "$AI_WORKER_TIMER")"
  case "$PREVIOUS_AI_WORKER_SERVICE_ACTIVE_STATE" in
    active) PREVIOUS_AI_WORKER_SERVICE_ACTIVE=1 ;;
    inactive) ;;
    *)
      echo "AI worker service has unsupported active state" >&2
      exit 2
      ;;
  esac
  case "$PREVIOUS_AI_WORKER_TIMER_ACTIVE_STATE" in
    active) PREVIOUS_AI_WORKER_TIMER_ACTIVE=1 ;;
    inactive) ;;
    *)
      echo "AI worker timer has unsupported active state" >&2
      exit 2
      ;;
  esac
  case "$PREVIOUS_AI_WORKER_SERVICE_UNIT_FILE_STATE" in
    enabled) PREVIOUS_AI_WORKER_SERVICE_ENABLED=1 ;;
    disabled|static) ;;
    masked|masked-runtime|linked|linked-runtime|alias|indirect|generated)
      echo "AI worker service unit-file state is intentionally blocked" >&2
      exit 2
      ;;
    *)
      echo "AI worker service has unsupported unit-file state" >&2
      exit 2
      ;;
  esac
  case "$PREVIOUS_AI_WORKER_TIMER_UNIT_FILE_STATE" in
    enabled) PREVIOUS_AI_WORKER_TIMER_ENABLED=1 ;;
    disabled) ;;
    masked|masked-runtime|static|linked|linked-runtime|alias|indirect|generated)
      echo "AI worker timer unit-file state is intentionally blocked" >&2
      exit 2
      ;;
    *)
      echo "AI worker timer has unsupported unit-file state" >&2
      exit 2
      ;;
  esac
  ;;
  *)
    echo "AI worker service/timer inventory is inconsistent or unsupported" >&2
    exit 2
    ;;
esac
CODE_REPOSITORY_URL=git@github.com:MingMG/probiga.git
ADATA_REPOSITORY_URL=https://github.com/1nchaos/adata.git
ADATA_GIT_CACHE=/var/lib/probiga/release-sources/adata.git
cleanup_staging_worktree() {
  if [ -n "$STAGING_WORKTREE" ]; then
    case "$STAGING_WORKTREE" in
      "$CODE_RELEASE_ROOT"/.build-"$EXPECTED_SHA"-*) ;;
      *) echo "refusing unsafe staging cleanup: $STAGING_WORKTREE" >&2; return 2 ;;
    esac
    test "$(dirname -- "$STAGING_WORKTREE")" = "$CODE_RELEASE_ROOT" || \
      return 2
    chmod -R u+rwX "$STAGING_WORKTREE" 2>/dev/null || true
    git --git-dir="$CODE_GIT_CACHE" worktree remove --force \
      "$STAGING_WORKTREE" 2>/dev/null || true
    rm -rf -- "$STAGING_WORKTREE" || return 2
    STAGING_WORKTREE=""
  fi
}
cleanup_prepare_artifacts() {
  local build_target=""
  local venv_in_use=0
  cleanup_staging_worktree || true
  [ -z "$RESOLVED_LOCK" ] || rm -f -- "$RESOLVED_LOCK"
  [ -z "$TRUSTED_WHEEL_MANIFEST" ] || rm -f -- "$TRUSTED_WHEEL_MANIFEST"
  if [ -n "$TRUSTED_WHEELHOUSE" ]; then
    case "$TRUSTED_WHEELHOUSE" in
      /var/lib/probiga/release-artifacts/.wheelhouse-*) \
        rm -rf -- "$TRUSTED_WHEELHOUSE" ;;
    esac
  fi
  [ -z "$HEALTH_RESPONSE" ] || rm -f -- "$HEALTH_RESPONSE"
  if [ -n "$ADATA_SOURCE_BUILD" ]; then
    case "$ADATA_SOURCE_BUILD" in
      "$ADATA_RUNTIME_ROOT"/.build-*) rm -rf -- "$ADATA_SOURCE_BUILD" ;;
    esac
  fi
  for temp_dir in "$ADATA_BUILD_SOURCE" "$ADATA_WHEEL_DIR"; do
    case "$temp_dir" in
      /var/lib/probiga/release-artifacts/.adata-source.*|\
      /var/lib/probiga/release-artifacts/.adata-wheel.*) \
        chmod -R u+rwX "$temp_dir" 2>/dev/null || true
        rm -rf -- "$temp_dir"
        ;;
    esac
  done
  if [ -n "$ADATA_CACHE_BUILD" ]; then
    case "$ADATA_CACHE_BUILD" in
      /var/lib/probiga/release-sources/adata-git.*) \
        rm -rf -- "$ADATA_CACHE_BUILD" ;;
    esac
  fi
  if [ "$DEPLOY_SUCCEEDED" -ne 1 ] && [ -n "$EXPECTED_BUILD" ]; then
    if [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] || \
      [ -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" ]; then
      # The forward runtime is a durable rollback dependency as soon as the
      # activation journal exists, even before a process references its venv.
      # A later successful rollback/finalization removes the journal before
      # normal retention pruning is allowed to reclaim the runtime.
      venv_in_use=1
      echo "Retained activation-transaction venv after failure" >&2
    elif path_is_runtime_referenced "$RELEASE_VENV_ROOT/$EXPECTED_SHA" || \
      path_is_runtime_referenced "$EXPECTED_BUILD"; then
      venv_in_use=1
      echo "Retained runtime-referenced prepared venv after failure" >&2
    fi
    if [ "$NEW_VENV_LINK" -eq 1 ] && \
      [ "$venv_in_use" -eq 0 ] && [ -L "$RELEASE_VENV_ROOT/$EXPECTED_SHA" ]; then
      build_target="$(readlink -f "$RELEASE_VENV_ROOT/$EXPECTED_SHA")"
      if [ "$build_target" = "$EXPECTED_BUILD" ]; then
        rm -f -- "$RELEASE_VENV_ROOT/$EXPECTED_SHA"
      fi
    fi
    case "$EXPECTED_BUILD:$venv_in_use" in
      "$RELEASE_VENV_ROOT"/build-*:0)
        chmod -R u+rwX "$EXPECTED_BUILD" 2>/dev/null || true
        rm -rf -- "$EXPECTED_BUILD"
        ;;
    esac
  fi
  rm -f -- "$PREVIOUS_DROPIN" "$PREVIOUS_SCHEDULER_DROPIN" \
    "$PREVIOUS_AI_WORKER_DROPIN"
  [ -z "$PREVIOUS_LEGACY_MAIN_DROPIN_DIR" ] || \
    rm -rf -- "$PREVIOUS_LEGACY_MAIN_DROPIN_DIR"
  [ -z "$PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR" ] || \
    rm -rf -- "$PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR"
  [ -z "$PREVIOUS_LOCK_SNAPSHOT" ] || rm -f -- "$PREVIOUS_LOCK_SNAPSHOT"
  [ -z "$GOVERNANCE_TASK_OLD_SOURCE" ] || \
    rm -f -- "$GOVERNANCE_TASK_OLD_SOURCE"
  [ -z "$GOVERNANCE_TASK_NEW_SOURCE" ] || \
    rm -f -- "$GOVERNANCE_TASK_NEW_SOURCE"
  [ -z "$PREPARED_MAIN_DROPIN" ] || rm -f -- "$PREPARED_MAIN_DROPIN"
  [ -z "$PREPARED_SCHEDULER_DROPIN" ] || \
    rm -f -- "$PREPARED_SCHEDULER_DROPIN"
  [ -z "$PREPARED_AI_WORKER_DROPIN" ] || \
    rm -f -- "$PREPARED_AI_WORKER_DROPIN"
}
verify_venv_dependency_lock() {
  local venv_path="$1"
  local observed_lock
  [[ "${EXPECTED_WHEEL_MANIFEST_SHA256:-}" =~ ^[0-9a-f]{64}$ ]] || return 1
  observed_lock="$(mktemp)"
  if ! "$venv_path/bin/python" -m pip freeze --all --exclude-editable \
    | awk 'tolower($0) !~ /^adata([[:space:]]|==|@)/' \
    | LC_ALL=C sort > "$observed_lock"; then
    rm -f "$observed_lock"
    return 2
  fi
  if ! test -f "$venv_path/.requirements.freeze" || \
    ! test -f "$venv_path/.requirements.freeze.sha256" || \
    ! test -f "$venv_path/.requirements.input" || \
    ! test -f "$venv_path/.requirements.input.sha256" || \
    ! test -f "$venv_path/.wheel-manifest.sha256" || \
    ! test -f "$venv_path/.release-tree.sha256" || \
    ! test -f "$venv_path/.adapter-registry-seal.sha256" || \
    ! cmp --silent "$observed_lock" "$venv_path/.requirements.freeze" || \
    ! test "$(sha256sum "$venv_path/.requirements.input" | cut -d' ' -f1)" = \
      "$EXPECTED_INPUT_LOCK_SHA256" || \
    ! test "$(sha256sum "$venv_path/.requirements.freeze" | cut -d' ' -f1)" = \
      "$(cat "$venv_path/.requirements.freeze.sha256")" || \
    ! test "$(cat "$venv_path/.requirements.input.sha256")" = \
      "$EXPECTED_INPUT_LOCK_SHA256" || \
    ! test "$(cat "$venv_path/.wheel-manifest.sha256")" = \
      "$EXPECTED_WHEEL_MANIFEST_SHA256" || \
    ! test "$(cat "$venv_path/.release-tree.sha256")" = \
      "$EXPECTED_RELEASE_TREE_SHA256" || \
    ! test "$(cat "$venv_path/.adapter-registry-seal.sha256")" = \
      "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256"; then
    rm -f "$observed_lock"
    return 1
  fi
  if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ] && \
    { ! test -f "$venv_path/.wheel-manifest" || \
      ! test -f "$venv_path/.artifact-mode" || \
      ! grep -Fx ci-resolved-freeze-v1 "$venv_path/.artifact-mode" >/dev/null || \
      ! test "$(sha256sum "$venv_path/.wheel-manifest" | cut -d' ' -f1)" = \
        "$EXPECTED_WHEEL_MANIFEST_SHA256" || \
      ! cmp --silent "$venv_path/.requirements.input" \
        "$venv_path/.requirements.freeze" || \
      ! test "$(cat "$venv_path/.requirements.freeze.sha256")" = \
        "$EXPECTED_INPUT_LOCK_SHA256"; }; then
    rm -f "$observed_lock"
    return 1
  fi
  if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ] && \
    ! "$venv_path/bin/python" -I -m pip check >/dev/null; then
    rm -f "$observed_lock"
    return 1
  fi
  rm -f "$observed_lock" || return 1
  return 0
}

clean_root_pip() {
  local python_bin="$1"
  shift
  /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/var/empty \
    LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    PIP_CONFIG_FILE=/dev/null \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_INPUT=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONNOUSERSITE=1 \
    "$python_bin" -I -m pip "$@"
}

validate_hashed_requirements_lock() {
  local lock_file="$1"
  "$BOOTSTRAP_PYTHON" -I - "$lock_file" <<'PY'
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
logical = []
pending = ""
for raw in text.splitlines():
    line = raw.strip()
    if not line or line.startswith("#"):
        continue
    pending += (" " if pending else "") + line.rstrip("\\").strip()
    if line.endswith("\\"):
        continue
    logical.append(pending)
    pending = ""
if pending or not logical:
    raise SystemExit(2)
for requirement in logical:
    if requirement.startswith(("--only-binary", "--no-index")):
        continue
    if "==" not in requirement:
        raise SystemExit(2)
    if not re.search(r"--hash=sha256:[0-9a-f]{64}(?:\s|$)", requirement):
        raise SystemExit(2)
PY
}

validate_ci_resolved_freeze() {
  local freeze_file="$1"
  "$BOOTSTRAP_PYTHON" -I - "$freeze_file" <<'PY'
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
if "\r" in text or not text.endswith("\n"):
    raise SystemExit(2)
lines = text.splitlines()
if not lines or lines != sorted(lines):
    raise SystemExit(2)
pattern = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]*=="
    r"[A-Za-z0-9][A-Za-z0-9._+!-]*"
)
normalized = []
for line in lines:
    if line != line.strip() or pattern.fullmatch(line) is None:
        raise SystemExit(2)
    name = line.split("==", 1)[0]
    normalized.append(re.sub(r"[-_.]+", "-", name).lower())
if len(normalized) != len(set(normalized)):
    raise SystemExit(2)
if "setuptools" not in normalized:
    raise SystemExit(2)
PY
}

prepare_trusted_wheelhouse() {
  local actual_files
  local expected_files
  local manifest_entries
  local wheel_file
  local wheel_sha
  local artifact_root=/var/lib/probiga/release-artifacts
  test ! -L "$artifact_root" || return 1
  install -d -o root -g root -m 0755 "$artifact_root" || return 1
  test "$(readlink -f "$artifact_root")" = "$artifact_root" || return 1
  TRUSTED_WHEELHOUSE="$(mktemp -d \
    "$artifact_root/.wheelhouse-$EXPECTED_SHA.XXXXXX")" || return 1
  chown "$BUILD_USER:$BUILD_USER" "$TRUSTED_WHEELHOUSE" || return 1
  chmod 0700 "$TRUSTED_WHEELHOUSE" || return 1
  sudo -u "$BUILD_USER" /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/var/empty \
    LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    PIP_CONFIG_FILE=/dev/null \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_INPUT=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONNOUSERSITE=1 \
    "$BOOTSTRAP_PYTHON" -I -m pip download \
      --require-hashes --only-binary=:all: --no-deps \
      --dest "$TRUSTED_WHEELHOUSE" -r "$RESOLVED_LOCK" || return 1
  chown -R root:root "$TRUSTED_WHEELHOUSE" || return 1
  chmod -R a+rX,a-w "$TRUSTED_WHEELHOUSE" || return 1
  manifest_entries="$(mktemp)" || return 1
  expected_files="$(mktemp)" || { rm -f -- "$manifest_entries"; return 1; }
  actual_files="$(mktemp)" || {
    rm -f -- "$manifest_entries" "$expected_files"
    return 1
  }
  if ! grep -E '^[0-9a-f]{64}  [A-Za-z0-9_.+-]+\.whl$' \
      "$TRUSTED_WHEEL_MANIFEST" > "$manifest_entries" || \
    ! awk '{print $2}' "$manifest_entries" | LC_ALL=C sort > "$expected_files" || \
    ! find "$TRUSTED_WHEELHOUSE" -mindepth 1 -maxdepth 1 -type f \
      -name '*.whl' -printf '%f\n' | LC_ALL=C sort > "$actual_files" || \
    ! cmp --silent "$expected_files" "$actual_files"; then
    rm -f -- "$manifest_entries" "$expected_files" "$actual_files"
    return 1
  fi
  while read -r wheel_sha wheel_file; do
    test "$(sha256sum "$TRUSTED_WHEELHOUSE/$wheel_file" | cut -d' ' -f1)" = \
      "$wheel_sha" || {
        rm -f -- "$manifest_entries" "$expected_files" "$actual_files"
        return 1
      }
  done < "$manifest_entries"
  rm -f -- "$manifest_entries" "$expected_files" "$actual_files" || return 1
  sudo -u "$SERVICE_USER" test ! -w "$TRUSTED_WHEELHOUSE" || return 1
  sudo -u "$BUILD_USER" test ! -w "$TRUSTED_WHEELHOUSE" || return 1
  return 0
}

prepare_ci_resolved_wheelhouse() {
  local actual_files
  local artifact_root=/var/lib/probiga/release-artifacts
  local wheel_file
  local wheel_sha
  test ! -L "$artifact_root" || return 1
  install -d -o root -g root -m 0755 "$artifact_root" || return 1
  test "$(readlink -f "$artifact_root")" = "$artifact_root" || return 1
  TRUSTED_WHEELHOUSE="$(mktemp -d \
    "$artifact_root/.wheelhouse-$EXPECTED_SHA.XXXXXX")" || return 1
  chown "$BUILD_USER:$BUILD_USER" "$TRUSTED_WHEELHOUSE" || return 1
  chmod 0700 "$TRUSTED_WHEELHOUSE" || return 1
  sudo -u "$BUILD_USER" /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/var/empty \
    LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    PIP_CONFIG_FILE=/dev/null \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_INPUT=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONNOUSERSITE=1 \
    "$BOOTSTRAP_PYTHON" -I -m pip download \
      --only-binary=:all: --no-deps \
      --dest "$TRUSTED_WHEELHOUSE" -r "$RESOLVED_LOCK" || return 1
  chown -R root:root "$TRUSTED_WHEELHOUSE" || return 1
  chmod -R a+rX,a-w "$TRUSTED_WHEELHOUSE" || return 1
  actual_files="$(mktemp)" || return 1
  if ! find "$TRUSTED_WHEELHOUSE" -mindepth 1 -maxdepth 1 -type f \
      -printf '%f\n' | LC_ALL=C sort > "$actual_files" || \
    ! test -s "$actual_files" || \
    grep -Ev '^[A-Za-z0-9_.+-]+\.whl$' "$actual_files" | grep -q .; then
    rm -f -- "$actual_files"
    return 1
  fi
  TRUSTED_WHEEL_MANIFEST="$(mktemp)" || {
    rm -f -- "$actual_files"
    return 1
  }
  printf '%s\n' \
    PROBIGA_RUNTIME_WHEEL_MANIFEST_VERSION=1 \
    TARGET=cp314-manylinux_2_17_x86_64 \
    SOURCE=ci-resolved-freeze-v1 > "$TRUSTED_WHEEL_MANIFEST" || {
      rm -f -- "$actual_files"
      return 1
    }
  while IFS= read -r wheel_file; do
    wheel_sha="$(sha256sum "$TRUSTED_WHEELHOUSE/$wheel_file" | \
      cut -d' ' -f1)" || {
        rm -f -- "$actual_files"
        return 1
      }
    [[ "$wheel_sha" =~ ^[0-9a-f]{64}$ ]] || {
      rm -f -- "$actual_files"
      return 1
    }
    printf '%s  %s\n' "$wheel_sha" "$wheel_file" \
      >> "$TRUSTED_WHEEL_MANIFEST" || {
        rm -f -- "$actual_files"
        return 1
      }
  done < "$actual_files"
  rm -f -- "$actual_files" || return 1
  EXPECTED_WHEEL_MANIFEST_SHA256="$(sha256sum \
    "$TRUSTED_WHEEL_MANIFEST" | cut -d' ' -f1)"
  [[ "$EXPECTED_WHEEL_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ ]] || return 1
  sudo -u "$SERVICE_USER" test ! -w "$TRUSTED_WHEELHOUSE" || return 1
  sudo -u "$BUILD_USER" test ! -w "$TRUSTED_WHEELHOUSE" || return 1
  return 0
}

prepare_code_staging() {
  local cache_parent
  cache_parent="$(dirname "$CODE_GIT_CACHE")"
  test ! -L "$cache_parent"
  test ! -L "$CODE_RELEASE_ROOT"
  install -d -o root -g root -m 0755 "$cache_parent" "$CODE_RELEASE_ROOT"
  test "$(readlink -f "$cache_parent")" = "$cache_parent"
  test "$(readlink -f "$CODE_RELEASE_ROOT")" = "$CODE_RELEASE_ROOT"
  sudo -u "$SERVICE_USER" test ! -w "$CODE_RELEASE_ROOT"
  assert_root_owned_bare_cache "$CODE_GIT_CACHE" "$CODE_REPOSITORY_URL"
  git --git-dir="$CODE_GIT_CACHE" cat-file -e "${EXPECTED_SHA}^{commit}"
  test "$(git --git-dir="$CODE_GIT_CACHE" rev-parse "$EXPECTED_SHA^{commit}")" = \
    "$EXPECTED_SHA"
  chown -R root:root "$CODE_GIT_CACHE"
  chmod -R u+rwX,go+rX,go-w "$CODE_GIT_CACHE"
  assert_root_owned_bare_cache "$CODE_GIT_CACHE" "$CODE_REPOSITORY_URL"
  test ! -L "$PREPARED_CODE_ROOT"
  if [ -d "$PREPARED_CODE_ROOT" ]; then
    test -f "$PREPARED_CODE_ROOT/.git"
    CODE_VALIDATION_ROOT="$PREPARED_CODE_ROOT"
  else
    STAGING_WORKTREE="$CODE_RELEASE_ROOT/.build-$EXPECTED_SHA-$RANDOM"
    git --git-dir="$CODE_GIT_CACHE" worktree add --detach \
      "$STAGING_WORKTREE" "$EXPECTED_SHA"
    CODE_VALIDATION_ROOT="$STAGING_WORKTREE"
  fi
  test "$(git -C "$CODE_VALIDATION_ROOT" rev-parse HEAD)" = "$EXPECTED_SHA"
  assert_service_cannot_write_release_paths "$CODE_VALIDATION_ROOT"
}
prepare_adata_release() {
  local seal_json
  local sealed_tree_sha
  test ! -L "$(dirname "$ADATA_GIT_CACHE")"
  test ! -L "$ADATA_RUNTIME_ROOT"
  install -d -o root -g root -m 0755 "$(dirname "$ADATA_GIT_CACHE")"
  test ! -L "$ADATA_GIT_CACHE"
  if [ ! -d "$ADATA_GIT_CACHE" ]; then
    ADATA_CACHE_BUILD="$(mktemp -d \
      "$(dirname "$ADATA_GIT_CACHE")/adata-git.XXXXXX")"
    git init --bare "$ADATA_CACHE_BUILD/repository.git"
    git --git-dir="$ADATA_CACHE_BUILD/repository.git" remote add origin \
      "$ADATA_REPOSITORY_URL"
    rm -rf -- "$ADATA_CACHE_BUILD/repository.git/hooks"
    install -d -o root -g root -m 0555 \
      "$ADATA_CACHE_BUILD/repository.git/hooks"
    mv "$ADATA_CACHE_BUILD/repository.git" "$ADATA_GIT_CACHE"
    rmdir "$ADATA_CACHE_BUILD"
    ADATA_CACHE_BUILD=""
  fi
  assert_root_owned_bare_cache "$ADATA_GIT_CACHE" "$ADATA_REPOSITORY_URL"
  if ! git --git-dir="$ADATA_GIT_CACHE" cat-file -e \
    "${EXPECTED_ADATA_SHA}^{commit}"; then
    git -c http.lowSpeedLimit=1024 -c http.lowSpeedTime=30 \
      --git-dir="$ADATA_GIT_CACHE" fetch --no-tags origin \
      "$EXPECTED_ADATA_SHA"
  fi
  chown -R root:root "$ADATA_GIT_CACHE"
  chmod -R u+rwX,go+rX,go-w "$ADATA_GIT_CACHE"
  assert_root_owned_bare_cache "$ADATA_GIT_CACHE" "$ADATA_REPOSITORY_URL"
  test "$(git --git-dir="$ADATA_GIT_CACHE" rev-parse \
    "${EXPECTED_ADATA_SHA}^{commit}")" = "$EXPECTED_ADATA_SHA"
  install -d -o root -g root -m 0755 "$ADATA_RUNTIME_ROOT"
  test "$(readlink -f "$ADATA_RUNTIME_ROOT")" = "$ADATA_RUNTIME_ROOT"
  ADATA_SOURCE_BUILD="$(mktemp -d \
    "$ADATA_RUNTIME_ROOT/.build-$EXPECTED_ADATA_SHA.XXXXXX")"
  git --git-dir="$ADATA_GIT_CACHE" archive "$EXPECTED_ADATA_SHA" \
    | tar -xf - -C "$ADATA_SOURCE_BUILD"
  seal_json="$("$BOOTSTRAP_PYTHON" -I \
    "$CODE_VALIDATION_ROOT/server/common/adata_release.py" seal \
    --source "$ADATA_SOURCE_BUILD" --git-sha "$EXPECTED_ADATA_SHA")"
  sealed_tree_sha="$(printf '%s' "$seal_json" | "$BOOTSTRAP_PYTHON" -I -c \
    'import json,sys; print(json.load(sys.stdin)["tree_sha256"])')"
  [[ "$sealed_tree_sha" =~ ^[0-9a-f]{64}$ ]]
  test "$sealed_tree_sha" = "$EXPECTED_ADATA_TREE_SHA256"
  ADATA_SOURCE="$ADATA_RUNTIME_ROOT/$EXPECTED_ADATA_SHA-$EXPECTED_ADATA_TREE_SHA256"
  test ! -L "$ADATA_SOURCE"
  if [ ! -d "$ADATA_SOURCE" ]; then
    chown -R root:root "$ADATA_SOURCE_BUILD"
    chmod -R a+rX,a-w "$ADATA_SOURCE_BUILD"
    mv "$ADATA_SOURCE_BUILD" "$ADATA_SOURCE"
    ADATA_SOURCE_BUILD=""
  else
    rm -rf -- "$ADATA_SOURCE_BUILD"
    ADATA_SOURCE_BUILD=""
  fi
  chown -R root:root "$ADATA_SOURCE"
  chmod -R a+rX,a-w "$ADATA_SOURCE"
  sudo -u "$SERVICE_USER" test ! -w "$ADATA_RUNTIME_ROOT"
  sudo -u "$SERVICE_USER" test ! -w "$(dirname "$ADATA_RUNTIME_ROOT")"
  sudo -u "$SERVICE_USER" test ! -w "$ADATA_SOURCE"
  sudo -u "$SERVICE_USER" test -x "$ADATA_SOURCE"
  sudo -u "$SERVICE_USER" test -r "$ADATA_SOURCE/.probiga-adata.gitsha"
  sudo -u "$SERVICE_USER" test -r "$ADATA_SOURCE/.probiga-adata.tree.sha256"
  "$BOOTSTRAP_PYTHON" -I "$CODE_VALIDATION_ROOT/server/common/adata_release.py" verify \
    --source "$ADATA_SOURCE" --git-sha "$EXPECTED_ADATA_SHA" \
    --tree-sha256 "$EXPECTED_ADATA_TREE_SHA256"
}
prepare_release_venv() {
  local -a adata_wheels=()
  local adata_lock
  local adata_wheel_sha
  test ! -L "$RELEASE_VENV_ROOT"
  install -d -o root -g root -m 0755 "$RELEASE_VENV_ROOT"
  test "$(readlink -f "$RELEASE_VENV_ROOT")" = "$RELEASE_VENV_ROOT"
  RESOLVED_LOCK="$(mktemp)"
  printf '%s' "$RESOLVED_REQUIREMENTS_B64" | base64 -d > "$RESOLVED_LOCK"
  test "$(sha256sum "$RESOLVED_LOCK" | cut -d' ' -f1)" = \
    "$EXPECTED_INPUT_LOCK_SHA256"
  if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]; then
    validate_ci_resolved_freeze "$RESOLVED_LOCK"
    # The isolated non-login build account downloads wheels, so it needs
    # read-only access to this non-secret package/version list.
    chmod 0444 "$RESOLVED_LOCK"
  else
    validate_hashed_requirements_lock "$RESOLVED_LOCK"
    TRUSTED_WHEEL_MANIFEST="$(mktemp)"
    printf '%s' "$TRUSTED_WHEEL_MANIFEST_B64" | base64 -d > \
      "$TRUSTED_WHEEL_MANIFEST"
    test "$(sha256sum "$TRUSTED_WHEEL_MANIFEST" | cut -d' ' -f1)" = \
      "$EXPECTED_WHEEL_MANIFEST_SHA256"
    grep -Fx PROBIGA_TRUSTED_WHEEL_MANIFEST_VERSION=1 \
      "$TRUSTED_WHEEL_MANIFEST" >/dev/null
    grep -Fx TARGET=cp314-manylinux_2_17_x86_64 \
      "$TRUSTED_WHEEL_MANIFEST" >/dev/null
    grep -Fx STATUS=READY "$TRUSTED_WHEEL_MANIFEST" >/dev/null
    prepare_trusted_wheelhouse
  fi
  if [ -e "$RELEASE_VENV_ROOT/$EXPECTED_SHA" ]; then
    test -L "$RELEASE_VENV_ROOT/$EXPECTED_SHA"
    EXPECTED_VENV_TARGET="$(readlink -f "$RELEASE_VENV_ROOT/$EXPECTED_SHA")"
    case "$EXPECTED_VENV_TARGET" in
      "$RELEASE_VENV_ROOT"/build-*) ;;
      *) echo "release venv target escaped its immutable root" >&2; return 2 ;;
    esac
    test "$(dirname "$EXPECTED_VENV_TARGET")" = "$RELEASE_VENV_ROOT"
    if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]; then
      test -f "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.artifact-mode"
      grep -Fx ci-resolved-freeze-v1 \
        "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.artifact-mode" >/dev/null
      EXPECTED_WHEEL_MANIFEST_SHA256="$(cat \
        "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.wheel-manifest.sha256")"
      [[ "$EXPECTED_WHEEL_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ ]]
    fi
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.requirements.input.sha256")" = \
      "$EXPECTED_INPUT_LOCK_SHA256"
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.wheel-manifest.sha256")" = \
      "$EXPECTED_WHEEL_MANIFEST_SHA256"
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.release-tree.sha256")" = \
      "$EXPECTED_RELEASE_TREE_SHA256"
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.adapter-registry-seal.sha256")" = \
      "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256"
    test "$(sha256sum "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.requirements.input" | \
      cut -d' ' -f1)" = "$EXPECTED_INPUT_LOCK_SHA256"
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.probiga.gitsha")" = \
      "$EXPECTED_SHA"
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.adata.gitsha")" = \
      "$EXPECTED_ADATA_SHA"
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.adata.tree.sha256")" = \
      "$EXPECTED_ADATA_TREE_SHA256"
    verify_venv_dependency_lock "$RELEASE_VENV_ROOT/$EXPECTED_SHA"
    assert_service_cannot_write_tree "$EXPECTED_VENV_TARGET" \
      "reused release virtual environment"
    EXPECTED_RESOLVED_FREEZE_SHA256="$(cat \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.requirements.freeze.sha256")"
    [[ "$EXPECTED_RESOLVED_FREEZE_SHA256" =~ ^[0-9a-f]{64}$ ]]
  else
    if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]; then
      prepare_ci_resolved_wheelhouse
    fi
    [[ "$EXPECTED_WHEEL_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ ]]
    EXPECTED_BUILD="$RELEASE_VENV_ROOT/build-$EXPECTED_SHA-$RANDOM"
    "$BOOTSTRAP_PYTHON" -I -m venv "$EXPECTED_BUILD"
    if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]; then
      clean_root_pip "$EXPECTED_BUILD/bin/python" install \
        --no-index --only-binary=:all: \
        --find-links "$TRUSTED_WHEELHOUSE" -r "$RESOLVED_LOCK" --quiet
    else
      clean_root_pip "$EXPECTED_BUILD/bin/python" install \
        --require-hashes --no-index --only-binary=:all: \
        --find-links "$TRUSTED_WHEELHOUSE" -r "$RESOLVED_LOCK" --quiet
    fi
    sudo -u "$BUILD_USER" test -x "$EXPECTED_BUILD/bin/python"
    sudo -u "$BUILD_USER" test ! -w "$EXPECTED_BUILD"
    ADATA_BUILD_SOURCE="$(mktemp -d \
      /var/lib/probiga/release-artifacts/.adata-source.XXXXXX)"
    ADATA_WHEEL_DIR="$(mktemp -d \
      /var/lib/probiga/release-artifacts/.adata-wheel.XXXXXX)"
    git --git-dir="$ADATA_GIT_CACHE" archive "$EXPECTED_ADATA_SHA" \
      | tar -xf - -C "$ADATA_BUILD_SOURCE"
    chown -R "$BUILD_USER:$BUILD_USER" "$ADATA_BUILD_SOURCE" "$ADATA_WHEEL_DIR"
    # Build with the backend version carried by the CI-resolved freeze.  The
    # bootstrap interpreter intentionally has no mutable build packages.
    sudo -u "$BUILD_USER" /usr/bin/env -i \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      HOME=/var/empty LANG=C.UTF-8 LC_ALL=C.UTF-8 \
      PIP_CONFIG_FILE=/dev/null PIP_DISABLE_PIP_VERSION_CHECK=1 \
      PIP_NO_INPUT=1 PIP_NO_CACHE_DIR=1 PYTHONNOUSERSITE=1 \
      "$EXPECTED_BUILD/bin/python" -I -m pip wheel --no-deps \
        --no-build-isolation --no-index \
        --wheel-dir "$ADATA_WHEEL_DIR" "$ADATA_BUILD_SOURCE" --quiet
    mapfile -t adata_wheels < <(find "$ADATA_WHEEL_DIR" -maxdepth 1 \
      -type f -name '*.whl' -print)
    test "${#adata_wheels[@]}" -eq 1
    chown -R root:root "$ADATA_BUILD_SOURCE" "$ADATA_WHEEL_DIR"
    chmod -R a+rX,a-w "$ADATA_BUILD_SOURCE" "$ADATA_WHEEL_DIR"
    adata_wheel_sha="$(sha256sum "${adata_wheels[0]}" | cut -d' ' -f1)"
    [[ "$adata_wheel_sha" =~ ^[0-9a-f]{64}$ ]]
    adata_lock="$(mktemp)"
    printf 'adata @ file://%s --hash=sha256:%s\n' \
      "${adata_wheels[0]}" "$adata_wheel_sha" > "$adata_lock"
    clean_root_pip "$EXPECTED_BUILD/bin/python" install \
      --require-hashes --no-index --no-deps -r "$adata_lock" --quiet
    rm -f -- "$adata_lock"
    install -o root -g root -m 0444 "$RESOLVED_LOCK" \
      "$EXPECTED_BUILD/.requirements.input"
    "$EXPECTED_BUILD/bin/python" -m pip freeze --all --exclude-editable \
      | awk 'tolower($0) !~ /^adata([[:space:]]|==|@)/' \
      | LC_ALL=C sort > "$EXPECTED_BUILD/.requirements.freeze"
    EXPECTED_RESOLVED_FREEZE_SHA256="$(sha256sum \
      "$EXPECTED_BUILD/.requirements.freeze" | cut -d' ' -f1)"
    [[ "$EXPECTED_RESOLVED_FREEZE_SHA256" =~ ^[0-9a-f]{64}$ ]]
    if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]; then
      cmp --silent "$RESOLVED_LOCK" "$EXPECTED_BUILD/.requirements.freeze"
      test "$EXPECTED_RESOLVED_FREEZE_SHA256" = "$EXPECTED_INPUT_LOCK_SHA256"
      install -o root -g root -m 0444 "$TRUSTED_WHEEL_MANIFEST" \
        "$EXPECTED_BUILD/.wheel-manifest"
      printf '%s\n' ci-resolved-freeze-v1 \
        > "$EXPECTED_BUILD/.artifact-mode"
    fi
    printf '%s\n' "$EXPECTED_INPUT_LOCK_SHA256" \
      > "$EXPECTED_BUILD/.requirements.input.sha256"
    printf '%s\n' "$EXPECTED_RESOLVED_FREEZE_SHA256" \
      > "$EXPECTED_BUILD/.requirements.freeze.sha256"
    printf '%s\n' "$EXPECTED_WHEEL_MANIFEST_SHA256" \
      > "$EXPECTED_BUILD/.wheel-manifest.sha256"
    printf '%s\n' "$EXPECTED_RELEASE_TREE_SHA256" \
      > "$EXPECTED_BUILD/.release-tree.sha256"
    printf '%s\n' "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
      > "$EXPECTED_BUILD/.adapter-registry-seal.sha256"
    printf '%s\n' "$EXPECTED_SHA" > "$EXPECTED_BUILD/.probiga.gitsha"
    printf '%s\n' "$EXPECTED_ADATA_SHA" > "$EXPECTED_BUILD/.adata.gitsha"
    printf '%s\n' "$EXPECTED_ADATA_TREE_SHA256" \
      > "$EXPECTED_BUILD/.adata.tree.sha256"
    verify_venv_dependency_lock "$EXPECTED_BUILD"
    chown -R root:root "$EXPECTED_BUILD"
    chmod -R a+rX,a-w "$EXPECTED_BUILD"
    sudo -u "$SERVICE_USER" test -x "$EXPECTED_BUILD/bin/python"
    sudo -u "$SERVICE_USER" "$EXPECTED_BUILD/bin/python" -I -c \
      'import sys; assert sys.version_info[:2] == (3, 14)'
    assert_service_cannot_write_tree "$EXPECTED_BUILD" \
      "new release virtual environment"
    ln -s "$EXPECTED_BUILD" "$RELEASE_VENV_ROOT/$EXPECTED_SHA"
    EXPECTED_VENV_TARGET="$EXPECTED_BUILD"
    NEW_VENV_LINK=1
  fi
  chmod 0555 "$RELEASE_VENV_ROOT"
  sudo -u "$SERVICE_USER" test ! -w "$RELEASE_VENV_ROOT"
  sudo -u "$SERVICE_USER" test -x "$RELEASE_VENV_ROOT"
  rm -f "$RESOLVED_LOCK"
  RESOLVED_LOCK=""
  [ -z "$TRUSTED_WHEEL_MANIFEST" ] || rm -f "$TRUSTED_WHEEL_MANIFEST"
  TRUSTED_WHEEL_MANIFEST=""
  if [ -n "$TRUSTED_WHEELHOUSE" ]; then
    chmod -R u+rwX "$TRUSTED_WHEELHOUSE"
    rm -rf "$TRUSTED_WHEELHOUSE"
  fi
  TRUSTED_WHEELHOUSE=""
  rm -rf "$ADATA_BUILD_SOURCE" "$ADATA_WHEEL_DIR"
  ADATA_BUILD_SOURCE=""
  ADATA_WHEEL_DIR=""
}
prepare_release() {
  prepare_code_staging
  if [ -n "$PREVIOUS_ADATA_TREE_SHA256" ]; then
    "$BOOTSTRAP_PYTHON" -I \
      "$CODE_VALIDATION_ROOT/server/common/adata_release.py" verify \
      --source "$PREVIOUS_ADATA_SOURCE" --git-sha "$PREVIOUS_ADATA_SHA" \
      --tree-sha256 "$PREVIOUS_ADATA_TREE_SHA256"
  fi
  prepare_adata_release
  prepare_release_venv
  COMPUTED_ADAPTER_REGISTRY_SEAL_SHA256="$(
    cd "$CODE_VALIDATION_ROOT"
    /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
      PROBIGA_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256" \
      "PYTHONPATH=$ADATA_SOURCE:$CODE_VALIDATION_ROOT" \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P -c \
      'from server.engine.strategy_execution_adapters import seal_strategy_execution_adapter_registry; print(seal_strategy_execution_adapter_registry())'
  )"
  test "$COMPUTED_ADAPTER_REGISTRY_SEAL_SHA256" = \
    "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256"
  (
    cd "$CODE_VALIDATION_ROOT"
    /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
      PROBIGA_DEPLOYMENT_MODE=production \
      PROBIGA_CODE_ROOT="$CODE_VALIDATION_ROOT" \
      PROBIGA_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256" \
      PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
      "PYTHONPATH=$ADATA_SOURCE:$CODE_VALIDATION_ROOT" \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P -c \
      'from server.engine.strategy_execution_adapters import bootstrap_strategy_execution_adapter_registry; p=bootstrap_strategy_execution_adapter_registry(); assert p["registry_sealed"] is True'
  )
  assert_service_cannot_write_release_paths "$CODE_VALIDATION_ROOT"
  (
    cd "$CODE_VALIDATION_ROOT"
    GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P \
      tools/validate_production_release_boundary.py \
      --require-git-anchor --expected-git-sha "$EXPECTED_SHA"
    sudo -u "$SERVICE_USER" /usr/bin/env -i \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin GIT_OPTIONAL_LOCKS=0 \
      PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
      "PYTHONPATH=$ADATA_SOURCE:$CODE_VALIDATION_ROOT" \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P \
      tools/ensure_quality_gate.py --validate-review-delivery
  )
  release_identity_check 1 "$CODE_VALIDATION_ROOT"
  if [ -n "$STAGING_WORKTREE" ]; then
    seal_release_checkout "$STAGING_WORKTREE"
    assert_service_cannot_write_release_paths "$STAGING_WORKTREE"
    git --git-dir="$CODE_GIT_CACHE" worktree move \
      "$STAGING_WORKTREE" "$PREPARED_CODE_ROOT"
    STAGING_WORKTREE=""
    CODE_VALIDATION_ROOT="$PREPARED_CODE_ROOT"
    NEW_CODE_RELEASE=1
  fi
  if [ "$NEW_CODE_RELEASE" -eq 1 ]; then
    seal_release_checkout "$PREPARED_CODE_ROOT"
  fi
  assert_service_cannot_write_release_paths "$PREPARED_CODE_ROOT"
  test "$(git -C "$PREPARED_CODE_ROOT" rev-parse HEAD)" = "$EXPECTED_SHA"
  release_identity_check 1 "$PREPARED_CODE_ROOT"
  PREPARED_MAIN_DROPIN="$(mktemp)"
  write_dropin "$EXPECTED_SHA" "$PREPARED_CODE_ROOT" \
    "$EXPECTED_ADATA_SHA" "$EXPECTED_ADATA_TREE_SHA256" "$ADATA_SOURCE" \
    "$EXPECTED_RELEASE_TREE_SHA256" \
    "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
    "$PREPARED_MAIN_DROPIN"
  chmod 0600 "$PREPARED_MAIN_DROPIN"
  grep -Fx 'Environment=API_EMBEDDED_SCHEDULER_ENABLED=false' \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_BUILD_COMMIT_SHA=$EXPECTED_SHA" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_RELEASE_TREE_SHA256=$EXPECTED_RELEASE_TREE_SHA256" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -Fx \
    "Environment=PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -Fx "Environment=PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -F -- 'PYTHONSAFEPATH=1' "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -F -- "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python -P " \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  PREPARED_SCHEDULER_DROPIN="$(mktemp)"
  write_scheduler_dropin "$EXPECTED_SHA" "$PREPARED_CODE_ROOT" \
    "$EXPECTED_ADATA_SHA" "$EXPECTED_ADATA_TREE_SHA256" "$ADATA_SOURCE" \
    "$EXPECTED_RELEASE_TREE_SHA256" \
    "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
    "$PREPARED_SCHEDULER_DROPIN"
  chmod 0600 "$PREPARED_SCHEDULER_DROPIN"
  grep -Fx 'Environment=API_EMBEDDED_SCHEDULER_ENABLED=false' \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_BUILD_COMMIT_SHA=$EXPECTED_SHA" \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_RELEASE_TREE_SHA256=$EXPECTED_RELEASE_TREE_SHA256" \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx \
    "Environment=PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx "Environment=PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -F -- "$PREPARED_CODE_ROOT/tools/run_scheduler_daemon.py" \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -F -- 'PYTHONSAFEPATH=1' "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    PREPARED_AI_WORKER_DROPIN="$(mktemp)"
    write_ai_worker_dropin "$EXPECTED_SHA" "$PREPARED_CODE_ROOT" \
      "$EXPECTED_ADATA_SHA" "$EXPECTED_ADATA_TREE_SHA256" "$ADATA_SOURCE" \
      "$EXPECTED_RELEASE_TREE_SHA256" \
      "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
      "$PREPARED_AI_WORKER_DROPIN"
    chmod 0600 "$PREPARED_AI_WORKER_DROPIN"
    grep -Fx "Environment=PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
      "$PREPARED_AI_WORKER_DROPIN" >/dev/null
    grep -Fx "Environment=PROBIGA_RELEASE_TREE_SHA256=$EXPECTED_RELEASE_TREE_SHA256" \
      "$PREPARED_AI_WORKER_DROPIN" >/dev/null
    grep -Fx \
      "Environment=PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
      "$PREPARED_AI_WORKER_DROPIN" >/dev/null
    grep -F -- "$PREPARED_CODE_ROOT/tools/run_ai_recommendation_worker.py" \
      "$PREPARED_AI_WORKER_DROPIN" >/dev/null
    grep -F -- 'PYTHONSAFEPATH=1' "$PREPARED_AI_WORKER_DROPIN" >/dev/null
  fi
}
assert_prepared_runtime_units_still_current() {
  controlled_guard_assert_file "$MAIN_RELEASE_DROPIN" 644 || return 1
  cmp --silent "$MAIN_RELEASE_DROPIN" "$PREPARED_MAIN_DROPIN" || return 1
  controlled_guard_assert_file "$SCHEDULER_UNIT" 644 || return 1
  cmp --silent "$SCHEDULER_UNIT" "$PREPARED_SCHEDULER_DROPIN" || return 1
  test "$(systemctl show -p NeedDaemonReload --value "$MAIN_SERVICE")" = \
    no || return 1
  test "$(systemctl show -p NeedDaemonReload --value probiga-scheduler)" = \
    no || return 1
  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    controlled_guard_assert_file "$AI_WORKER_DROPIN" 644 || return 1
    cmp --silent "$AI_WORKER_DROPIN" "$PREPARED_AI_WORKER_DROPIN" || \
      return 1
    test "$(systemctl show -p NeedDaemonReload --value \
      "$AI_WORKER_SERVICE")" = no || return 1
    test "$(systemctl show -p NeedDaemonReload --value \
      "$AI_WORKER_TIMER")" = no || return 1
  fi
  return 0
}
prepared_request_is_already_active() {
  local expected_scheduler_dropin_paths
  local main_dropin_path
  local main_dropin_paths
  local main_pid
  local scheduler_dropin_paths
  local scheduler_pid
  local -a main_cmdline=()
  local -a scheduler_cmdline=()
  local -a expected_env=(
    "PROBIGA_EXPECTED_GIT_SHA=$EXPECTED_SHA"
    "PROBIGA_BUILD_COMMIT_SHA=$EXPECTED_SHA"
    "PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT"
    "PROBIGA_EXPECTED_ADATA_SHA=$EXPECTED_ADATA_SHA"
    "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$EXPECTED_ADATA_TREE_SHA256"
    "PROBIGA_ADATA_SOURCE_DIR=$ADATA_SOURCE"
    "PROBIGA_RELEASE_TREE_SHA256=$EXPECTED_RELEASE_TREE_SHA256"
    "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256"
    "API_EMBEDDED_SCHEDULER_ENABLED=false"
    "PROBIGA_DEPLOYMENT_MODE=production"
    "PYTHONSAFEPATH=1"
  )
  test "$PREVIOUS_SHA" = "$EXPECTED_SHA" || return 1
  finalized_receipt_matches_current_v2_request || return 1
  test "$PREVIOUS_INPUT_LOCK_SHA256" = "$EXPECTED_INPUT_LOCK_SHA256" || \
    return 1
  test "$PREVIOUS_RESOLVED_FREEZE_SHA256" = \
    "$EXPECTED_RESOLVED_FREEZE_SHA256" || return 1
  test "$PREVIOUS_ADATA_SHA" = "$EXPECTED_ADATA_SHA" || return 1
  test "$PREVIOUS_ADATA_TREE_SHA256" = "$EXPECTED_ADATA_TREE_SHA256" || \
    return 1
  test "$PREVIOUS_ADATA_SOURCE" = "$ADATA_SOURCE" || return 1
  test "$PREVIOUS_CODE_ROOT" = "$PREPARED_CODE_ROOT" || return 1
  test "$PREVIOUS_VENV" = "$RELEASE_VENV_ROOT/$EXPECTED_SHA" || return 1
  test "$PREVIOUS_DROPIN_PRESENT" -eq 1 || return 1
  test "${#PREVIOUS_LEGACY_MAIN_DROPINS[@]}" -eq 0 || return 1
  cmp --silent "$PREVIOUS_DROPIN" "$PREPARED_MAIN_DROPIN" || return 1
  controlled_guard_assert_file "$MAIN_RELEASE_DROPIN" 644 || return 1
  cmp --silent "$MAIN_RELEASE_DROPIN" "$PREPARED_MAIN_DROPIN" || return 1
  for main_dropin_path in "${LEGACY_MAIN_OVERRIDE_DROPINS[@]}"; do
    test ! -e "$main_dropin_path" || return 1
    test ! -L "$main_dropin_path" || return 1
  done
  [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ] || return 1
  test "$PREVIOUS_SCHEDULER_DROPIN_PRESENT" -eq 1 || return 1
  test "${#PREVIOUS_LEGACY_SCHEDULER_DROPINS[@]}" -eq 0 || return 1
  cmp --silent "$PREVIOUS_SCHEDULER_DROPIN" \
    "$PREPARED_SCHEDULER_DROPIN" || return 1
  controlled_guard_assert_file "$SCHEDULER_UNIT" 644 || return 1
  cmp --silent "$SCHEDULER_UNIT" "$PREPARED_SCHEDULER_DROPIN" || return 1
  for main_dropin_path in "${LEGACY_SCHEDULER_OVERRIDE_DROPINS[@]}"; do
    test ! -e "$main_dropin_path" || return 1
    test ! -L "$main_dropin_path" || return 1
  done
  assert_database_writer_guard_dropins_loaded || return 1
  main_dropin_paths="$(systemctl show "$MAIN_SERVICE" \
    --property=DropInPaths --value)" || return 1
  case " $main_dropin_paths " in
    *" $MAIN_RELEASE_DROPIN "*) ;;
    *) return 1 ;;
  esac
  for main_dropin_path in $main_dropin_paths; do
    case "$main_dropin_path" in
      "$MAIN_RELEASE_DROPIN"|"$MAIN_LIMITS_DROPIN"|\
      "$MAIN_MARKET_RADAR_DROPIN"|"$MAIN_SERVICE_USER_DROPIN"|\
      "$MAIN_DATABASE_WRITER_GUARD_DROPIN") ;;
      *) return 1 ;;
    esac
  done
  scheduler_dropin_paths="$(systemctl show probiga-scheduler \
    --property=DropInPaths --value)" || return 1
  expected_scheduler_dropin_paths="$SCHEDULER_DATABASE_WRITER_GUARD_DROPIN"
  if sudo test -f "$SCHEDULER_LIMITS_DROPIN"; then
    expected_scheduler_dropin_paths="$expected_scheduler_dropin_paths $SCHEDULER_LIMITS_DROPIN"
  fi
  test "$scheduler_dropin_paths" = "$expected_scheduler_dropin_paths" || \
    return 1
  assert_prepared_runtime_units_still_current || return 1
  test "$PREVIOUS_MAIN_ACTIVE_STATE" = active || return 1
  test "$(systemctl show -p ActiveState --value "$MAIN_SERVICE")" = \
    "$PREVIOUS_MAIN_ACTIVE_STATE" || return 1
  test "$(systemctl show -p UnitFileState --value "$MAIN_SERVICE")" = \
    "$PREVIOUS_MAIN_UNIT_FILE_STATE" || return 1
  test "$(systemctl show -p ActiveState --value probiga-scheduler)" = active || \
    return 1
  test "$(systemctl show -p UnitFileState --value probiga-scheduler)" = \
    enabled || return 1
  main_pid="$(systemctl show -p MainPID --value "$MAIN_SERVICE")" || return 1
  scheduler_pid="$(systemctl show -p MainPID --value probiga-scheduler)" || \
    return 1
  case "$main_pid:$scheduler_pid" in
    *[!0-9:]*|0:*|*:0|:*|*:) return 1 ;;
  esac
  for expected_value in "${expected_env[@]}"; do
    grep -zFx -- "$expected_value" "/proc/$main_pid/environ" >/dev/null || \
      return 1
    grep -zFx -- "$expected_value" "/proc/$scheduler_pid/environ" \
      >/dev/null || return 1
  done
  grep -zFx -- "PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
    "/proc/$main_pid/environ" >/dev/null || return 1
  mapfile -d '' -t main_cmdline < "/proc/$main_pid/cmdline" || return 1
  test "${#main_cmdline[@]}" -ge 7 || return 1
  test "${main_cmdline[0]}" = \
    "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" || return 1
  test "${main_cmdline[1]}" = -P || return 1
  test "${main_cmdline[2]}" = -m || return 1
  test "${main_cmdline[3]}" = uvicorn || return 1
  test "${main_cmdline[4]}" = server.api.main:app || return 1
  test "${main_cmdline[5]}" = --app-dir || return 1
  test "${main_cmdline[6]}" = "$PREPARED_CODE_ROOT" || return 1
  mapfile -d '' -t scheduler_cmdline < "/proc/$scheduler_pid/cmdline" || \
    return 1
  test "${#scheduler_cmdline[@]}" -ge 3 || return 1
  test "${scheduler_cmdline[0]}" = \
    "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" || return 1
  test "${scheduler_cmdline[1]}" = -P || return 1
  test "${scheduler_cmdline[2]}" = \
    "$PREPARED_CODE_ROOT/tools/run_scheduler_daemon.py" || return 1
  curl --fail --silent --show-error --retry 3 --retry-all-errors \
    --retry-delay 1 --retry-connrefused \
    http://127.0.0.1/api/health >/dev/null || return 1
  curl --fail --silent --show-error --retry 3 --retry-all-errors \
    --retry-delay 1 --retry-connrefused \
    http://127.0.0.1/api/health/runtime >/dev/null || return 1
  assert_nginx_static_matches_checkout "$PREPARED_CODE_ROOT" || return 1
  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    test "$PREVIOUS_AI_WORKER_DROPIN_PRESENT" -eq 1 || return 1
    cmp --silent "$PREVIOUS_AI_WORKER_DROPIN" \
      "$PREPARED_AI_WORKER_DROPIN" || return 1
    controlled_guard_assert_file "$AI_WORKER_DROPIN" 644 || return 1
    cmp --silent "$AI_WORKER_DROPIN" "$PREPARED_AI_WORKER_DROPIN" || \
      return 1
    assert_ai_worker_runtime "$EXPECTED_SHA" \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA" "$PREPARED_CODE_ROOT" || return 1
    assert_ai_worker_previous_state_restored || return 1
  else
    test "$PREVIOUS_AI_WORKER_DROPIN_PRESENT" -eq 0 || return 1
    test -z "$PREPARED_AI_WORKER_DROPIN" || return 1
  fi
  run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/check_strategy_governance_health.py" \
    --compact --expected-build-sha "$EXPECTED_SHA" || return 1
  assert_scheduler_triggers_quiescent || return 1
  assert_nginx_static_matches_checkout "$PREPARED_CODE_ROOT" || return 1
  assert_prepared_runtime_units_still_current || return 1
  test "$(systemctl show -p MainPID --value "$MAIN_SERVICE")" = \
    "$main_pid" || return 1
  test "$(systemctl show -p MainPID --value probiga-scheduler)" = \
    "$scheduler_pid" || return 1
  test "$(systemctl show -p ActiveState --value "$MAIN_SERVICE")" = active || \
    return 1
  test "$(systemctl show -p ActiveState --value probiga-scheduler)" = active || \
    return 1
  return 0
}
install_prepared_dropins() {
  activation_snapshot_set_phase "$EXPECTED_SHA" runtime-units-installing || \
    return 1
  test -s "$PREPARED_MAIN_DROPIN" || return 1
  sudo install -d -o root -g root -m 0755 \
    "$(dirname "$MAIN_RELEASE_DROPIN")" || return 1
  for legacy_main_dropin in "${LEGACY_MAIN_OVERRIDE_DROPINS[@]}"; do
    sudo rm -f "$legacy_main_dropin" || return 1
  done
  sudo install -o root -g root -m 0644 "$PREPARED_MAIN_DROPIN" \
    "$MAIN_RELEASE_DROPIN" || return 1
  test -s "$PREPARED_SCHEDULER_DROPIN" || return 1
  sudo install -d -o root -g root -m 0755 \
    "$(dirname "$SCHEDULER_UNIT")" || return 1
  SCHEDULER_UNIT_TOUCHED=1
  sudo install -o root -g root -m 0644 "$PREPARED_SCHEDULER_DROPIN" \
    "$SCHEDULER_UNIT" || return 1
  for legacy_scheduler_dropin in "${LEGACY_SCHEDULER_OVERRIDE_DROPINS[@]}"; do
    sudo rm -f "$legacy_scheduler_dropin" || return 1
  done
  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    test -s "$PREPARED_AI_WORKER_DROPIN" || return 1
    sudo install -d -o root -g root -m 0755 \
      "$(dirname "$AI_WORKER_DROPIN")" || return 1
    sudo install -o root -g root -m 0644 "$PREPARED_AI_WORKER_DROPIN" \
      "$AI_WORKER_DROPIN" || return 1
  fi
  sync -f /etc/systemd/system || return 1
  return 0
}
run_prepared_python_tool() {
  (
    cd "$PREPARED_CODE_ROOT" || return 1
    sudo -u "$SERVICE_USER" /usr/bin/env -i \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      GIT_OPTIONAL_LOCKS=0 \
      PYTHONDONTWRITEBYTECODE=1 \
      PYTHONSAFEPATH=1 \
      PROBIGA_DEPLOYMENT_MODE=production \
      PROBIGA_EXPECTED_GIT_SHA="$EXPECTED_SHA" \
      PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA" \
      PROBIGA_EXPECTED_ADATA_SHA="$EXPECTED_ADATA_SHA" \
      PROBIGA_EXPECTED_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256" \
      PROBIGA_ADATA_SOURCE_DIR="$ADATA_SOURCE" \
      PROBIGA_CODE_ROOT="$PREPARED_CODE_ROOT" \
      PROBIGA_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256" \
      PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
      "PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P "$@"
  )
}
prepared_governance_snapshot() {
  local action="$1"
  local entrypoint="$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py"
  local snapshot="$2"
  case "$action" in
    restore|verify) ;;
    *)
      echo "prepared_governance_snapshot invalid_action" >&2
      return 1
      ;;
  esac
  if [ "$PREPARED_CODE_ROOT" != "$CODE_RELEASE_ROOT/$EXPECTED_SHA" ]; then
    echo "prepared_governance_snapshot invalid_code_root" >&2
    return 1
  fi
  if [ ! -d "$PREPARED_CODE_ROOT" ] || [ -L "$PREPARED_CODE_ROOT" ]; then
    echo "prepared_governance_snapshot invalid_code_tree" >&2
    return 1
  fi
  if [ ! -f "$entrypoint" ] || [ -L "$entrypoint" ]; then
    echo "prepared_governance_snapshot invalid_entrypoint" >&2
    return 1
  fi
  if [ "$(stat -c '%U:%G' "$entrypoint")" != root:root ]; then
    echo "prepared_governance_snapshot invalid_entrypoint_owner" >&2
    return 1
  fi
  if ! sudo -u "$SERVICE_USER" test ! -w "$entrypoint"; then
    echo "prepared_governance_snapshot entrypoint_write_check_failed" >&2
    return 1
  fi
  case "$snapshot" in
    "$GOVERNANCE_TASK_OLD_SOURCE")
      if [ "$action" != verify ]; then
        echo "prepared_governance_snapshot source_restore_rejected" >&2
        return 1
      fi
      ;;
    "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT")
      if ! controlled_guard_assert_file \
        "$ACTIVATION_GOVERNANCE_OLD_SHA" 600 || \
        [ "$(<"$ACTIVATION_GOVERNANCE_OLD_SHA")" != \
          "$(sha256sum "$snapshot" | cut -d' ' -f1)" ]; then
        echo "prepared_governance_snapshot invalid_old_seal" >&2
        return 1
      fi
      ;;
    "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT")
      if [ "$action" != verify ]; then
        echo "prepared_governance_snapshot new_restore_rejected" >&2
        return 1
      fi
      if ! activation_snapshot_validate_governance_new; then
        echo "prepared_governance_snapshot invalid_new_seal" >&2
        return 1
      fi
      ;;
    *)
      echo "prepared_governance_snapshot invalid_snapshot" >&2
      return 1
      ;;
  esac
  if ! controlled_guard_assert_file "$snapshot" 600 || \
    [ ! -s "$snapshot" ]; then
    echo "prepared_governance_snapshot invalid_snapshot_file" >&2
    return 1
  fi
  if ! run_prepared_python_tool "$entrypoint" \
    "--${action}-snapshot" - < "$snapshot"; then
    echo "prepared_governance_snapshot ${action}_failed" >&2
    return 1
  fi
  return 0
}
prepared_restore_and_verify_governance_snapshot() {
  # The scheduler row is normally unchanged.  Verify first so rollback avoids
  # an unnecessary write, then restore the exact sealed state only on mismatch.
  if prepared_governance_snapshot verify "$1" >/dev/null 2>&1; then
    return 0
  fi
  prepared_governance_snapshot restore "$1" || return 1
  prepared_governance_snapshot verify "$1" || return 1
  return 0
}
prepared_v2_rollback_release_database_guard() {
  local ai_service_record
  local ai_timer_record
  local main_record
  local old_runtime_sha
  local scheduler_record
  test "$DEPLOY_OPERATION" = deploy || return 1
  test "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 || return 1
  test "$EXTERNAL_WRITER_BLOCKED" -eq 0 || return 1
  test "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1 || return 1
  test "$DATABASE_WRITER_GUARD_PERSISTED" -eq 1 || return 1
  test "$DATABASE_WRITER_RESTORE_PERSISTED" -eq 1 || return 1
  old_runtime_sha="$(activation_snapshot_old_release "$EXPECTED_SHA")" || \
    return 1
  test "$old_runtime_sha" = "$PREVIOUS_RELEASE_REVISION" || return 1
  read -r main_record scheduler_record ai_service_record ai_timer_record \
    < <(database_writer_guard_inventory) || return 1
  controlled_guard_assert_restore_file "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  controlled_guard_assert_boundary "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  activation_snapshot_restore_old_set "$EXPECTED_SHA" || return 1
  systemctl daemon-reload || return 1
  activation_snapshot_assert_old_set "$EXPECTED_SHA" || return 1
  controlled_guard_assert_boundary "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  prepared_governance_snapshot verify \
    "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" || return 1
  controlled_guard_capture_current_governance_snapshot "$EXPECTED_SHA" \
    "$old_runtime_sha" || return 1
  assert_scheduler_triggers_quiescent || return 1
  controlled_guard_cleanup "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  return 0
}
run_prepared_database_migration_tool() {
  local entrypoint="$1"
  shift
  test "$entrypoint" = \
    "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_schema.py"
  test -f "$entrypoint"
  test "$(stat -c '%U' "$entrypoint")" = root
  sudo -u "$SERVICE_USER" test ! -w "$entrypoint"
  if [ "$#" -eq 2 ] && \
    [ "$1" = --phase ] && [ "$2" = preflight ]; then
    :
  elif [ "$#" -eq 2 ] && \
    [ "$1" = --phase ] && [ "$2" = recover ]; then
    :
  elif [ "$#" -eq 3 ] && \
    [ "$1" = --phase ] && [ "$2" = cutover ] && \
    [ "$3" = --writers-fenced ]; then
    :
  else
    echo "database migration runner rejected non-allowlisted arguments" >&2
    return 2
  fi
  (
    cd "$PREPARED_CODE_ROOT"
    /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      GIT_OPTIONAL_LOCKS=0 \
      PYTHONDONTWRITEBYTECODE=1 \
      PYTHONSAFEPATH=1 \
      PROBIGA_DEPLOYMENT_MODE=production \
      PROBIGA_EXPECTED_GIT_SHA="$EXPECTED_SHA" \
      PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA" \
      PROBIGA_EXPECTED_ADATA_SHA="$EXPECTED_ADATA_SHA" \
      PROBIGA_EXPECTED_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256" \
      PROBIGA_ADATA_SOURCE_DIR="$ADATA_SOURCE" \
      PROBIGA_CODE_ROOT="$PREPARED_CODE_ROOT" \
      PROBIGA_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256" \
      PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
      "PYTHONPATH=$PREPARED_CODE_ROOT" \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P \
      "$entrypoint" "$@"
  )
}
rollback() {
  local failed_status="${1:-$?}"
  local failed_line="${2:-0}"
  if [ "$BASHPID" != "$DEPLOY_MAIN_BASHPID" ]; then
    trap - ERR TERM INT HUP
    exit "$failed_status"
  fi
  detach_failure_handler_from_transport
  set +e
  if [ "${DEPLOY_SUCCEEDED:-0}" -eq 1 ]; then
    exit "$failed_status"
  fi
  local rollback_failed=0
  local current_sha=""
  local committed_phase=""
  local guard_ai_service_record=""
  local guard_ai_timer_record=""
  local guard_governance_runtime=""
  local guard_main_record=""
  local guard_scheduler_record=""
  local observed_scheduler_active=0
  local observed_scheduler_enabled=0
  local restoration_ready=1
  local service_active_state=""
  local services_quiescent=1
  if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ]; then
    DATABASE_WRITER_GUARD_PERSISTED=1
  fi
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    DATABASE_WRITER_RESTORE_PERSISTED=1
  fi
  committed_phase="$(activation_snapshot_committed_phase_for_release \
    "$EXPECTED_SHA" 2>/dev/null)" || committed_phase=""
  if [ -n "$committed_phase" ]; then
    case "$committed_phase" in
      new-runtime-verified|finalized)
        DEPLOY_SUCCEEDED=1
        echo "New runtime was already fully verified; refusing an unsafe rollback. Run the exact controlled recovery to finish journal cleanup." >&2
        exit "$failed_status"
        ;;
      old-runtime-verified)
        echo "Old runtime was already fully verified; preserving it online for retryable journal cleanup." >&2
        exit "$failed_status"
        ;;
    esac
  fi
  if [ "$CUTOVER_STARTED" -eq 1 ]; then
    printf 'deploy_failure phase=cutover cutover_step=%s line=%s status=%s\n' \
      "$CUTOVER_STEP" "$failed_line" "$failed_status" >&2
  else
    printf 'deploy_failure phase=preparation step=%s line=%s status=%s\n' \
      "$CUTOVER_STEP" "$failed_line" "$failed_status" >&2
  fi
  if [ "$CUTOVER_STARTED" -eq 0 ]; then
    echo "Release preparation failed; the running services were not stopped" >&2
    if [ "$DATABASE_FORWARD_MIGRATION_STARTED" -eq 1 ]; then
      echo "Forward-only QMT schema preparation may remain installed; no database object will be rolled back or dropped" >&2
    fi
    current_sha="$(git -C "$PREVIOUS_CODE_ROOT" rev-parse HEAD 2>/dev/null)"
    ACTIVE_INPUT_LOCK_SHA256="$PREVIOUS_INPUT_LOCK_SHA256"
    ACTIVE_RESOLVED_FREEZE_SHA256="$PREVIOUS_RESOLVED_FREEZE_SHA256"
    ACTIVE_ADATA_SHA="$PREVIOUS_ADATA_SHA"
    ACTIVE_ADATA_TREE_SHA256="$PREVIOUS_ADATA_TREE_SHA256"
    if [ "$current_sha" != "$PREVIOUS_SHA" ] || \
      ! systemctl is-active --quiet "$MAIN_SERVICE" || \
      ! curl --fail --silent --show-error --retry 3 --retry-all-errors \
        --retry-delay 1 --retry-connrefused \
        http://127.0.0.1/api/health >/dev/null; then
      echo "Preparation failed and the untouched service could not be verified" >&2
      write_receipt "PREPARATION_FAILED_UNVERIFIED" "$current_sha" || true
    else
      write_receipt "PREPARATION_FAILED" "$PREVIOUS_SHA" || true
    fi
    exit "$failed_status"
  fi
  echo "Deployment failed; rolling back to $PREVIOUS_SHA" >&2
  if [ "$DATABASE_FORWARD_MIGRATION_STARTED" -eq 1 ]; then
    echo "Database schema changes are forward-only additive; runtime and scheduler are being restored without dropping schema objects" >&2
  fi

  rollback_failure() {
    echo "Rollback step failed: $1" >&2
    rollback_failed=1
  }

  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    sudo systemctl stop "$AI_WORKER_TIMER" || \
      rollback_failure "stop AI recommendation worker timer"
    sudo systemctl stop "$AI_WORKER_SERVICE" || \
      rollback_failure "stop AI recommendation worker"
    if systemctl is-active --quiet "$AI_WORKER_TIMER"; then
      rollback_failure "AI recommendation worker timer remained active before rollback"
      services_quiescent=0
    fi
    if systemctl is-active --quiet "$AI_WORKER_SERVICE"; then
      rollback_failure "AI recommendation worker remained active before rollback"
      services_quiescent=0
    fi
  fi
  if [ "$services_quiescent" -eq 1 ] && \
    { [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ] || \
      [ "$SCHEDULER_UNIT_TOUCHED" -eq 1 ]; }; then
    sudo systemctl stop probiga-scheduler || \
      rollback_failure "stop probiga-scheduler"
    if ! service_active_state="$(systemctl show -p ActiveState --value \
      probiga-scheduler)"; then
      rollback_failure "inspect probiga-scheduler stop state"
      services_quiescent=0
    else
      case "$service_active_state" in
        inactive|failed) ;;
        *)
          rollback_failure \
            "probiga-scheduler remained $service_active_state before rollback"
          services_quiescent=0
          ;;
      esac
    fi
  fi
  if [ "$API_STOPPED" -eq 1 ]; then
  sudo systemctl stop "$MAIN_SERVICE" || rollback_failure "stop probiga"
  if ! service_active_state="$(systemctl show -p ActiveState --value \
    "$MAIN_SERVICE")"; then
    rollback_failure "inspect probiga stop state"
    services_quiescent=0
  else
    case "$service_active_state" in
      inactive|failed) ;;
      *)
        rollback_failure \
          "probiga remained $service_active_state before rollback"
        services_quiescent=0
        ;;
    esac
  fi
  if [ "$services_quiescent" -eq 1 ]; then
    if [ "$GOVERNANCE_TASK_TOUCHED" -eq 1 ]; then
      if [ ! -s "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" ]; then
        rollback_failure "strategy governance task snapshot is missing"
        restoration_ready=0
      elif ! prepared_restore_and_verify_governance_snapshot \
        "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"; then
        rollback_failure "restore previous strategy governance task"
        restoration_ready=0
      fi
    fi
    if [ "$restoration_ready" -eq 1 ]; then
      if [ "$PREVIOUS_DROPIN_PRESENT" -eq 1 ]; then
        if ! sudo install -o root -g root -m 0644 "$PREVIOUS_DROPIN" \
          "$MAIN_RELEASE_DROPIN"; then
          rollback_failure "restore previous probiga drop-in"
          restoration_ready=0
        fi
      elif ! sudo rm -f "$MAIN_RELEASE_DROPIN"; then
        rollback_failure "remove release probiga drop-in"
        restoration_ready=0
      fi
    fi
    if [ "$restoration_ready" -eq 1 ]; then
      for legacy_main_dropin in "${PREVIOUS_LEGACY_MAIN_DROPINS[@]}"; do
        if ! sudo install -o root -g root -m 0644 \
          "$PREVIOUS_LEGACY_MAIN_DROPIN_DIR/$(basename "$legacy_main_dropin")" \
          "$legacy_main_dropin"; then
          rollback_failure "restore previous legacy probiga drop-in"
          restoration_ready=0
          break
        fi
      done
    fi
    if [ "$restoration_ready" -eq 1 ]; then
      if [ "$PREVIOUS_SCHEDULER_DROPIN_PRESENT" -eq 1 ]; then
        if ! sudo install -o root -g root -m 0644 \
          "$PREVIOUS_SCHEDULER_DROPIN" "$SCHEDULER_UNIT"; then
          rollback_failure "restore previous scheduler drop-in"
          restoration_ready=0
        fi
      else
        if [ "$SCHEDULER_UNIT_PRESENT" -eq 0 ] && \
          ! sudo systemctl disable probiga-scheduler; then
          rollback_failure "disable first-install scheduler unit"
          restoration_ready=0
        fi
        if ! sudo rm -f "$SCHEDULER_UNIT"; then
          rollback_failure "remove release scheduler unit"
          restoration_ready=0
        fi
      fi
    fi
    if [ "$restoration_ready" -eq 1 ]; then
      for legacy_scheduler_dropin in \
        "${PREVIOUS_LEGACY_SCHEDULER_DROPINS[@]}"; do
        if ! sudo install -o root -g root -m 0644 \
          "$PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR/$(basename "$legacy_scheduler_dropin")" \
          "$legacy_scheduler_dropin"; then
          rollback_failure "restore previous legacy scheduler drop-in"
          restoration_ready=0
          break
        fi
      done
    fi
    if [ "$restoration_ready" -eq 1 ] && \
      [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
      if [ "$PREVIOUS_AI_WORKER_DROPIN_PRESENT" -eq 1 ]; then
        if ! sudo install -o root -g root -m 0644 \
          "$PREVIOUS_AI_WORKER_DROPIN" "$AI_WORKER_DROPIN"; then
          rollback_failure "restore previous AI recommendation worker drop-in"
          restoration_ready=0
        fi
      elif ! sudo rm -f "$AI_WORKER_DROPIN"; then
        rollback_failure "remove AI recommendation worker release drop-in"
        restoration_ready=0
      fi
    fi
    if [ "$restoration_ready" -eq 1 ] && \
      ! sudo systemctl daemon-reload; then
      rollback_failure "systemd daemon-reload"
      restoration_ready=0
    fi
    if [ "$restoration_ready" -eq 1 ] && \
      ! point_static_release_to_checkout "$PREVIOUS_CODE_ROOT"; then
      rollback_failure "point Nginx static assets at previous code release"
      restoration_ready=0
    fi
    if [ "$restoration_ready" -eq 1 ] && \
      ! assert_nginx_static_matches_checkout "$PREVIOUS_CODE_ROOT"; then
      rollback_failure "verify previous Nginx static assets"
      restoration_ready=0
    fi
    if [ "$restoration_ready" -eq 1 ] && \
      [ "$EXTERNAL_WRITER_BLOCKED" -eq 0 ] && \
      [ "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1 ] && \
      [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]; then
      if ! prepared_v2_rollback_release_database_guard; then
        rollback_failure \
          "certify and release the v2 database guard for previous runtime"
      else
        DATABASE_WRITER_GUARD_PERSISTED=0
        DATABASE_GUARD_MIGRATION_UNVERIFIED=0
      fi
    fi
    if [ "$restoration_ready" -eq 1 ] && \
      { [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ] || \
        [ "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1 ]; }; then
      sudo systemctl disable "$MAIN_SERVICE" || \
        rollback_failure "disable probiga after database writer block"
      sudo systemctl stop "$MAIN_SERVICE" || \
        rollback_failure "keep probiga stopped after database writer block"
    elif [ "$restoration_ready" -eq 1 ]; then
      if [ "$PREVIOUS_MAIN_ENABLED" -eq 1 ]; then
        sudo systemctl enable "$MAIN_SERVICE" || \
          rollback_failure "enable probiga"
      else
        sudo systemctl disable "$MAIN_SERVICE" || \
          rollback_failure "disable probiga"
      fi
      sudo systemctl start "$MAIN_SERVICE" || rollback_failure "start probiga"
    else
      rollback_failure "probiga restart skipped after unsafe restore state"
    fi
  else
    rollback_failure "services were not quiescent; runtime restoration skipped"
    restoration_ready=0
  fi
  else
    echo "Cutover aborted before the API stop; leaving its checkout untouched" >&2
  fi
  if [ "$restoration_ready" -eq 1 ] && \
    [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
    if [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ] || \
      [ "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1 ]; then
      sudo systemctl disable probiga-scheduler || \
        rollback_failure "disable scheduler after database writer block"
      sudo systemctl stop probiga-scheduler || \
        rollback_failure "keep scheduler stopped after database writer block"
    else
      if [ "$PREVIOUS_SCHEDULER_ENABLED" -eq 1 ]; then
        sudo systemctl enable probiga-scheduler || \
          rollback_failure "enable probiga-scheduler"
      else
        sudo systemctl disable probiga-scheduler || \
          rollback_failure "disable probiga-scheduler"
      fi
      if [ "$PREVIOUS_SCHEDULER_ACTIVE" -eq 1 ]; then
        sudo systemctl start probiga-scheduler || \
          rollback_failure "start probiga-scheduler"
      else
        sudo systemctl stop probiga-scheduler || \
          rollback_failure "keep probiga-scheduler stopped"
      fi
    fi
  fi
  if [ "$restoration_ready" -eq 1 ] && \
    [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    if [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ] || \
      [ "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1 ]; then
      sudo systemctl disable "$AI_WORKER_TIMER" || \
        rollback_failure "disable AI worker timer after database writer block"
      sudo systemctl disable "$AI_WORKER_SERVICE" || \
        rollback_failure "disable AI worker service after database writer block"
      sudo systemctl stop "$AI_WORKER_TIMER" || \
        rollback_failure "stop AI worker timer after database writer block"
      sudo systemctl stop "$AI_WORKER_SERVICE" || \
        rollback_failure "stop AI worker service after database writer block"
      assert_ai_worker_writer_fence || \
        rollback_failure \
          "AI worker service/timer fence failed after database writer block"
    else
      restore_ai_worker_previous_state || \
        rollback_failure "restore previous AI worker service/timer state"
      assert_ai_worker_previous_state_restored || \
        rollback_failure "verify previous AI worker service/timer state"
    fi
    if [ "$EXTERNAL_WRITER_BLOCKED" -eq 0 ] && \
      [ "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 0 ] && \
      [ "$PREVIOUS_AI_WORKER_DROPIN_PRESENT" -eq 1 ] && \
      [ -n "$PREVIOUS_RELEASE_REVISION" ]; then
      assert_ai_worker_runtime "$PREVIOUS_RELEASE_REVISION" \
        "$PREVIOUS_VENV" "$PREVIOUS_CODE_ROOT" legacy-rollback || \
        rollback_failure "verify previous AI recommendation worker runtime"
    fi
  fi
  if [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ] || \
    [ "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1 ]; then
    if ! read -r guard_main_record guard_scheduler_record \
      guard_ai_service_record guard_ai_timer_record \
      < <(database_writer_guard_inventory); then
      rollback_failure "read persistent database writer inventory"
    elif [ "$DATABASE_WRITER_RESTORE_PERSISTED" -ne 1 ] || \
      ! controlled_guard_assert_restore_file "$EXPECTED_SHA" \
        "$guard_main_record" "$guard_scheduler_record" \
        "$guard_ai_service_record" "$guard_ai_timer_record"; then
      rollback_failure "persistent activation journal is missing or changed"
    elif ! controlled_guard_refence_after_restore_failure "$EXPECTED_SHA" \
      "$guard_main_record" "$guard_scheduler_record" \
      "$guard_ai_service_record" "$guard_ai_timer_record"; then
      rollback_failure "re-latch persistent database writer guard"
    else
      DATABASE_WRITER_GUARD_PERSISTED=1
    fi
    if systemctl is-active --quiet "$MAIN_SERVICE"; then
      rollback_failure "probiga restarted after database writer block"
    fi
    if systemctl is-enabled --quiet "$MAIN_SERVICE"; then
      rollback_failure "probiga remained enabled after database writer block"
    fi
    if systemctl is-active --quiet probiga-scheduler; then
      rollback_failure \
        "probiga-scheduler restarted after database writer block"
    fi
    if systemctl is-enabled --quiet probiga-scheduler; then
      rollback_failure \
        "probiga-scheduler remained enabled after database writer block"
    fi
    if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
      assert_ai_worker_writer_fence || \
        rollback_failure \
          "AI worker service/timer escaped database writer block"
    fi
    if [ "$DATABASE_WRITER_GUARD_PERSISTED" -eq 1 ]; then
      if ! read -r guard_main_record guard_scheduler_record \
        guard_ai_service_record guard_ai_timer_record \
        < <(database_writer_guard_inventory) || \
        ! controlled_guard_assert_marker "$EXPECTED_SHA" \
          "$guard_main_record" "$guard_scheduler_record" \
          "$guard_ai_service_record" "$guard_ai_timer_record"; then
        rollback_failure "persistent database writer guard was removed or changed"
      fi
      assert_database_writer_guard_dropins_loaded || \
        rollback_failure "database writer guard drop-ins were removed or unloaded"
    fi
  else
    sudo systemctl is-active --quiet "$MAIN_SERVICE" || \
      rollback_failure "verify probiga is active"
    curl --fail --silent --show-error --retry 15 --retry-all-errors \
      --retry-delay 2 --retry-connrefused \
      http://127.0.0.1/api/health >/dev/null || \
      rollback_failure "verify previous API health"
  fi
  current_sha="$(git -C "$PREVIOUS_CODE_ROOT" rev-parse HEAD 2>/dev/null)"
  if [ "$current_sha" != "$PREVIOUS_SHA" ]; then
    rollback_failure "verify previous Git revision"
  fi
  if [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ] && \
    [ "$EXTERNAL_WRITER_BLOCKED" -eq 0 ] && \
    [ "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 0 ]; then
    systemctl is-active --quiet probiga-scheduler && \
      observed_scheduler_active=1
    systemctl is-enabled --quiet probiga-scheduler && \
      observed_scheduler_enabled=1
    if [ "$observed_scheduler_active" -ne "$PREVIOUS_SCHEDULER_ACTIVE" ]; then
      rollback_failure "restore probiga-scheduler active state"
    fi
    if [ "$observed_scheduler_enabled" -ne "$PREVIOUS_SCHEDULER_ENABLED" ]; then
      rollback_failure "restore probiga-scheduler enabled state"
    fi
  elif [ "$SCHEDULER_UNIT_TOUCHED" -eq 1 ] && \
    [ "$SCHEDULER_UNIT_PRESENT" -eq 0 ]; then
    if systemctl is-active --quiet probiga-scheduler; then
      rollback_failure "first-install scheduler remained active after rollback"
    fi
    if systemctl is-enabled --quiet probiga-scheduler; then
      rollback_failure "first-install scheduler remained enabled after rollback"
    fi
  fi
  assert_scheduler_triggers_quiescent || \
    rollback_failure "verify scheduler activation units remain quiescent"

  case "$DEPLOY_ARTIFACT_MODE" in
    ci-resolved-freeze-v1) guard_governance_runtime=prepared ;;
    static-wheel-lock-v2) guard_governance_runtime=controlled ;;
    *) rollback_failure "select the rollback governance runtime" ;;
  esac
  if [ "$rollback_failed" -eq 0 ] && \
    [ "$EXTERNAL_WRITER_BLOCKED" -eq 0 ] && \
    [ "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 0 ]; then
    if ! read -r guard_main_record guard_scheduler_record \
      guard_ai_service_record guard_ai_timer_record \
      < <(database_writer_guard_inventory) || \
      [ "$DATABASE_WRITER_RESTORE_PERSISTED" -ne 1 ] || \
      ! controlled_guard_restore_and_finalize "$EXPECTED_SHA" \
        "$guard_main_record" "$guard_scheduler_record" \
        "$guard_ai_service_record" "$guard_ai_timer_record" \
        "$guard_governance_runtime"; then
      rollback_failure "finalize the activation recovery journal"
    else
      DATABASE_WRITER_RESTORE_PERSISTED=0
      if [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] || \
        [ -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" ]; then
        activation_snapshot_remove_old_runtime_verified || \
          rollback_failure "remove the verified old-runtime activation journal"
      fi
    fi
  fi
  if [ "$rollback_failed" -ne 0 ]; then
    committed_phase="$(activation_snapshot_committed_phase_for_release \
      "$EXPECTED_SHA" 2>/dev/null)" || committed_phase=""
    case "$committed_phase" in
      old-runtime-verified|new-runtime-verified|finalized)
        echo "Verified runtime commit $committed_phase remains online; cleanup will resume on the next controlled recovery." >&2
        ;;
      *)
        if read -r guard_main_record guard_scheduler_record \
          guard_ai_service_record guard_ai_timer_record \
          < <(database_writer_guard_inventory); then
          controlled_guard_write_restore_file "$EXPECTED_SHA" \
            "$guard_main_record" "$guard_scheduler_record" \
            "$guard_ai_service_record" "$guard_ai_timer_record" || true
          controlled_guard_refence_after_restore_failure "$EXPECTED_SHA" \
            "$guard_main_record" "$guard_scheduler_record" \
            "$guard_ai_service_record" "$guard_ai_timer_record" || true
        fi
        ;;
    esac
  fi

  if [ "$rollback_failed" -ne 0 ]; then
    ACTIVE_INPUT_LOCK_SHA256=""
    ACTIVE_RESOLVED_FREEZE_SHA256=""
    ACTIVE_ADATA_SHA=""
    ACTIVE_ADATA_TREE_SHA256=""
    echo "Rollback verification failed" >&2
    write_receipt "ROLLBACK_FAILED" "$current_sha" || true
  elif [ "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1 ]; then
    ACTIVE_INPUT_LOCK_SHA256="$PREVIOUS_INPUT_LOCK_SHA256"
    ACTIVE_RESOLVED_FREEZE_SHA256="$PREVIOUS_RESOLVED_FREEZE_SHA256"
    ACTIVE_ADATA_SHA="$PREVIOUS_ADATA_SHA"
    ACTIVE_ADATA_TREE_SHA256="$PREVIOUS_ADATA_TREE_SHA256"
    write_receipt "BLOCKED_DATABASE_GUARDS" "$PREVIOUS_SHA" || true
  elif [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ]; then
    ACTIVE_INPUT_LOCK_SHA256="$PREVIOUS_INPUT_LOCK_SHA256"
    ACTIVE_RESOLVED_FREEZE_SHA256="$PREVIOUS_RESOLVED_FREEZE_SHA256"
    ACTIVE_ADATA_SHA="$PREVIOUS_ADATA_SHA"
    ACTIVE_ADATA_TREE_SHA256="$PREVIOUS_ADATA_TREE_SHA256"
    write_receipt "BLOCKED_EXTERNAL_WRITER" "$PREVIOUS_SHA" || true
  else
    ACTIVE_INPUT_LOCK_SHA256="$PREVIOUS_INPUT_LOCK_SHA256"
    ACTIVE_RESOLVED_FREEZE_SHA256="$PREVIOUS_RESOLVED_FREEZE_SHA256"
    ACTIVE_ADATA_SHA="$PREVIOUS_ADATA_SHA"
    ACTIVE_ADATA_TREE_SHA256="$PREVIOUS_ADATA_TREE_SHA256"
    write_receipt "ROLLED_BACK" "$PREVIOUS_SHA" || true
  fi
  exit "$failed_status"
}
trap 'rollback "$?" "$LINENO"' ERR
trap 'rollback 143' TERM
trap 'rollback 130' INT
trap 'rollback 129' HUP
# PREPARE: all network, dependency, and release validation work happens while
# the old API remains active. This phase must not mutate the live checkout.
CUTOVER_STEP=prepare_release
prepare_release
if [ "$PREVIOUS_SHA" = "$EXPECTED_SHA" ]; then
  if ! prepared_request_is_already_active; then
    echo "existing release SHA does not match the complete finalized request identity" >&2
    false
  fi
  ACTIVE_INPUT_LOCK_SHA256="$EXPECTED_INPUT_LOCK_SHA256"
  ACTIVE_RESOLVED_FREEZE_SHA256="$EXPECTED_RESOLVED_FREEZE_SHA256"
  ACTIVE_ADATA_SHA="$EXPECTED_ADATA_SHA"
  ACTIVE_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256"
  CUTOVER_STEP=write_idempotent_deployed_receipt
  trap '' TERM INT HUP
  if ! write_receipt DEPLOYED "$EXPECTED_SHA"; then
    trap 'rollback 143' TERM
    trap 'rollback 130' INT
    trap 'rollback 129' HUP
    false
  fi
  DEPLOY_SUCCEEDED=1
  trap - ERR TERM INT HUP
  exit 0
fi

# PREPARE DATABASE: read-only verification of the fixed TLS administrator and
# migrator identities, target server, grants, trust=OFF and existing schema.
# No DDL or DML is accepted while the old writers remain active.
if [ "$DEPLOY_ARTIFACT_MODE" = static-wheel-lock-v2 ]; then
  CUTOVER_STEP=preflight_strategy_governance_database_schema
  run_prepared_database_migration_tool \
    "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_schema.py" \
    --phase preflight
fi
CUTOVER_STEP=preflight_qmt_local_history_provenance_schema
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/migrate_qmt_local_history_provenance.py" \
  --check-via-primary
CUTOVER_STEP=preflight_strategy_governance_qmt_history_readiness
QMT_HISTORY_PREFLIGHT_OUTPUT="$(run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_qmt_history.py" \
  --readiness-only)"
printf '%s\n' "$QMT_HISTORY_PREFLIGHT_OUTPUT"
QMT_HISTORY_WINDOW="$(
  printf '%s' "$QMT_HISTORY_PREFLIGHT_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
    'import json,re,sys; from datetime import date; p=json.load(sys.stdin); keys={"status","mode","target_trade_date","session_count","start_date","end_date","session_window_sha256","target_rows","native_qmt_rows","exact_matched_rows","database_writes","automatic_real_order_submission"}; values=[p.get(k) for k in ("target_trade_date","start_date","end_date")]; canonical=isinstance(p,dict) and all(isinstance(x,str) and date.fromisoformat(x).isoformat()==x for x in values); counts=type(p.get("target_rows")) is int and p["target_rows"]>0 and p.get("native_qmt_rows")==p["target_rows"] and p.get("exact_matched_rows")==p["target_rows"]; ok=set(p)==keys and p.get("status")=="ok" and p.get("mode")=="readiness-only" and p.get("session_count")==120 and canonical and values[0]==values[2] and values[1]<=values[2] and isinstance(p.get("session_window_sha256"),str) and re.fullmatch(r"[0-9a-f]{64}",p["session_window_sha256"]) and counts and p.get("database_writes") is False and p.get("automatic_real_order_submission") is False; print(*values,p.get("session_window_sha256","")) if ok else None; raise SystemExit(0 if ok else 2)'
)"
read -r QMT_HISTORY_TARGET_TRADE_DATE QMT_HISTORY_START_DATE \
  QMT_HISTORY_END_DATE QMT_HISTORY_SESSION_WINDOW_SHA256 \
  QMT_HISTORY_WINDOW_EXTRA <<< "$QMT_HISTORY_WINDOW"
test -z "$QMT_HISTORY_WINDOW_EXTRA"
readonly QMT_HISTORY_TARGET_TRADE_DATE QMT_HISTORY_START_DATE \
  QMT_HISTORY_END_DATE QMT_HISTORY_SESSION_WINDOW_SHA256
GOVERNANCE_TASK_OLD_SOURCE="$(mktemp)"
chown "$SERVICE_USER:$SERVICE_USER" "$GOVERNANCE_TASK_OLD_SOURCE"
chmod 0600 "$GOVERNANCE_TASK_OLD_SOURCE"
CUTOVER_STEP=capture_strategy_governance_task_before_cutover
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
  --capture-snapshot "$GOVERNANCE_TASK_OLD_SOURCE"
test -s "$GOVERNANCE_TASK_OLD_SOURCE"
chown root:root "$GOVERNANCE_TASK_OLD_SOURCE"
chmod 0600 "$GOVERNANCE_TASK_OLD_SOURCE"
controlled_guard_assert_file "$GOVERNANCE_TASK_OLD_SOURCE" 600
CUTOVER_STEP=preflight_strategy_governance_rollback_channel
prepared_governance_snapshot verify "$GOVERNANCE_TASK_OLD_SOURCE"

# CUTOVER: persist the exact pre-cutover activation journal before the first
# stop/disable.  A completed journal is always present before any writer state
# changes; the marker and permanent drop-ins then make an interrupted fence
# recoverable without trusting caller-supplied state.
CUTOVER_STEP=persist_database_writer_restore_journal
persist_database_writer_restore_journal
CUTOVER_STARTED=1
DATABASE_GUARD_MIGRATION_UNVERIFIED=1
CUTOVER_STEP=install_database_writer_guard_dropins
install_database_writer_guard_dropins
CUTOVER_STEP=persist_database_writer_guard
persist_database_writer_guard
DATABASE_WRITER_GUARD_PERSISTED=1
CUTOVER_STEP=load_database_writer_guard_dropins
sudo systemctl daemon-reload
assert_database_writer_guard_dropins_loaded

# Quiesce writers, install the prevalidated runtime and governance schema/task,
# run the bounded daily close, then start and prove health/static.  The live
# checkout remains untouched.
CUTOVER_STEP=stop_auxiliary_writers
if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
  sudo systemctl disable "$AI_WORKER_TIMER"
  sudo systemctl disable "$AI_WORKER_SERVICE"
  sudo systemctl stop "$AI_WORKER_TIMER"
  sudo systemctl stop "$AI_WORKER_SERVICE"
  assert_ai_worker_writer_fence
fi
CUTOVER_STEP=stop_scheduler
if [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
  sudo systemctl stop probiga-scheduler
  ! systemctl is-active --quiet probiga-scheduler
  sudo systemctl disable probiga-scheduler
fi
CUTOVER_STEP=stop_api
API_STOPPED=1
sudo systemctl disable "$MAIN_SERVICE"
sudo systemctl stop "$MAIN_SERVICE"
! systemctl is-enabled --quiet "$MAIN_SERVICE"
# Persist the Layer-4 writer fence while both scheduler implementations are
# stopped. Activation is a separate, schema-gated maintenance operation.
CUTOVER_STEP=writer_fence
WRITER_FENCE_STATUS=0
(
  cd "$PREPARED_CODE_ROOT"
  sudo -u "$SERVICE_USER" /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin GIT_OPTIONAL_LOCKS=0 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
    PROBIGA_DEPLOYMENT_MODE=production \
    PROBIGA_EXPECTED_GIT_SHA="$EXPECTED_SHA" \
    PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA" \
    PROBIGA_CODE_ROOT="$PREPARED_CODE_ROOT" \
    PROBIGA_EXPECTED_ADATA_SHA="$EXPECTED_ADATA_SHA" \
    PROBIGA_EXPECTED_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256" \
    PROBIGA_ADATA_SOURCE_DIR="$ADATA_SOURCE" \
    PROBIGA_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256" \
    PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
    "PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
    "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P \
    tools/add_trading_v3_tasks.py --writer-fence \
      --require-no-live-scheduler-writers \
      --writer-drain-timeout-seconds 150 \
      --writer-drain-poll-seconds 5
) || WRITER_FENCE_STATUS=$?
if [ "$WRITER_FENCE_STATUS" -ne 0 ]; then
  if [ "$WRITER_FENCE_STATUS" -eq 3 ]; then
    EXTERNAL_WRITER_BLOCKED=1
  fi
  false
fi
! systemctl is-active --quiet "$MAIN_SERVICE"
! systemctl is-enabled --quiet "$MAIN_SERVICE"
if [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
  ! systemctl is-active --quiet probiga-scheduler
  ! systemctl is-enabled --quiet probiga-scheduler
fi
if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
  assert_ai_worker_writer_fence
fi
if [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]; then
  # Alibaba Cloud RDS keeps binary logging enabled and does not expose
  # log_bin_trust_function_creators/SUPER to tenants.  The v2 release path
  # therefore applies the RDS-safe additive schema through the runtime account
  # only after every writer is fenced.  The task stays disabled until the same
  # schema is revalidated by the normal installation step below.
  CUTOVER_STEP=prepare_strategy_governance_rds_safe_schema
  DATABASE_FORWARD_MIGRATION_STARTED=1
  if ! run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
    --disabled --writers-fenced-schema-preparation; then
    GOVERNANCE_TASK_TOUCHED=1
    false
  fi
  GOVERNANCE_TASK_TOUCHED=1
else
  CUTOVER_STEP=prepare_strategy_governance_database_schema
  DATABASE_FORWARD_MIGRATION_STARTED=1
  run_prepared_database_migration_tool \
    "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_schema.py" \
    --phase cutover --writers-fenced
  CUTOVER_STEP=recover_strategy_governance_database_trust
  run_prepared_database_migration_tool \
    "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_schema.py" \
    --phase recover
fi
CUTOVER_STEP=prepare_strategy_governance_qmt_history
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_qmt_history.py" \
  --expected-target-trade-date "$QMT_HISTORY_TARGET_TRADE_DATE" \
  --expected-start-date "$QMT_HISTORY_START_DATE" \
  --expected-end-date "$QMT_HISTORY_END_DATE" \
  --expected-session-window-sha256 \
    "$QMT_HISTORY_SESSION_WINDOW_SHA256"
CUTOVER_STEP=install_runtime_units
install_prepared_dropins
CUTOVER_STEP=verify_installed_runtime_units
# ExecStart's `systemctl show` rendering varies across systemd releases and is
# not the installed unit source of truth. Compare the exact files here; the
# post-start /proc checks below independently prove the effective processes.
cmp --silent "$PREPARED_MAIN_DROPIN" \
  "$MAIN_RELEASE_DROPIN"
cmp --silent "$PREPARED_SCHEDULER_DROPIN" "$SCHEDULER_UNIT"
if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
  cmp --silent "$PREPARED_AI_WORKER_DROPIN" "$AI_WORKER_DROPIN"
fi
for legacy_main_dropin in "${LEGACY_MAIN_OVERRIDE_DROPINS[@]}"; do
  test ! -e "$legacy_main_dropin"
  test ! -L "$legacy_main_dropin"
done
for legacy_scheduler_dropin in "${LEGACY_SCHEDULER_OVERRIDE_DROPINS[@]}"; do
  test ! -e "$legacy_scheduler_dropin"
  test ! -L "$legacy_scheduler_dropin"
done
activation_snapshot_set_phase "$EXPECTED_SHA" runtime-units-installed
CUTOVER_STEP=install_strategy_governance
DATABASE_FORWARD_MIGRATION_STARTED=1
if ! run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
  --disabled --schema-prepared; then
  GOVERNANCE_TASK_TOUCHED=1
  false
fi
GOVERNANCE_TASK_TOUCHED=1
CUTOVER_STEP=run_strategy_governance
GOVERNANCE_RUN_OUTPUT=""
GOVERNANCE_RUN_STATUS=0
if GOVERNANCE_RUN_OUTPUT="$(run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/run_strategy_governance_daily.py")"; then
  GOVERNANCE_RUN_STATUS=0
else
  GOVERNANCE_RUN_STATUS=$?
fi
printf '%s\n' "$GOVERNANCE_RUN_OUTPUT"
GOVERNANCE_JSON_STATUS="$(
  printf '%s' "$GOVERNANCE_RUN_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
    'import json,sys; from datetime import date; payload=json.load(sys.stdin); status=payload.get("status") if isinstance(payload,dict) else None; blocked_keys={"status","reason","target_trade_date","input_trade_date","automatic_real_order_submission"}; target=payload.get("target_trade_date") if isinstance(payload,dict) else None; input_day=payload.get("input_trade_date") if isinstance(payload,dict) else None; dates_valid=isinstance(target,str) and isinstance(input_day,str) and all(not value or date.fromisoformat(value).isoformat()==value for value in (target,input_day)); blocked=status=="blocked" and set(payload)==blocked_keys and isinstance(payload.get("reason"),str) and bool(payload["reason"].strip()) and dates_valid and bool(target or not input_day) and payload.get("automatic_real_order_submission") is False; ok=status=="ok" and payload.get("automatic_real_order_submission") is False; normalized="blocked" if blocked else "ok" if ok else ""; print(normalized); raise SystemExit(0 if normalized else 2)'
)"
case "$GOVERNANCE_RUN_STATUS:$GOVERNANCE_JSON_STATUS" in
  0:ok) ;;
  2:blocked)
    GOVERNANCE_INPUT_NOT_READY=1
    echo "Strategy governance input is not ready; schema and task checks remain mandatory" >&2
    ;;
  *)
    printf 'strategy_governance invalid_result exit=%s json_status=%q\n' \
      "$GOVERNANCE_RUN_STATUS" "$GOVERNANCE_JSON_STATUS" >&2
    false
    ;;
esac
CUTOVER_STEP=enable_strategy_governance_task
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py"
GOVERNANCE_TASK_NEW_SOURCE="$(mktemp)"
chown "$SERVICE_USER:$SERVICE_USER" "$GOVERNANCE_TASK_NEW_SOURCE"
chmod 0600 "$GOVERNANCE_TASK_NEW_SOURCE"
CUTOVER_STEP=capture_strategy_governance_task_after_enable
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
  --capture-snapshot "$GOVERNANCE_TASK_NEW_SOURCE"
activation_snapshot_install_governance_new "$GOVERNANCE_TASK_NEW_SOURCE"
prepared_governance_snapshot verify "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
declare -a GOVERNANCE_HEALTH_ARGS=(
  --compact
  --expected-build-sha "$EXPECTED_SHA"
)
if [ "$GOVERNANCE_INPUT_NOT_READY" -eq 1 ]; then
  GOVERNANCE_HEALTH_ARGS+=(--allow-input-not-ready)
fi
CUTOVER_STEP=verify_strategy_governance_before_start
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/check_strategy_governance_health.py" \
  "${GOVERNANCE_HEALTH_ARGS[@]}"
CUTOVER_STEP=daemon_reload
sudo systemctl daemon-reload
assert_database_writer_guard_dropins_loaded
CUTOVER_STEP=verify_no_scheduler_dropins
MAIN_DROPIN_PATHS="$(systemctl show "$MAIN_SERVICE" \
  --property=DropInPaths --value)"
case " $MAIN_DROPIN_PATHS " in
  *" $MAIN_RELEASE_DROPIN "*) ;;
  *)
    printf 'main_identity missing_release_dropin=%q\n' \
      "$MAIN_DROPIN_PATHS" >&2
    false
    ;;
esac
for main_dropin_path in $MAIN_DROPIN_PATHS; do
  case "$main_dropin_path" in
    "$MAIN_RELEASE_DROPIN"|"$MAIN_LIMITS_DROPIN"|\
    "$MAIN_MARKET_RADAR_DROPIN"|"$MAIN_SERVICE_USER_DROPIN"|\
    "$MAIN_DATABASE_WRITER_GUARD_DROPIN") ;;
    *)
      printf 'main_identity unexpected_dropin=%q\n' \
        "$main_dropin_path" >&2
      false
      ;;
  esac
done
SCHEDULER_DROPIN_PATHS="$(systemctl show probiga-scheduler \
  --property=DropInPaths --value)"
EXPECTED_SCHEDULER_DROPIN_PATHS="$SCHEDULER_DATABASE_WRITER_GUARD_DROPIN"
if sudo test -f "$SCHEDULER_LIMITS_DROPIN"; then
  # This root-owned operational drop-in supplies production resource/runtime
  # limits.  The permanent writer-guard condition and this limits file are the
  # only permitted drop-ins; live process checks below independently prove code,
  # revision, adata, interpreter and script identity.
  EXPECTED_SCHEDULER_DROPIN_PATHS="$SCHEDULER_DATABASE_WRITER_GUARD_DROPIN $SCHEDULER_LIMITS_DROPIN"
fi
if [ "$SCHEDULER_DROPIN_PATHS" != "$EXPECTED_SCHEDULER_DROPIN_PATHS" ]; then
  printf 'scheduler_identity unexpected_dropins=%q\n' \
    "$SCHEDULER_DROPIN_PATHS" >&2
  false
fi
CUTOVER_STEP=sync_activation_journal_before_guard_removal
read -r ACTIVATION_MAIN_RECORD ACTIVATION_SCHEDULER_RECORD \
  ACTIVATION_AI_SERVICE_RECORD ACTIVATION_AI_TIMER_RECORD \
  < <(database_writer_guard_inventory)
controlled_guard_sync_activation_journal "$EXPECTED_SHA" \
  "$ACTIVATION_MAIN_RECORD" "$ACTIVATION_SCHEDULER_RECORD" \
  "$ACTIVATION_AI_SERVICE_RECORD" "$ACTIVATION_AI_TIMER_RECORD"
CUTOVER_STEP=remove_database_writer_guard_after_full_prestart
remove_database_writer_guard_after_recovery
DATABASE_WRITER_GUARD_PERSISTED=0
DATABASE_GUARD_MIGRATION_UNVERIFIED=0
CUTOVER_STEP=start_api
if [ "$PREVIOUS_MAIN_ENABLED" -eq 1 ]; then
  sudo systemctl enable "$MAIN_SERVICE"
else
  sudo systemctl disable "$MAIN_SERVICE"
fi
sudo systemctl start "$MAIN_SERVICE"
CUTOVER_STEP=enable_scheduler
sudo systemctl enable probiga-scheduler
CUTOVER_STEP=start_scheduler
sudo systemctl restart probiga-scheduler
CUTOVER_STEP=verify_api_process
SERVICE_MAIN_PID="$(systemctl show "$MAIN_SERVICE" --property=MainPID --value)"
case "$SERVICE_MAIN_PID" in
  ''|0|*[!0-9]*)
    echo "probiga did not expose a valid main PID after restart" >&2
    false
    ;;
esac
grep -zFx -- 'API_EMBEDDED_SCHEDULER_ENABLED=false' \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_EXPECTED_GIT_SHA=$EXPECTED_SHA" \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_BUILD_COMMIT_SHA=$EXPECTED_SHA" \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_EXPECTED_ADATA_SHA=$EXPECTED_ADATA_SHA" \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
grep -zFx -- \
  "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$EXPECTED_ADATA_TREE_SHA256" \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_ADATA_SOURCE_DIR=$ADATA_SOURCE" \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_RELEASE_TREE_SHA256=$EXPECTED_RELEASE_TREE_SHA256" \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
grep -zFx -- \
  "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
grep -zFx -- 'PYTHONSAFEPATH=1' \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
grep -zFx -- "PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
  "/proc/$SERVICE_MAIN_PID/environ" >/dev/null
mapfile -d '' -t MAIN_CMDLINE < "/proc/$SERVICE_MAIN_PID/cmdline"
case "${MAIN_CMDLINE[0]}" in
  "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python"|\
  "$EXPECTED_VENV_TARGET/bin/python") ;;
  *)
    printf 'main_identity unexpected_argv0=%q\n' "${MAIN_CMDLINE[0]}" >&2
    false
    ;;
esac
test "${MAIN_CMDLINE[1]}" = -P
test "${MAIN_CMDLINE[2]}" = -m
test "${MAIN_CMDLINE[3]}" = uvicorn
test "${MAIN_CMDLINE[4]}" = server.api.main:app
test "${MAIN_CMDLINE[5]}" = --app-dir
test "${MAIN_CMDLINE[6]}" = "$PREPARED_CODE_ROOT"
CUTOVER_STEP=verify_scheduler_process
SCHEDULER_MAIN_PID="$(systemctl show probiga-scheduler --property=MainPID --value)"
case "$SCHEDULER_MAIN_PID" in
  ''|0|*[!0-9]*)
    echo "probiga-scheduler did not expose a valid main PID after restart" >&2
    false
    ;;
esac
grep -zFx -- 'API_EMBEDDED_SCHEDULER_ENABLED=false' \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_EXPECTED_GIT_SHA=$EXPECTED_SHA" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_BUILD_COMMIT_SHA=$EXPECTED_SHA" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_EXPECTED_ADATA_SHA=$EXPECTED_ADATA_SHA" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- \
  "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$EXPECTED_ADATA_TREE_SHA256" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_ADATA_SOURCE_DIR=$ADATA_SOURCE" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_RELEASE_TREE_SHA256=$EXPECTED_RELEASE_TREE_SHA256" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- \
  "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- 'PYTHONSAFEPATH=1' \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
# The installed unit was already byte-compared with its prepared source, which
# pins PYTHONPATH twice (Environment and /usr/bin/env).  Verify the live
# interpreter and script directly here; /proc environ can omit PYTHONPATH after
# interpreter startup on this production image even though the unit is exact.
mapfile -d '' -t SCHEDULER_CMDLINE < "/proc/$SCHEDULER_MAIN_PID/cmdline"
case "${SCHEDULER_CMDLINE[0]}" in
  "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python"|\
  "$EXPECTED_VENV_TARGET/bin/python") ;;
  *)
    printf 'scheduler_identity unexpected_argv0=%q\n' \
      "${SCHEDULER_CMDLINE[0]}" >&2
    false
    ;;
esac
test "${SCHEDULER_CMDLINE[1]}" = -P
test "${SCHEDULER_CMDLINE[2]}" = \
  "$PREPARED_CODE_ROOT/tools/run_scheduler_daemon.py"
CUTOVER_STEP=verify_health
HEALTH_RESPONSE="$(mktemp)"
if ! curl --fail-with-body --silent --show-error --retry 15 \
  --retry-all-errors --retry-delay 2 --retry-connrefused \
  --output "$HEALTH_RESPONSE" http://127.0.0.1/api/health; then
  cat "$HEALTH_RESPONSE" >&2
  rm -f "$HEALTH_RESPONSE"
  false
fi
cat "$HEALTH_RESPONSE"
rm -f "$HEALTH_RESPONSE"
HEALTH_RESPONSE=""
sudo systemctl is-active --quiet "$MAIN_SERVICE"
if [ "$PREVIOUS_MAIN_ENABLED" -eq 1 ]; then
  systemctl is-enabled --quiet "$MAIN_SERVICE"
else
  ! systemctl is-enabled --quiet "$MAIN_SERVICE"
fi
point_static_release_to_checkout "$PREPARED_CODE_ROOT"
assert_nginx_static_matches_checkout "$PREPARED_CODE_ROOT"
systemctl is-active --quiet probiga-scheduler
systemctl is-enabled --quiet probiga-scheduler
if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
  CUTOVER_STEP=restore_ai_worker_previous_state
  restore_ai_worker_previous_state
  assert_ai_worker_runtime "$EXPECTED_SHA" \
    "$RELEASE_VENV_ROOT/$EXPECTED_SHA" "$PREPARED_CODE_ROOT"
  assert_ai_worker_previous_state_restored
fi
CUTOVER_STEP=verify_scheduler_triggers_quiescent
assert_scheduler_triggers_quiescent
CUTOVER_STEP=verify_premarket_quality_gate
sudo -u "$SERVICE_USER" /usr/bin/env -i \
  PATH=/usr/sbin:/usr/bin:/sbin:/bin PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
  PROBIGA_DEPLOYMENT_MODE=production \
  PROBIGA_EXPECTED_GIT_SHA="$EXPECTED_SHA" \
  PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA" \
  PROBIGA_CODE_ROOT="$PREPARED_CODE_ROOT" \
  PROBIGA_EXPECTED_ADATA_SHA="$EXPECTED_ADATA_SHA" \
  PROBIGA_EXPECTED_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256" \
  PROBIGA_ADATA_SOURCE_DIR="$ADATA_SOURCE" \
  PROBIGA_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256" \
  PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
  "PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
  "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P \
  "$PREPARED_CODE_ROOT/tools/ensure_quality_gate.py" \
  --task-type analysis_premarket_external
ACTIVE_INPUT_LOCK_SHA256="$EXPECTED_INPUT_LOCK_SHA256"
ACTIVE_RESOLVED_FREEZE_SHA256="$EXPECTED_RESOLVED_FREEZE_SHA256"
ACTIVE_ADATA_SHA="$EXPECTED_ADATA_SHA"
ACTIVE_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256"
CUTOVER_STEP=persist_deployed_receipt_pending
persist_deployed_receipt_pending
CUTOVER_STEP=finalize_activation_journal
controlled_guard_finalize_successful_activation "$EXPECTED_SHA" \
  "$ACTIVATION_MAIN_RECORD" "$ACTIVATION_SCHEDULER_RECORD" \
  "$ACTIVATION_AI_SERVICE_RECORD" "$ACTIVATION_AI_TIMER_RECORD"
CUTOVER_STEP=write_verified_activation_receipt
publish_deployed_receipt_pending "$EXPECTED_SHA"
DEPLOY_SUCCEEDED=1
trap '' TERM INT HUP
CUTOVER_STEP=remove_finalized_activation_journal
activation_snapshot_remove_finalized_before_deploy
trap - ERR TERM INT HUP
if ! prune_release_venvs "$EXPECTED_SHA" "$PREVIOUS_RELEASE_REVISION"; then
  echo "Warning: release venv cleanup failed after activation" >&2
fi
if ! prune_code_releases "$PREPARED_CODE_ROOT" "$PREVIOUS_CODE_ROOT"; then
  echo "Warning: immutable code release cleanup failed after activation" >&2
fi
if ! prune_release_temp_files; then
  echo "Warning: release temp cleanup failed after activation" >&2
fi
df -h / >&2
