#!/usr/bin/env bash
# Production deployment logic invoked by the installed root-owned broker.
# Inputs are passed explicitly through the broker's sealed environment contract.
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
CONTROLLED_GOVERNANCE_CONTRACT_TOOL=""
CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256=""
GOVERNANCE_CONTRACT_FAILURE_CODE=""
RESTORED_RUNTIME_FAILURE_CODE=""
RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE=""
RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH=""
RELEASE_VENV_ROOT=/var/lib/probiga/release-venvs
RELEASE_ARTIFACT_ROOT=/var/lib/probiga/release-artifacts
ADATA_RUNTIME_ROOT=/var/lib/probiga/release-sources/adata
QMT_ANNOUNCEMENT_CHECKPOINT_ROOT=/var/lib/probiga/qmt-announcement-checkpoints
QMT_FULL_MARKET_HISTORY_STATE_ROOT=/var/lib/probiga/qmt-full-market-history
QMT_LOCAL_GAP_REPAIR_STATE_ROOT=/var/lib/probiga/qmt-local-gap-repair
PROBIGA_JOB_LOG_ROOT=/var/lib/probiga/jobs
RELEASE_DATA_READINESS_STATUS_ROOT=/var/lib/probiga/release-data-readiness
LEGACY_RELEASE_VENV_ROOT=/opt/ProBigA/.release_venvs
DEPLOY_LOCK_ROOT=/run/probiga
DEPLOY_LOCK_FILE="$DEPLOY_LOCK_ROOT/production-deploy.lock"
REQUIRED_DEPLOY_PROTOCOL_V4=probiga-production-deploy-v4
RETIRED_DEPLOY_PROTOCOL_V2=probiga-production-deploy-v2
REQUIRED_RECOVERY_PROTOCOL=probiga-database-guard-recovery-v2
readonly QMT_EDGE_DEPLOY_BLOCKING=0
# Market-history attestation is a data-readiness concern, not a code-release
# prerequisite.  Keep the legacy exact-window gate available for controlled
# maintenance, but production publication must not stop the API/scheduler to
# rescan 120 sessions before it can install a new immutable release.
readonly QMT_HISTORY_DEPLOY_BLOCKING=0
# Publish verified code independently of missing market/strategy results.
# Schema, identity, writer fencing and the final activation grant remain hard
# gates. The exact-build data observer runs after EVERY successful release.
readonly RELEASE_DATA_VALIDATION_BLOCKING=0
# Reviewed first installation only: install compatible readers/controllers
# through the existing v1 hold/grant sequence before writing any new context.
# The privileged tool rejects this path once any protected context exists.
# Both hosts proved exact-ready on d52a79b on 2026-09-05. New attempts now use
# the prior trusted controller and its protected, unchanged-schema recovery.
readonly QMT_EDGE_RECOVERY_COMPATIBILITY_INSTALL=0
readonly DEPENDENCY_DOWNLOAD_TIMEOUT=30m
DEPLOY_ARTIFACT_MODE=""
prepare_qmt_announcement_checkpoint_root() {
  local parent_root=/var/lib/probiga
  local unsafe_link
  if [ -e "$parent_root" ] || [ -L "$parent_root" ]; then
    test -d "$parent_root" || return 2
    test ! -L "$parent_root" || return 2
  fi
  install -d -o root -g root -m 0755 "$parent_root" || return 2
  test "$(readlink -f -- "$parent_root")" = "$parent_root" || return 2
  if [ -e "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" ] || \
    [ -L "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" ]; then
    test -d "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" || return 2
    test ! -L "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" || return 2
  fi
  install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0700 \
    "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" || return 2
  test ! -L "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" || return 2
  test "$(readlink -f -- "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT")" = \
    "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" || return 2
  test "$(stat -c '%U:%G' -- "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT")" = \
    "$SERVICE_USER:$SERVICE_USER" || return 2
  test "$(stat -c '%a' -- "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT")" = 700 || \
    return 2
  unsafe_link="$(find -P "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" -mindepth 1 \
    -type l -print -quit)" || return 2
  if [ -n "$unsafe_link" ]; then
    echo "QMT announcement checkpoint tree contains a symlink: $unsafe_link" >&2
    return 2
  fi
  sudo -u "$SERVICE_USER" test -r "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" || \
    return 2
  sudo -u "$SERVICE_USER" test -w "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" || \
    return 2
  sudo -u "$SERVICE_USER" test -x "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" || \
    return 2
  return 0
}
prepare_qmt_full_market_history_state_root() {
  local parent_root=/var/lib/probiga
  local unsafe_entry
  if [ -e "$parent_root" ] || [ -L "$parent_root" ]; then
    test -d "$parent_root" || return 2
    test ! -L "$parent_root" || return 2
  fi
  install -d -o root -g root -m 0755 "$parent_root" || return 2
  test "$(readlink -f -- "$parent_root")" = "$parent_root" || return 2
  if [ -e "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" ] || \
    [ -L "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" ]; then
    test -d "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" || return 2
    test ! -L "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" || return 2
    test "$(stat -c '%U:%G' -- "$QMT_FULL_MARKET_HISTORY_STATE_ROOT")" = \
      "$SERVICE_USER:$SERVICE_USER" || return 2
    test "$(stat -c '%a' -- "$QMT_FULL_MARKET_HISTORY_STATE_ROOT")" = 700 || \
      return 2
  else
    install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0700 \
      "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" || return 2
  fi
  test ! -L "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" || return 2
  test "$(readlink -f -- "$QMT_FULL_MARKET_HISTORY_STATE_ROOT")" = \
    "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" || return 2
  test "$(stat -c '%U:%G' -- "$QMT_FULL_MARKET_HISTORY_STATE_ROOT")" = \
    "$SERVICE_USER:$SERVICE_USER" || return 2
  test "$(stat -c '%a' -- "$QMT_FULL_MARKET_HISTORY_STATE_ROOT")" = 700 || \
    return 2
  unsafe_entry="$(find -P "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" \
    -mindepth 1 -maxdepth 1 \
    \( ! -type f -o ! -user "$SERVICE_USER" -o ! -group "$SERVICE_USER" \
       -o ! -perm 0600 -o -perm /7177 \) -print -quit)" || return 2
  if [ -n "$unsafe_entry" ]; then
    echo "QMT full-market history state entry is unsafe: $unsafe_entry" >&2
    return 2
  fi
  sudo -u "$SERVICE_USER" test -r "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" || \
    return 2
  sudo -u "$SERVICE_USER" test -w "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" || \
    return 2
  sudo -u "$SERVICE_USER" test -x "$QMT_FULL_MARKET_HISTORY_STATE_ROOT" || \
    return 2
  return 0
}
prepare_qmt_local_gap_repair_state_root() {
  local parent_root=/var/lib/probiga
  local unsafe_entry
  if [ -e "$parent_root" ] || [ -L "$parent_root" ]; then
    test -d "$parent_root" || return 2
    test ! -L "$parent_root" || return 2
  fi
  install -d -o root -g root -m 0755 "$parent_root" || return 2
  test "$(readlink -f -- "$parent_root")" = "$parent_root" || return 2
  if [ -e "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" ] || \
    [ -L "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" ]; then
    test -d "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" || return 2
    test ! -L "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" || return 2
    test "$(stat -c '%U:%G' -- "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT")" = \
      "$SERVICE_USER:$SERVICE_USER" || return 2
    test "$(stat -c '%a' -- "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT")" = 700 || \
      return 2
  else
    install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0700 \
      "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" || return 2
  fi
  test ! -L "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" || return 2
  test "$(readlink -f -- "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT")" = \
    "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" || return 2
  test "$(stat -c '%U:%G' -- "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT")" = \
    "$SERVICE_USER:$SERVICE_USER" || return 2
  test "$(stat -c '%a' -- "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT")" = 700 || \
    return 2
  unsafe_entry="$(find -P "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" \
    -mindepth 1 -maxdepth 1 \
    \( ! -type f -o ! -user "$SERVICE_USER" -o ! -group "$SERVICE_USER" \
       -o ! -perm 0600 -o -perm /7177 \) -print -quit)" || return 2
  if [ -n "$unsafe_entry" ]; then
    echo "QMT local gap-repair state entry is unsafe: $unsafe_entry" >&2
    return 2
  fi
  sudo -u "$SERVICE_USER" test -r "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" || \
    return 2
  sudo -u "$SERVICE_USER" test -w "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" || \
    return 2
  sudo -u "$SERVICE_USER" test -x "$QMT_LOCAL_GAP_REPAIR_STATE_ROOT" || \
    return 2
  return 0
}
migrate_legacy_flow_progress() {
  /usr/bin/python3.14 -I - "$PROBIGA_JOB_LOG_ROOT" \
    /var/lib/probiga/linux-flow-repair "$(id -u -- "$SERVICE_USER")" \
    "$(id -g -- "$SERVICE_USER")" "$1" <<'PY' || return 2
import os
import re
import stat
import sys
from pathlib import Path

jobs, state = map(Path, sys.argv[1:3])
uid, gid = map(int, sys.argv[3:5])
apply = sys.argv[5] == "apply"

def check(path, directory=False):
    info = path.lstat()
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    modes = {0o700} if directory else {0o600, 0o644}
    if not (kind(info.st_mode) and info.st_uid == uid and info.st_gid == gid
            and stat.S_IMODE(info.st_mode) in modes
            and (directory or info.st_nlink == 1)):
        raise SystemExit(f"unsafe flow progress entry: {path}")
    return info.st_dev, info.st_ino

def inspect_tree(root):
    identity = check(root, directory=True)
    for item in root.iterdir():
        if re.fullmatch(r"attempt-[A-Za-z0-9_-]+", item.name):
            check(item, directory=True)
            for evidence in item.iterdir():
                if evidence.name not in {"manifest.json", "capital-flow-corrected-rows-before.jsonl.gz"}:
                    raise SystemExit(f"unknown flow evidence: {evidence}")
                check(evidence)
        elif item.name == "flow-fetch-progress.json" or re.fullmatch(r"flow-progress-[A-Za-z0-9_-]+", item.name):
            check(item)
        else:
            raise SystemExit(f"unknown flow progress entry: {item}")
    return identity

if jobs.exists():
    check(jobs, directory=True)
if state.exists() or state.is_symlink():
    check(state, directory=True)
    for item in state.iterdir():
        if not re.fullmatch(r"flow-\d{4}-\d{2}-\d{2}", item.name):
            raise SystemExit(f"unknown flow state entry: {item}")
        inspect_tree(item)
candidates = []
for item in jobs.iterdir() if jobs.exists() else ():
    if re.fullmatch(r"flow-\d{4}-\d{2}-\d{2}", item.name):
        identity = inspect_tree(item)
        if (state / item.name).exists():
            raise SystemExit(f"flow progress destination already exists: {item.name}")
        candidates.append((item, identity))
if apply:
    if not state.exists():
        state.mkdir(mode=0o700)
        os.chown(state, uid, gid)
    check(state, directory=True)
    for item, identity in candidates:
        if inspect_tree(item) != identity:
            raise SystemExit(f"flow progress identity changed: {item}")
        item.rename(state / item.name)
    print(f"flow progress migration preserved {len(candidates)} directories")
PY
}
prepare_probiga_job_log_root() {
  local parent_root=/var/lib/probiga
  local service_gid
  local service_uid
  if [ -e "$parent_root" ] || [ -L "$parent_root" ]; then
    test -d "$parent_root" || return 2
    test ! -L "$parent_root" || return 2
  fi
  install -d -o root -g root -m 0755 "$parent_root" || return 2
  test "$(readlink -f -- "$parent_root")" = "$parent_root" || return 2
  if [ -e "$PROBIGA_JOB_LOG_ROOT" ] || [ -L "$PROBIGA_JOB_LOG_ROOT" ]; then
    test -d "$PROBIGA_JOB_LOG_ROOT" || return 2
    test ! -L "$PROBIGA_JOB_LOG_ROOT" || return 2
    test "$(stat -c '%U:%G' -- "$PROBIGA_JOB_LOG_ROOT")" = \
      "$SERVICE_USER:$SERVICE_USER" || return 2
    test "$(stat -c '%a' -- "$PROBIGA_JOB_LOG_ROOT")" = 700 || return 2
  else
    install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0700 \
      "$PROBIGA_JOB_LOG_ROOT" || return 2
  fi
  test ! -L "$PROBIGA_JOB_LOG_ROOT" || return 2
  test "$(readlink -f -- "$PROBIGA_JOB_LOG_ROOT")" = \
    "$PROBIGA_JOB_LOG_ROOT" || return 2
  test "$(stat -c '%U:%G' -- "$PROBIGA_JOB_LOG_ROOT")" = \
    "$SERVICE_USER:$SERVICE_USER" || return 2
  test "$(stat -c '%a' -- "$PROBIGA_JOB_LOG_ROOT")" = 700 || return 2
  service_uid="$(id -u -- "$SERVICE_USER")" || return 2
  service_gid="$(id -g -- "$SERVICE_USER")" || return 2
  # The old writers may still be running here. This pass is deliberately
  # read-only: the two exact legacy basenames may be 0600 or 0644, while every
  # other entry must already satisfy the final detached-job state contract.
  /usr/bin/python3.14 -I - "$PROBIGA_JOB_LOG_ROOT" \
    "$service_uid" "$service_gid" <<'PY' || return 2
import os
import re
import stat
import sys

root, expected_uid_text, expected_gid_text = sys.argv[1:]
expected_uid = int(expected_uid_text)
expected_gid = int(expected_gid_text)
legacy_names = frozenset((
    "stock-finance-daily-v2.json",
    "target-turnover-snapshot-v1.json",
))
directory_fd = os.open(
    root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
)
try:
    for name in os.listdir(directory_fd):
        observed = os.lstat(name, dir_fd=directory_fd)
        # Validated read-only by migrate_legacy_flow_progress inspect; moved
        # only after all writers stop, before the final flat-log check.
        if re.fullmatch(r"flow-\d{4}-\d{2}-\d{2}", name) and stat.S_ISDIR(observed.st_mode):
            continue
        observed_mode = stat.S_IMODE(observed.st_mode)
        allowed_modes = {0o600, 0o644} if name in legacy_names else {0o600}
        if not (
            stat.S_ISREG(observed.st_mode)
            and observed.st_uid == expected_uid
            and observed.st_gid == expected_gid
            and observed.st_nlink == 1
            and observed_mode in allowed_modes
        ):
            raise SystemExit(f"unsafe detached job log entry: {name!r}")
finally:
    os.close(directory_fd)
PY
  return 0
}
migrate_probiga_job_log_legacy_modes() {
  local service_gid
  local service_uid
  local unsafe_entry
  service_uid="$(id -u -- "$SERVICE_USER")" || return 2
  service_gid="$(id -g -- "$SERVICE_USER")" || return 2
  sudo -u "$SERVICE_USER" /usr/bin/python3.14 -I - \
    "$PROBIGA_JOB_LOG_ROOT" "$service_uid" "$service_gid" <<'PY' || return 2
import os
import stat
import sys

root, expected_uid_text, expected_gid_text = sys.argv[1:]
expected_uid = int(expected_uid_text)
expected_gid = int(expected_gid_text)
legacy_names = frozenset((
    "stock-finance-daily-v2.json",
    "target-turnover-snapshot-v1.json",
))


def require_file(metadata, *, modes, name):
    mode = stat.S_IMODE(metadata.st_mode)
    if not (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == expected_uid
        and metadata.st_gid == expected_gid
        and metadata.st_nlink == 1
        and mode in modes
    ):
        raise SystemExit(f"unsafe detached job log entry: {name!r}")


directory_fd = os.open(
    root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
)
try:
    directory = os.fstat(directory_fd)
    if not (
        stat.S_ISDIR(directory.st_mode)
        and directory.st_uid == expected_uid
        and directory.st_gid == expected_gid
        and stat.S_IMODE(directory.st_mode) == 0o700
    ):
        raise SystemExit("unsafe detached job log root")

    names = set(os.listdir(directory_fd))
    identities = {}
    for name in names:
        metadata = os.lstat(name, dir_fd=directory_fd)
        require_file(
            metadata,
            modes={0o600, 0o644} if name in legacy_names else {0o600},
            name=name,
        )
        identities[name] = (metadata.st_dev, metadata.st_ino)

    open_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
    for name in sorted(names & legacy_names):
        descriptor = os.open(name, open_flags, dir_fd=directory_fd)
        try:
            opened = os.fstat(descriptor)
            require_file(opened, modes={0o600, 0o644}, name=name)
            if (opened.st_dev, opened.st_ino) != identities[name]:
                raise SystemExit(f"detached job log entry changed: {name!r}")
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            secured = os.fstat(descriptor)
            require_file(secured, modes={0o600}, name=name)
            observed = os.lstat(name, dir_fd=directory_fd)
            require_file(observed, modes={0o600}, name=name)
            if (observed.st_dev, observed.st_ino) != (
                secured.st_dev,
                secured.st_ino,
            ):
                raise SystemExit(f"detached job log entry changed: {name!r}")
        finally:
            os.close(descriptor)

    probe_name = ".probiga-deploy-write-probe"
    probe_flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW
    probe = os.open(probe_name, probe_flags, 0o600, dir_fd=directory_fd)
    try:
        os.write(probe, b"writable\n")
        os.fsync(probe)
    finally:
        os.close(probe)
        os.unlink(probe_name, dir_fd=directory_fd)

    if set(os.listdir(directory_fd)) != names:
        raise SystemExit("detached job log tree changed during migration")
    for name in names:
        observed = os.lstat(name, dir_fd=directory_fd)
        require_file(observed, modes={0o600}, name=name)
    os.fsync(directory_fd)
finally:
    os.close(directory_fd)
PY
  unsafe_entry="$(find -P "$PROBIGA_JOB_LOG_ROOT" -mindepth 1 -maxdepth 1 \
    \( ! -type f -o ! -user "$SERVICE_USER" -o ! -group "$SERVICE_USER" \
       -o ! -links 1 -o ! -perm 0600 -o -perm /7177 \) \
    -print -quit)" || return 2
  if [ -n "$unsafe_entry" ]; then
    echo "detached job log entry is unsafe: $unsafe_entry" >&2
    return 2
  fi
  return 0
}
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
  "$RETIRED_DEPLOY_PROTOCOL_V2")
    echo "v2 production deploy protocol is retired and unsupported" >&2
    exit 2
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
  : "${PROBIGA_RECOVERY_TOOL_SHA:?broker-validated recovery tool SHA is required}"
  [[ "$PROBIGA_RECOVERY_GUARD_SHA" =~ ^[0-9a-f]{40}$ ]]
  [[ "$PROBIGA_RECOVERY_TOOL_SHA" =~ ^[0-9a-f]{40}$ ]]
elif [ -n "${PROBIGA_RECOVERY_GUARD_SHA:-}" ] || \
  [ -n "${PROBIGA_RECOVERY_TOOL_SHA:-}" ]; then
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
  if [ -n "${CONTROLLED_GOVERNANCE_CONTRACT_TOOL:-}" ]; then
    case "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" in
      /tmp/.probiga-governance-contract.*)
        rm -f -- "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" || true
        ;;
    esac
    CONTROLLED_GOVERNANCE_CONTRACT_TOOL=""
    CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256=""
  fi
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
ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/qmt-announcement-task-old.json"
ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/qmt-announcement-task-old.sha256"
ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/qmt-announcement-task-new.json"
ACTIVATION_QMT_ANNOUNCEMENT_NEW_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/qmt-announcement-task-new.sha256"
ACTIVATION_RECEIPT_PENDING="$ACTIVATION_UNIT_SNAPSHOT_DIR/deployed-receipt-pending.json"
ACTIVATION_RECEIPT_PENDING_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/deployed-receipt-pending.sha256"
V2_FORWARD_FINALIZED_SHA=""
V2_FORWARD_FINALIZED_REQUEST_MATCH=0
V2_FORWARD_PRESERVED_NO_RECEIPT_SHA=""
V2_FORWARD_PRESERVED_MAIN_RECORD=""
V2_FORWARD_PRESERVED_SCHEDULER_RECORD=""
V2_FORWARD_PRESERVED_AI_SERVICE_RECORD=""
V2_FORWARD_PRESERVED_AI_TIMER_RECORD=""
V2_RECOVERY_STEP=not-started
# A completed production health scan has taken about 21 minutes.  Bound each
# direct database gate at 30 minutes so a wedged query cannot leave every
# writer fenced forever, while preserving measured headroom for a healthy run.
CONTROLLED_DATABASE_GATE_TIMEOUT=30m
CONTROLLED_DATABASE_GATE_KILL_AFTER=30s
CONTROLLED_RECOVERY_CUTOVER_RESERVE_SECONDS=10800
CONTROLLED_RECOVERY_UNIT_START_TIMEOUT=2m
readonly CONTROLLED_DATABASE_GATE_TIMEOUT CONTROLLED_DATABASE_GATE_KILL_AFTER \
  CONTROLLED_RECOVERY_CUTOVER_RESERVE_SECONDS \
  CONTROLLED_RECOVERY_UNIT_START_TIMEOUT
ACTIVATION_RELEASE_IDENTITY="$ACTIVATION_UNIT_SNAPSHOT_DIR/release-identity"
ACTIVATION_RELEASE_IDENTITY_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/release-identity.sha256"
RECEIPT_DIR=/var/lib/probiga/deploy-receipts
DEPLOY_FAILURE_AUDIT_DIR=/var/lib/probiga/deploy-failure-audit
DATABASE_WRITER_GUARD_DROPIN_NAME=database-writer-guard.conf
RECOVERY_CUTOVER_DROPIN_NAME=zzzzzz-probiga-governance-cutover.conf
MAIN_RELEASE_DROPIN=/etc/systemd/system/probiga.service.d/scheduler.conf
SCHEDULER_UNIT=/etc/systemd/system/probiga-scheduler.service
AI_WORKER_DROPIN=/etc/systemd/system/probiga-ai-recommendation-worker.service.d/release-runtime.conf
STATIC_RELEASE_LINK=/opt/ProBigA-current
MAIN_DATABASE_WRITER_GUARD_DROPIN="/etc/systemd/system/probiga.service.d/$DATABASE_WRITER_GUARD_DROPIN_NAME"
SCHEDULER_DATABASE_WRITER_GUARD_DROPIN="/etc/systemd/system/probiga-scheduler.service.d/$DATABASE_WRITER_GUARD_DROPIN_NAME"
AI_SERVICE_DATABASE_WRITER_GUARD_DROPIN="/etc/systemd/system/probiga-ai-recommendation-worker.service.d/$DATABASE_WRITER_GUARD_DROPIN_NAME"
AI_TIMER_DATABASE_WRITER_GUARD_DROPIN="/etc/systemd/system/probiga-ai-recommendation-worker.timer.d/$DATABASE_WRITER_GUARD_DROPIN_NAME"
MAIN_RECOVERY_CUTOVER_DROPIN="/etc/systemd/system/probiga.service.d/$RECOVERY_CUTOVER_DROPIN_NAME"
SCHEDULER_RECOVERY_CUTOVER_DROPIN="/etc/systemd/system/probiga-scheduler.service.d/$RECOVERY_CUTOVER_DROPIN_NAME"
AI_SERVICE_RECOVERY_CUTOVER_DROPIN="/etc/systemd/system/probiga-ai-recommendation-worker.service.d/$RECOVERY_CUTOVER_DROPIN_NAME"
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
  controlled_guard_assert_file \
    "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" 600 || return 1
  controlled_guard_assert_file "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA" 600 || \
    return 1
  controlled_guard_assert_file "$ACTIVATION_RELEASE_IDENTITY" 600 || return 1
  controlled_guard_assert_file "$ACTIVATION_RELEASE_IDENTITY_SHA" 600 || \
    return 1
  test "$(<"$ACTIVATION_UNIT_SNAPSHOT_STATE_SHA")" = \
    "$(sha256sum "$ACTIVATION_UNIT_SNAPSHOT_STATE" | cut -d' ' -f1)" || return 1
  test "$(<"$ACTIVATION_GOVERNANCE_OLD_SHA")" = \
    "$(sha256sum "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" | cut -d' ' -f1)" || \
    return 1
  test "$(<"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA")" = \
    "$(sha256sum "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" | cut -d' ' -f1)" || \
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
  local sha
  local snapshot
  for snapshot in "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" \
    "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT"; do
    case "$snapshot" in
      "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT")
        sha="$ACTIVATION_GOVERNANCE_NEW_SHA"
        ;;
      "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT")
        sha="$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SHA"
        ;;
      *) return 1 ;;
    esac
    controlled_guard_assert_file "$snapshot" 600 || return 1
    if [ -e "$sha" ] || [ -L "$sha" ]; then
      controlled_guard_assert_file "$sha" 600 || return 1
      test "$(<"$sha")" = "$(sha256sum "$snapshot" | cut -d' ' -f1)" || \
        return 1
    else
      # The canonical snapshot is fsynced and atomically renamed before its
      # redundant checksum, so a crash in between remains safely recoverable.
      test ! -e "$sha" || return 1
      test ! -L "$sha" || return 1
    fi
  done
  return 0
}
activation_snapshot_install_task_new() {
  local source="$1"
  local target_snapshot="$2"
  local target_sha="$3"
  local temporary_prefix="$4"
  local snapshot_tmp
  local sha_tmp
  activation_snapshot_validate "$EXPECTED_SHA" >/dev/null || return 1
  test -f "$source" || return 1
  test ! -L "$source" || return 1
  snapshot_tmp="$(mktemp "$ACTIVATION_UNIT_SNAPSHOT_DIR/.${temporary_prefix}-new.XXXXXX")" || \
    return 1
  sha_tmp="$(mktemp "$ACTIVATION_UNIT_SNAPSHOT_DIR/.${temporary_prefix}-new-sha.XXXXXX")" || {
    rm -f -- "$snapshot_tmp"
    return 1
  }
  if ! install -o root -g root -m 0600 "$source" "$snapshot_tmp" || \
    ! printf '%s\n' "$(sha256sum "$snapshot_tmp" | cut -d' ' -f1)" \
      > "$sha_tmp" || ! chown root:root "$sha_tmp" || ! chmod 0600 "$sha_tmp" || \
    ! sync -f "$snapshot_tmp" || ! sync -f "$sha_tmp" || \
    ! mv -fT "$snapshot_tmp" "$target_snapshot" || \
    ! mv -fT "$sha_tmp" "$target_sha" || \
    ! sync -f "$ACTIVATION_UNIT_SNAPSHOT_DIR"; then
    rm -f -- "$snapshot_tmp" "$sha_tmp"
    return 1
  fi
  controlled_guard_assert_file "$target_snapshot" 600 || return 1
  controlled_guard_assert_file "$target_sha" 600 || return 1
  test "$(<"$target_sha")" = \
    "$(sha256sum "$target_snapshot" | cut -d' ' -f1)" || return 1
  return 0
}
activation_snapshot_install_governance_new() {
  activation_snapshot_install_task_new "$1" \
    "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" \
    "$ACTIVATION_GOVERNANCE_NEW_SHA" governance
}
activation_snapshot_install_qmt_announcement_new() {
  activation_snapshot_install_task_new "$1" \
    "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT" \
    "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SHA" qmt-announcement || return 1
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
import hashlib
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
  test -n "${QMT_ANNOUNCEMENT_TASK_OLD_SOURCE:-}" || {
    rm -rf -- "$build_dir"
    return 1
  }
  test -f "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE" || {
    rm -rf -- "$build_dir"
    return 1
  }
  test ! -L "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE" || {
    rm -rf -- "$build_dir"
    return 1
  }
  install -o root -g root -m 0600 "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE" \
    "$build_dir/qmt-announcement-task-old.json" || {
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
  printf '%s\n' \
    "$(sha256sum "$build_dir/qmt-announcement-task-old.json" | cut -d' ' -f1)" \
    > "$build_dir/qmt-announcement-task-old.sha256" || {
      rm -rf -- "$build_dir"
      return 1
    }
  printf '%s\n' "$(sha256sum "$release_identity_tmp" | cut -d' ' -f1)" \
    > "$build_dir/release-identity.sha256" || {
      rm -rf -- "$build_dir"
      return 1
    }
  chown root:root "$build_dir/writer-state.sha256" \
    "$build_dir/governance-task-old.sha256" \
    "$build_dir/qmt-announcement-task-old.json" \
    "$build_dir/qmt-announcement-task-old.sha256" "$release_identity_tmp" \
    "$build_dir/release-identity.sha256" || {
      rm -rf -- "$build_dir"
      return 1
    }
  chmod 0600 "$build_dir/writer-state.sha256" \
    "$build_dir/governance-task-old.sha256" \
    "$build_dir/qmt-announcement-task-old.json" \
    "$build_dir/qmt-announcement-task-old.sha256" "$release_identity_tmp" \
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
  sync -f "$build_dir/qmt-announcement-task-old.json" || {
    rm -rf -- "$build_dir"
    return 1
  }
  sync -f "$build_dir/qmt-announcement-task-old.sha256" || {
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
controlled_guard_assert_activation_deadline() {
  local deadline_epoch="$1"
  local now_epoch
  [[ "$deadline_epoch" =~ ^[1-9][0-9]{9,11}$ ]] || return 1
  now_epoch="$(/usr/bin/date +%s)" || return 1
  [[ "$now_epoch" =~ ^[1-9][0-9]{9,11}$ ]] || return 1
  test "$now_epoch" -lt "$deadline_epoch" || return 1
  return 0
}
controlled_guard_cutover_exec_line() {
  local deadline_epoch="$1"
  [[ "$deadline_epoch" =~ ^[1-9][0-9]{9,11}$ ]] || return 1
  printf "%s%s%s\n" \
    "ExecStartPre=/usr/bin/python3.14 -I -c 'import sys,time;" \
    "sys.exit(0 if int(time.time()) < $deadline_epoch" \
    " else 1)'"
}
controlled_guard_assert_recovery_cutover_dropin() {
  local deadline_epoch="${2:-}"
  local embedded_deadline
  local path="$1"
  local prefix
  local suffix
  local -a lines=()
  case "$path" in
    "$MAIN_RECOVERY_CUTOVER_DROPIN"|\
    "$SCHEDULER_RECOVERY_CUTOVER_DROPIN"|\
    "$AI_SERVICE_RECOVERY_CUTOVER_DROPIN") ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_file "$path" 644 || return 1
  mapfile -t lines < "$path" || return 1
  test "${#lines[@]}" -eq 2 || return 1
  test "${lines[0]}" = '[Service]' || return 1
  prefix="ExecStartPre=/usr/bin/python3.14 -I -c 'import sys,time;sys.exit(0 if int(time.time()) < "
  suffix=" else 1)'"
  case "${lines[1]}" in
    "$prefix"*"$suffix") ;;
    *) return 1 ;;
  esac
  embedded_deadline="${lines[1]#"$prefix"}"
  embedded_deadline="${embedded_deadline%"$suffix"}"
  [[ "$embedded_deadline" =~ ^[1-9][0-9]{9,11}$ ]] || return 1
  if [ -n "$deadline_epoch" ]; then
    test "$embedded_deadline" = "$deadline_epoch" || return 1
  fi
  return 0
}
controlled_guard_install_recovery_cutover_dropin() {
  local deadline_epoch="$2"
  local parent
  local path="$1"
  local prepared_dropin
  controlled_guard_assert_activation_deadline "$deadline_epoch" || return 1
  case "$path" in
    "$MAIN_RECOVERY_CUTOVER_DROPIN"|\
    "$SCHEDULER_RECOVERY_CUTOVER_DROPIN"|\
    "$AI_SERVICE_RECOVERY_CUTOVER_DROPIN") ;;
    *) return 1 ;;
  esac
  parent="$(dirname "$path")" || return 1
  test -d "$parent" || return 1
  test ! -L "$parent" || return 1
  test "$(readlink -f "$parent")" = "$parent" || return 1
  test "$(stat -c '%U:%G' "$parent")" = root:root || return 1
  test "$(stat -c '%a' "$parent")" = 755 || return 1
  if [ -e "$path" ] || [ -L "$path" ]; then
    controlled_guard_assert_recovery_cutover_dropin "$path" || return 1
  fi
  prepared_dropin="$(mktemp "$parent/.recovery-cutover.XXXXXX")" || return 1
  if ! printf '%s\n' '[Service]' > "$prepared_dropin" || \
    ! controlled_guard_cutover_exec_line "$deadline_epoch" \
      >> "$prepared_dropin" || \
    ! chown root:root "$prepared_dropin" || \
    ! chmod 0644 "$prepared_dropin" || \
    ! sync -f "$prepared_dropin" || \
    ! mv -fT "$prepared_dropin" "$path" || \
    ! sync -f "$parent"; then
    rm -f -- "$prepared_dropin"
    return 1
  fi
  controlled_guard_assert_recovery_cutover_dropin \
    "$path" "$deadline_epoch" || return 1
  return 0
}
controlled_guard_assert_recovery_cutover_loaded() {
  local deadline_epoch="$3"
  local path="$2"
  local unit="$1"
  local -a loaded_dropins=()
  controlled_guard_assert_recovery_cutover_dropin \
    "$path" "$deadline_epoch" || return 1
  read -r -a loaded_dropins <<< \
    "$(systemctl show -p DropInPaths --value "$unit")" || return 1
  test "${#loaded_dropins[@]}" -gt 0 || return 1
  test "${loaded_dropins[-1]}" = "$path" || return 1
  return 0
}
controlled_guard_assert_recovery_cutover_unloaded() {
  local loaded_dropin
  local path="$2"
  local unit="$1"
  local -a loaded_dropins=()
  read -r -a loaded_dropins <<< \
    "$(systemctl show -p DropInPaths --value "$unit")" || return 1
  for loaded_dropin in "${loaded_dropins[@]}"; do
    test "$loaded_dropin" != "$path" || return 1
  done
  return 0
}
controlled_guard_install_recovery_cutover_dropins() {
  local ai_service_load="${3%%,*}"
  local deadline_epoch="$1"
  local scheduler_load="${2%%,*}"
  controlled_guard_install_recovery_cutover_dropin \
    "$MAIN_RECOVERY_CUTOVER_DROPIN" "$deadline_epoch" || return 1
  if [ "$scheduler_load" = loaded ]; then
    controlled_guard_install_recovery_cutover_dropin \
      "$SCHEDULER_RECOVERY_CUTOVER_DROPIN" "$deadline_epoch" || return 1
  else
    test "$scheduler_load" = not-found || return 1
    test ! -e "$SCHEDULER_RECOVERY_CUTOVER_DROPIN" || return 1
    test ! -L "$SCHEDULER_RECOVERY_CUTOVER_DROPIN" || return 1
  fi
  if [ "$ai_service_load" = loaded ]; then
    controlled_guard_install_recovery_cutover_dropin \
      "$AI_SERVICE_RECOVERY_CUTOVER_DROPIN" "$deadline_epoch" || return 1
  else
    test "$ai_service_load" = not-found || return 1
    test ! -e "$AI_SERVICE_RECOVERY_CUTOVER_DROPIN" || return 1
    test ! -L "$AI_SERVICE_RECOVERY_CUTOVER_DROPIN" || return 1
  fi
  systemctl daemon-reload || return 1
  controlled_guard_assert_recovery_cutover_loaded probiga \
    "$MAIN_RECOVERY_CUTOVER_DROPIN" "$deadline_epoch" || return 1
  if [ "$scheduler_load" = loaded ]; then
    controlled_guard_assert_recovery_cutover_loaded probiga-scheduler \
      "$SCHEDULER_RECOVERY_CUTOVER_DROPIN" "$deadline_epoch" || return 1
  fi
  if [ "$ai_service_load" = loaded ]; then
    controlled_guard_assert_recovery_cutover_loaded \
      probiga-ai-recommendation-worker.service \
      "$AI_SERVICE_RECOVERY_CUTOVER_DROPIN" "$deadline_epoch" || return 1
  fi
  return 0
}
controlled_guard_remove_recovery_cutover_dropin() {
  local deadline_epoch="$2"
  local parent
  local path="$1"
  controlled_guard_assert_recovery_cutover_dropin \
    "$path" "$deadline_epoch" || return 1
  parent="$(dirname "$path")" || return 1
  rm -f -- "$path" || return 1
  sync -f "$parent" || return 1
  test ! -e "$path" || return 1
  test ! -L "$path" || return 1
  return 0
}
controlled_guard_remove_recovery_cutover_dropins() {
  local ai_service_load="${3%%,*}"
  local deadline_epoch="$1"
  local scheduler_load="${2%%,*}"
  controlled_guard_remove_recovery_cutover_dropin \
    "$MAIN_RECOVERY_CUTOVER_DROPIN" "$deadline_epoch" || return 1
  if [ "$scheduler_load" = loaded ]; then
    controlled_guard_remove_recovery_cutover_dropin \
      "$SCHEDULER_RECOVERY_CUTOVER_DROPIN" "$deadline_epoch" || return 1
  fi
  if [ "$ai_service_load" = loaded ]; then
    controlled_guard_remove_recovery_cutover_dropin \
      "$AI_SERVICE_RECOVERY_CUTOVER_DROPIN" "$deadline_epoch" || return 1
  fi
  systemctl daemon-reload || return 1
  for path in "$MAIN_RECOVERY_CUTOVER_DROPIN" \
    "$SCHEDULER_RECOVERY_CUTOVER_DROPIN" \
    "$AI_SERVICE_RECOVERY_CUTOVER_DROPIN"; do
    test ! -e "$path" || return 1
    test ! -L "$path" || return 1
  done
  controlled_guard_assert_recovery_cutover_unloaded probiga \
    "$MAIN_RECOVERY_CUTOVER_DROPIN" || return 1
  if [ "$scheduler_load" = loaded ]; then
    controlled_guard_assert_recovery_cutover_unloaded probiga-scheduler \
      "$SCHEDULER_RECOVERY_CUTOVER_DROPIN" || return 1
  fi
  if [ "$ai_service_load" = loaded ]; then
    controlled_guard_assert_recovery_cutover_unloaded \
      probiga-ai-recommendation-worker.service \
      "$AI_SERVICE_RECOVERY_CUTOVER_DROPIN" || return 1
  fi
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
  local activation_deadline_epoch="${6:-}"
  local guarded_sha="$1"
  local main_record="$2"
  local scheduler_record="$3"
  local scheduler_load="${scheduler_record%%,*}"
  local ai_service_load="${ai_service_record%%,*}"
  local ai_timer_load="${ai_timer_record%%,*}"
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  if [ -n "$activation_deadline_epoch" ]; then
    controlled_guard_assert_activation_deadline \
      "$activation_deadline_epoch" || return 1
  fi
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
  local activation_deadline_epoch="${3:-}"
  local exec_main_pid
  local expected_active
  local expected_load
  local expected_unit_file
  local main_pid
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
    active)
      if [ -n "$activation_deadline_epoch" ]; then
        controlled_guard_assert_activation_deadline \
          "$activation_deadline_epoch" || return 1
        [[ "$CONTROLLED_RECOVERY_UNIT_START_TIMEOUT" =~ \
          ^[1-9][0-9]*[smh]$ ]] || return 1
        test -x /usr/bin/timeout || return 1
        /usr/bin/timeout --signal=TERM --kill-after=10s \
          "$CONTROLLED_RECOVERY_UNIT_START_TIMEOUT" \
          systemctl start "$unit" || return 1
      else
        systemctl start "$unit" || return 1
      fi
      ;;
    inactive) systemctl stop "$unit" || return 1 ;;
    *) return 1 ;;
  esac
  test "$(systemctl show -p LoadState --value "$unit")" = loaded || return 1
  test "$(systemctl show -p UnitFileState --value "$unit")" = \
    "$expected_unit_file" || return 1
  active_state="$(systemctl show -p ActiveState --value "$unit")" || return 1
  test "$active_state" = "$expected_active" || return 1
  if [ "$expected_active" = inactive ]; then
    main_pid="$(systemctl show -p MainPID --value "$unit")" || return 1
    exec_main_pid="$(systemctl show -p ExecMainPID --value "$unit")" || return 1
    test "${main_pid:-0}" = 0 || return 1
    test "${exec_main_pid:-0}" = 0 || return 1
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
controlled_guard_run_service_gate_with_deadline() {
  local service_user="$1"
  shift
  test -n "$service_user" || return 1
  test "$service_user" != root || return 1
  [[ "$CONTROLLED_DATABASE_GATE_TIMEOUT" =~ ^[1-9][0-9]*[smh]$ ]] || \
    return 1
  [[ "$CONTROLLED_DATABASE_GATE_KILL_AFTER" =~ ^[1-9][0-9]*[smh]$ ]] || \
    return 1
  test "$#" -gt 0 || return 1
  test -x /usr/bin/sudo || return 1
  test -x /usr/bin/timeout || return 1
  # GNU timeout's default process-group mode is intentional: after TERM and the
  # grace period, KILL must cover every Python/DB descendant.  Run timeout after
  # sudo has dropped privileges so sudo/use_pty cannot put Python outside the
  # process group which timeout owns.
  /usr/bin/sudo -u "$service_user" /usr/bin/timeout --signal=TERM \
    "--kill-after=$CONTROLLED_DATABASE_GATE_KILL_AFTER" \
    "$CONTROLLED_DATABASE_GATE_TIMEOUT" "$@" || return 1
  return 0
}
controlled_guard_capture_service_gate_with_deadline() {
  local service_user="$1"
  local output_file="$2"
  local working_directory="$3"
  local gate_status=0
  shift 3
  test -n "$service_user" || return 1
  test "$service_user" != root || return 1
  [[ "$CONTROLLED_DATABASE_GATE_TIMEOUT" =~ ^[1-9][0-9]*[smh]$ ]] || \
    return 1
  [[ "$CONTROLLED_DATABASE_GATE_KILL_AFTER" =~ ^[1-9][0-9]*[smh]$ ]] || \
    return 1
  test "$#" -gt 0 || return 1
  test -x /usr/bin/sudo || return 1
  test -x /usr/bin/timeout || return 1
  controlled_guard_assert_file "$output_file" 600 || return 1
  test -d "$working_directory" || return 1
  test ! -L "$working_directory" || return 1
  (
    # Apply the limit before the captured descriptor reaches any child so a
    # broken sealed producer cannot fill the filesystem before post-validation.
    # Git for Windows exposes ulimit -f but cannot change it; the production
    # broker always runs this path under Linux Bash.
    case "$OSTYPE" in
      linux*) ulimit -f 1024 || exit 1 ;;
    esac
    cd "$working_directory" || exit 1
    /usr/bin/sudo -u "$service_user" /usr/bin/timeout --signal=TERM \
      "--kill-after=$CONTROLLED_DATABASE_GATE_KILL_AFTER" \
      "$CONTROLLED_DATABASE_GATE_TIMEOUT" "$@"
  ) > "$output_file" || gate_status=$?
  controlled_guard_assert_file "$output_file" 600 || return 1
  test "$(stat -c '%s' "$output_file")" -le 1048576 || return 1
  return "$gate_status"
}
controlled_guard_parse_governance_health_result() {
  local result_file="$1"
  local expected_sha="$2"
  local expected_disposition="$3"
  local expected_trade_date="${4:-}"
  local expected_scheduler_pid="${5:-}"
  controlled_guard_assert_file "$result_file" 600 || return 1
  [[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  case "$expected_disposition" in
    completed|input_not_ready) ;;
    *) return 1 ;;
  esac
  case "$expected_scheduler_pid" in
    ''|[1-9][0-9]*) ;;
    *) return 1 ;;
  esac
  /usr/bin/python3.14 -I - "$result_file" "$expected_sha" \
    "$expected_disposition" "$expected_trade_date" \
    "$expected_scheduler_pid" <<'PY'
import hashlib
import json
import re
import socket
import sys
from datetime import date
from pathlib import Path

path = Path(sys.argv[1])
expected_sha, expected_disposition, expected_date, expected_scheduler_pid = (
    sys.argv[2:]
)
def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result
try:
    payload = json.loads(
        path.read_text(encoding="utf-8"), object_pairs_hook=unique_object
    )
except Exception:
    raise SystemExit(2)
if not isinstance(payload, dict):
    raise SystemExit(2)
expected = payload.get("expected")
checks = payload.get("checks")
adapter = payload.get("adapter_registry")
contract_version = "probiga.strategy-governance-health.v1"
expected_source = (
    "command_line_verified_against_calendar"
    if expected_date
    else "authoritative_closed_trading_calendar_day"
)
valid = (
    set(payload)
    == {
        "contract_version",
        "status",
        "run_disposition",
        "expected",
        "checks",
        "automatic_real_order_submission",
        "adapter_registry",
    }
    and payload.get("contract_version") == contract_version
    and payload.get("status") == "PASS"
    and payload.get("run_disposition") == expected_disposition
    and payload.get("automatic_real_order_submission") is False
    and isinstance(expected, dict)
    and set(expected)
    == {"build_commit_sha", "trade_date", "trade_date_source"}
    and expected.get("build_commit_sha") == expected_sha
    and expected.get("trade_date_source") == expected_source
    and isinstance(checks, list)
    and bool(checks)
    and isinstance(adapter, dict)
    and set(adapter)
    == {
        "registry_sealed",
        "registry_seal_hash",
        "registry_integrity_ready",
        "adapter_configured",
        "candidate_execution_ready",
        "funding_pipeline_ready",
        "governance_paper_execution_ready",
        "production_execution_ready",
        "real_order_submission_enabled",
        "automatic_real_order_submission",
        "adapter_count",
    }
    and adapter.get("registry_sealed") is True
    and adapter.get("registry_integrity_ready") is True
    and re.fullmatch(
        r"[0-9a-f]{64}", str(adapter.get("registry_seal_hash") or "")
    ) is not None
    and isinstance(adapter.get("adapter_count"), int)
    and not isinstance(adapter.get("adapter_count"), bool)
    and adapter.get("adapter_count") >= 0
    and adapter.get("adapter_configured")
    is (adapter.get("adapter_count") > 0)
    and adapter.get("candidate_execution_ready")
    is (
        adapter.get("registry_integrity_ready") is True
        and adapter.get("adapter_configured") is True
    )
    and isinstance(adapter.get("funding_pipeline_ready"), bool)
    and (
        adapter.get("funding_pipeline_ready") is False
        or adapter.get("candidate_execution_ready") is True
    )
    and adapter.get("governance_paper_execution_ready")
    is (
        adapter.get("candidate_execution_ready") is True
        and adapter.get("funding_pipeline_ready") is True
    )
    and adapter.get("production_execution_ready")
    is adapter.get("governance_paper_execution_ready")
    and adapter.get("real_order_submission_enabled") is False
    and adapter.get("automatic_real_order_submission") is False
)
trade_date = str(expected.get("trade_date") or "") if isinstance(expected, dict) else ""
try:
    valid = valid and date.fromisoformat(trade_date).isoformat() == trade_date
except (TypeError, ValueError):
    valid = False
if expected_date:
    valid = valid and trade_date == expected_date
names = []
waived = []
check_details = {}
if isinstance(checks, list):
    for check in checks:
        if (
            not isinstance(check, dict)
            or set(check) != {"name", "passed", "waived", "detail"}
            or check.get("passed") is not True
        ):
            valid = False
            continue
        name = check.get("name")
        if not isinstance(name, str) or not name:
            valid = False
            continue
        names.append(name)
        check_details[name] = check.get("detail")
        if check.get("waived") is True:
            waived.append(name)
        elif check.get("waived") not in (False, None):
            valid = False
if len(names) != len(set(names)):
    valid = False
column_tables = {
    "st_strategy_governance_schema_migration",
    "st_strategy_registry",
    "st_strategy_version",
    "st_strategy_lifecycle_event",
    "st_strategy_metric_input",
    "st_strategy_health_snapshot",
    "st_strategy_combination",
    "st_strategy_combination_version",
    "st_strategy_combination_health_snapshot",
    "st_strategy_governance_run",
    "st_strategy_pool_snapshot",
    "st_strategy_allocation_snapshot",
    "st_strategy_adapter_run_receipt",
    "st_strategy_industry_history",
    "st_strategy_governance_audit",
    "st_strategy_adapter_candidate_fact",
    "st_dynamic_shadow_trial_plan",
    "st_dynamic_shadow_trial_chain",
    "st_dynamic_shadow_trial_exit_binding",
    "st_scheduled_tasks",
    }
column_tables -= {
    "st_strategy_adapter_candidate_fact",
    "st_dynamic_shadow_trial_plan",
    "st_dynamic_shadow_trial_chain",
    "st_dynamic_shadow_trial_exit_binding",
    }
index_tables = column_tables - {"st_scheduled_tasks"}
common_required_names = {
    "required_tables",
    "daily_scheduler_task_unique",
    "daily_scheduler_task_contract",
    "qmt_announcement_scheduler_task_unique",
    "qmt_announcement_scheduler_task_contract",
    "qmt_operations_scheduler_tasks_unique",
    "qmt_operations_scheduler_tasks_contract",
    "supporting_release_trigger_inventory_exact",
    "full_database_trigger_inventory_exact",
    "qmt_reference_physical_schema_and_seal",
    "qmt_history_coverage_physical_schema_and_seal",
    "qmt_history_capability_matrix_fail_closed",
    "qmt_windows_edge_executor_and_last_success",
    "qmt_windows_edge_release_bootstrap",
    "scheduler_task_history_physical_schema",
    "pit_fact_physical_schema_exact",
    "latest_qmt_announcement_full_market_batch",
    "strategy_metric_input_application_state_machine",
    "governance_append_only_application_integrity",
    "strategy_funding_schema_exact",
    "dynamic_shadow_ledger_schema_exact",
    "dynamic_shadow_candidate_plan_fill_forward_ledger",
    "forward_strategy_version_schema",
    "forward_strategy_version_relations",
    "v2_raw_fill_cash_ledgers_are_immutable",
    "forward_exit_allocation_v3_frozen_schema",
    "forward_exit_allocation_v3_fifo_conservation",
    "qmt_pre_close_v2_frozen_schema",
    "governance_canonical_revision_migration",
    "authoritative_trade_date",
    "dynamic_strategy_registry",
    "strategy_lifecycle_domain",
    "strategy_current_versions",
    "dynamic_combination_registry",
    "combination_lifecycle_domain",
    "combination_current_versions",
    "all_immutable_version_hashes",
    "all_lifecycle_and_audit_payload_hashes_and_run_bindings",
    "registry_lifecycle_projection_matches_immutable_events",
    "strategy_industry_history_exact_qmt_full_replay",
    "all_governance_detail_snapshot_hashes_and_run_bindings",
    "metric_evidence_state_domain",
    "all_metric_evidence_submission_and_review_audits",
    "metric_and_challenger_evidence_hashes_globally_unique",
    "global_real_order_authority_closed",
    "historical_canonical_run_inventory",
    "authoritative_date_has_one_canonical_revision",
    } | {f"schema_columns:{name}" for name in column_tables} | {
    f"schema_indexes:{name}" for name in index_tables
    } | {f"schema_index_contracts:{name}" for name in index_tables}
if expected_scheduler_pid:
    common_required_names.add(
        "linux_standalone_scheduler_heartbeat_current"
    )
if expected_disposition == "input_not_ready":
    required_names = common_required_names | {
        "expected_build_date_run",
        "authoritative_session_windows_qmt_close_attested",
        "qmt_pre_close_v2_rows_bind_current_kline",
        "no_historical_canonical_run",
    }
else:
    required_names = common_required_names | {
        "candidate_pool_industry_snapshot_binds_exact_qmt_history",
        "authoritative_session_windows_qmt_close_attested",
        "qmt_pre_close_v2_rows_bind_current_kline",
        "latest_completed_run_identity",
        "expected_build_date_run_unique",
        "expected_run_identity",
        "expected_run_completed",
        "expected_run_input_fresh",
        "completed_run_has_hash_valid_audit",
        "funding_checkpoint_manifest_partition_and_persistence",
        "run_registry_counts",
        "market_router_snapshot_is_reproducible",
        "current_canonical_metrics_replay_from_raw_ledgers",
        "strategy_health_three_windows",
        "combination_health_one_snapshot_each",
        "funding_snapshots_use_confirmed_evidence",
        "pool_counts_and_dates_match_run",
        "pool_rows_snapshot_hash_and_funding_references",
        "allocation_candidate_snapshot_and_decision_hashes",
        "paper_allocation_exactly_closed",
        "allocation_targets_are_funding_eligible",
        "allocation_lifecycle_budget_exact",
        "allocation_obeys_market_router_risk_budget",
    }
funding_schema_detail = check_details.get("strategy_funding_schema_exact")
metric_trigger_detail = check_details.get(
    "strategy_metric_input_application_state_machine"
)
append_trigger_detail = check_details.get(
    "governance_append_only_application_integrity"
)
projection_detail = check_details.get(
    "registry_lifecycle_projection_matches_immutable_events"
)
qmt_task_unique_detail = check_details.get(
    "qmt_announcement_scheduler_task_unique"
)
qmt_task_contract_detail = check_details.get(
    "qmt_announcement_scheduler_task_contract"
)
qmt_operations_unique_detail = check_details.get(
    "qmt_operations_scheduler_tasks_unique"
)
qmt_operations_contract_detail = check_details.get(
    "qmt_operations_scheduler_tasks_contract"
)
scheduler_heartbeat_detail = check_details.get(
    "linux_standalone_scheduler_heartbeat_current"
)
supporting_trigger_detail = check_details.get(
    "supporting_release_trigger_inventory_exact"
)
full_trigger_detail = check_details.get(
    "full_database_trigger_inventory_exact"
)
qmt_reference_detail = check_details.get(
    "qmt_reference_physical_schema_and_seal"
)
qmt_coverage_detail = check_details.get(
    "qmt_history_coverage_physical_schema_and_seal"
)
qmt_capability_detail = check_details.get(
    "qmt_history_capability_matrix_fail_closed"
)
qmt_edge_detail = check_details.get(
    "qmt_windows_edge_executor_and_last_success"
)
qmt_edge_release_detail = check_details.get(
    "qmt_windows_edge_release_bootstrap"
)
scheduler_task_history_detail = check_details.get(
    "scheduler_task_history_physical_schema"
)
pit_fact_detail = check_details.get("pit_fact_physical_schema_exact")
qmt_announcement_detail = check_details.get(
    "latest_qmt_announcement_full_market_batch"
)
expected_funding_table_counts = {
    "st_strategy_funding_daily_fact": {
        "column_count": 29, "index_count": 9,
        "foreign_key_count": 3, "check_count": 7,
    },
    "st_strategy_funding_checkpoint": {
        "column_count": 46, "index_count": 12,
        "foreign_key_count": 7, "check_count": 13,
    },
    }
expected_funding_contract_hash = (
    "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde"
)
expected_append_physical_hash = (
    "bf537f9ed5fb1d31195092ae6a24262511de6f45bf9addacefebc88e25b6b9d8"
)
expected_metric_physical_hash = (
    "c217a42eb6c2a5f7bed592bb7c7e724499546f997061c4daad1db957317bdf28"
)
expected_core_append_hash = (
    "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943"
)
expected_core_metric_hash = (
    "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84"
)
expected_trigger_source_hash = (
    "5a1a19e0664c715ae0cac7cfa8dd87c47da1b63b1d2df869561cecf3c995f01f"
)
expected_supporting_trigger_source_hash = (
    "7c261eaff759e562b883d19880ef345c6733cacf911218437adc72ba864934e2"
)
expected_full_trigger_nameset_hash = (
    "6df9585376ec190a8d78c996336ff9f2c68bf1a4860e88809561a55df7cbfde5"
)
expected_full_with_v4_trigger_nameset_hash = (
    "a1d2a23569adc5318b5806e3040487cedcb9e31a60da3dae7756ed7bdf7044d7"
)
expected_v2_trigger_source_hash = (
    "5167f36ee731c2544be73590e4e00716f334c58b5746f776e610254904cf8883"
)
expected_managed_trigger_source_hash = (
    "7e154c081f807ce3d88311dc6d7db74170951abe890130a02343010466dc2f75"
)
expected_qmt_reference_contract_hash = (
    "64982c16c517f7e5c0e6ee9b88b1bf33df98f9aebf66440eedc916eae76f3dd5"
)
expected_pit_fact_contract_hash = (
    "c374e0ba62eb2e5b9bef802ce2bdd89fae0c63391d918e922ff21781707863ae"
)
expected_supporting_owner_counts = {
    "market_field_capture": 5,
    "pit_facts": 6,
    "qmt_attestation": 6,
    "qmt_history_coverage": 4,
    "qmt_membership": 6,
    "qmt_reference": 10,
    "scheduler_task_history": 3,
    "schema_recovery_evidence": 2,
    "strategy_governance": 40,
    }
expected_qmt_task = {
    "task_name": "国金QMT全市场公告PIT同步",
    "task_type": "qmt_announcement_pit",
    "group_name": "strategy_governance",
    "script_path": "tools/sync_qmt_announcement_pit.py",
    "script_args": "--window-days 30 --overlap-days 3 --batch-size 100 --fallback-provider cninfo --checkpoint-dir /var/lib/probiga/qmt-announcement-checkpoints",
    "cron_time": "18:20",
    "interval_minutes": 0,
    "date_param": "",
    "enabled": 1,
    }
expected_qmt_operations_tasks = {
    "qmt_local_gap_repair_execute": {
        "task_name": "Guojin QMT local history gap repair execute",
        "task_type": "qmt_local_gap_repair_execute",
        "group_name": "Guojin QMT",
        "script_path": "tools/backfill_guojin_qmt_local_history.py",
        "script_args": "from-gaps --gap-limit 2 --apply --state-root /var/lib/probiga/qmt-local-gap-repair --lock-path /var/lib/probiga/qmt-local-gap-repair/qmt-local-gap-repair.lock --json",
        "cron_time": "07:05",
        "interval_minutes": 0,
        "date_param": "",
        "enabled": 1,
    },
    "qmt_nightly_reconciliation": {
        "task_name": "国金QMT凌晨缺口扫描",
        "task_type": "qmt_nightly_reconciliation",
        "group_name": "国金QMT",
        "script_path": "tools/nightly_guojin_qmt_reconciliation.py",
        "script_args": "--scan-days 20 --json",
        "cron_time": "01:30",
        "interval_minutes": 0,
        "date_param": "",
        "enabled": 1,
    },
    "qmt_local_history_2024": {
        "task_name": "国金QMT本地历史补数(2024起)",
        "task_type": "qmt_local_history_2024",
        "group_name": "国金QMT",
        "script_path": "tools/run_guojin_qmt_full_market_history.py",
        "script_args": "--start-date 2024-01-01 --mode all --daily-batch-size 120 --minute-batch-size 80 --sleep-seconds 0.2 --stop-at 07:00 --state-root /var/lib/probiga/qmt-full-market-history --lock-path /var/lib/probiga/qmt-full-market-history/qmt-full-market-history.lock --log-path /var/lib/probiga/qmt-full-market-history/qmt-full-market-history-2024.jsonl --json",
        "cron_time": "00:00",
        "interval_minutes": 0,
        "date_param": "",
        "enabled": 1,
    },
    "qmt_reference_incremental": {
        "task_name": "国金QMT基础目录增量同步",
        "task_type": "qmt_reference_incremental",
        "group_name": "国金QMT",
        "script_path": "tools/sync_guojin_qmt_reference_data.py",
        "script_args": "--skip-refresh --include-calendar --json",
        "cron_time": "03:20",
        "interval_minutes": 0,
        "date_param": "",
        "enabled": 1,
    },
    "qmt_gap_repair_plan": {
        "task_name": "国金QMT历史缺口修复队列",
        "task_type": "qmt_gap_repair_plan",
        "group_name": "国金QMT",
        "script_path": "tools/repair_guojin_qmt_gaps.py",
        "script_args": "--limit 50 --json",
        "cron_time": "02:00",
        "interval_minutes": 0,
        "date_param": "",
        "enabled": 1,
    },
    }
qmt_edge_current = (
    qmt_edge_detail.get("current")
    if isinstance(qmt_edge_detail, dict)
    else None
)
qmt_edge_tasks = (
    qmt_edge_detail.get("tasks")
    if isinstance(qmt_edge_detail, dict)
    else None
)
qmt_edge_task_types = {
    "qmt_local_gap_repair_execute",
    "qmt_local_history_2024",
    "qmt_reference_incremental",
    }
qmt_edge_valid = (
    isinstance(qmt_edge_detail, dict)
    and qmt_edge_detail.get("status") == "AVAILABLE"
    and qmt_edge_detail.get("strategy_eligible") is True
    and qmt_edge_detail.get("executor_role") == "qmt_windows_edge"
    and qmt_edge_detail.get("expected_build_sha") == expected_sha
    and qmt_edge_detail.get("expected_poll_seconds") == 60
    and isinstance(qmt_edge_detail.get("role_row_count"), int)
    and qmt_edge_detail.get("role_row_count") >= 1
    and qmt_edge_detail.get("fresh_row_count") == 1
    and qmt_edge_detail.get("future_row_count") == 0
    and qmt_edge_detail.get("required_task_types")
    == [
        "qmt_local_gap_repair_execute",
        "qmt_local_history_2024",
        "qmt_reference_incremental",
    ]
    and qmt_edge_detail.get("task_count") == 3
    and qmt_edge_detail.get("last_success_count") == 3
    and qmt_edge_detail.get("success_max_age_seconds") == 345600
    and qmt_edge_detail.get("errors") == []
    and isinstance(qmt_edge_current, dict)
    and qmt_edge_current.get("mode") == "standalone"
    and qmt_edge_current.get("executor_role") == "qmt_windows_edge"
    and qmt_edge_current.get("build_sha") == expected_sha
    and qmt_edge_current.get("poll_seconds") == 60
    and isinstance(qmt_edge_current.get("heartbeat_age_seconds"), int)
    and not isinstance(qmt_edge_current.get("heartbeat_age_seconds"), bool)
    and 0 <= qmt_edge_current.get("heartbeat_age_seconds") <= 120
    and isinstance(qmt_edge_current.get("host_name"), str)
    and 0 < len(qmt_edge_current.get("host_name")) <= 128
    and isinstance(qmt_edge_current.get("pid"), int)
    and not isinstance(qmt_edge_current.get("pid"), bool)
    and qmt_edge_current.get("pid") > 0
    and qmt_edge_current.get("instance_id")
    == f"{qmt_edge_current.get('host_name')}-{qmt_edge_current.get('pid')}"
    and isinstance(qmt_edge_tasks, dict)
    and set(qmt_edge_tasks) == qmt_edge_task_types
    and all(
        isinstance(item, dict)
        and isinstance(item.get("task_id"), int)
        and not isinstance(item.get("task_id"), bool)
        and item.get("task_id") > 0
        and item.get("last_run_status") in {"success", "running"}
        and isinstance(item.get("last_success_age_seconds"), int)
        and not isinstance(item.get("last_success_age_seconds"), bool)
        and 0 <= item.get("last_success_age_seconds") <= 345600
        and item.get("last_success_host")
        == qmt_edge_current.get("host_name")
        and isinstance(item.get("last_success_instance_id"), str)
        and item.get("last_success_instance_id").startswith(
            f"{qmt_edge_current.get('host_name')}-"
        )
        for item in qmt_edge_tasks.values()
    )
)
qmt_edge_release_receipt = (
    qmt_edge_release_detail.get("receipt")
    if isinstance(qmt_edge_release_detail, dict) else None
)
qmt_edge_release_identity = (
    qmt_edge_release_detail.get("identity")
    if isinstance(qmt_edge_release_detail, dict) else None
)
qmt_edge_release_current = (
    qmt_edge_release_identity.get("current")
    if isinstance(qmt_edge_release_identity, dict) else None
)
qmt_edge_release_valid = (
    isinstance(qmt_edge_release_detail, dict)
    and qmt_edge_release_detail.get("status") == "AVAILABLE"
    and qmt_edge_release_detail.get("strategy_eligible") is True
    and qmt_edge_release_detail.get("expected_build_sha") == expected_sha
    and qmt_edge_release_detail.get("expected_poll_seconds") == 60
    and qmt_edge_release_detail.get("receipt_count") == 1
    and qmt_edge_release_detail.get("immutable_reference_verified") is True
    and qmt_edge_release_detail.get("errors") == []
    and isinstance(qmt_edge_release_current, dict)
    and qmt_edge_release_current.get("build_sha") == expected_sha
    and qmt_edge_release_current.get("executor_role") == "qmt_windows_edge"
    and isinstance(qmt_edge_release_receipt, dict)
    and qmt_edge_release_receipt.get("build_sha") == expected_sha
    and qmt_edge_release_receipt.get("request_run_uid")
    == f"qmt-edge-request-{expected_sha}"
    and qmt_edge_release_receipt.get("host_name")
    == qmt_edge_release_current.get("host_name")
    and qmt_edge_release_receipt.get("scheduler_instance_id")
    == qmt_edge_release_current.get("instance_id")
    and str(qmt_edge_release_receipt.get("catalog_batch_id") or "").startswith(
        f"qmt_rel_{expected_sha}_"
    )
    and qmt_edge_release_receipt.get("catalog_batch_id")
    == qmt_edge_release_receipt.get("calendar_batch_id")
    and re.fullmatch(
        r"[0-9a-f]{64}",
        str(qmt_edge_release_receipt.get("receipt_hash") or ""),
    ) is not None
)
authoritative_announcement_sources = {
    "qmt.announcement",
    "cninfo.announcement",
    "eastmoney.notice",
 }
announcement_fallback_sources = {
    "cninfo.announcement",
    "eastmoney.notice",
 }
announcement_fallback_reason_codes = {
    "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED",
    "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN",
    "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE",
    "QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE",
 }
qmt_announcement_source = (
    str(qmt_announcement_detail.get("source") or "")
    if isinstance(qmt_announcement_detail, dict)
    else ""
)
qmt_announcement_source_valid = (
    qmt_announcement_source == "qmt.announcement"
    or (
        qmt_announcement_source in announcement_fallback_sources
        and qmt_announcement_detail.get("primary_source")
        == "qmt.announcement"
        and qmt_announcement_detail.get("fallback_reason")
        in announcement_fallback_reason_codes
    )
)
full_trigger_names = (
    full_trigger_detail.get("expected_names")
    if isinstance(full_trigger_detail, dict)
    else None
)
full_trigger_nameset_hash = (
    hashlib.sha256(
        json.dumps(
            {
                "schema": "probiga.full-release-trigger-names.v1",
                "names": full_trigger_names,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    if isinstance(full_trigger_names, list)
    else ""
)
full_managed_contract = (
    full_trigger_detail.get("managed_contract")
    if isinstance(full_trigger_detail, dict)
    else None
)
full_optional_v4_count = (
    full_trigger_detail.get("optional_v4_count")
    if isinstance(full_trigger_detail, dict)
    else None
)
expected_full_count = 175 if full_optional_v4_count == 32 else 143
expected_full_nameset_hash = (
    expected_full_with_v4_trigger_nameset_hash
    if full_optional_v4_count == 32
    else expected_full_trigger_nameset_hash
)
valid = valid and (
    isinstance(funding_schema_detail, dict)
    and funding_schema_detail.get("table_count") == 2
    and funding_schema_detail.get("tables") == expected_funding_table_counts
    and funding_schema_detail.get("trigger_count") == 4
    and funding_schema_detail.get("contract_hash")
    == expected_funding_contract_hash
    and funding_schema_detail.get("checkpoint_target_average_bytes") == 8192
    and funding_schema_detail.get("checkpoint_total_target_bytes") == 8388608
    and funding_schema_detail.get("checkpoint_total_hard_bytes") == 16777216
    and funding_schema_detail.get("batch_max_rows") == 100
    and funding_schema_detail.get("batch_max_bytes") == 4194304
    and funding_schema_detail.get("manifest_max_bytes") == 1048576
    and funding_schema_detail.get("audit_max_bytes") == 131072
    and isinstance(metric_trigger_detail, dict)
    and metric_trigger_detail.get("trigger_count") == 2
    and metric_trigger_detail.get("expected_trigger_count") == 2
    and metric_trigger_detail.get("required_count") == 2
    and metric_trigger_detail.get("observed_count") == 2
    and metric_trigger_detail.get("database_triggers_required") is True
    and metric_trigger_detail.get("metadata_frozen") is True
    and metric_trigger_detail.get("definer")
    == "probiga_migrator@127.0.0.1"
    and metric_trigger_detail.get("contract_hash")
    == expected_metric_physical_hash
    and metric_trigger_detail.get("source_contract_hash")
    == expected_trigger_source_hash
    and metric_trigger_detail.get("core_append_only_contract_hash")
    == expected_core_append_hash
    and metric_trigger_detail.get("core_metric_review_contract_hash")
    == expected_core_metric_hash
    and isinstance(append_trigger_detail, dict)
    and append_trigger_detail.get("trigger_count") == 38
    and append_trigger_detail.get("expected_trigger_count") == 38
    and append_trigger_detail.get("total_governance_trigger_count") == 40
    and append_trigger_detail.get("required_count") == 38
    and append_trigger_detail.get("observed_count") == 38
    and append_trigger_detail.get("database_triggers_required") is True
    and append_trigger_detail.get("metadata_frozen") is True
    and append_trigger_detail.get("definer")
    == "probiga_migrator@127.0.0.1"
    and append_trigger_detail.get("contract_hash")
    == expected_append_physical_hash
    and append_trigger_detail.get("source_contract_hash")
    == expected_trigger_source_hash
    and append_trigger_detail.get("core_contract_hash")
    == expected_core_append_hash
    and append_trigger_detail.get("core_metric_review_contract_hash")
    == expected_core_metric_hash
    and append_trigger_detail.get("funding_contract_hash")
    == expected_funding_contract_hash
    and isinstance(projection_detail, dict)
    and projection_detail.get("invalid_count") == 0
    and projection_detail.get("registry_count")
    == projection_detail.get("projected_count")
    and re.fullmatch(
        r"[0-9a-f]{64}",
        str(projection_detail.get("projection_hash") or ""),
    ) is not None
    and isinstance(qmt_task_unique_detail, dict)
    and qmt_task_unique_detail.get("row_count") == 1
    and isinstance(qmt_task_unique_detail.get("rows"), list)
    and len(qmt_task_unique_detail.get("rows")) == 1
    and isinstance(qmt_task_contract_detail, dict)
    and qmt_task_contract_detail.get("expected") == expected_qmt_task
    and all(
        qmt_task_contract_detail.get("actual", {}).get(key) == value
        for key, value in expected_qmt_task.items()
    )
    and qmt_task_contract_detail.get("pipeline_order") == {
        "qmt_announcement_minutes": 1100,
        "analysis_minutes": 1130,
        "governance_minutes": 1355,
    }
    and isinstance(qmt_operations_unique_detail, dict)
    and qmt_operations_unique_detail.get("row_count") == 5
    and qmt_operations_unique_detail.get("expected_row_count") == 5
    and qmt_operations_unique_detail.get("match_counts")
    == {key: 1 for key in expected_qmt_operations_tasks}
    and isinstance(qmt_operations_unique_detail.get("rows"), list)
    and len(qmt_operations_unique_detail.get("rows")) == 5
    and isinstance(qmt_operations_contract_detail, dict)
    and qmt_operations_contract_detail.get("expected")
    == expected_qmt_operations_tasks
    and qmt_operations_contract_detail.get("actual")
    == expected_qmt_operations_tasks
    and isinstance(supporting_trigger_detail, dict)
    and supporting_trigger_detail.get("required_count") == 82
    and supporting_trigger_detail.get("optional_count") == 0
    and supporting_trigger_detail.get("observed_count") == 82
    and supporting_trigger_detail.get("expected_trigger_count") == 82
    and supporting_trigger_detail.get("owner_counts")
    == expected_supporting_owner_counts
    and supporting_trigger_detail.get("expected_owner_counts")
    == expected_supporting_owner_counts
    and supporting_trigger_detail.get("source_contract_hash")
    == expected_supporting_trigger_source_hash
    and supporting_trigger_detail.get("database_triggers_required") is True
    and supporting_trigger_detail.get("metadata_frozen") is True
    and supporting_trigger_detail.get("definer")
    == "probiga_migrator@127.0.0.1"
    and isinstance(full_trigger_detail, dict)
    and set(full_trigger_detail) == {
        "expected_count", "observed_count", "v2_count", "managed_count",
        "optional_v4_count", "base_nameset_sha256",
        "expected_names", "nameset_sha256", "v2_source_contract_sha256",
        "managed_source_contract_sha256", "observed_metadata_sha256",
        "managed_contract", "metadata_frozen", "read_only",
    }  # expected_full_inventory_keys
    and type(full_optional_v4_count) is int
    and full_optional_v4_count in {0, 32}
    and full_trigger_detail.get("expected_count") == expected_full_count
    and full_trigger_detail.get("observed_count") == expected_full_count
    and full_trigger_detail.get("v2_count") == 41
    and full_trigger_detail.get("managed_count") == 102
    and full_trigger_names == sorted(set(full_trigger_names or []))
    and len(full_trigger_names or []) == expected_full_count
    and full_trigger_nameset_hash == expected_full_nameset_hash
    and full_trigger_detail.get("nameset_sha256")
    == expected_full_nameset_hash
    and full_trigger_detail.get("base_nameset_sha256")
    == expected_full_trigger_nameset_hash
    and full_trigger_detail.get("v2_source_contract_sha256")
    == expected_v2_trigger_source_hash
    and full_trigger_detail.get("managed_source_contract_sha256")
    == expected_managed_trigger_source_hash
    and re.fullmatch(
        r"[0-9a-f]{64}",
        str(full_trigger_detail.get("observed_metadata_sha256") or ""),
    ) is not None
    and full_trigger_detail.get("metadata_frozen") is True
    and full_trigger_detail.get("read_only") is True
    and isinstance(full_managed_contract, dict)
    and full_managed_contract.get("required_count") == 102
    and full_managed_contract.get("optional_count") == 0
    and full_managed_contract.get("observed_count") == 102
    and full_managed_contract.get("definer")
    == "probiga_migrator@127.0.0.1"
    and full_managed_contract.get("metadata_frozen") is True
    and full_managed_contract.get("legacy_rehome_names") == []
    and isinstance(qmt_reference_detail, dict)
    and qmt_reference_detail.get("contract_key") == "qmt_reference_truth_v2"
    and qmt_reference_detail.get("contract_hash")
    == expected_qmt_reference_contract_hash
    and qmt_reference_detail.get("table_count") == 5
    and qmt_reference_detail.get("trigger_count") == 10
    and qmt_reference_detail.get("expected_trigger_count") == 10
    and qmt_reference_detail.get("physical_schema_verified") is True
    and qmt_reference_detail.get("physical_seal_verified") is True
    and isinstance(qmt_coverage_detail, dict)
    and qmt_coverage_detail.get("database") == "probiga"
    and qmt_coverage_detail.get("table_count") == 2
    and qmt_coverage_detail.get("foreign_key_count") == 3
    and qmt_coverage_detail.get("trigger_count") == 4
    and qmt_coverage_detail.get("expected_trigger_count") == 4
    and qmt_coverage_detail.get("runtime_ddl_required") is False
    and qmt_coverage_detail.get("physical_schema_verified") is True
    and qmt_coverage_detail.get("physical_seal_verified") is True
    and isinstance(qmt_capability_detail, dict)
    and qmt_capability_detail.get("schema")
    == "probiga.qmt-history-capability-matrix.v1"
    and qmt_capability_detail.get("status") == "HEALTHY"
    and qmt_capability_detail.get("evidence_healthy") is True
    and qmt_capability_detail.get("dataset_count") == 19
    and qmt_capability_detail.get("strategy_eligible_dataset_count") == 0
    and qmt_capability_detail.get("strategy_ineligible_dataset_count") == 19
    and qmt_capability_detail.get("required_scope_dataset_count") == 0
    and qmt_capability_detail.get("fail_closed_verified") is True
    and qmt_capability_detail.get("automatic_real_order_submission") is False
    and qmt_capability_detail.get("real_order_authority") is False
    and qmt_capability_detail.get("errors") == []
    and isinstance(qmt_capability_detail.get("datasets"), list)
    and len(qmt_capability_detail.get("datasets")) == 19
    and all(
        isinstance(item, dict)
        and item.get("status") == "UNAVAILABLE"
        and item.get("strategy_eligible") is False
        for item in qmt_capability_detail.get("datasets")
    )
    and isinstance(scheduler_task_history_detail, dict)
    and scheduler_task_history_detail.get("table")
    == "st_scheduled_task_history"
    and scheduler_task_history_detail.get("required_index_count") == 3
    and scheduler_task_history_detail.get("physical_contract_verified") is True
    and scheduler_task_history_detail.get("runtime_ddl_required") is False
    and scheduler_task_history_detail.get("read_only") is True
    and qmt_edge_valid
    and qmt_edge_release_valid
    and isinstance(pit_fact_detail, dict)
    and pit_fact_detail.get("schema")
    == "probiga.pit-fact-schema-health.v1"
    and pit_fact_detail.get("status") == "HEALTHY"
    and pit_fact_detail.get("valid") is True
    and pit_fact_detail.get("table_count") == 3
    and pit_fact_detail.get("expected_table_count") == 3
    and pit_fact_detail.get("trigger_count") == 6
    and pit_fact_detail.get("expected_trigger_count") == 6
    and pit_fact_detail.get("contract_hash") == expected_pit_fact_contract_hash
    and pit_fact_detail.get("physical_schema_verified") is True
    and isinstance(qmt_announcement_detail, dict)
    and qmt_announcement_detail.get("status") == "COMPLETE"
    and qmt_announcement_detail.get("trade_date") == trade_date
    and qmt_announcement_source in authoritative_announcement_sources
    and qmt_announcement_source_valid
    and qmt_announcement_detail.get("reason_code")
    == (
        "QMT_ANNOUNCEMENT_EXISTING_FULL_MARKET_COMPLETE"
        if qmt_announcement_source == "qmt.announcement"
        else "ANNOUNCEMENT_FALLBACK_EXISTING_FULL_MARKET_COMPLETE"
    )
    and qmt_announcement_detail.get("funding_eligible") is True
    and qmt_announcement_detail.get("database_writes") is False
    and qmt_announcement_detail.get("automatic_real_order_submission") is False
    and qmt_announcement_detail.get("real_order_authority") is False
    and isinstance(qmt_announcement_detail.get("catalog_member_count"), int)
    and qmt_announcement_detail.get("catalog_member_count") > 0
    and qmt_announcement_detail.get("coverage_row_count")
    == qmt_announcement_detail.get("catalog_member_count")
    and re.fullmatch(
        r"[0-9a-f]{64}",
        str(qmt_announcement_detail.get("batch_root_hash") or ""),
    ) is not None
)
if expected_scheduler_pid:
    expected_pid = int(expected_scheduler_pid)
    expected_host = socket.gethostname()
    current = (
        scheduler_heartbeat_detail.get("current")
        if isinstance(scheduler_heartbeat_detail, dict)
        else None
    )
    poll_seconds = (
        current.get("poll_seconds") if isinstance(current, dict) else None
    )
    heartbeat_age = (
        current.get("heartbeat_age_seconds")
        if isinstance(current, dict)
        else None
    )
    valid = valid and (
        isinstance(scheduler_heartbeat_detail, dict)
        and set(scheduler_heartbeat_detail) == {
            "executor_role", "role_row_count", "fresh_row_count",
            "future_row_count", "expected_host", "expected_pid",
            "expected_build_sha", "expected_poll_seconds", "current",
            "errors",
        }
        and scheduler_heartbeat_detail.get("executor_role")
        == "linux_standalone"
        and isinstance(scheduler_heartbeat_detail.get("role_row_count"), int)
        and scheduler_heartbeat_detail.get("role_row_count") >= 1
        and scheduler_heartbeat_detail.get("fresh_row_count") == 1
        and scheduler_heartbeat_detail.get("future_row_count") == 0
        and scheduler_heartbeat_detail.get("expected_host") == expected_host
        and scheduler_heartbeat_detail.get("expected_pid") == expected_pid
        and scheduler_heartbeat_detail.get("expected_build_sha") == expected_sha
        and scheduler_heartbeat_detail.get("expected_poll_seconds") == 60
        and scheduler_heartbeat_detail.get("errors") == []
        and isinstance(current, dict)
        and set(current) == {
            "instance_id", "mode", "host_name", "pid", "build_sha",
            "executor_role", "heartbeat_age_seconds", "poll_seconds",
            "max_concurrent_tasks",
        }
        and current.get("instance_id") == f"{expected_host}-{expected_pid}"
        and current.get("mode") == "standalone"
        and current.get("host_name") == expected_host
        and current.get("pid") == expected_pid
        and current.get("build_sha") == expected_sha
        and current.get("executor_role") == "linux_standalone"
        and isinstance(poll_seconds, int)
        and not isinstance(poll_seconds, bool)
        and poll_seconds == 60
        and isinstance(heartbeat_age, int)
        and not isinstance(heartbeat_age, bool)
        and 0 <= heartbeat_age <= 2 * poll_seconds
        and isinstance(current.get("max_concurrent_tasks"), int)
        and not isinstance(current.get("max_concurrent_tasks"), bool)
        and current.get("max_concurrent_tasks") >= 1
    )
if expected_disposition != "input_not_ready":
    manifest_detail = check_details.get(
        "funding_checkpoint_manifest_partition_and_persistence"
    )
    valid = valid and (
        isinstance(manifest_detail, dict)
        and manifest_detail.get("invalid_count") == 0
        and isinstance(manifest_detail.get("current_entity_count"), int)
        and not isinstance(manifest_detail.get("current_entity_count"), bool)
        and manifest_detail.get("current_entity_count") >= 0
        and isinstance(manifest_detail.get("strategy_checkpoint_count"), int)
        and isinstance(manifest_detail.get("combination_recipe_count"), int)
        and isinstance(manifest_detail.get("ineligible_count"), int)
        and manifest_detail.get("current_entity_count")
        == manifest_detail.get("strategy_checkpoint_count")
        + manifest_detail.get("combination_recipe_count")
        + manifest_detail.get("ineligible_count")
        and manifest_detail.get("checkpoint_count")
        == manifest_detail.get("strategy_checkpoint_count")
        and manifest_detail.get("funding_ready_count")
        == manifest_detail.get("strategy_checkpoint_count")
        + manifest_detail.get("combination_recipe_count")
        and isinstance(manifest_detail.get("daily_fact_count"), int)
        and not isinstance(manifest_detail.get("daily_fact_count"), bool)
        and 0 <= manifest_detail.get("daily_fact_count")
        <= manifest_detail.get("current_entity_count") * 370
        and manifest_detail.get("total_storage_bytes")
        == manifest_detail.get("checkpoint_storage_bytes")
        + manifest_detail.get("fact_storage_bytes")
        and manifest_detail.get("total_storage_bytes") <= 16777216
        and isinstance(manifest_detail.get("target_total_met"), bool)
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(manifest_detail.get("manifest_hash") or ""),
        ) is not None
        and all(
            re.fullmatch(
                r"[0-9a-f]{64}",
                str(manifest_detail.get(field) or ""),
            ) is not None
            for field in (
                "checkpoint_root_hash",
                "combination_recipe_root_hash",
                "ineligible_root_hash",
            )
        )
    )
valid = valid and set(names) == required_names and len(names) == len(required_names)
expected_waived = (
    {
        "authoritative_date_has_one_canonical_revision",
        "expected_build_date_run",
    }
    if expected_disposition == "input_not_ready"
    else set()
)
valid = valid and set(waived) == expected_waived and len(waived) == len(expected_waived)
if not valid:
    raise SystemExit(2)
print(trade_date)
PY
}
controlled_guard_governance_cutover_probe_code() {
  # This producer is owned by the authenticated recovery engine rather than by
  # the guarded release.  That is required when recovering a legacy release
  # which predates the cutover probe, while all imported clock/database code is
  # still loaded from the sealed guarded release and its virtual environment.
  /usr/bin/cat <<'PY'
import json
import sys
from datetime import datetime, timedelta

from server.common.authoritative_market_clock import (
    DAILY_CLOSE_READY_HOUR,
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from tools.env_config import create_tool_engine, load_project_env

reserve = int(sys.argv[1])
if reserve <= 0:
    raise SystemExit(2)
load_project_env()
sample = datetime.now(PRODUCTION_TIMEZONE)
cutoff = sample.replace(
    hour=DAILY_CLOSE_READY_HOUR,
    minute=0,
    second=0,
    microsecond=0,
)
if sample >= cutoff:
    cutoff += timedelta(days=1)
safe_before = cutoff - timedelta(seconds=reserve)
engine = create_tool_engine()
try:
    trade_date = authoritative_closed_trade_date(engine, now=sample)
finally:
    engine.dispose()
payload = {
    "trade_date": trade_date,
    "sample_epoch": int(sample.timestamp()),
    "next_cutoff_epoch": int(cutoff.timestamp()),
    "safe_before_epoch": int(safe_before.timestamp()),
    "reserve_seconds": reserve,
    }
print(json.dumps(payload, separators=(",", ":"), sort_keys=True))
PY
}
controlled_guard_parse_governance_cutover_result() {
  local result_file="$1"
  local expected_trade_date="$2"
  controlled_guard_assert_file "$result_file" 600 || return 1
  /usr/bin/python3.14 -I - "$result_file" "$expected_trade_date" \
    "$CONTROLLED_RECOVERY_CUTOVER_RESERVE_SECONDS" <<'PY'
import json
import sys
from datetime import date
from pathlib import Path

path = Path(sys.argv[1])
expected_date = sys.argv[2]
reserve = int(sys.argv[3])
def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result
try:
    payload = json.loads(
        path.read_text(encoding="utf-8"), object_pairs_hook=unique_object
    )
    expected_date = date.fromisoformat(expected_date).isoformat()
except Exception:
    raise SystemExit(2)
required = {
    "trade_date",
    "sample_epoch",
    "next_cutoff_epoch",
    "safe_before_epoch",
    "reserve_seconds",
    }
valid = isinstance(payload, dict) and set(payload) == required
if valid:
    integer_fields = (
        "sample_epoch",
        "next_cutoff_epoch",
        "safe_before_epoch",
        "reserve_seconds",
    )
    valid = all(
        type(payload.get(field)) is int and payload[field] > 0
        for field in integer_fields
    )
if valid:
    sample = payload["sample_epoch"]
    cutoff = payload["next_cutoff_epoch"]
    safe_before = payload["safe_before_epoch"]
    valid = (
        payload.get("trade_date") == expected_date
        and payload["reserve_seconds"] == reserve
        and cutoff > sample
        and cutoff - safe_before == reserve
        and sample < safe_before
        and cutoff - sample <= 86400
    )
if not valid:
    raise SystemExit(2)
print(payload["safe_before_epoch"])
PY
}
controlled_guard_parse_governance_runner_result() {
  local result_file="$1"
  local runner_status="$2"
  local expected_trade_date="$3"
  controlled_guard_assert_file "$result_file" 600 || return 1
  case "$runner_status" in 0|2) ;; *) return 1 ;; esac
  /usr/bin/python3.14 -I - "$result_file" "$runner_status" \
    "$expected_trade_date" <<'PY'
import json
import re
import sys
from datetime import date
from pathlib import Path

path = Path(sys.argv[1])
runner_status = int(sys.argv[2])
expected_date = sys.argv[3]
def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result
try:
    payload = json.loads(
        path.read_text(encoding="utf-8"), object_pairs_hook=unique_object
    )
    expected_date = date.fromisoformat(expected_date).isoformat()
except Exception:
    raise SystemExit(2)
if not isinstance(payload, dict):
    raise SystemExit(2)
if runner_status == 0:
    required = {
        "status",
        "run_uid",
        "trade_date",
        "summary",
        "lifecycle_transitions",
        "allocations",
        "automatic_real_order_submission",
    }
    valid = (
        set(payload) == required
        and payload.get("status") == "ok"
        and re.fullmatch(r"[0-9a-f]{32}", str(payload.get("run_uid") or ""))
        is not None
        and payload.get("trade_date") == expected_date
        and isinstance(payload.get("summary"), dict)
        and isinstance(payload.get("lifecycle_transitions"), list)
        and isinstance(payload.get("allocations"), list)
        and payload.get("automatic_real_order_submission") is False
    )
    disposition = "completed"
else:
    required = {
        "status",
        "reason",
        "target_trade_date",
        "input_trade_date",
        "automatic_real_order_submission",
    }
    input_date = payload.get("input_trade_date")
    valid = (
        set(payload) == required
        and payload.get("status") == "blocked"
        and isinstance(payload.get("reason"), str)
        and bool(payload["reason"].strip())
        and payload.get("target_trade_date") == expected_date
        and input_date in ("", expected_date)
        and payload.get("automatic_real_order_submission") is False
    )
    disposition = "input_not_ready"
if not valid:
    raise SystemExit(2)
print(disposition)
PY
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
  local input_readiness_mode="${7:-strict}"
  local activation_deadline_epoch="${8:-}"
  local code_root="$CODE_RELEASE_ROOT/$expected_sha"
  local release_venv="$RELEASE_VENV_ROOT/$expected_sha"
  local python_path="$release_venv/bin/python"
  local adata_sha adata_tree_sha adata_source service_user pid main_pid
  local release_tree_sha="" adapter_registry_seal_sha=""
  local deferred_expected_sha="" deferred_code_root=""
  local deferred_scheduler_fenced=0
  local deferred_venv="" deferred_venv_target="" deferred_python_path=""
  local deferred_adata_sha=""
  local deferred_adata_tree_sha="" deferred_adata_source=""
  local deferred_release_tree_sha="" deferred_adapter_registry_seal_sha=""
  local scheduler_expected_sha scheduler_build_sha scheduler_code_root
  local scheduler_python_path scheduler_adata_sha scheduler_adata_tree_sha
  local scheduler_adata_source scheduler_release_tree_sha
  local scheduler_adapter_registry_seal_sha scheduler_exec_start scheduler_identity
  local scheduler_has_attested_identity
  local active_state inactive_restore
  local ai_expected_sha ai_code_root ai_python_path ai_adata_sha
  local ai_adata_tree_sha ai_adata_source ai_release_tree_sha
  local ai_adapter_registry_seal_sha ai_exec_start ai_has_attested_identity
  local governance_result_file=""
  local governance_result_status=0
  local governance_trade_date=""
  local cutover_deadline_epoch=""
  local cutover_probe_code=""
  local runner_disposition=""
  local require_attested_identity=0
  local has_attested_identity=0
  local snapshot_release=""
  local -a cmdline=()
  local -a attested_env=()
  local -a guarded_command_prefix=()
  local -a governance_health_args=()
  RESTORED_RUNTIME_FAILURE_CODE=runtime-identity
  RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE=""
  RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH=""
  case "$verification_mode" in
    full|rollback-only) ;;
    *) return 1 ;;
  esac
  case "$input_readiness_mode" in
    strict|recover-input-readiness) ;;
    *) return 1 ;;
  esac
  if [ -n "$activation_deadline_epoch" ]; then
    test "$verification_mode:$input_readiness_mode" = \
      rollback-only:strict || return 1
    controlled_guard_assert_activation_deadline \
      "$activation_deadline_epoch" || return 1
  fi
  [[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  IFS=, read -r main_load main_active main_unit_file <<< "$main_record" || return 1
  IFS=, read -r scheduler_load scheduler_active scheduler_unit_file \
    <<< "$scheduler_record" || return 1
  IFS=, read -r ai_service_load ai_service_active ai_service_unit_file \
    <<< "$ai_service_record" || return 1
  IFS=, read -r ai_timer_load ai_timer_active ai_timer_unit_file \
    <<< "$ai_timer_record" || return 1
  controlled_guard_apply_unit_state probiga "$main_record" \
    "$activation_deadline_epoch" || return 1
  controlled_guard_apply_unit_state probiga-scheduler "$scheduler_record" \
    "$activation_deadline_epoch" || return 1
  controlled_guard_apply_unit_state probiga-ai-recommendation-worker.service \
    "$ai_service_record" "$activation_deadline_epoch" || return 1
  controlled_guard_apply_unit_state probiga-ai-recommendation-worker.timer \
    "$ai_timer_record" "$activation_deadline_epoch" || return 1
  if [ "$verification_mode" = rollback-only ] && \
    [ "$main_active" = inactive ]; then
    inactive_restore=1
    for active_state in \
      "$scheduler_active" "$ai_service_active" "$ai_timer_active"; do
      case "$active_state" in
        inactive|not-found) ;;
        *) inactive_restore=0 ;;
      esac
    done
    if [ "$inactive_restore" -eq 1 ]; then
      # The exact saved unit states above prove that no restored writer can
      # execute.  A rollback to a fully stopped production state therefore
      # does not depend on an old immutable runtime already pruned after the
      # replacement services had passed their process and health checks.
      RESTORED_RUNTIME_FAILURE_CODE=inactive-rollback-verified
      return 0
    fi
  fi
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
    adapter_registry_seal_sha="$(/usr/bin/cat -- \
      "$release_venv/.adapter-registry-seal.sha256")" || return 1
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
    main_pid="$pid"
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
  # A legacy DEFERRED_DB release may attest one older auxiliary runtime while
  # the API advances.  Current releases require the scheduler to be inactive
  # and disabled, but the prior identity remains useful for rollback and an
  # existing AI worker.  Never let a stale unit file widen this allowlist.
  if [ "$main_active" = active ] && \
    grep -zFx -- 'PROBIGA_STRATEGY_GOVERNANCE_MODE=DEFERRED_DB' \
      "/proc/$main_pid/environ" >/dev/null; then
    deferred_expected_sha="$(tr '\0' '\n' < "/proc/$main_pid/environ" | \
      sed -n 's/^PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA=//p' | \
      tail -n 1)" || return 1
    deferred_code_root="$(tr '\0' '\n' < "/proc/$main_pid/environ" | \
      sed -n 's/^PROBIGA_DEFERRED_SCHEDULER_CODE_ROOT=//p' | tail -n 1)" || \
      return 1
    [[ "$deferred_expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
    if [ "$scheduler_active:$scheduler_unit_file" = inactive:disabled ]; then
      deferred_scheduler_fenced=1
    fi
    test "$deferred_code_root" = \
      "$CODE_RELEASE_ROOT/$deferred_expected_sha" || return 1
    test -d "$deferred_code_root" || return 1
    test ! -L "$deferred_code_root" || return 1
    test "$(git -C "$deferred_code_root" rev-parse HEAD)" = \
      "$deferred_expected_sha" || return 1
    test "$(readlink -f "$deferred_code_root")" = "$deferred_code_root" || \
      return 1
    test "$(stat -c '%U:%G' "$deferred_code_root")" = root:root || return 1
    test -z "$(find -P "$deferred_code_root" -xdev \
      \( ! -user root -o -perm /022 \) -print -quit)" || return 1
    controlled_guard_assert_recovery_code_tree_clean \
      "$deferred_code_root" "$deferred_expected_sha" || return 1
    sudo -u "$service_user" test ! -w "$deferred_code_root" || return 1
    deferred_venv="$RELEASE_VENV_ROOT/$deferred_expected_sha"
    test -L "$deferred_venv" || return 1
    deferred_venv_target="$(readlink -f "$deferred_venv")" || return 1
    case "$deferred_venv_target" in
      "$RELEASE_VENV_ROOT"/build-*) ;;
      *) return 1 ;;
    esac
    test "$(dirname "$deferred_venv_target")" = "$RELEASE_VENV_ROOT" || \
      return 1
    test "$(stat -c '%U:%G' "$deferred_venv_target")" = root:root || \
      return 1
    controlled_guard_assert_immutable_venv_tree "$deferred_venv_target" || \
      return 1
    sudo -u "$service_user" test ! -w "$deferred_venv_target" || return 1
    deferred_python_path="$deferred_venv/bin/python"
    test -x "$deferred_python_path" || return 1
    test "$(<"$deferred_venv/.probiga.gitsha")" = \
      "$deferred_expected_sha" || return 1
    deferred_adata_sha="$(<"$deferred_venv/.adata.gitsha")" || \
      return 1
    deferred_adata_tree_sha="$(<"$deferred_venv/.adata.tree.sha256")" || \
      return 1
    [[ "$deferred_adata_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
    [[ "$deferred_adata_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    deferred_adata_source="$ADATA_RUNTIME_ROOT/$deferred_adata_sha-$deferred_adata_tree_sha"
    test -d "$deferred_adata_source" || return 1
    test ! -L "$deferred_adata_source" || return 1
    test "$(readlink -f "$deferred_adata_source")" = \
      "$deferred_adata_source" || return 1
    test "$(stat -c '%U:%G' "$deferred_adata_source")" = root:root || \
      return 1
    test "$(<"$deferred_adata_source/.probiga-adata.gitsha")" = \
      "$deferred_adata_sha" || return 1
    test "$(<"$deferred_adata_source/.probiga-adata.tree.sha256")" = \
      "$deferred_adata_tree_sha" || return 1
    test -z "$(find -P "$deferred_adata_source" -xdev \
      \( ! -user root -o -perm /022 \) -print -quit)" || return 1
    sudo -u "$service_user" test ! -w "$deferred_adata_source" || return 1
    deferred_release_tree_sha="$(<"$deferred_venv/.release-tree.sha256")" || \
      return 1
    deferred_adapter_registry_seal_sha="$(/usr/bin/cat -- \
      "$deferred_venv/.adapter-registry-seal.sha256")" || \
      return 1
    [[ "$deferred_release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    [[ "$deferred_adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || \
      return 1
  fi
  if [ "$scheduler_load" = loaded ]; then
    if [ "$deferred_scheduler_fenced" -eq 1 ]; then
      test "$scheduler_active" = inactive || return 1
      test "$scheduler_unit_file" = disabled || return 1
      test "$(systemctl show -p MainPID --value probiga-scheduler)" = 0 || \
        return 1
    else
    scheduler_expected_sha="$expected_sha"
    scheduler_build_sha="$expected_sha"
    scheduler_code_root="$code_root"
    scheduler_python_path="$python_path"
    scheduler_adata_sha="$adata_sha"
    scheduler_adata_tree_sha="$adata_tree_sha"
    scheduler_adata_source="$adata_source"
    scheduler_release_tree_sha="$release_tree_sha"
    scheduler_adapter_registry_seal_sha="$adapter_registry_seal_sha"
    scheduler_has_attested_identity="$has_attested_identity"
    scheduler_exec_start="$(systemctl show -p ExecStart --value \
      probiga-scheduler)" || return 1
    if printf '%s' "$scheduler_exec_start" | grep -F -- \
        "$python_path -P $code_root/tools/run_scheduler_daemon.py" \
        >/dev/null; then
      :
    else
      test -n "$deferred_expected_sha" || return 1
      printf '%s' "$scheduler_exec_start" | grep -F -- \
        "$deferred_python_path -P $deferred_code_root/tools/run_scheduler_daemon.py" \
        >/dev/null || return 1
      scheduler_expected_sha="$deferred_expected_sha"
      scheduler_build_sha="$deferred_expected_sha"
      scheduler_code_root="$deferred_code_root"
      scheduler_python_path="$deferred_python_path"
      scheduler_adata_sha="$deferred_adata_sha"
      scheduler_adata_tree_sha="$deferred_adata_tree_sha"
      scheduler_adata_source="$deferred_adata_source"
      scheduler_release_tree_sha="$deferred_release_tree_sha"
      scheduler_adapter_registry_seal_sha="$deferred_adapter_registry_seal_sha"
      scheduler_has_attested_identity=1
    fi
    for scheduler_identity in \
      "PROBIGA_EXPECTED_GIT_SHA=$scheduler_expected_sha" \
      "PROBIGA_BUILD_COMMIT_SHA=$scheduler_build_sha" \
      "PROBIGA_CODE_ROOT=$scheduler_code_root" \
      "PROBIGA_EXPECTED_ADATA_SHA=$scheduler_adata_sha" \
      "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$scheduler_adata_tree_sha" \
      "PROBIGA_ADATA_SOURCE_DIR=$scheduler_adata_source" \
      "PYTHONPATH=$scheduler_adata_source:$scheduler_code_root"; do
      printf '%s' "$scheduler_exec_start" | grep -F -- \
        "$scheduler_identity" >/dev/null || return 1
    done
    if [ "$scheduler_has_attested_identity" -eq 1 ]; then
      for scheduler_identity in \
        "PROBIGA_RELEASE_TREE_SHA256=$scheduler_release_tree_sha" \
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$scheduler_adapter_registry_seal_sha"; do
        printf '%s' "$scheduler_exec_start" | grep -F -- \
          "$scheduler_identity" >/dev/null || return 1
      done
    fi
    if [ "$scheduler_active" = active ]; then
      pid="$(systemctl show -p MainPID --value probiga-scheduler)" || return 1
      case "$pid" in ''|0|*[!0-9]*) return 1 ;; esac
      for scheduler_identity in \
        "PROBIGA_EXPECTED_GIT_SHA=$scheduler_expected_sha" \
        "PROBIGA_BUILD_COMMIT_SHA=$scheduler_build_sha" \
        "PROBIGA_CODE_ROOT=$scheduler_code_root" \
        "PROBIGA_EXPECTED_ADATA_SHA=$scheduler_adata_sha" \
        "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$scheduler_adata_tree_sha" \
        "PROBIGA_ADATA_SOURCE_DIR=$scheduler_adata_source" \
        "PYTHONPATH=$scheduler_adata_source:$scheduler_code_root"; do
        grep -zFx -- "$scheduler_identity" "/proc/$pid/environ" \
          >/dev/null || return 1
      done
      if [ "$scheduler_has_attested_identity" -eq 1 ]; then
        for scheduler_identity in \
          "PROBIGA_RELEASE_TREE_SHA256=$scheduler_release_tree_sha" \
          "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$scheduler_adapter_registry_seal_sha"; do
          grep -zFx -- "$scheduler_identity" "/proc/$pid/environ" \
            >/dev/null || return 1
        done
      fi
      mapfile -d '' -t cmdline < "/proc/$pid/cmdline" || return 1
      test "${#cmdline[@]}" -ge 3 || return 1
      test "${cmdline[0]}" = "$scheduler_python_path" || return 1
      test "${cmdline[1]}" = -P || return 1
      test "${cmdline[2]}" = \
        "$scheduler_code_root/tools/run_scheduler_daemon.py" || return 1
    else
      test "$scheduler_active" = inactive || return 1
    fi
    fi
  else
    test "$scheduler_load:$scheduler_active:$scheduler_unit_file" = \
      not-found:not-found:not-found || return 1
  fi
  if [ "$ai_service_load" = loaded ]; then
    ai_expected_sha="$expected_sha"
    ai_code_root="$code_root"
    ai_python_path="$python_path"
    ai_adata_sha="$adata_sha"
    ai_adata_tree_sha="$adata_tree_sha"
    ai_adata_source="$adata_source"
    ai_release_tree_sha="$release_tree_sha"
    ai_adapter_registry_seal_sha="$adapter_registry_seal_sha"
    ai_has_attested_identity="$has_attested_identity"
    ai_exec_start="$(systemctl show -p ExecStart --value \
      probiga-ai-recommendation-worker.service)" || return 1
    if printf '%s' "$ai_exec_start" | grep -F -- \
        "$python_path -P $code_root/tools/run_ai_recommendation_worker.py --once" \
        >/dev/null; then
      :
    else
      test -n "$deferred_expected_sha" || return 1
      printf '%s' "$ai_exec_start" | grep -F -- \
        "$deferred_python_path -P $deferred_code_root/tools/run_ai_recommendation_worker.py --once" \
        >/dev/null || return 1
      ai_expected_sha="$deferred_expected_sha"
      ai_code_root="$deferred_code_root"
      ai_python_path="$deferred_python_path"
      ai_adata_sha="$deferred_adata_sha"
      ai_adata_tree_sha="$deferred_adata_tree_sha"
      ai_adata_source="$deferred_adata_source"
      ai_release_tree_sha="$deferred_release_tree_sha"
      ai_adapter_registry_seal_sha="$deferred_adapter_registry_seal_sha"
      ai_has_attested_identity=1
    fi
    printf '%s' "$ai_exec_start" | grep -F -- \
      "PROBIGA_EXPECTED_GIT_SHA=$ai_expected_sha" >/dev/null || return 1
    printf '%s' "$ai_exec_start" | grep -F -- \
      "PROBIGA_CODE_ROOT=$ai_code_root" >/dev/null || return 1
    printf '%s' "$ai_exec_start" | grep -F -- \
      "PROBIGA_EXPECTED_ADATA_SHA=$ai_adata_sha" >/dev/null || return 1
    printf '%s' "$ai_exec_start" | grep -F -- \
      "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$ai_adata_tree_sha" \
      >/dev/null || return 1
    printf '%s' "$ai_exec_start" | grep -F -- \
      "PROBIGA_ADATA_SOURCE_DIR=$ai_adata_source" >/dev/null || return 1
    printf '%s' "$ai_exec_start" | grep -F -- \
      "PYTHONPATH=$ai_adata_source:$ai_code_root" >/dev/null || return 1
    if [ "$ai_has_attested_identity" -eq 1 ]; then
      printf '%s' "$ai_exec_start" | grep -F -- \
        "PROBIGA_RELEASE_TREE_SHA256=$ai_release_tree_sha" \
        >/dev/null || return 1
      printf '%s' "$ai_exec_start" | grep -F -- \
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$ai_adapter_registry_seal_sha" \
        >/dev/null || return 1
    fi
    if [ "$ai_service_active" = active ]; then
      pid="$(systemctl show -p MainPID --value \
        probiga-ai-recommendation-worker.service)" || return 1
      case "$pid" in ''|0|*[!0-9]*) return 1 ;; esac
      grep -zFx -- "PROBIGA_EXPECTED_GIT_SHA=$ai_expected_sha" "/proc/$pid/environ" \
        >/dev/null || return 1
      grep -zFx -- "PROBIGA_CODE_ROOT=$ai_code_root" "/proc/$pid/environ" \
        >/dev/null || return 1
      grep -zFx -- "PROBIGA_EXPECTED_ADATA_SHA=$ai_adata_sha" \
        "/proc/$pid/environ" >/dev/null || return 1
      grep -zFx -- "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$ai_adata_tree_sha" \
        "/proc/$pid/environ" >/dev/null || return 1
      grep -zFx -- "PROBIGA_ADATA_SOURCE_DIR=$ai_adata_source" \
        "/proc/$pid/environ" >/dev/null || return 1
      grep -zFx -- "PYTHONPATH=$ai_adata_source:$ai_code_root" \
        "/proc/$pid/environ" >/dev/null || return 1
      if [ "$ai_has_attested_identity" -eq 1 ]; then
        grep -zFx -- "PROBIGA_RELEASE_TREE_SHA256=$ai_release_tree_sha" \
          "/proc/$pid/environ" >/dev/null || return 1
        grep -zFx -- \
          "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$ai_adapter_registry_seal_sha" \
          "/proc/$pid/environ" >/dev/null || return 1
      fi
      mapfile -d '' -t cmdline < "/proc/$pid/cmdline" || return 1
      test "${cmdline[0]}" = "$ai_python_path" || return 1
      test "${cmdline[1]}" = -P || return 1
      test "${cmdline[2]}" = \
        "$ai_code_root/tools/run_ai_recommendation_worker.py" || return 1
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
    RESTORED_RUNTIME_FAILURE_CODE=""
    return 0
  fi
  governance_health_args=(
    --compact
    --expected-build-sha "$expected_sha"
  )
  guarded_command_prefix=(
    /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin
    GIT_OPTIONAL_LOCKS=0
    PYTHONDONTWRITEBYTECODE=1
    PYTHONSAFEPATH=1
    PROBIGA_DEPLOYMENT_MODE=production
    "PROBIGA_EXPECTED_GIT_SHA=$expected_sha"
    "PROBIGA_BUILD_COMMIT_SHA=$expected_sha"
    "PROBIGA_CODE_ROOT=$code_root"
    "PROBIGA_EXPECTED_ADATA_SHA=$adata_sha"
    "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha"
    "PROBIGA_ADATA_SOURCE_DIR=$adata_source"
    "${attested_env[@]}"
    "PYTHONPATH=$adata_source:$code_root"
  )
  if [ "$RELEASE_DATA_VALIDATION_BLOCKING" -eq 0 ]; then
    # Recovery still proves the immutable runtime identity above and installs
    # the task contracts below, but it does not read or regenerate market data.
    if [ "$input_readiness_mode" = recover-input-readiness ]; then
      test "$main_load:$main_active" = loaded:inactive || return 1
      test "$scheduler_load:$scheduler_active" = loaded:inactive || return 1
      test "$snapshot_release" = "$expected_sha" || return 1
      activation_snapshot_validate "$expected_sha" >/dev/null || return 1
      activation_snapshot_validate_governance_new || return 1
    fi
    RESTORED_RUNTIME_FAILURE_CODE=premarket-task-ensure
    controlled_guard_run_service_gate_with_deadline "$service_user" \
      "${guarded_command_prefix[@]}" "$python_path" -P \
      "$code_root/tools/ensure_quality_gate.py" || return 1
    if [ "$input_readiness_mode" = recover-input-readiness ]; then
      cutover_deadline_epoch="$(($(date +%s) + CONTROLLED_RECOVERY_CUTOVER_RESERVE_SECONDS))"
      RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE="$(date -u +%F)"
      RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH="$cutover_deadline_epoch"
    fi
    RESTORED_RUNTIME_FAILURE_CODE=""
    return 0
  fi
  if [ "$input_readiness_mode" = recover-input-readiness ]; then
    # Legacy activation journals did not seal the daily runner disposition.
    # Never infer it from a missing row: first try strict health, then use the
    # allow-mode checker only as an exact, read-only probe.  A clean missing-run
    # probe must be followed by a fresh guarded runner result bound to the same
    # trade date before any waiver can authorize the final health gate.
    test "$main_load:$main_active" = loaded:inactive || return 1
    test "$scheduler_load:$scheduler_active" = loaded:inactive || return 1
    case "$ai_service_load:$ai_service_active" in
      loaded:inactive|not-found:not-found) ;;
      *) return 1 ;;
    esac
    case "$ai_timer_load:$ai_timer_active" in
      loaded:inactive|not-found:not-found) ;;
      *) return 1 ;;
    esac
    test "$snapshot_release" = "$expected_sha" || return 1
    activation_snapshot_validate "$expected_sha" >/dev/null || return 1
    activation_snapshot_validate_governance_new || return 1
    sudo -u "$service_user" test ! -w "$code_root" || return 1
    sudo -u "$service_user" test -r \
      "$code_root/tools/check_strategy_governance_health.py" || return 1
    sudo -u "$service_user" test -r \
      "$code_root/tools/run_strategy_governance_daily.py" || return 1
    cutover_probe_code="$(
      controlled_guard_governance_cutover_probe_code
    )" || return 1
    test -n "$cutover_probe_code" || return 1

    governance_result_file="$(mktemp \
      "$ACTIVATION_UNIT_SNAPSHOT_DIR/.governance-health-strict.XXXXXX")" || \
      return 1
    controlled_guard_assert_file "$governance_result_file" 600 || return 1
    RESTORED_RUNTIME_FAILURE_CODE=governance-health-strict
    if controlled_guard_capture_service_gate_with_deadline "$service_user" \
        "$governance_result_file" "$code_root" \
        "${guarded_command_prefix[@]}" "$python_path" -P \
        "$code_root/tools/check_strategy_governance_health.py" \
        "${governance_health_args[@]}"; then
      governance_result_status=0
    else
      governance_result_status=$?
    fi
    if [ "$governance_result_status" -eq 0 ]; then
      if ! governance_trade_date="$(
          controlled_guard_parse_governance_health_result \
            "$governance_result_file" "$expected_sha" completed
        )"; then
        rm -f -- "$governance_result_file"
        return 1
      fi
      /usr/bin/cat -- "$governance_result_file"
      rm -f -- "$governance_result_file" || return 1
      governance_result_file=""
    else
      rm -f -- "$governance_result_file" || return 1
      governance_result_file=""
      test "$governance_result_status" -eq 1 || return 1
      governance_result_file="$(mktemp \
        "$ACTIVATION_UNIT_SNAPSHOT_DIR/.governance-health-probe.XXXXXX")" || \
        return 1
      controlled_guard_assert_file "$governance_result_file" 600 || return 1
      RESTORED_RUNTIME_FAILURE_CODE=governance-health-probe
      if controlled_guard_capture_service_gate_with_deadline "$service_user" \
          "$governance_result_file" "$code_root" \
          "${guarded_command_prefix[@]}" "$python_path" -P \
          "$code_root/tools/check_strategy_governance_health.py" \
          "${governance_health_args[@]}" --allow-input-not-ready; then
        governance_result_status=0
      else
        governance_result_status=$?
      fi
      if [ "$governance_result_status" -ne 0 ]; then
        rm -f -- "$governance_result_file"
        return 1
      fi
      if ! governance_trade_date="$(
          controlled_guard_parse_governance_health_result \
            "$governance_result_file" "$expected_sha" input_not_ready
        )"; then
        rm -f -- "$governance_result_file"
        return 1
      fi
      /usr/bin/cat -- "$governance_result_file"
      rm -f -- "$governance_result_file" || return 1
      governance_result_file=""

      controlled_guard_apply_unit_state probiga "$main_record" || return 1
      controlled_guard_apply_unit_state probiga-scheduler \
        "$scheduler_record" || return 1
      controlled_guard_apply_unit_state \
        probiga-ai-recommendation-worker.service "$ai_service_record" || \
        return 1
      controlled_guard_apply_unit_state \
        probiga-ai-recommendation-worker.timer "$ai_timer_record" || return 1
      governance_result_file="$(mktemp \
        "$ACTIVATION_UNIT_SNAPSHOT_DIR/.governance-recheck.XXXXXX")" || \
        return 1
      controlled_guard_assert_file "$governance_result_file" 600 || return 1
      RESTORED_RUNTIME_FAILURE_CODE=governance-recheck
      if controlled_guard_capture_service_gate_with_deadline "$service_user" \
          "$governance_result_file" "$code_root" \
          "${guarded_command_prefix[@]}" "$python_path" -P \
          "$code_root/tools/run_strategy_governance_daily.py" \
          --trade-date "$governance_trade_date"; then
        governance_result_status=0
      else
        governance_result_status=$?
      fi
      case "$governance_result_status" in 0|2) ;; *)
        rm -f -- "$governance_result_file"
        return 1
      esac
      if ! runner_disposition="$(
          controlled_guard_parse_governance_runner_result \
            "$governance_result_file" "$governance_result_status" \
            "$governance_trade_date"
        )"; then
        rm -f -- "$governance_result_file"
        return 1
      fi
      rm -f -- "$governance_result_file" || return 1
      governance_result_file=""
      printf 'recovery_governance_recheck disposition=%s trade_date=%s\n' \
        "$runner_disposition" "$governance_trade_date"

      governance_health_args+=(
        --expected-trade-date "$governance_trade_date"
      )
      if [ "$runner_disposition" = input_not_ready ]; then
        governance_health_args+=(--allow-input-not-ready)
      else
        test "$runner_disposition" = completed || return 1
      fi
      governance_result_file="$(mktemp \
        "$ACTIVATION_UNIT_SNAPSHOT_DIR/.governance-health-final.XXXXXX")" || \
        return 1
      controlled_guard_assert_file "$governance_result_file" 600 || return 1
      RESTORED_RUNTIME_FAILURE_CODE=governance-health-final
      if controlled_guard_capture_service_gate_with_deadline "$service_user" \
          "$governance_result_file" "$code_root" \
          "${guarded_command_prefix[@]}" "$python_path" -P \
          "$code_root/tools/check_strategy_governance_health.py" \
          "${governance_health_args[@]}"; then
        governance_result_status=0
      else
        governance_result_status=$?
      fi
      if [ "$governance_result_status" -ne 0 ]; then
        rm -f -- "$governance_result_file"
        return 1
      fi
      if ! controlled_guard_parse_governance_health_result \
          "$governance_result_file" "$expected_sha" "$runner_disposition" \
          "$governance_trade_date" >/dev/null; then
        rm -f -- "$governance_result_file"
        return 1
      fi
      /usr/bin/cat -- "$governance_result_file"
      rm -f -- "$governance_result_file" || return 1
      governance_result_file=""
    fi
  else
    RESTORED_RUNTIME_FAILURE_CODE=governance-health
    controlled_guard_run_service_gate_with_deadline "$service_user" \
      "${guarded_command_prefix[@]}" "$python_path" -P \
      "$code_root/tools/check_strategy_governance_health.py" \
      "${governance_health_args[@]}" || return 1
  fi
  RESTORED_RUNTIME_FAILURE_CODE=premarket-task-ensure
  controlled_guard_run_service_gate_with_deadline "$service_user" \
    "${guarded_command_prefix[@]}" "$python_path" -P \
    "$code_root/tools/ensure_quality_gate.py" || return 1
  if [ "$input_readiness_mode" = recover-input-readiness ]; then
    RESTORED_RUNTIME_FAILURE_CODE=governance-date-final
    governance_result_file="$(mktemp \
      "$ACTIVATION_UNIT_SNAPSHOT_DIR/.governance-date-final.XXXXXX")" || \
      return 1
    controlled_guard_assert_file "$governance_result_file" 600 || return 1
    if controlled_guard_capture_service_gate_with_deadline "$service_user" \
        "$governance_result_file" "$code_root" \
        "${guarded_command_prefix[@]}" "$python_path" -P -c \
        "$cutover_probe_code" \
        "$CONTROLLED_RECOVERY_CUTOVER_RESERVE_SECONDS"; then
      governance_result_status=0
    else
      governance_result_status=$?
    fi
    if [ "$governance_result_status" -ne 0 ]; then
      rm -f -- "$governance_result_file"
      return 1
    fi
    if ! cutover_deadline_epoch="$(
        controlled_guard_parse_governance_cutover_result \
          "$governance_result_file" "$governance_trade_date"
      )"; then
      rm -f -- "$governance_result_file"
      return 1
    fi
    /usr/bin/cat -- "$governance_result_file"
    rm -f -- "$governance_result_file" || return 1
    governance_result_file=""
    controlled_guard_assert_activation_deadline \
      "$cutover_deadline_epoch" || return 1
    RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE="$governance_trade_date"
    RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH="$cutover_deadline_epoch"
  fi
  RESTORED_RUNTIME_FAILURE_CODE=""
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
  adapter_registry_seal_sha="$(/usr/bin/cat -- \
    "$release_venv/.adapter-registry-seal.sha256")" || return 1
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
  controlled_guard_run_service_gate_with_deadline "$service_user" \
    /usr/bin/env -i \
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
controlled_guard_qmt_announcement_snapshot() {
  local action="$1"
  local guarded_sha="$2"
  local snapshot="$3"
  local code_root="$CODE_RELEASE_ROOT/$guarded_sha"
  local release_venv="$RELEASE_VENV_ROOT/$guarded_sha"
  local service_user adata_sha adata_tree_sha adata_source
  local release_tree_sha adapter_registry_seal_sha
  case "$snapshot" in
    "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT")
      controlled_guard_assert_file \
        "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" 600 || return 1
      test "$(<"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA")" = \
        "$(sha256sum "$snapshot" | cut -d' ' -f1)" || return 1
      ;;
    "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT")
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
  adapter_registry_seal_sha="$(/usr/bin/cat -- \
    "$release_venv/.adapter-registry-seal.sha256")" || return 1
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
  controlled_guard_run_service_gate_with_deadline "$service_user" \
    /usr/bin/env -i \
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
    "$code_root/tools/add_qmt_announcement_task.py" \
    "--${action}-snapshot" - < "$snapshot" || return 1
  return 0
}
materialize_controlled_governance_contract_tool() {
  local source_digest
  local source_sha="$1"
  local tool_size
  test -z "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" || return 1
  test -z "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256" || return 1
  [[ "$source_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  git --git-dir="$CODE_GIT_CACHE" cat-file -e "${source_sha}^{commit}" || \
    return 1
  test -d /tmp || return 1
  test ! -L /tmp || return 1
  test "$(readlink -f /tmp)" = /tmp || return 1
  test "$(stat -c '%U:%G' /tmp)" = root:root || return 1
  test "$(stat -c '%a' /tmp)" = 1777 || return 1
  CONTROLLED_GOVERNANCE_CONTRACT_TOOL="$(mktemp \
    /tmp/.probiga-governance-contract.XXXXXX)" || return 1
  case "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" in
    /tmp/.probiga-governance-contract.*) ;;
    *) return 1 ;;
  esac
  if ! git --git-dir="$CODE_GIT_CACHE" show \
      "${source_sha}:deploy/production_governance_contract_recovery.py" \
      > "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" || \
    ! chown root:root "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" || \
    ! chmod 0444 "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" || \
    ! sync -f "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL"; then
    return 1
  fi
  controlled_guard_assert_file "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" 444 || \
    return 1
  test "$(dirname "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL")" = /tmp || return 1
  test "$(readlink -f "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL")" = \
    "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" || return 1
  tool_size="$(stat -c '%s' "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL")" || return 1
  [[ "$tool_size" =~ ^[0-9]+$ ]] || return 1
  test "$tool_size" -gt 0 || return 1
  test "$tool_size" -le 131072 || return 1
  source_digest="$(git --git-dir="$CODE_GIT_CACHE" show \
    "${source_sha}:deploy/production_governance_contract_recovery.py" | \
    sha256sum | cut -d' ' -f1)" || return 1
  [[ "$source_digest" =~ ^[0-9a-f]{64}$ ]] || return 1
  CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256="$(
    sha256sum "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" | cut -d' ' -f1
  )" || return 1
  test "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256" = "$source_digest" || \
    return 1
  return 0
}
controlled_guard_governance_contract_snapshot() {
  local action="$1"
  local guarded_sha="$2"
  local snapshot="$3"
  local snapshot_kind="${4:-forward-governance}"
  local code_root="$CODE_RELEASE_ROOT/$guarded_sha"
  local release_venv="$RELEASE_VENV_ROOT/$guarded_sha"
  local gate_output=""
  local service_user adata_sha adata_tree_sha adata_source
  local release_tree_sha adapter_registry_seal_sha tool_digest
  GOVERNANCE_CONTRACT_FAILURE_CODE=""
  case "$action" in
    restore|verify) ;;
    *) GOVERNANCE_CONTRACT_FAILURE_CODE=action; return 1 ;;
  esac
  case "$snapshot_kind:$snapshot" in
    "forward-governance:$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT")
      if ! activation_snapshot_validate_governance_new; then
        GOVERNANCE_CONTRACT_FAILURE_CODE=snapshot-seal
        return 1
      fi
      ;;
    "rollback-governance:$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT")
      if ! controlled_guard_assert_file \
          "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" 600 || \
        ! controlled_guard_assert_file \
          "$ACTIVATION_GOVERNANCE_OLD_SHA" 600 || \
        [ "$(<"$ACTIVATION_GOVERNANCE_OLD_SHA")" != \
          "$(sha256sum "$snapshot" | cut -d' ' -f1)" ]; then
        GOVERNANCE_CONTRACT_FAILURE_CODE=snapshot-seal
        return 1
      fi
      ;;
    "rollback-qmt:$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT")
      if ! controlled_guard_assert_file \
          "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" 600 || \
        ! controlled_guard_assert_file \
          "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA" 600 || \
        [ "$(<"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA")" != \
          "$(sha256sum "$snapshot" | cut -d' ' -f1)" ]; then
        GOVERNANCE_CONTRACT_FAILURE_CODE=snapshot-seal
        return 1
      fi
      ;;
    *)
      GOVERNANCE_CONTRACT_FAILURE_CODE=snapshot-path
      return 1
      ;;
  esac
  if ! controlled_guard_assert_governance_restore_runtime "$guarded_sha"; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=runtime-attestation
    return 1
  fi
  if [ -z "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" ]; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=tool-presence
    return 1
  fi
  [[ "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256" =~ ^[0-9a-f]{64}$ ]] || \
    { GOVERNANCE_CONTRACT_FAILURE_CODE=tool-digest; return 1; }
  case "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" in
    /tmp/.probiga-governance-contract.*) ;;
    *) GOVERNANCE_CONTRACT_FAILURE_CODE=tool-path; return 1 ;;
  esac
  controlled_guard_assert_file "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" 444 || \
    { GOVERNANCE_CONTRACT_FAILURE_CODE=tool-permission; return 1; }
  if [ "$(readlink -f "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL")" != \
      "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" ]; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=tool-path
    return 1
  fi
  if ! tool_digest="$(sha256sum "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" | \
      cut -d' ' -f1)"; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=tool-digest
    return 1
  fi
  test "$tool_digest" = "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256" || \
    { GOVERNANCE_CONTRACT_FAILURE_CODE=tool-digest; return 1; }
  if ! adata_sha="$(/usr/bin/cat -- "$release_venv/.adata.gitsha")"; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=release-metadata
    return 1
  fi
  if ! adata_tree_sha="$(/usr/bin/cat -- \
      "$release_venv/.adata.tree.sha256")"; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=release-metadata
    return 1
  fi
  if ! release_tree_sha="$(/usr/bin/cat -- \
      "$release_venv/.release-tree.sha256")"; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=release-metadata
    return 1
  fi
  adapter_registry_seal_sha="$(/usr/bin/cat -- \
    "$release_venv/.adapter-registry-seal.sha256")" || \
    { GOVERNANCE_CONTRACT_FAILURE_CODE=release-metadata; return 1; }
  if [[ ! "$adata_sha" =~ ^[0-9a-f]{40}$ ]] || \
    [[ ! "$adata_tree_sha" =~ ^[0-9a-f]{64}$ ]] || \
    [[ ! "$release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || \
    [[ ! "$adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]]; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=release-metadata
    return 1
  fi
  adata_source="$ADATA_RUNTIME_ROOT/$adata_sha-$adata_tree_sha"
  if ! service_user="$(systemctl show -p User --value probiga)"; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=service-user
    return 1
  fi
  if [ -z "$service_user" ] || [ "$service_user" = root ]; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=service-user
    return 1
  fi
  if ! sudo -u "$service_user" test -r \
      "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL"; then
    GOVERNANCE_CONTRACT_FAILURE_CODE=tool-readability
    return 1
  fi
  if gate_output="$(
    (
      cd "$code_root" || exit 1
      controlled_guard_run_service_gate_with_deadline "$service_user" \
        /usr/bin/env -i \
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
        "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" "$action" \
          "$snapshot_kind" < "$snapshot"
    ) 2>&1
  )"; then
    if [ "$snapshot_kind" = forward-governance ]; then
      if ! controlled_guard_qmt_announcement_snapshot "$action" "$guarded_sha" \
          "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT"; then
        GOVERNANCE_CONTRACT_FAILURE_CODE=qmt-announcement-task
        return 1
      fi
    fi
    return 0
  fi
  case "$gate_output" in
    *"probiga_governance_contract_failure=snapshot-envelope"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=snapshot-envelope ;;
    *"probiga_governance_contract_failure=sealed-identity"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=sealed-identity ;;
    *"probiga_governance_contract_failure=contract-shape"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=contract-shape ;;
    *"probiga_governance_contract_failure=engine-schema"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=engine-schema ;;
    *"probiga_governance_contract_failure=live-count"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=live-count ;;
    *"probiga_governance_contract_failure=live-id"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=live-id ;;
    *"probiga_governance_contract_failure=live-identity"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=live-identity ;;
    *"probiga_governance_contract_failure=projection"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=projection ;;
    *"probiga_governance_contract_failure=update-rowcount"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=update-rowcount ;;
    *"probiga_governance_contract_failure=volatile-drift"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=volatile-drift ;;
    *"probiga_governance_contract_failure=database-runtime"*)
      GOVERNANCE_CONTRACT_FAILURE_CODE=database-runtime ;;
    *) GOVERNANCE_CONTRACT_FAILURE_CODE=runner ;;
  esac
  return 1
}
controlled_guard_restore_and_verify_governance_snapshot() {
  # Most rollback attempts fail before the scheduler row is changed.  Prove
  # that case first and avoid unnecessary database writes.  A mismatch falls
  # through to the exact restore, whose own failure remains visible.
  local qmt_snapshot="$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT"
  test "$2" = "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" || return 1
  if controlled_guard_governance_contract_snapshot verify "$1" "$2" \
      rollback-governance \
      >/dev/null 2>&1 && \
    controlled_guard_governance_contract_snapshot verify "$1" \
      "$qmt_snapshot" rollback-qmt >/dev/null 2>&1; then
    return 0
  fi
  controlled_guard_governance_contract_snapshot restore "$1" "$2" \
    rollback-governance || return 1
  controlled_guard_governance_contract_snapshot verify "$1" "$2" \
    rollback-governance || return 1
  controlled_guard_governance_contract_snapshot restore "$1" \
    "$qmt_snapshot" rollback-qmt || return 1
  controlled_guard_governance_contract_snapshot verify "$1" \
    "$qmt_snapshot" rollback-qmt || return 1
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
controlled_guard_assert_recovery_code_tree_clean() {
  local code_root="$1"
  local expected_release="$2"
  local expected_source_tree_sha
  local manifest_path="$code_root/probiga.release.json"
  local tree_oid
  local -a untracked_paths=()
  # Older Windows-prepared releases can differ from their Git tree only by a
  # terminal CR byte.  Recovery may trust that exact semantic equivalence,
  # while still rejecting staged changes and every other worktree byte
  # difference.  The broker intentionally creates one root-owned, read-only
  # release manifest after checking out Git; it is the only permitted
  # untracked path and its complete identity is revalidated below.
  [[ "$expected_release" =~ ^[0-9a-f]{40}$ ]] || return 1
  test "$(git -C "$code_root" rev-parse HEAD)" = "$expected_release" || \
    return 1
  tree_oid="$(git -C "$code_root" rev-parse "${expected_release}^{tree}")" || \
    return 1
  [[ "$tree_oid" =~ ^[0-9a-f]{40,64}$ ]] || return 1
  expected_source_tree_sha="$(printf \
    '{"kind":"git-tree","tree":"%s"}' "$tree_oid" | \
    sha256sum | cut -d' ' -f1)" || return 1
  [[ "$expected_source_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  mapfile -d '' -t untracked_paths < <(
    git -C "$code_root" ls-files --others --exclude-standard -z
  ) || return 1
  test "${#untracked_paths[@]}" -eq 1 || return 1
  test "${untracked_paths[0]}" = probiga.release.json || return 1
  git -C "$code_root" diff --no-ext-diff --cached --quiet || return 1
  git -C "$code_root" diff --no-ext-diff --ignore-cr-at-eol --quiet || return 1
  controlled_guard_assert_file "$manifest_path" 444 || return 1
  /usr/bin/python3.14 -I - "$manifest_path" "$expected_release" \
    "$expected_source_tree_sha" <<'PY' || return 1
import datetime
import hashlib
import json
import re
import sys

manifest_path, expected_release, expected_tree = sys.argv[1:]


def reject_duplicate_keys(pairs):
    payload = dict()
    for key, value in pairs:
        if key in payload:
            raise ValueError("duplicate key")
        payload[key] = value
    return payload


with open(manifest_path, "r", encoding="utf-8") as handle:
    payload = json.load(
        handle,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=lambda _value: (_ for _ in ()).throw(
            ValueError("non-finite value")
        ),
    )
required_fields = frozenset((
    "schema",
    "release_id",
    "source_tree_hash",
    "migration_version",
    "built_at",
    "artifact_hash",
    "manifest_sha256",
))
if not isinstance(payload, dict) or set(payload) != required_fields:
    raise SystemExit(1)
if any(type(payload[field]) is not str for field in required_fields):
    raise SystemExit(1)
if payload["schema"] != "probiga.release-manifest.v1":
    raise SystemExit(1)
if payload["release_id"] != expected_release:
    raise SystemExit(1)
if payload["source_tree_hash"] != expected_tree:
    raise SystemExit(1)
if re.fullmatch(r"[0-9a-f]{64}", payload["artifact_hash"]) is None:
    raise SystemExit(1)
if not payload["migration_version"] or len(payload["migration_version"]) > 128:
    raise SystemExit(1)
try:
    built_at = datetime.datetime.fromisoformat(
        payload["built_at"].replace("Z", "+00:00")
    )
except ValueError:
    raise SystemExit(1)
if built_at.tzinfo is None:
    raise SystemExit(1)
core = dict(
    (key, value)
    for key, value in payload.items()
    if key != "manifest_sha256"
)
seal = hashlib.sha256(
    json.dumps(
        core,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
).hexdigest()
if payload["manifest_sha256"] != seal:
    raise SystemExit(1)
PY
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
  V2_RECOVERY_STEP=rollback-validate-forward-snapshot
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
  V2_RECOVERY_STEP=rollback-validate-forward-code-path
  test -d "$code_root" || return 1
  test ! -L "$code_root" || return 1
  test "$(readlink -f "$code_root")" = "$code_root" || return 1
  V2_RECOVERY_STEP=rollback-validate-forward-code-owner
  test "$(stat -c '%U:%G' "$code_root")" = root:root || return 1
  test -z "$(find -P "$code_root" -xdev \
    \( ! -user root -o -perm /022 \) -print -quit)" || return 1
  V2_RECOVERY_STEP=rollback-validate-forward-code-git-identity
  test "$(git -C "$code_root" rev-parse HEAD)" = "$guarded_sha" || return 1
  V2_RECOVERY_STEP=rollback-validate-forward-code-git-clean
  controlled_guard_assert_recovery_code_tree_clean \
    "$code_root" "$guarded_sha" || return 1
  V2_RECOVERY_STEP=rollback-validate-forward-code-tools
  controlled_guard_assert_file \
    "$code_root/tools/add_strategy_governance_task.py" 444 || return 1
  controlled_guard_assert_file \
    "$code_root/tools/add_qmt_announcement_task.py" 444 || return 1
  V2_RECOVERY_STEP=rollback-validate-forward-venv-identity
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
  V2_RECOVERY_STEP=rollback-validate-forward-venv-tree
  controlled_guard_assert_immutable_venv_tree "$release_venv_target" || return 1
  adata_sha="$(<"$release_venv/.adata.gitsha")" || return 1
  adata_tree_sha="$(<"$release_venv/.adata.tree.sha256")" || return 1
  [[ "$adata_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$adata_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  V2_RECOVERY_STEP=rollback-validate-forward-adata
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
  V2_RECOVERY_STEP=rollback-validate-forward-service-access
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
  local rollback_verification_action="${3:-verify}"
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
  local tool_digest
  local -a release_identity_lines=()
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$old_runtime_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  test "$old_runtime_sha" != "$guarded_sha" || return 1
  case "$rollback_verification_action" in
    verify|verify-stable) ;;
    *) return 1 ;;
  esac
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
  controlled_guard_assert_file \
    "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" 600 || return 1
  controlled_guard_assert_file \
    "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA" 600 || return 1
  test "$(<"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA")" = \
    "$(sha256sum "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" | \
      cut -d' ' -f1)" || return 1
  test -d "$code_root" || return 1
  test ! -L "$code_root" || return 1
  test "$(readlink -f "$code_root")" = "$code_root" || return 1
  test "$(stat -c '%U:%G' "$code_root")" = root:root || return 1
  test -z "$(find -P "$code_root" -xdev \
    \( ! -user root -o -perm /022 \) -print -quit)" || return 1
  test "$(git -C "$code_root" rev-parse HEAD)" = "$guarded_sha" || return 1
  controlled_guard_assert_recovery_code_tree_clean \
    "$code_root" "$guarded_sha" || return 1
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
    runtime_release_tree_sha="$(/usr/bin/cat -- \
      "$release_venv/.release-tree.sha256")" || return 1
    runtime_adapter_registry_seal_sha="$(/usr/bin/cat -- \
      "$release_venv/.adapter-registry-seal.sha256")" || return 1
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
  test -n "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" || return 1
  [[ "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256" =~ ^[0-9a-f]{64}$ ]] || \
    return 1
  case "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" in
    /tmp/.probiga-governance-contract.*) ;;
    *) return 1 ;;
  esac
  controlled_guard_assert_file "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" 444 || \
    return 1
  test "$(readlink -f "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL")" = \
    "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" || return 1
  tool_digest="$(sha256sum "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" | \
    cut -d' ' -f1)" || return 1
  test "$tool_digest" = "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256" || \
    return 1
  sudo -u "$service_user" test -r \
    "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" || return 1
  # The old runtime interpreter is still the only guaranteed executable after
  # a failed cutover, but verification logic comes from the authenticated
  # incoming release.  It compares only the sealed OLD projection, allowing
  # additive live columns while preserving the old runtime trust boundary.
  (
    cd "$code_root" || exit 1
    controlled_guard_run_service_gate_with_deadline "$service_user" \
      /usr/bin/env -i \
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
      "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" \
        "$rollback_verification_action" rollback-governance \
      < "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"
  ) >/dev/null 2>&1 || return 1
  (
    cd "$code_root" || exit 1
    controlled_guard_run_service_gate_with_deadline "$service_user" \
      /usr/bin/env -i \
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
      "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" \
        "$rollback_verification_action" rollback-qmt \
      < "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT"
  ) >/dev/null 2>&1 || return 1
  return 0
}
controlled_guard_restore_and_finalize() {
  local ai_service_active ai_service_load ai_service_unit_file
  local ai_service_record="$4"
  local ai_timer_active ai_timer_load ai_timer_unit_file
  local ai_timer_record="$5"
  local guarded_sha="$1"
  local governance_runtime="${6:-controlled}"
  local main_active main_load main_unit_file
  local main_record="$2"
  local old_runtime_sha="$guarded_sha"
  local restore_verification_mode=full
  local safe_ai_service_record="$ai_service_record"
  local safe_ai_timer_record="$ai_timer_record"
  local safe_main_record="$main_record"
  local safe_scheduler_record="$scheduler_record"
  local scheduler_active scheduler_load scheduler_unit_file
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
        controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
          "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" \
          rollback-governance || return 1
        controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
          "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" \
          rollback-qmt || return 1
        ;;
    esac
  fi
  IFS=, read -r main_load main_active main_unit_file <<< "$main_record" || \
    return 1
  IFS=, read -r scheduler_load scheduler_active scheduler_unit_file \
    <<< "$scheduler_record" || return 1
  IFS=, read -r ai_service_load ai_service_active ai_service_unit_file \
    <<< "$ai_service_record" || return 1
  IFS=, read -r ai_timer_load ai_timer_active ai_timer_unit_file \
    <<< "$ai_timer_record" || return 1
  if [ "$DEPLOY_OPERATION" = recover-database-guard ] && \
    { [ ! -d "$CODE_RELEASE_ROOT/$old_runtime_sha" ] || \
      [ ! -L "$RELEASE_VENV_ROOT/$old_runtime_sha" ] || \
      [ ! -x "$RELEASE_VENV_ROOT/$old_runtime_sha/bin/python" ]; }; then
    test "$main_load" = loaded || return 1
    safe_main_record=loaded,inactive,disabled
    case "$scheduler_load" in
      loaded) safe_scheduler_record=loaded,inactive,disabled ;;
      not-found) safe_scheduler_record=not-found,not-found,not-found ;;
      *) return 1 ;;
    esac
    case "$ai_service_load:$ai_service_unit_file" in
      loaded:enabled|loaded:disabled)
        safe_ai_service_record=loaded,inactive,disabled
        ;;
      loaded:static)
        safe_ai_service_record=loaded,inactive,static
        ;;
      not-found:not-found)
        safe_ai_service_record=not-found,not-found,not-found
        ;;
      *) return 1 ;;
    esac
    case "$ai_timer_load" in
      loaded)
        safe_ai_timer_record=loaded,inactive,disabled
        ;;
      not-found)
        safe_ai_timer_record=not-found,not-found,not-found
        ;;
      *) return 1 ;;
    esac
    restore_verification_mode=rollback-only
    echo "old-runtime-missing-safe-fence main=$safe_main_record scheduler=$safe_scheduler_record ai_service=$safe_ai_service_record ai_timer=$safe_ai_timer_record" >&2
  fi
  if ! controlled_guard_restore_previous_writer_states "$safe_main_record" \
      "$safe_scheduler_record" "$safe_ai_service_record" \
      "$safe_ai_timer_record" || \
    ! controlled_guard_verify_restored_runtime "$safe_main_record" \
      "$safe_scheduler_record" "$old_runtime_sha" "$safe_ai_service_record" \
      "$safe_ai_timer_record" "$restore_verification_mode"; then
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
controlled_v2_keep_no_receipt_evidence() {
  local guarded_sha="$1"
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  case "$DEPLOY_OPERATION:$DEPLOY_ARTIFACT_MODE" in
    recover-database-guard:static-wheel-lock-v2)
      return 0
      ;;
    deploy:ci-resolved-freeze-v1)
      test "$EXPECTED_SHA" = "$guarded_sha" || return 1
      V2_FORWARD_PRESERVED_NO_RECEIPT_SHA="$guarded_sha"
      return 0
      ;;
    *) return 1 ;;
  esac
}
controlled_v2_assert_preserved_no_receipt_transaction() {
  local ai_service_record
  local ai_timer_record
  local expected_sha="$1"
  local guarded_sha
  local main_record
  local phase
  local scheduler_record
  local -a state_lines=()
  [[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  guarded_sha="$(activation_snapshot_recorded_release)" || return 1
  test "$guarded_sha" = "$expected_sha" || return 1
  phase="$(activation_snapshot_phase)" || return 1
  test "$phase" = new-runtime-preserved-no-receipt || return 1
  activation_snapshot_validate "$guarded_sha" >/dev/null || return 1
  activation_snapshot_validate_new "$guarded_sha" || return 1
  activation_snapshot_validate_governance_new || return 1
  activation_snapshot_assert_pending_receipt_absent || return 1
  activation_snapshot_assert_new_set "$guarded_sha" || return 1
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
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
  controlled_guard_assert_state_record scheduler "$scheduler_record" || \
    return 1
  controlled_guard_assert_state_record ai-service "$ai_service_record" || \
    return 1
  controlled_guard_assert_state_record ai-timer "$ai_timer_record" || return 1
  controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  V2_FORWARD_PRESERVED_MAIN_RECORD="$main_record"
  V2_FORWARD_PRESERVED_SCHEDULER_RECORD="$scheduler_record"
  V2_FORWARD_PRESERVED_AI_SERVICE_RECORD="$ai_service_record"
  V2_FORWARD_PRESERVED_AI_TIMER_RECORD="$ai_timer_record"
  return 0
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
  local governance_failure_code=""
  local governance_trade_date=""
  local cutover_deadline_epoch=""
  local runtime_failure_code=""
  local -a state_lines=()
  RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE=""
  RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH=""
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
    controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
      "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
    activation_snapshot_assert_pending_receipt_absent || return 1
    if controlled_v2_keep_no_receipt_evidence "$guarded_sha"; then
      V2_RECOVERY_STEP=forward-commit-preserve-evidence
      controlled_guard_write_restore_file "$guarded_sha" "$main_record" \
        "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
        return 1
      controlled_v2_assert_preserved_no_receipt_transaction \
        "$guarded_sha" || return 1
      echo "v2 recovery preserved runtime $guarded_sha pending authenticated receipt" \
        >&2
      V2_RECOVERY_STEP=complete
      return 0
    fi
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
    # The sealed NEW governance pair is created only after the guarded release
    # installed, captured and verified its task contract.  Persist forward
    # intent before changing that state so an interruption can never fall back
    # to the now-incompatible OLD restore path.
    V2_RECOVERY_STEP=forward-begin-governance-restore
    if ! activation_snapshot_set_phase "$guarded_sha" \
        restoring-new-no-receipt; then
      controlled_guard_refence_after_restore_failure "$guarded_sha" \
        "$main_record" "$scheduler_record" "$ai_service_record" \
        "$ai_timer_record" || true
      return 1
    fi
  fi
  V2_RECOVERY_STEP=forward-governance-verify-current
  if ! controlled_guard_governance_contract_snapshot verify \
      "$guarded_sha" "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"; then
    V2_RECOVERY_STEP=forward-governance-restore-exec
    if ! controlled_guard_governance_contract_snapshot restore \
        "$guarded_sha" "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"; then
      governance_failure_code="${GOVERNANCE_CONTRACT_FAILURE_CODE:-unknown}"
      case "$governance_failure_code" in
        action|snapshot-path|snapshot-seal|runtime-attestation|tool-presence|\
        tool-digest|tool-path|tool-permission|release-metadata|service-user|\
        tool-readability|snapshot-envelope|sealed-identity|contract-shape|\
        engine-schema|live-count|live-id|live-identity|projection|\
        update-rowcount|volatile-drift|database-runtime|runner) ;;
        *) governance_failure_code=unknown ;;
      esac
      V2_RECOVERY_STEP="forward-governance-restore-$governance_failure_code"
      controlled_guard_refence_after_restore_failure "$guarded_sha" \
        "$main_record" "$scheduler_record" "$ai_service_record" \
        "$ai_timer_record" || true
      return 1
    fi
    V2_RECOVERY_STEP=forward-governance-verify-after-restore
    if ! controlled_guard_governance_contract_snapshot verify \
        "$guarded_sha" "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"; then
      governance_failure_code="${GOVERNANCE_CONTRACT_FAILURE_CODE:-unknown}"
      case "$governance_failure_code" in
        action|snapshot-path|snapshot-seal|runtime-attestation|tool-presence|\
        tool-digest|tool-path|tool-permission|release-metadata|service-user|\
        tool-readability|snapshot-envelope|sealed-identity|contract-shape|\
        engine-schema|live-count|live-id|live-identity|projection|\
        update-rowcount|volatile-drift|database-runtime|runner) ;;
        *) governance_failure_code=unknown ;;
      esac
      V2_RECOVERY_STEP="forward-governance-verify-$governance_failure_code"
      controlled_guard_refence_after_restore_failure "$guarded_sha" \
        "$main_record" "$scheduler_record" "$ai_service_record" \
        "$ai_timer_record" || true
      return 1
    fi
  fi
  V2_RECOVERY_STEP=forward-governance-boundary
  if ! controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
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
  if ! controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
      "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-verify-gates-fenced
  if ! controlled_guard_verify_restored_runtime "$fenced_main_record" \
      "$fenced_scheduler_record" "$guarded_sha" \
      "$fenced_ai_service_record" "$fenced_ai_timer_record" full \
      recover-input-readiness; then
    runtime_failure_code="${RESTORED_RUNTIME_FAILURE_CODE:-unknown}"
    case "$runtime_failure_code" in
      runtime-identity|governance-health|governance-health-strict|\
      governance-health-probe|governance-recheck|governance-health-final|\
      governance-date-final|premarket-task-ensure) ;;
      *) runtime_failure_code=unknown ;;
    esac
    V2_RECOVERY_STEP="forward-verify-gates-fenced-$runtime_failure_code"
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  governance_trade_date="$RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE"
  cutover_deadline_epoch="$RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH"
  if [[ ! "$governance_trade_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || \
    [[ ! "$cutover_deadline_epoch" =~ ^[1-9][0-9]{9,11}$ ]] || \
    ! controlled_guard_assert_activation_deadline \
      "$cutover_deadline_epoch"; then
    V2_RECOVERY_STEP=forward-verify-cutover-result-fenced
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-install-cutover-deadline-fenced
  if ! controlled_guard_install_recovery_cutover_dropins \
      "$cutover_deadline_epoch" "$scheduler_record" \
      "$ai_service_record"; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-verify-boundary-fenced
  if ! controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-verify-cutover-deadline-fenced
  if ! controlled_guard_assert_activation_deadline \
      "$cutover_deadline_epoch"; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-remove-fence
  if ! controlled_guard_cleanup "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" \
      "$cutover_deadline_epoch"; then
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
      "$ai_timer_record" rollback-only strict "$cutover_deadline_epoch" || \
    ! controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
      "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || \
    ! activation_snapshot_assert_pending_receipt_absent; then
    controlled_guard_refence_after_restore_failure "$guarded_sha" \
      "$main_record" "$scheduler_record" "$ai_service_record" \
      "$ai_timer_record" || true
    return 1
  fi
  V2_RECOVERY_STEP=forward-remove-cutover-deadline
  if ! controlled_guard_remove_recovery_cutover_dropins \
      "$cutover_deadline_epoch" "$scheduler_record" \
      "$ai_service_record"; then
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
  if controlled_v2_keep_no_receipt_evidence "$guarded_sha"; then
    V2_RECOVERY_STEP=forward-preserve-evidence
    controlled_guard_write_restore_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
      return 1
    controlled_v2_assert_preserved_no_receipt_transaction "$guarded_sha" || \
      return 1
    echo "v2 recovery preserved verified runtime $guarded_sha pending authenticated receipt" \
      >&2
    V2_RECOVERY_STEP=complete
    return 0
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
controlled_v2_retire_fenced_old_set_for_newer_deploy() {
  local ai_service_record="$5"
  local ai_timer_record="$6"
  local guarded_sha="$1"
  local main_record="$3"
  local old_runtime_sha="$2"
  local phase
  local scheduler_record="$4"
  test "$DEPLOY_OPERATION" = deploy || return 1
  [[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$old_runtime_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  test "$EXPECTED_SHA" != "$guarded_sha" || return 1
  test "$old_runtime_sha" != "$guarded_sha" || return 1
  phase="$(activation_snapshot_phase)" || return 1
  test "$phase" = old-set-restored || return 1
  activation_snapshot_validate "$guarded_sha" >/dev/null || return 1
  activation_snapshot_assert_old_set "$guarded_sha" || return 1
  controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
    "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" rollback-governance || return 1
  controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
    "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" rollback-qmt || return 1
  controlled_guard_force_all_writers_fenced "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  activation_snapshot_set_phase "$guarded_sha" old-runtime-verified || return 1
  controlled_guard_cleanup "$guarded_sha" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || return 1
    sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
  fi
  activation_snapshot_remove_old_runtime_verified || return 1
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -L "$DATABASE_WRITER_GUARD_FILE" || return 1
  test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test ! -L "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  printf 'v2 recovery retired fenced old set release=%s replacement=%s\n' \
    "$guarded_sha" "$EXPECTED_SHA" >&7
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
  case "$DEPLOY_ARTIFACT_MODE" in
    ci-resolved-freeze-v1|static-wheel-lock-v2) ;;
    *) return 1 ;;
  esac
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
      # changed; in that case the sealed old runtime must independently execute
      # the incoming verifier and prove an exact OLD projection match.
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
      test ! -e "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT" || return 1
      test ! -L "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT" || return 1
      test ! -e "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SHA" || return 1
      test ! -L "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SHA" || return 1
      ;;
  esac
  activation_snapshot_validate_rollback_receipt_state \
    "$guarded_sha" "$phase" || return 1
  V2_RECOVERY_STEP=rollback-validate-writer-directory
  controlled_guard_assert_directory || return 1
  V2_RECOVERY_STEP=rollback-validate-writer-file
  controlled_guard_assert_file "$ACTIVATION_UNIT_SNAPSHOT_STATE" 600 || \
    return 1
  V2_RECOVERY_STEP=rollback-read-writer-state
  mapfile -t state_lines < "$ACTIVATION_UNIT_SNAPSHOT_STATE" || return 1
  V2_RECOVERY_STEP=rollback-validate-writer-line-count
  test "${#state_lines[@]}" -eq 6 || return 1
  V2_RECOVERY_STEP=rollback-validate-writer-schema
  test "${state_lines[0]}" = probiga.database-writer-restore.v1 || return 1
  V2_RECOVERY_STEP=rollback-validate-writer-release
  test "${state_lines[1]}" = "release=$guarded_sha" || return 1
  V2_RECOVERY_STEP=rollback-parse-writer-main
  case "${state_lines[2]}" in
    main_unit=*) main_record="${state_lines[2]#main_unit=}" ;;
    *) return 1 ;;
  esac
  V2_RECOVERY_STEP=rollback-parse-writer-scheduler
  case "${state_lines[3]}" in
    scheduler_unit=*) scheduler_record="${state_lines[3]#scheduler_unit=}" ;;
    *) return 1 ;;
  esac
  V2_RECOVERY_STEP=rollback-parse-writer-ai-service
  case "${state_lines[4]}" in
    ai_service_unit=*) ai_service_record="${state_lines[4]#ai_service_unit=}" ;;
    *) return 1 ;;
  esac
  V2_RECOVERY_STEP=rollback-parse-writer-ai-timer
  case "${state_lines[5]}" in
    ai_timer_unit=*) ai_timer_record="${state_lines[5]#ai_timer_unit=}" ;;
    *) return 1 ;;
  esac
  V2_RECOVERY_STEP=rollback-validate-writer-main
  controlled_guard_assert_state_record main "$main_record" || return 1
  V2_RECOVERY_STEP=rollback-validate-writer-scheduler
  controlled_guard_assert_state_record scheduler "$scheduler_record" || return 1
  V2_RECOVERY_STEP=rollback-validate-writer-ai-service
  controlled_guard_assert_state_record ai-service "$ai_service_record" || return 1
  V2_RECOVERY_STEP=rollback-validate-writer-ai-timer
  controlled_guard_assert_state_record ai-timer "$ai_timer_record" || return 1
  if [ "$phase" = old-set-restored ] && \
    [ "$guarded_sha" != "$EXPECTED_SHA" ]; then
    V2_RECOVERY_STEP=rollback-retire-fenced-old-set
    controlled_v2_retire_fenced_old_set_for_newer_deploy \
      "$guarded_sha" "$old_runtime_sha" "$main_record" "$scheduler_record" \
      "$ai_service_record" "$ai_timer_record"
    V2_RECOVERY_STEP=complete
    return 0
  fi
  if [ "$phase" = old-runtime-verified ] && \
    [ ! -e "$DATABASE_WRITER_GUARD_FILE" ] && \
    [ ! -L "$DATABASE_WRITER_GUARD_FILE" ]; then
    # The old runtime is already the committed safe state.  Revalidate it
    # read-only and finish cleanup without recreating the guard or stopping
    # writers again.  Its scheduler execution/audit columns may have advanced
    # since recovery committed, so compare only sealed task identities and
    # configuration here; all earlier rollback phases retain exact comparison.
    if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
      [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
      V2_RECOVERY_STEP=rollback-fast-validate-restore
      controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
        "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
        return 1
    fi
    V2_RECOVERY_STEP=rollback-fast-assert-old-set
    activation_snapshot_assert_old_set "$guarded_sha" || return 1
    V2_RECOVERY_STEP=rollback-fast-verify-old-governance
    controlled_guard_capture_current_governance_snapshot "$guarded_sha" \
      "$old_runtime_sha" verify-stable || return 1
    V2_RECOVERY_STEP=rollback-fast-verify-old-runtime
    controlled_guard_verify_restored_runtime "$main_record" \
      "$scheduler_record" "$old_runtime_sha" "$ai_service_record" \
      "$ai_timer_record" rollback-only || return 1
    if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
      [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
      V2_RECOVERY_STEP=rollback-fast-remove-restore
      rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || return 1
      sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
      test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
      test ! -L "$DATABASE_WRITER_RESTORE_FILE" || return 1
    fi
    V2_RECOVERY_STEP=rollback-fast-retire-journal
    activation_snapshot_remove_old_runtime_verified || return 1
    V2_RECOVERY_STEP=rollback-fast-verify-retired
    test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
    test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
    echo "v2 rollback-only recovery finalized verified runtime $old_runtime_sha" \
      >&2
    V2_RECOVERY_STEP=complete
    return 0
  fi
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    V2_RECOVERY_STEP=rollback-validate-restore
    controlled_guard_assert_restore_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  else
    V2_RECOVERY_STEP=rollback-create-restore
    activation_snapshot_allows_missing_guard_for_recovery "$phase" || return 1
    controlled_guard_write_restore_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  fi
  if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ]; then
    V2_RECOVERY_STEP=rollback-validate-guard
    controlled_guard_assert_marker "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  else
    # Normal activation removes the marker only after the complete pre-start
    # checks, while the durable restore journal deliberately remains until the
    # post-start boundary is finalized.  A disconnect in that window therefore
    # has runtime-units-installed plus no marker and must be re-fenced here.
    V2_RECOVERY_STEP=rollback-recreate-guard
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
    activation_snapshot_validate_governance_new && \
    controlled_guard_assert_governance_restore_runtime "$guarded_sha" && \
    controlled_guard_assert_boundary "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    # Prefer the sealed forward target even when a prior OLD rollback attempt
    # partially changed live governance.  The dedicated recovery durably
    # commits forward intent, restores only the sealed NEW stable task contract
    # (never scheduler runtime/audit columns) and any partially changed NEW unit
    # set, then re-attests everything while all writers remain fenced.
    V2_RECOVERY_STEP=forward-preserve
    controlled_v2_forward_preserve_no_receipt_recovery || return 1
    return 0
  fi
  if [ "$restore_forward_governance" -eq 1 ]; then
    V2_RECOVERY_STEP=rollback-probe-old-governance
    if controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
        "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" rollback-governance \
        >/dev/null 2>&1 && \
      controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
        "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" rollback-qmt \
        >/dev/null 2>&1; then
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
  # sealed old interpreter with the authenticated incoming projection verifier;
  # any OLD-column drift remains fenced without depending on the removed venv.
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
  printf 'v2 rollback records main=%s scheduler=%s ai_service=%s ai_timer=%s\n' \
    "$main_record" "$scheduler_record" "$ai_service_record" \
    "$ai_timer_record" >&2
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
  controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
    "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
  V2_RECOVERY_STEP=forward-finalize-qmt-activation
  local qmt_activation_output
  qmt_activation_output="$(controlled_guard_run_qmt_activation_tool \
    "$CODE_RELEASE_ROOT/$guarded_sha" "$RELEASE_VENV_ROOT/$guarded_sha" \
    "$guarded_sha" --activation-grant-latest)" || return 1
  printf '%s' "$qmt_activation_output" | \
    controlled_guard_validate_qmt_activation_json \
      "$RELEASE_VENV_ROOT/$guarded_sha/bin/python" "$guarded_sha" \
      activation-grant-latest || return 1
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
  local adata_source
  local adata_tree_sha
  local previous_git_sha
  local release_tree_sha=""
  local adapter_registry_seal_sha=""
  local -a phase_args=(--phase "$phase")
  local -a attested_env=()
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  previous_git_sha="$(activation_snapshot_old_release "$guarded_sha")" || \
    return 1
  [[ "$previous_git_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  test "$previous_git_sha" != 0000000000000000000000000000000000000000 || \
    return 1
  test "$previous_git_sha" != "$guarded_sha" || return 1
  case "$phase" in
    resume) phase_args+=(--writers-fenced) ;;
    preflight|recover) ;;
    *) return 2 ;;
  esac
  adata_sha="$(cat "$release_venv/.adata.gitsha")" || return 1
  adata_tree_sha="$(cat "$release_venv/.adata.tree.sha256")" || return 1
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
  if [ -e "$release_venv/.release-tree.sha256" ] || \
    [ -e "$release_venv/.adapter-registry-seal.sha256" ]; then
    test -f "$release_venv/.release-tree.sha256" || return 1
    test -f "$release_venv/.adapter-registry-seal.sha256" || return 1
    release_tree_sha="$(<"$release_venv/.release-tree.sha256")" || return 1
    adapter_registry_seal_sha="$(/usr/bin/cat -- \
      "$release_venv/.adapter-registry-seal.sha256")" || return 1
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
      PROBIGA_PREVIOUS_GIT_SHA="$previous_git_sha" \
      PROBIGA_CODE_ROOT="$code_root" \
      PROBIGA_EXPECTED_ADATA_SHA="$adata_sha" \
      PROBIGA_EXPECTED_ADATA_TREE_SHA256="$adata_tree_sha" \
      PROBIGA_ADATA_SOURCE_DIR="$adata_source" \
      "${attested_env[@]}" \
      "PYTHONPATH=$code_root" \
      "$release_venv/bin/python" -P \
      "$code_root/tools/prepare_strategy_governance_schema.py" \
      "${phase_args[@]}"
  )
}
controlled_guard_run_qmt_activation_tool() {
  local code_root="$1"
  local release_venv="$2"
  local guarded_sha="$3"
  local mode="$4"
  local deployment_attempt_id="${5:-}"
  local target_build_sha="${6:-}"
  local adata_sha
  local adata_source
  local adata_tree_sha
  local release_tree_sha=""
  local adapter_registry_seal_sha=""
  local -a attested_env=()
  local -a mode_args=()
  [[ "$guarded_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  test "$code_root" = "$CODE_RELEASE_ROOT/$guarded_sha" || return 1
  test "$release_venv" = "$RELEASE_VENV_ROOT/$guarded_sha" || return 1
  test -x "$release_venv/bin/python" || return 1
  test -f "$code_root/tools/run_qmt_windows_edge_release_bootstrap.py" || \
    return 1
  case "$mode" in
    --activation-grant-latest)
      test -z "$deployment_attempt_id" || return 1
      mode_args=("$mode")
      ;;
    --activation-grant|--request-compatibility-quiescence)
      [[ "$deployment_attempt_id" =~ ^[0-9a-f]{32}$ ]] || return 1
      mode_args=("$mode" --deployment-attempt-id "$deployment_attempt_id")
      ;;
    --request-forward-quiescence)
      [[ "$deployment_attempt_id" =~ ^[0-9a-f]{32}$ ]] || return 1
      [[ "$target_build_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
      test "$target_build_sha" != "$guarded_sha" || return 1
      # This controller runs from the prepared target; the sixth argument is
      # the actual current Linux prior, never permission to resume that prior.
      mode_args=("$mode" --deployment-attempt-id "$deployment_attempt_id"
        --prior-build-sha "$target_build_sha")
      ;;
    --request-recoverable-quiescence|--abort-precutover)
      [[ "$deployment_attempt_id" =~ ^[0-9a-f]{32}$ ]] || return 1
      [[ "$target_build_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
      test "$target_build_sha" != "$guarded_sha" || return 1
      # The trusted PRIOR release must contain the compatible controller and
      # reader. Never silently fall back to the old stop-and-overwrite flow.
      test -f "$code_root/server/common/qmt_edge_release_recovery.py" || {
        echo "RECOVERY_BLOCKED: prior edge requires controlled compatibility bootstrap" >&2
        return 1
      }
      mode_args=("$mode" --deployment-attempt-id "$deployment_attempt_id"
        --target-build-sha "$target_build_sha")
      ;;
    *) return 1 ;;
  esac
  adata_sha="$(cat "$release_venv/.adata.gitsha")" || return 1
  adata_tree_sha="$(cat "$release_venv/.adata.tree.sha256")" || return 1
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
  if [ -e "$release_venv/.release-tree.sha256" ] || \
    [ -e "$release_venv/.adapter-registry-seal.sha256" ]; then
    test -f "$release_venv/.release-tree.sha256" || return 1
    test -f "$release_venv/.adapter-registry-seal.sha256" || return 1
    release_tree_sha="$(<"$release_venv/.release-tree.sha256")" || return 1
    adapter_registry_seal_sha="$(/usr/bin/cat -- \
      "$release_venv/.adapter-registry-seal.sha256")" || return 1
    [[ "$release_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    [[ "$adapter_registry_seal_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    attested_env+=(
      "PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha"
      "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha"
    )
  fi
  (
    cd "$code_root"
    /usr/bin/env -i \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      GIT_OPTIONAL_LOCKS=0 \
      PYTHONDONTWRITEBYTECODE=1 \
      PYTHONSAFEPATH=1 \
      PROBIGA_DEPLOYMENT_MODE=production \
      PROBIGA_STRATEGY_GOVERNANCE_MODE="$STRATEGY_GOVERNANCE_MODE" \
      PROBIGA_EXPECTED_GIT_SHA="$guarded_sha" \
      PROBIGA_BUILD_COMMIT_SHA="$guarded_sha" \
      PROBIGA_EXPECTED_ADATA_SHA="$adata_sha" \
      PROBIGA_EXPECTED_ADATA_TREE_SHA256="$adata_tree_sha" \
      PROBIGA_ADATA_SOURCE_DIR="$adata_source" \
      PROBIGA_CODE_ROOT="$code_root" \
      "${attested_env[@]}" \
      "PYTHONPATH=$adata_source:$code_root" \
      "$release_venv/bin/python" -P \
      "$code_root/tools/run_qmt_windows_edge_release_bootstrap.py" \
      "${mode_args[@]}" --expected-build-sha "$guarded_sha" --compact
  )
}
controlled_guard_validate_qmt_activation_json() {
  local validation_python="$1"
  local expected_sha="$2"
  local expected_mode="$3"
  local expected_attempt="${4:-}"
  "$validation_python" -I -c '
import json
import re
import sys
from datetime import datetime

payload = json.load(sys.stdin)
expected_sha, expected_mode, expected_attempt = sys.argv[1:]
expected_fields = {
    "mode", "status", "schema", "build_sha", "deployment_attempt_id",
    "grant_run_uid", "hold_run_uid", "hold_hash", "granted_at",
    "schema_cutover_verified", "real_order", "grant_hash",
    "activation_granted", "database_writes",
}
attempt = payload.get("deployment_attempt_id")
timestamp = payload.get("granted_at")
valid = (
    isinstance(payload, dict)
    and set(payload) == expected_fields
    and payload.get("mode") == expected_mode
    and payload.get("status") in {"inserted", "idempotent"}
    and payload.get("schema") == "probiga.qmt-windows-edge-release-activation.v1"
    and payload.get("build_sha") == expected_sha
    and isinstance(attempt, str)
    and re.fullmatch(r"[0-9a-f]{32}", attempt) is not None
    and attempt != "0" * 32
    and (not expected_attempt or attempt == expected_attempt)
    and payload.get("grant_run_uid") == f"qmt-edge-grant-{attempt}"
    and payload.get("hold_run_uid") == f"qmt-edge-hold-{attempt}"
    and re.fullmatch(r"[0-9a-f]{64}", str(payload.get("hold_hash") or ""))
    is not None
    and isinstance(timestamp, str)
    and datetime.fromisoformat(timestamp).isoformat(timespec="seconds") == timestamp
    and payload.get("schema_cutover_verified") is True
    and payload.get("real_order") is False
    and re.fullmatch(r"[0-9a-f]{64}", str(payload.get("grant_hash") or ""))
    is not None
    and payload.get("activation_granted") is True
    and payload.get("database_writes") is True
)
raise SystemExit(0 if valid else 2)
' "$expected_sha" "$expected_mode" "$expected_attempt"
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
    adapter_registry_seal_sha="$(/usr/bin/cat -- \
      "$release_venv/.adapter-registry-seal.sha256")" || return 1
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
      -u MYSQL_URL -u DATABASE_URL -u MYSQL_PWD \
      -u MYSQL_UNIX_PORT -u MYSQL_TEST_LOGIN_FILE \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin \
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
      --writer-drain-timeout-seconds 660 \
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
    '
import hashlib
import json
import re
import sys

p = json.load(sys.stdin)
migrations = p.get("v3_migrations") if isinstance(p, dict) else None
trigger = p.get("trigger_contract") if isinstance(p, dict) else None
permission_summary = (
    p.get("runtime_grant_summary") if isinstance(p, dict) else None
)
repair = p.get("legacy_trigger_repair") if isinstance(p, dict) else None
candidates = repair.get("candidate_names") if isinstance(repair, dict) else None
repaired = repair.get("repaired_names") if isinstance(repair, dict) else None
windows = p.get("trigger_trust_window_names") if isinstance(p, dict) else None
binding = p.get("legacy_binding_plan") if isinstance(p, dict) else None
funding = p.get("funding_checkpoint_schema") if isinstance(p, dict) else None
governance_source = (
    p.get("governance_trigger_source_contract")
    if isinstance(p, dict) else None
)
supporting_source = (
    p.get("supporting_trigger_source_contract")
    if isinstance(p, dict) else None
)
pit_schema = p.get("pit_fact_schema") if isinstance(p, dict) else None
qmt_reference = (
    p.get("qmt_reference_schema") if isinstance(p, dict) else None
)
qmt_coverage = (
    p.get("qmt_history_coverage_schema") if isinstance(p, dict) else None
)
qmt_coverage_runtime = (
    p.get("qmt_history_coverage_runtime_schema")
    if isinstance(p, dict) else None
)
scheduler_history = (
    p.get("scheduler_task_history_schema") if isinstance(p, dict) else None
)
scheduler_history_runtime = (
    p.get("scheduler_task_history_runtime_schema")
    if isinstance(p, dict) else None
)
runtime_bundle = (
    p.get("runtime_schema_bundle") if isinstance(p, dict) else None
)
runtime_bundle_runtime = (
    p.get("runtime_schema_bundle_validation")
    if isinstance(p, dict) else None
)
full_trigger_inventory = (
    p.get("full_trigger_inventory") if isinstance(p, dict) else None
)
expected_runtime_bundle_hash = (
    "61f9ddfb3179f30c9976a090fce00adb8613d4e38d698c6cfc954f957084845f"
)
expected_recovery_planners = [
    "ai_bridge",
    "analysis_output",
    "recommended_run_history",
    "sim_trade",
    "qmt_catalog",
    "qmt_audit",
]

def recovery_hashes_exact(plan):
    def valid_hash(value):
        return (
            isinstance(value, str)
            and re.fullmatch(r"[0-9a-f]{64}", value) is not None
        )
    if not isinstance(plan, dict) or not valid_hash(plan.get("plan_sha256")):
        return False
    canonical = plan["plan_sha256"]
    recovery_bundle = plan.get("recovery_bundle_sha256")
    atomic = plan.get("atomic_plan_sha256")
    if recovery_bundle is not None and (
        not valid_hash(recovery_bundle) or recovery_bundle != canonical
    ):
        return False
    if atomic is not None and (
        not valid_hash(atomic)
        or (recovery_bundle is None and atomic != canonical)
    ):
        return False
    return True

expected_funding_contract_hash = (
    "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde"
)
expected_trigger_source_hash = (
    "5a1a19e0664c715ae0cac7cfa8dd87c47da1b63b1d2df869561cecf3c995f01f"
)
expected_trigger_names_hash = (
    "a2f74c8b1d4fa984e2d6aadb6169e13e8d041a1f414f2523aeb5835dc4376e13"
)
expected_supporting_source_hash = (
    "7c261eaff759e562b883d19880ef345c6733cacf911218437adc72ba864934e2"
)
expected_supporting_names_hash = (
    "f26aa672a479a6dfbfba6861d0f86d675aba4494839bc218c64197ec7eceabe7"
)
expected_supporting_owner_counts = {
    "market_field_capture": 5,
    "pit_facts": 6,
    "qmt_attestation": 6,
    "qmt_history_coverage": 4,
    "qmt_membership": 6,
    "qmt_reference": 10,
    "scheduler_task_history": 3,
    "schema_recovery_evidence": 2,
    "strategy_governance": 40,
}  # expected_supporting_owner_counts
expected_full_trigger_nameset_hash = (
    "6df9585376ec190a8d78c996336ff9f2c68bf1a4860e88809561a55df7cbfde5"
)
expected_full_with_v4_trigger_nameset_hash = (
    "a1d2a23569adc5318b5806e3040487cedcb9e31a60da3dae7756ed7bdf7044d7"
)
expected_v2_trigger_source_hash = (
    "5167f36ee731c2544be73590e4e00716f334c58b5746f776e610254904cf8883"
)
expected_managed_trigger_source_hash = (
    "7e154c081f807ce3d88311dc6d7db74170951abe890130a02343010466dc2f75"
)
expected_pit_contract_hash = (
    "c374e0ba62eb2e5b9bef802ce2bdd89fae0c63391d918e922ff21781707863ae"
)
expected_qmt_reference_contract_hash = (
    "64982c16c517f7e5c0e6ee9b88b1bf33df98f9aebf66440eedc916eae76f3dd5"
)
expected_qmt_tables = [
    "qmt_stock_catalog_batch",
    "qmt_stock_catalog_member",
    "qmt_trade_calendar_batch",
    "qmt_trade_calendar_session",
    "qmt_reference_schema_contract",
]
expected_qmt_triggers = [
    "trg_qmt_calendar_batch_no_delete",
    "trg_qmt_calendar_batch_no_update",
    "trg_qmt_calendar_session_no_delete",
    "trg_qmt_calendar_session_no_update",
    "trg_qmt_reference_contract_no_delete",
    "trg_qmt_reference_contract_no_update",
    "trg_qmt_stock_catalog_batch_no_delete",
    "trg_qmt_stock_catalog_batch_no_update",
    "trg_qmt_stock_catalog_member_no_delete",
    "trg_qmt_stock_catalog_member_no_update",
]
expected_append_physical_hash = (
    "bf537f9ed5fb1d31195092ae6a24262511de6f45bf9addacefebc88e25b6b9d8"
)
expected_metric_physical_hash = (
    "c217a42eb6c2a5f7bed592bb7c7e724499546f997061c4daad1db957317bdf28"
)
expected_core_append_hash = (
    "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943"
)
expected_core_metric_hash = (
    "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84"
)
expected_funding_tables = [
    "st_strategy_funding_checkpoint",
    "st_strategy_funding_daily_fact",
]
expected_funding_table_counts = {
    "st_strategy_funding_daily_fact": {
        "column_count": 29, "index_count": 9,
        "foreign_key_count": 3, "check_count": 7,
    },
    "st_strategy_funding_checkpoint": {
        "column_count": 46, "index_count": 12,
        "foreign_key_count": 7, "check_count": 13,
    },
}  # expected_funding_table_counts
legacy = (
    isinstance(binding, dict)
    and isinstance(binding.get("legacy_run_count"), int)
    and binding["legacy_run_count"] >= 0
    and isinstance(binding.get("legacy_binding_plan_hash"), str)
    and bool(binding["legacy_binding_plan_hash"])
    and binding.get("legacy_binding_pending") is False
    and binding.get("legacy_binding_marker_present")
    is bool(binding["legacy_run_count"])
)
permission_audit_skipped = (
    p.get("permission_audit_status") == "SKIPPED_BY_USER_AUTHORIZATION"
    and p.get("permission_audit_verified") is False
    and p.get("runtime_privilege_boundary_verified") is False
    and p.get("runtime_least_privilege_verified") is False
    and p.get("runtime_legacy_ddl_compatibility") is False
    and p.get("runtime_current_user") == "probiga_runtime@127.0.0.1"
    and p.get("runtime_session_user") == "probiga_runtime@127.0.0.1"
    and p.get("runtime_tls_verified") is True
    and p.get("runtime_grant_count") is None
    and p.get("runtime_grant_contract_hash") == ""
    and isinstance(permission_summary, dict)
    and set(permission_summary) == {
        "permission_audit_status", "permission_audit_verified",
        "runtime_grant_count", "runtime_grant_contract_hash",
    }  # permission_summary_keys
    and permission_summary.get("permission_audit_status")
    == p.get("permission_audit_status")
    and permission_summary.get("permission_audit_verified")
    is p.get("permission_audit_verified")
    and permission_summary.get("runtime_grant_count")
    is p.get("runtime_grant_count")
    and permission_summary.get("runtime_grant_contract_hash")
    == p.get("runtime_grant_contract_hash")
    and p.get("routine_inventory_audit_status")
    == "SKIPPED_BY_USER_AUTHORIZATION"
    and p.get("runtime_self_definer_routine_count") is None
    and p.get("migrator_self_definer_routine_count") is None
    and p.get("runtime_definer_routine_count") is None
    and p.get("runtime_definer_routine_inventory_verified") is False
    and p.get("runtime_definer_routine_inventory_complete") is False
    and p.get("runtime_definer_routine_inventory_authority") == ""
    and p.get("runtime_definer_routine_inventory_schemas") == []
)
funding_hash = (
    str(funding.get("contract_hash") or "")
    if isinstance(funding, dict)
    else ""
)
funding_exact = (
    isinstance(funding, dict)
    and funding.get("table_count") == 2
    and funding.get("tables") == expected_funding_table_counts
    and funding.get("trigger_count") == 4
    and funding_hash == expected_funding_contract_hash
    and re.fullmatch(
        r"[0-9a-f]{64}", str(funding.get("trigger_contract_hash") or "")
    ) is not None
    and p.get("funding_checkpoint_contract_hash") == funding_hash
    and p.get("funding_checkpoint_table_count") == 2
    and p.get("funding_checkpoint_trigger_count") == 4
    and p.get("governance_append_only_trigger_count") == 38
    and p.get("governance_metric_review_trigger_count") == 2
    and isinstance(p.get("governance_trigger_count"), int)
    and p["governance_trigger_count"] == 40
    and p.get("governance_trigger_source_contract_hash")
    == expected_trigger_source_hash
    and p.get("governance_append_only_physical_contract_hash")
    == expected_append_physical_hash
    and p.get("governance_metric_review_physical_contract_hash")
    == expected_metric_physical_hash
    and p.get("governance_append_only_core_contract_hash")
    == expected_core_append_hash
    and p.get("governance_metric_review_core_contract_hash")
    == expected_core_metric_hash
    and funding.get("daily_path_base_authoritative_sessions") == 120
    and funding.get("daily_path_max_incremental_replay_sessions") == 370
    and funding.get("maximum_holding_buffer_sessions") == 250
    and funding.get("bootstrap_mode")
    == "EXPLICIT_FULL_HISTORY_ONCE_PER_VERSION_ACCOUNT"
    and funding.get("bootstrap_is_bounded") is False
    and funding.get("rolling_history_storage")
    == "ADDRESSABLE_APPEND_ONLY_DAILY_FACT_CHAIN"
    and funding.get("checkpoint_target_average_bytes") == 8192
    and funding.get("checkpoint_total_target_bytes") == 8388608
    and funding.get("checkpoint_total_hard_bytes") == 16777216
    and funding.get("batch_max_rows") == 100
    and funding.get("batch_max_bytes") == 4194304
    and funding.get("manifest_max_bytes") == 1048576
    and funding.get("audit_max_bytes") == 131072
    and funding.get("automatic_real_order_submission") is False
    and funding.get("real_order_authority") is False
)
governance_names = (
    governance_source.get("expected_names")
    if isinstance(governance_source, dict) else None
)
governance_names_hash = (
    hashlib.sha256(
        json.dumps(
            governance_names, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    if isinstance(governance_names, list) else ""
)
governance_source_exact = (
    isinstance(governance_source, dict)
    and governance_source.get("source_contract_hash")
    == expected_trigger_source_hash
    and governance_source.get("append_only_physical_contract_hash")
    == expected_append_physical_hash
    and governance_source.get("metric_review_physical_contract_hash")
    == expected_metric_physical_hash
    and governance_source.get("core_append_only_contract_hash")
    == expected_core_append_hash
    and governance_source.get("core_metric_review_contract_hash")
    == expected_core_metric_hash
    and governance_source.get("funding_schema_contract_hash")
    == expected_funding_contract_hash
    and governance_source.get("observed_count") == 40
    and governance_source.get("required_count") == 40
    and isinstance(governance_source.get("created_count"), int)
    and not isinstance(governance_source.get("created_count"), bool)
    and governance_source.get("created_count") >= 0
    and governance_source.get("created_names")
    == sorted(set(governance_source.get("created_names") or []))
    and len(governance_source.get("created_names") or [])
    == governance_source.get("created_count")
    and governance_names == sorted(set(governance_names or []))
    and len(governance_names or []) == 40
    and governance_names_hash == expected_trigger_names_hash
)
supporting_names = (
    supporting_source.get("expected_names")
    if isinstance(supporting_source, dict) else None
)
supporting_names_hash = (
    hashlib.sha256(
        json.dumps(
            supporting_names, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    if isinstance(supporting_names, list) else ""
)
supporting_source_exact = (
    isinstance(supporting_source, dict)
    and supporting_source.get("source_contract_hash")
    == expected_supporting_source_hash
    and supporting_source.get("owner_counts")
    == expected_supporting_owner_counts
    and supporting_source.get("required_count") == 82
    and supporting_source.get("optional_count") == 0
    and supporting_source.get("observed_count") == 82
    and supporting_source.get("definer")
    == "probiga_migrator@127.0.0.1"
    and supporting_source.get("metadata_frozen") is True
    and supporting_source.get("legacy_rehome_names") == []
    and isinstance(supporting_source.get("created_count"), int)
    and not isinstance(supporting_source.get("created_count"), bool)
    and supporting_source.get("created_count") >= 0
    and supporting_source.get("created_names")
    == sorted(set(supporting_source.get("created_names") or []))
    and len(supporting_source.get("created_names") or [])
    == supporting_source.get("created_count")
    and set(supporting_source.get("created_names") or [])
    <= set(supporting_names or [])
    and supporting_names == sorted(set(supporting_names or []))
    and len(supporting_names or []) == 82
    and supporting_names_hash == expected_supporting_names_hash
)
full_trigger_names = (
    full_trigger_inventory.get("expected_names")
    if isinstance(full_trigger_inventory, dict)
    else None
)
full_trigger_names_hash = (
    hashlib.sha256(
        json.dumps(
            {
                "schema": "probiga.full-release-trigger-names.v1",
                "names": full_trigger_names,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    if isinstance(full_trigger_names, list)
    else ""
)
full_managed_contract = (
    full_trigger_inventory.get("managed_contract")
    if isinstance(full_trigger_inventory, dict)
    else None
)
full_optional_v4_count = (
    full_trigger_inventory.get("optional_v4_count")
    if isinstance(full_trigger_inventory, dict)
    else None
)
expected_full_count = 175 if full_optional_v4_count == 32 else 143
expected_full_nameset_hash = (
    expected_full_with_v4_trigger_nameset_hash
    if full_optional_v4_count == 32
    else expected_full_trigger_nameset_hash
)
full_trigger_inventory_exact = (
    isinstance(full_trigger_inventory, dict)
    and set(full_trigger_inventory) == {
        "expected_count", "observed_count", "v2_count", "managed_count",
        "optional_v4_count", "base_nameset_sha256",
        "expected_names", "nameset_sha256", "v2_source_contract_sha256",
        "managed_source_contract_sha256", "observed_metadata_sha256",
        "managed_contract", "metadata_frozen", "read_only",
    }  # expected_full_inventory_keys
    and type(full_optional_v4_count) is int
    and full_optional_v4_count in {0, 32}
    and full_trigger_inventory.get("expected_count") == expected_full_count
    and full_trigger_inventory.get("observed_count") == expected_full_count
    and full_trigger_inventory.get("v2_count") == 41
    and full_trigger_inventory.get("managed_count") == 102
    and full_trigger_names == sorted(set(full_trigger_names or []))
    and len(full_trigger_names or []) == expected_full_count
    and full_trigger_names_hash == expected_full_nameset_hash
    and full_trigger_inventory.get("nameset_sha256")
    == expected_full_nameset_hash
    and full_trigger_inventory.get("base_nameset_sha256")
    == expected_full_trigger_nameset_hash
    and full_trigger_inventory.get("v2_source_contract_sha256")
    == expected_v2_trigger_source_hash
    and full_trigger_inventory.get("managed_source_contract_sha256")
    == expected_managed_trigger_source_hash
    and re.fullmatch(
        r"[0-9a-f]{64}",
        str(full_trigger_inventory.get("observed_metadata_sha256") or ""),
    ) is not None
    and full_trigger_inventory.get("metadata_frozen") is True
    and full_trigger_inventory.get("read_only") is True
    and isinstance(full_managed_contract, dict)
    and full_managed_contract.get("required_count") == 102
    and full_managed_contract.get("optional_count") == 0
    and full_managed_contract.get("observed_count") == 102
    and full_managed_contract.get("definer")
    == "probiga_migrator@127.0.0.1"
    and full_managed_contract.get("metadata_frozen") is True
    and full_managed_contract.get("legacy_rehome_names") == []
)
pit_schema_exact = (
    isinstance(pit_schema, dict)
    and pit_schema.get("schema") == "probiga.pit-fact-schema-health.v1"
    and pit_schema.get("status") == "HEALTHY"
    and pit_schema.get("valid") is True
    and pit_schema.get("table_count") == 3
    and pit_schema.get("trigger_count") == 6
    and pit_schema.get("missing_tables") == []
    and pit_schema.get("missing_columns") == {}
    and pit_schema.get("missing_triggers") == []
    and pit_schema.get("contract_hash") == expected_pit_contract_hash
)
qmt_reference_exact = (
    isinstance(qmt_reference, dict)
    and qmt_reference.get("contract_key") == "qmt_reference_truth_v2"
    and qmt_reference.get("contract_hash")
    == expected_qmt_reference_contract_hash
    and qmt_reference.get("table_names") == expected_qmt_tables
    and qmt_reference.get("trigger_names") == expected_qmt_triggers
    and qmt_reference.get("table_ddl_count") == 5
    and qmt_reference.get("migration_ddl_count") == 14
    and qmt_reference.get("runtime_ddl_required") is False
    and re.fullmatch(
        r"[0-9a-f]{64}",
        str(qmt_reference.get("table_contract_hash") or ""),
    ) is not None
    and re.fullmatch(
        r"[0-9a-f]{64}",
        str(qmt_reference.get("trigger_contract_hash") or ""),
    ) is not None
)
qmt_coverage_exact = (
    isinstance(qmt_coverage, dict)
    and qmt_coverage.get("database") == "probiga"
    and qmt_coverage.get("table_count") == 2
    and qmt_coverage.get("foreign_key_count") == 3
    and qmt_coverage.get("trigger_count") == 4
    and qmt_coverage.get("runtime_ddl_required") is False
    and qmt_coverage.get("physical_schema_verified") is True
    and qmt_coverage.get("physical_seal_verified") is True
    and isinstance(qmt_coverage_runtime, dict)
    and qmt_coverage_runtime.get("database") == "probiga"
    and qmt_coverage_runtime.get("table_count") == 2
    and qmt_coverage_runtime.get("trigger_count") == 4
    and qmt_coverage_runtime.get("physical_schema_verified") is True
    and qmt_coverage_runtime.get("physical_seal_verified") is True
)
scheduler_history_exact = (
    isinstance(scheduler_history, dict)
    and scheduler_history.get("table") == "st_scheduled_task_history"
    and scheduler_history.get("status") == "ok"
    and scheduler_history.get("required_index_count") == 3
    and scheduler_history.get("physical_contract_verified") is True
    and scheduler_history.get("runtime_ddl_required") is False
    and isinstance(scheduler_history.get("runtime_validation"), dict)
    and scheduler_history["runtime_validation"].get(
        "physical_contract_verified"
    ) is True
    and isinstance(scheduler_history_runtime, dict)
    and scheduler_history_runtime.get("table")
    == "st_scheduled_task_history"
    and scheduler_history_runtime.get("required_index_count") == 3
    and scheduler_history_runtime.get("physical_contract_verified") is True
    and scheduler_history_runtime.get("runtime_ddl_required") is False
    and scheduler_history_runtime.get("read_only") is True
)
runtime_bundle_exact = (
    isinstance(runtime_bundle, dict)
    and runtime_bundle.get("schema")
    == "probiga.production-runtime-schema-bundle.v1"
    and runtime_bundle.get("contract_hash") == expected_runtime_bundle_hash
    and runtime_bundle.get("migration_count") == 30
    and runtime_bundle.get("seed_count") == 3
    and runtime_bundle.get("validator_count") == 33
    and runtime_bundle.get("recovery_planner_count") == 6
    and runtime_bundle.get("recovery_planner_names")
    == expected_recovery_planners
    and runtime_bundle.get("trigger_installation_policy")
    == "FROZEN_RELEASE_BROKER_ONLY"
    and runtime_bundle.get("broker_owned_trigger_migration_names") == [
        "qmt_stock_catalog_truth",
        "qmt_trade_calendar",
        "market_field_capture",
        "auxiliary_runtime",
    ]
    and isinstance(runtime_bundle.get("migration_names"), list)
    and isinstance(runtime_bundle.get("seed_names"), list)
    and isinstance(runtime_bundle.get("validator_names"), list)
    and isinstance(runtime_bundle.get("migrations"), dict)
    and set(runtime_bundle["migrations"])
    == set(runtime_bundle["migration_names"])
    and isinstance(runtime_bundle.get("seeds"), dict)
    and set(runtime_bundle["seeds"]) == set(runtime_bundle["seed_names"])
    and runtime_bundle.get("recovery_plan_count") == 6
    and isinstance(runtime_bundle.get("recovery_plans"), dict)
    and set(runtime_bundle["recovery_plans"])
    == set(expected_recovery_planners)
    and all(
        isinstance(runtime_bundle["recovery_plans"].get(name), dict)
        and runtime_bundle["recovery_plans"][name].get("status") == "PLANNED"
        and runtime_bundle["recovery_plans"][name].get("read_only") is True
        and runtime_bundle["recovery_plans"][name].get(
            "ready_for_privileged_apply"
        ) is True
        and recovery_hashes_exact(runtime_bundle["recovery_plans"][name])
        for name in expected_recovery_planners
    )
    and runtime_bundle.get("recovery_ready_for_privileged_apply") is True
    and runtime_bundle.get("runtime_ddl_required") is False
    and runtime_bundle.get("privileged_migration") is True
    and runtime_bundle.get("trigger_validation_deferred") is False
    and isinstance(runtime_bundle.get("runtime_validation"), dict)
    and runtime_bundle["runtime_validation"].get("contract_hash")
    == expected_runtime_bundle_hash
    and runtime_bundle["runtime_validation"].get(
        "required_surface_verified"
    ) is True
    and runtime_bundle["runtime_validation"].get("read_only") is True
    and runtime_bundle["runtime_validation"].get("recovery_planner_count") == 6
    and runtime_bundle["runtime_validation"].get("recovery_planner_names")
    == expected_recovery_planners
    and isinstance(runtime_bundle["runtime_validation"].get("contracts"), dict)
    and set(runtime_bundle["runtime_validation"]["contracts"])
    == set(runtime_bundle["validator_names"])
    and isinstance(runtime_bundle_runtime, dict)
    and runtime_bundle_runtime.get("contract_hash")
    == expected_runtime_bundle_hash
    and runtime_bundle_runtime.get("required_surface_verified") is True
    and runtime_bundle_runtime.get("read_only") is True
    and runtime_bundle_runtime.get("recovery_planner_count") == 6
    and runtime_bundle_runtime.get("recovery_planner_names")
    == expected_recovery_planners
    and isinstance(runtime_bundle_runtime.get("contracts"), dict)
    and set(runtime_bundle_runtime["contracts"])
    == set(runtime_bundle_runtime.get("validator_names") or [])
)
allowed = {
    "trg_trade_account_v2_real_disabled_bi",
    "trg_trade_account_v2_real_disabled_bu",
}  # allowed
ok = (
    isinstance(p, dict)
    and p.get("status") == "ok"
    and p.get("phase") == "resume"
    and permission_audit_skipped
    and legacy
    and funding_exact
    and governance_source_exact
    and supporting_source_exact
    and full_trigger_inventory_exact
    and pit_schema_exact
    and qmt_reference_exact
    and qmt_coverage_exact
    and scheduler_history_exact
    and runtime_bundle_exact
    and p.get("trust_restoration_verified") is True
    and p.get("restore_primary_verified") is True
    and p.get("restore_secondary_verified") is True
    and p.get("runtime_trust_off_verified") is True
    and isinstance(repair, dict)
    and repair.get("post_validation_verified") is True
    and isinstance(candidates, list)
    and candidates == sorted(set(candidates))
    and set(candidates) <= allowed
    and repaired == candidates
    and isinstance(windows, list)
    and all(isinstance(x, str) for x in windows)
    and windows == list(dict.fromkeys(windows))
    and p.get("trigger_trust_window_count") == len(windows)
    and p.get("global_trust_changed") is bool(windows)
    and isinstance(migrations, list)
    and bool(migrations)
    and all(
        isinstance(x, dict) and x.get("status") in {"applied", "exists"}
        for x in migrations
    )
    and isinstance(trigger, dict)
    and trigger.get("metadata_frozen") is True
    and trigger.get("legacy_rehome_names") == []
    and trigger.get("definer") == "probiga_migrator@127.0.0.1"
    and trigger.get("required_count") == 102
    and trigger.get("optional_count") == 0
    and trigger.get("observed_count") == 102
    and isinstance(p.get("seeded_strategy_count"), int)
    and p["seeded_strategy_count"] > 0
    and p.get("automatic_real_order_submission") is False
)
raise SystemExit(0 if ok else 2)
'
}
controlled_guard_validate_preflight_json() {
  local python_bin="$1"
  "$python_bin" -I -c \
    '
import hashlib
import json
import re
import sys

p = json.load(sys.stdin)
migrations = p.get("v3_migrations") if isinstance(p, dict) else None
trigger = p.get("trigger_contract") if isinstance(p, dict) else None
permission_summary = (
    p.get("runtime_grant_summary") if isinstance(p, dict) else None
)
binding = p.get("legacy_binding_plan") if isinstance(p, dict) else None
governance_source = (
    p.get("governance_trigger_source_contract")
    if isinstance(p, dict) else None
)
supporting_source = (
    p.get("supporting_trigger_source_contract")
    if isinstance(p, dict) else None
)
pit_schema = p.get("pit_fact_schema") if isinstance(p, dict) else None
qmt_reference = (
    p.get("qmt_reference_schema") if isinstance(p, dict) else None
)
qmt_coverage = (
    p.get("qmt_history_coverage_schema") if isinstance(p, dict) else None
)
scheduler_history = (
    p.get("scheduler_task_history_schema") if isinstance(p, dict) else None
)
runtime_bundle = (
    p.get("runtime_schema_bundle") if isinstance(p, dict) else None
)
expected_runtime_bundle_hash = (
    "61f9ddfb3179f30c9976a090fce00adb8613d4e38d698c6cfc954f957084845f"
)
expected_recovery_planners = [
    "ai_bridge",
    "analysis_output",
    "recommended_run_history",
    "sim_trade",
    "qmt_catalog",
    "qmt_audit",
]

def recovery_hashes_exact(plan):
    def valid_hash(value):
        return (
            isinstance(value, str)
            and re.fullmatch(r"[0-9a-f]{64}", value) is not None
        )
    if not isinstance(plan, dict) or not valid_hash(plan.get("plan_sha256")):
        return False
    canonical = plan["plan_sha256"]
    recovery_bundle = plan.get("recovery_bundle_sha256")
    atomic = plan.get("atomic_plan_sha256")
    if recovery_bundle is not None and (
        not valid_hash(recovery_bundle) or recovery_bundle != canonical
    ):
        return False
    if atomic is not None and (
        not valid_hash(atomic)
        or (recovery_bundle is None and atomic != canonical)
    ):
        return False
    return True

expected_funding_contract_hash = (
    "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde"
)
expected_trigger_source_hash = (
    "5a1a19e0664c715ae0cac7cfa8dd87c47da1b63b1d2df869561cecf3c995f01f"
)
expected_trigger_names_hash = (
    "a2f74c8b1d4fa984e2d6aadb6169e13e8d041a1f414f2523aeb5835dc4376e13"
)
expected_supporting_source_hash = (
    "7c261eaff759e562b883d19880ef345c6733cacf911218437adc72ba864934e2"
)
expected_supporting_names_hash = (
    "f26aa672a479a6dfbfba6861d0f86d675aba4494839bc218c64197ec7eceabe7"
)
expected_supporting_owner_counts = {
    "market_field_capture": 5,
    "pit_facts": 6,
    "qmt_attestation": 6,
    "qmt_history_coverage": 4,
    "qmt_membership": 6,
    "qmt_reference": 10,
    "scheduler_task_history": 3,
    "schema_recovery_evidence": 2,
    "strategy_governance": 40,
}  # expected_supporting_owner_counts
expected_pit_contract_hash = (
    "c374e0ba62eb2e5b9bef802ce2bdd89fae0c63391d918e922ff21781707863ae"
)
expected_qmt_reference_contract_hash = (
    "64982c16c517f7e5c0e6ee9b88b1bf33df98f9aebf66440eedc916eae76f3dd5"
)
expected_qmt_tables = [
    "qmt_stock_catalog_batch",
    "qmt_stock_catalog_member",
    "qmt_trade_calendar_batch",
    "qmt_trade_calendar_session",
    "qmt_reference_schema_contract",
]
expected_qmt_triggers = [
    "trg_qmt_calendar_batch_no_delete",
    "trg_qmt_calendar_batch_no_update",
    "trg_qmt_calendar_session_no_delete",
    "trg_qmt_calendar_session_no_update",
    "trg_qmt_reference_contract_no_delete",
    "trg_qmt_reference_contract_no_update",
    "trg_qmt_stock_catalog_batch_no_delete",
    "trg_qmt_stock_catalog_batch_no_update",
    "trg_qmt_stock_catalog_member_no_delete",
    "trg_qmt_stock_catalog_member_no_update",
]
expected_append_physical_hash = (
    "bf537f9ed5fb1d31195092ae6a24262511de6f45bf9addacefebc88e25b6b9d8"
)
expected_metric_physical_hash = (
    "c217a42eb6c2a5f7bed592bb7c7e724499546f997061c4daad1db957317bdf28"
)
expected_core_append_hash = (
    "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943"
)
expected_core_metric_hash = (
    "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84"
)
legacy = (
    isinstance(binding, dict)
    and isinstance(binding.get("legacy_run_count"), int)
    and binding["legacy_run_count"] >= 0
    and isinstance(binding.get("legacy_binding_plan_hash"), str)
    and bool(binding["legacy_binding_plan_hash"])
    and binding.get("legacy_binding_pending") is False
    and binding.get("legacy_binding_marker_present")
    is bool(binding["legacy_run_count"])
)
permission_audit_skipped = (
    p.get("permission_audit_status") == "SKIPPED_BY_USER_AUTHORIZATION"
    and p.get("permission_audit_verified") is False
    and p.get("runtime_privilege_boundary_verified") is False
    and p.get("runtime_least_privilege_verified") is False
    and p.get("runtime_legacy_ddl_compatibility") is False
    and p.get("runtime_current_user") == "probiga_runtime@127.0.0.1"
    and p.get("runtime_session_user") == "probiga_runtime@127.0.0.1"
    and p.get("runtime_tls_verified") is True
    and p.get("runtime_grant_count") is None
    and p.get("runtime_grant_contract_hash") == ""
    and isinstance(permission_summary, dict)
    and set(permission_summary) == {
        "permission_audit_status", "permission_audit_verified",
        "runtime_grant_count", "runtime_grant_contract_hash",
    }  # permission_summary_keys
    and permission_summary.get("permission_audit_status")
    == p.get("permission_audit_status")
    and permission_summary.get("permission_audit_verified")
    is p.get("permission_audit_verified")
    and permission_summary.get("runtime_grant_count")
    is p.get("runtime_grant_count")
    and permission_summary.get("runtime_grant_contract_hash")
    == p.get("runtime_grant_contract_hash")
    and p.get("routine_inventory_audit_status")
    == "SKIPPED_BY_USER_AUTHORIZATION"
    and p.get("runtime_self_definer_routine_count") is None
    and p.get("migrator_self_definer_routine_count") is None
    and p.get("runtime_definer_routine_count") is None
    and p.get("runtime_definer_routine_inventory_verified") is False
    and p.get("runtime_definer_routine_inventory_complete") is False
    and p.get("runtime_definer_routine_inventory_authority") == ""
    and p.get("runtime_definer_routine_inventory_schemas") == []
)
governance_names = (
    governance_source.get("trigger_names")
    if isinstance(governance_source, dict) else None
)
governance_names_hash = (
    hashlib.sha256(
        json.dumps(
            governance_names, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    if isinstance(governance_names, list) else ""
)
governance_source_exact = (
    isinstance(governance_source, dict)
    and governance_source.get("trigger_count") == 40
    and governance_source.get("source_contract_hash")
    == expected_trigger_source_hash
    and governance_source.get("append_only_physical_contract_hash")
    == expected_append_physical_hash
    and governance_source.get("metric_review_physical_contract_hash")
    == expected_metric_physical_hash
    and governance_source.get("core_append_only_contract_hash")
    == expected_core_append_hash
    and governance_source.get("core_metric_review_contract_hash")
    == expected_core_metric_hash
    and governance_source.get("funding_schema_contract_hash")
    == expected_funding_contract_hash
    and governance_names == sorted(set(governance_names or []))
    and len(governance_names or []) == 40
    and governance_names_hash == expected_trigger_names_hash
)
supporting_names = (
    supporting_source.get("trigger_names")
    if isinstance(supporting_source, dict) else None
)
supporting_names_hash = (
    hashlib.sha256(
        json.dumps(
            supporting_names, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    if isinstance(supporting_names, list) else ""
)
supporting_source_exact = (
    isinstance(supporting_source, dict)
    and supporting_source.get("trigger_count") == 82
    and supporting_source.get("source_contract_hash")
    == expected_supporting_source_hash
    and supporting_source.get("owner_counts")
    == expected_supporting_owner_counts
    and supporting_names == sorted(set(supporting_names or []))
    and len(supporting_names or []) == 82
    and supporting_names_hash == expected_supporting_names_hash
)
pit_schema_exact = (
    isinstance(pit_schema, dict)
    and pit_schema.get("schema") == "probiga.pit-fact-schema-health.v1"
    and pit_schema.get("status") == "HEALTHY"
    and pit_schema.get("valid") is True
    and pit_schema.get("table_count") == 3
    and pit_schema.get("trigger_count") == 6
    and pit_schema.get("missing_tables") == []
    and pit_schema.get("missing_columns") == {}
    and pit_schema.get("missing_triggers") == []
    and pit_schema.get("contract_hash") == expected_pit_contract_hash
)
qmt_reference_exact = (
    isinstance(qmt_reference, dict)
    and qmt_reference.get("status")
    == "READY_FOR_PHYSICAL_ATTESTATION"
    and qmt_reference.get("read_only") is True
    and qmt_reference.get("contract_key") == "qmt_reference_truth_v2"
    and qmt_reference.get("contract_hash")
    == expected_qmt_reference_contract_hash
    and qmt_reference.get("table_names") == expected_qmt_tables
    and qmt_reference.get("trigger_names") == expected_qmt_triggers
    and qmt_reference.get("present_tables") == expected_qmt_tables
    and qmt_reference.get("absent_tables") == []
    and qmt_reference.get("missing_columns") == {}
    and qmt_reference.get("table_ddl_count") == 5
    and qmt_reference.get("migration_ddl_count") == 14
    and qmt_reference.get("trigger_ddl_count") == 10
)
qmt_coverage_exact = (
    isinstance(qmt_coverage, dict)
    and qmt_coverage.get("status")
    in {"EMPTY", "READY_FOR_TRIGGER_CUTOVER"}
    and qmt_coverage.get("database") == "probiga"
    and qmt_coverage.get("table_names") == [
        "qmt_history_coverage_manifest",
        "qmt_history_coverage_entity",
    ]
    and qmt_coverage.get("trigger_names") == [
        "trg_qmt_history_coverage_no_update",
        "trg_qmt_history_coverage_no_delete",
        "trg_qmt_history_coverage_entity_no_update",
        "trg_qmt_history_coverage_entity_no_delete",
    ]
    and qmt_coverage.get("runtime_ddl_required") is False
    and qmt_coverage.get("read_only") is True
    and (
        (
            qmt_coverage.get("status") == "EMPTY"
            and qmt_coverage.get("table_count") == 0
            and qmt_coverage.get("physical_schema_verified") is False
        )
        or (
            qmt_coverage.get("status") == "READY_FOR_TRIGGER_CUTOVER"
            and qmt_coverage.get("table_count") == 2
            and qmt_coverage.get("foreign_key_count") == 3
            and qmt_coverage.get("physical_schema_verified") is True
        )
    )
)
scheduler_history_exact = (
    isinstance(scheduler_history, dict)
    and scheduler_history.get("table") == "st_scheduled_task_history"
    and scheduler_history.get("status") in {"READY", "MIGRATION_REQUIRED"}
    and scheduler_history.get("runtime_ddl_required") is False
    and scheduler_history.get("read_only") is True
    and (
        scheduler_history.get("status") == "MIGRATION_REQUIRED"
        or (
            scheduler_history.get("required_index_count") == 3
            and scheduler_history.get("physical_contract_verified") is True
        )
    )
)
runtime_bundle_exact = (
    isinstance(runtime_bundle, dict)
    and runtime_bundle.get("schema")
    == "probiga.production-runtime-schema-bundle.v1"
    and runtime_bundle.get("contract_hash") == expected_runtime_bundle_hash
    and runtime_bundle.get("migration_count") == 30
    and runtime_bundle.get("seed_count") == 3
    and runtime_bundle.get("validator_count") == 33
    and runtime_bundle.get("recovery_planner_count") == 6
    and runtime_bundle.get("recovery_planner_names")
    == expected_recovery_planners
    and runtime_bundle.get("trigger_installation_policy")
    == "FROZEN_RELEASE_BROKER_ONLY"
    and runtime_bundle.get("broker_owned_trigger_migration_names") == [
        "qmt_stock_catalog_truth",
        "qmt_trade_calendar",
        "market_field_capture",
        "auxiliary_runtime",
    ]
    and isinstance(runtime_bundle.get("validator_names"), list)
    and isinstance(runtime_bundle.get("contracts"), dict)
    and set(runtime_bundle["contracts"])
    == set(runtime_bundle["validator_names"])
    and runtime_bundle.get("recovery_plan_count") == 6
    and isinstance(runtime_bundle.get("recovery_plans"), dict)
    and set(runtime_bundle["recovery_plans"])
    == set(expected_recovery_planners)
    and all(
        isinstance(runtime_bundle["recovery_plans"].get(name), dict)
        and runtime_bundle["recovery_plans"][name].get("status") == "PLANNED"
        and runtime_bundle["recovery_plans"][name].get("read_only") is True
        and runtime_bundle["recovery_plans"][name].get(
            "ready_for_privileged_apply"
        ) is True
        and recovery_hashes_exact(runtime_bundle["recovery_plans"][name])
        for name in expected_recovery_planners
    )
    and runtime_bundle.get("recovery_ready_for_privileged_apply") is True
    and all(
        isinstance(item, dict)
        and item.get("status") in {"READY", "MIGRATION_REQUIRED"}
        and item.get("read_only") is True
        for item in runtime_bundle["contracts"].values()
    )
    and runtime_bundle.get("migration_required")
    is (
        any(
            item.get("status") != "READY"
            for item in runtime_bundle["contracts"].values()
        )
        or not runtime_bundle.get("recovery_ready_for_privileged_apply")
    )
    and runtime_bundle.get("read_only") is True
)
ok = (
    isinstance(p, dict)
    and p.get("status") == "ok"
    and p.get("phase") == "preflight"
    and permission_audit_skipped
    and legacy
    and governance_source_exact
    and supporting_source_exact
    and pit_schema_exact
    and qmt_reference_exact
    and qmt_coverage_exact
    and scheduler_history_exact
    and runtime_bundle_exact
    and p.get("global_trust_changed") is False
    and p.get("trust_restoration_verified") is True
    and p.get("pending_v3_versions") == []
    and isinstance(migrations, list)
    and bool(migrations)
    and all(
        isinstance(x, dict) and x.get("status") == "exists"
        for x in migrations
    )
    and isinstance(trigger, dict)
    and trigger.get("metadata_frozen") is True
    and trigger.get("legacy_rehome_names") == []
    and trigger.get("definer") == "probiga_migrator@127.0.0.1"
    and trigger.get("required_count") == 20
    and trigger.get("optional_count") == 82
    and trigger.get("observed_count") in {50, 102}
    and p.get("qmt_table_count") == 4
    and p.get("governance_table_count") == 15
    and p.get("automatic_real_order_submission") is False
)
raise SystemExit(0 if ok else 2)
'
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
  controlled_guard_assert_recovery_code_tree_clean \
    "$code_root" "$guarded_sha"
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
  controlled_guard_assert_immutable_venv_tree "$release_venv_target" || return 1
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
  local qmt_activation_output
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
    controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
      "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
    controlled_guard_write_restore_file "$guarded_sha" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record" || \
      return 1
    controlled_v2_assert_preserved_no_receipt_transaction "$guarded_sha" || \
      return 1
    echo "v2 recovery retained sealed runtime evidence pending authenticated deploy" \
      >&2
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
  controlled_guard_governance_contract_snapshot restore "$guarded_sha" \
    "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
  controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
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
  activation_snapshot_set_phase "$guarded_sha" finalized || return 1
  activation_snapshot_assert_new_set "$guarded_sha" || return 1
  publish_deployed_receipt_pending "$guarded_sha" || return 1
  qmt_activation_output="$(controlled_guard_run_qmt_activation_tool \
    "$CODE_RELEASE_ROOT/$guarded_sha" "$RELEASE_VENV_ROOT/$guarded_sha" \
    "$guarded_sha" --activation-grant-latest)" || return 1
  printf '%s' "$qmt_activation_output" | \
    controlled_guard_validate_qmt_activation_json \
      "$RELEASE_VENV_ROOT/$guarded_sha/bin/python" "$guarded_sha" \
      activation-grant-latest || return 1
  if [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
    rm -f -- "$DATABASE_WRITER_RESTORE_FILE" || return 1
    sync -f "$DATABASE_WRITER_GUARD_DIR" || return 1
  fi
  activation_snapshot_remove_finalized_before_deploy || return 1
  return 0
}
explicit_v2_recovery_failure() {
  local failed_status="$1"
  trap - ERR
  printf 'v2 recovery failed step=%s\n' \
    "${V2_RECOVERY_STEP:-unknown}" >&2 || true
  exit "$failed_status"
}
if [ "$DEPLOY_OPERATION" = recover-database-guard ]; then
  V2_RECOVERY_STEP=dispatch
  trap 'explicit_v2_recovery_failure "$?"' ERR
  materialize_controlled_governance_contract_tool \
    "$PROBIGA_RECOVERY_TOOL_SHA"
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
  trap - ERR
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
# Keep one sanitized diagnostic descriptor attached to the authenticated caller.
# The rollback handler deliberately detaches stdout/stderr so a closed SSH
# transport cannot interrupt recovery.  Descriptor 6 is used only for the
# bounded failure checkpoint below; no command output, SQL, or credentials are
# ever copied to it.
exec 6>&2
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
persist_deploy_failure_audit() {
  local audit_sha
  local audit_target
  local audit_tmp
  local cutover_started=false
  local failed_line="$3"
  local failed_status="$4"
  local phase="$1"
  local recorded_at
  local step="$2"
  case "$phase" in preflight|preparation|cutover) ;; *) return 1 ;; esac
  [[ "$step" =~ ^[a-z0-9][a-z0-9_]*$ ]] || step=unknown
  [[ "$failed_line" =~ ^[0-9]+$ ]] || return 1
  [[ "$failed_status" =~ ^[0-9]+$ ]] || return 1
  test "$failed_status" -le 255 || return 1
  if [ "$phase" = cutover ]; then
    cutover_started=true
  fi
  recorded_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)" || return 1
  test ! -L "$DEPLOY_FAILURE_AUDIT_DIR" || return 1
  install -d -o root -g root -m 0700 "$DEPLOY_FAILURE_AUDIT_DIR" || return 1
  test "$(readlink -f "$DEPLOY_FAILURE_AUDIT_DIR")" = \
    "$DEPLOY_FAILURE_AUDIT_DIR" || return 1
  test "$(stat -c '%U:%G' "$DEPLOY_FAILURE_AUDIT_DIR")" = root:root || return 1
  test "$(stat -c '%a' "$DEPLOY_FAILURE_AUDIT_DIR")" = 700 || return 1
  audit_tmp="$(mktemp "$DEPLOY_FAILURE_AUDIT_DIR/.failure.XXXXXX")" || return 1
  if ! printf '{"schema_version":"probiga.production-deploy-failure-audit.v1","phase":"%s","cutover_step":"%s","cutover_started":%s,"line":%s,"status":%s,"expected_sha":"%s","previous_sha":"%s","started_at":"%s","recorded_at":"%s"}\n' \
      "$phase" "$step" "$cutover_started" "$failed_line" "$failed_status" \
      "$EXPECTED_SHA" "$PREVIOUS_SHA" "$DEPLOY_STARTED_AT" "$recorded_at" \
      > "$audit_tmp" || \
    ! chown root:root "$audit_tmp" || ! chmod 0444 "$audit_tmp" || \
    ! sync -f "$audit_tmp"; then
    rm -f -- "$audit_tmp"
    return 1
  fi
  if ! audit_sha="$(sha256sum "$audit_tmp" | cut -d' ' -f1)"; then
    rm -f -- "$audit_tmp"
    return 1
  fi
  if [[ ! "$audit_sha" =~ ^[0-9a-f]{64}$ ]]; then
    rm -f -- "$audit_tmp"
    return 1
  fi
  audit_target="$DEPLOY_FAILURE_AUDIT_DIR/$RECEIPT_ID-failure-$audit_sha.json"
  if [ -e "$audit_target" ] || [ -L "$audit_target" ]; then
    if ! test -f "$audit_target" || test -L "$audit_target" || \
      ! cmp --silent "$audit_tmp" "$audit_target"; then
        rm -f -- "$audit_tmp"
        return 1
    fi
    rm -f -- "$audit_tmp" || return 1
  else
    if ! mv -fT "$audit_tmp" "$audit_target"; then
      rm -f -- "$audit_tmp"
      return 1
    fi
  fi
  test "$(stat -c '%U:%G' "$audit_target")" = root:root || return 1
  test "$(stat -c '%a' "$audit_target")" = 444 || return 1
  test "$(sha256sum "$audit_target" | cut -d' ' -f1)" = "$audit_sha" || return 1
  sync -f "$DEPLOY_FAILURE_AUDIT_DIR" || return 1
  printf '%s\n' "$audit_sha"
}
emit_deploy_failure_checkpoint() {
  local audit_sha="${5:-unavailable}"
  local expected_sha="${EXPECTED_SHA:-unavailable}"
  local failed_line="$3"
  local failed_status="$4"
  local phase="$1"
  local previous_sha="${PREVIOUS_SHA:-unavailable}"
  local step="$2"
  case "$phase" in preflight|preparation|cutover) ;; *) phase=unknown ;; esac
  [[ "$step" =~ ^[a-z0-9][a-z0-9_]*$ ]] || step=unknown
  [[ "$failed_line" =~ ^[0-9]+$ ]] || failed_line=0
  if [[ ! "$failed_status" =~ ^[0-9]+$ ]] || \
    [ "$failed_status" -gt 255 ]; then
    failed_status=255
  fi
  [[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || expected_sha=unavailable
  [[ "$previous_sha" =~ ^[0-9a-f]{40}$ ]] || previous_sha=unavailable
  [[ "$audit_sha" =~ ^[0-9a-f]{64}$ ]] || audit_sha=unavailable
  printf 'deploy_failure_checkpoint schema=probiga.production-deploy-failure-audit.v1 phase=%s cutover_step=%s line=%s status=%s expected_sha=%s previous_sha=%s audit_sha256=%s\n' \
    "$phase" "$step" "$failed_line" "$failed_status" "$expected_sha" \
    "$previous_sha" "$audit_sha" \
    >&6 || true
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
  local failure_audit_sha=""
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
  failure_audit_sha="$(persist_deploy_failure_audit preflight \
    "${CUTOVER_STEP:-unknown}" "$failed_line" "$failed_status" 2>/dev/null)" || \
    failure_audit_sha=unavailable
  emit_deploy_failure_checkpoint preflight "${CUTOVER_STEP:-unknown}" \
    "$failed_line" "$failed_status" "$failure_audit_sha"
  printf 'deploy_failure phase=preflight line=%s status=%s\n' \
    "$failed_line" "$failed_status" >&2
  write_receipt "PREFLIGHT_FAILED" "$PREVIOUS_SHA" || true
  exit "$failed_status"
}
v2_recovery_failure() {
  local failed_status="$1"
  local failed_line="$2"
  local recovery_step="${V2_RECOVERY_STEP:-unknown}"
  local audit_recovery_step=unknown
  if [[ "$recovery_step" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
    audit_recovery_step="$recovery_step"
    CUTOVER_STEP="v2_${recovery_step//-/_}"
  else
    CUTOVER_STEP=v2_unknown
  fi
  printf 'v2 recovery failed step=%s\n' "$audit_recovery_step" >&7 || true
  precutover_failure "$failed_status" "$failed_line"
}
trap 'precutover_failure "$?" "$LINENO"' ERR
trap 'precutover_failure 143 "$LINENO"' TERM
trap 'precutover_failure 130 "$LINENO"' INT
trap 'precutover_failure 129 "$LINENO"' HUP
MAIN_SERVICE=probiga
STRATEGY_GOVERNANCE_MODE=REQUIRED
if [ ! -e /etc/probiga/mysql-trigger-admin.ini ] && \
  [ ! -L /etc/probiga/mysql-trigger-admin.ini ] && \
  [ ! -e /etc/probiga/mysql-migrator.ini ] && \
  [ ! -L /etc/probiga/mysql-migrator.ini ] && \
  [ ! -e /home/probiga-deploy/.probiga-db-boundary-stage ] && \
  [ ! -L /home/probiga-deploy/.probiga-db-boundary-stage ]; then
  # The ordinary application account can install additive governance tables,
  # columns and indexes, but it cannot create the reviewed trigger boundary.
  # Keep the release usable in an explicit fail-closed mode until the fixed
  # database identities are provisioned; never represent this as READY.
  STRATEGY_GOVERNANCE_MODE=DEFERRED_DB
fi
readonly STRATEGY_GOVERNANCE_MODE
SERVICE_USER="$(systemctl show -p User --value "$MAIN_SERVICE")"
test -n "$SERVICE_USER"
test "$SERVICE_USER" != root
prepare_qmt_announcement_checkpoint_root
prepare_qmt_full_market_history_state_root
prepare_qmt_local_gap_repair_state_root
migrate_legacy_flow_progress inspect
prepare_probiga_job_log_root
materialize_controlled_governance_contract_tool "$EXPECTED_SHA"
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
if { [ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ] || \
    [ "$DEPLOY_ARTIFACT_MODE" = static-wheel-lock-v2 ]; } && \
  { [ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ] || \
    [ -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" ]; } && \
  { [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
    [ -L "$DATABASE_WRITER_RESTORE_FILE" ] || \
      { [ -f "$ACTIVATION_UNIT_SNAPSHOT_PHASE" ] && \
      [ ! -L "$ACTIVATION_UNIT_SNAPSHOT_PHASE" ] && \
      { [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = prepared ] || \
        [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = \
          runtime-units-installing ] || \
        [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = \
          runtime-units-installed ] || \
        [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = restoring-old ] || \
        [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = old-set-restored ] || \
        [ "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = old-runtime-verified ] || \
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
PREVIOUS_MAIN_OBSERVED_ACTIVE_STATE="$(systemctl show \
  -p ActiveState --value "$MAIN_SERVICE")"
PREVIOUS_MAIN_ACTIVE_STATE="$PREVIOUS_MAIN_OBSERVED_ACTIVE_STATE"
PREVIOUS_MAIN_UNIT_FILE_STATE="$(systemctl show \
  -p UnitFileState --value "$MAIN_SERVICE")"
PREVIOUS_MAIN_WAS_STOPPED=0
PREVIOUS_MAIN_NEEDS_FAILED_RESET=0
case "$PREVIOUS_MAIN_ACTIVE_STATE" in
  active) ;;
  inactive) PREVIOUS_MAIN_WAS_STOPPED=1 ;;
  failed)
    # A failed unit with no process is a stopped runtime.  Normalize the
    # rollback contract to inactive after its immutable identity is proven;
    # never revive it merely to inspect /proc or an HTTP endpoint.
    PREVIOUS_MAIN_WAS_STOPPED=1
    PREVIOUS_MAIN_NEEDS_FAILED_RESET=1
    PREVIOUS_MAIN_ACTIVE_STATE=inactive
    ;;
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
  if [ "$V2_FORWARD_PRESERVED_NO_RECEIPT_SHA" = "$EXPECTED_SHA" ]; then
    controlled_v2_assert_preserved_no_receipt_transaction "$EXPECTED_SHA"
  else
    if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
      [ -L "$DATABASE_WRITER_GUARD_FILE" ] || \
      [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
      [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
      echo "persistent activation transaction requires controlled recovery" >&2
      false
    fi
    activation_snapshot_remove_finalized_before_deploy
  fi
fi
if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
  [ -L "$DATABASE_WRITER_GUARD_FILE" ] || \
  [ -e "$DATABASE_WRITER_RESTORE_FILE" ] || \
  [ -L "$DATABASE_WRITER_RESTORE_FILE" ]; then
  if [ "$V2_FORWARD_PRESERVED_NO_RECEIPT_SHA" = "$EXPECTED_SHA" ]; then
    controlled_v2_assert_preserved_no_receipt_transaction "$EXPECTED_SHA"
  else
    echo "persistent database writer guard/restore state requires controlled recovery" >&2
    false
  fi
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
PREBUILD_MIN_AVAILABLE_BYTES=2147483648

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

remove_retired_qmt_server_project() {
  local retired_path
  local retired_real
  local unit
  for unit in qmt-agent.service qmt-agent-scheduler.service; do
    systemctl disable --now "$unit" 2>/dev/null || true
    rm -f -- "/etc/systemd/system/$unit" || return 2
  done
  systemctl daemon-reload || return 2
  for retired_path in /opt/qmt-agent /opt/qmt-agent-data; do
    if [ ! -e "$retired_path" ] && [ ! -L "$retired_path" ]; then
      continue
    fi
    test -d "$retired_path" || return 2
    test ! -L "$retired_path" || return 2
    retired_real="$(readlink -f -- "$retired_path")" || return 2
    test "$retired_real" = "$retired_path" || return 2
    case "$retired_real" in
      /opt/qmt-agent|/opt/qmt-agent-data) ;;
      *) echo "refusing retired QMT path outside exact allowlist" >&2; return 2 ;;
    esac
    rm -rf -- "$retired_real" || return 2
    test ! -e "$retired_real" || return 2
    test ! -L "$retired_real" || return 2
    echo "Removed retired QMT server project path: $retired_real" >&2
  done
}

prebuild_reclaim_release_space() {
  local available_bytes
  local build_temp_probe
  local previous_code_name
  local protected_venv
  local journal_path
  local legacy_active_runtime=0
  local space_path

  # Recovery runs before this point.  If any durable activation state remains,
  # do not infer its references and do not delete a byte: the bounded recovery
  # path must resolve that state first.
  for journal_path in \
    "$ACTIVATION_UNIT_SNAPSHOT_DIR" \
    "$DATABASE_WRITER_GUARD_FILE" \
    "$DATABASE_WRITER_RESTORE_FILE"; do
    if [ -e "$journal_path" ] || [ -L "$journal_path" ]; then
      echo "refusing prebuild cleanup while activation journal exists: $journal_path" >&2
      return 2
    fi
  done

  remove_retired_qmt_server_project || return 2

  # Failed same-day releases can leave bounded tar/bundle artifacts in /tmp.
  # Reclaim only ProBigA-owned names that have been idle for at least ten
  # minutes before enforcing the build-space floor.
  prune_release_temp_files || return 2

  [[ "$PREVIOUS_RELEASE_REVISION" =~ ^[0-9a-f]{40}$ ]] || return 2
  [[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || return 2
  protected_venv="$RELEASE_VENV_ROOT/$PREVIOUS_RELEASE_REVISION"
  case "$PREVIOUS_VENV" in
    "$protected_venv")
      # Preserve the currently running release and the normal second rollback
      # generation selected by prune_release_venvs.  The incoming SHA is only
      # protected when its fully published SHA link already exists; orphaned
      # build-$EXPECTED_SHA-* trees are incomplete and are safe to reclaim.
      prune_release_venvs "$PREVIOUS_RELEASE_REVISION" "$EXPECTED_SHA" || \
        return 2
      ;;
    "$LEGACY_RELEASE_VENV_ROOT/$PREVIOUS_RELEASE_REVISION")
      # The active legacy runtime lives outside this cleanup root.  Without an
      # in-root protected link, retention ordering cannot prove a safe rollback
      # set, so leave every external release venv untouched and fail later if
      # the free-space floor is not already satisfied.
      echo "Skipped prebuild release venv cleanup for legacy active runtime" >&2
      legacy_active_runtime=1
      ;;
    *)
      echo "refusing prebuild cleanup for an unknown active venv path" >&2
      return 2
      ;;
  esac

  if [ "$legacy_active_runtime" -eq 1 ]; then
    echo "Skipped prebuild code release cleanup for legacy active runtime" >&2
  else
    previous_code_name="${PREVIOUS_CODE_ROOT#"$CODE_RELEASE_ROOT"/}"
    if [ "$PREVIOUS_CODE_ROOT" = "$CODE_RELEASE_ROOT/$previous_code_name" ] && \
      [[ "$previous_code_name" =~ ^[0-9a-f]{40}$ ]]; then
      prune_code_releases "$PREVIOUS_CODE_ROOT" "$REPOSITORY_ROOT" || return 2
    elif [ "$PREVIOUS_CODE_ROOT" = "$REPOSITORY_ROOT" ]; then
      echo "Skipped prebuild code release cleanup for legacy active runtime" >&2
    else
      echo "refusing prebuild code cleanup for an unknown active code root" >&2
      return 2
    fi
  fi

  test ! -L "$RELEASE_ARTIFACT_ROOT" || return 2
  install -d -o root -g root -m 0755 "$RELEASE_ARTIFACT_ROOT" || return 2
  test "$(readlink -f -- "$RELEASE_ARTIFACT_ROOT")" = \
    "$RELEASE_ARTIFACT_ROOT" || return 2
  # `df` Available is f_bavail, the capacity available to the non-root build
  # identity.  f_bfree/root write probes include ext4 reserved blocks and would
  # miss the exact pip-wheel failure this gate prevents.
  for space_path in \
    /tmp \
    /var/tmp \
    "$RELEASE_VENV_ROOT" \
    "$RELEASE_ARTIFACT_ROOT" \
    "$CODE_RELEASE_ROOT"; do
    test -d "$space_path" || return 2
    available_bytes="$(df -P -B1 -- "$space_path" | \
      awk 'NR == 2 {print $4}')" || return 2
    [[ "$available_bytes" =~ ^[0-9]+$ ]] || return 2
    if [ "$available_bytes" -lt "$PREBUILD_MIN_AVAILABLE_BYTES" ]; then
      echo "insufficient prebuild disk space: path=$space_path available=$available_bytes required=$PREBUILD_MIN_AVAILABLE_BYTES" >&2
      return 2
    fi
    echo "Prebuild disk space verified: path=$space_path available=$available_bytes required=$PREBUILD_MIN_AVAILABLE_BYTES" >&2
  done
  build_temp_probe="$(sudo -u "$BUILD_USER" \
    mktemp -d /tmp/.probiga-prebuild.XXXXXX)" || return 2
  case "$build_temp_probe" in
    /tmp/.probiga-prebuild.*) ;;
    *) echo "build-user temp probe escaped /tmp" >&2; return 2 ;;
  esac
  test -d "$build_temp_probe" || return 2
  test ! -L "$build_temp_probe" || return 2
  test "$(dirname -- "$build_temp_probe")" = /tmp || return 2
  sudo -u "$BUILD_USER" rmdir -- "$build_temp_probe" || return 2
  test ! -e "$build_temp_probe" || return 2
  test ! -L "$build_temp_probe" || return 2
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
    -mmin +10 -print0)
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
    "ExecStart=/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin API_EMBEDDED_SCHEDULER_ENABLED=false PROBIGA_IN_APP_DEPLOY_ENABLED=0 PROBIGA_DEPLOYMENT_MODE=production PROBIGA_STRATEGY_GOVERNANCE_MODE=$STRATEGY_GOVERNANCE_MODE PROBIGA_STRATEGY_GOVERNANCE_BASE_SCHEMA_READY=true PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA=$DEFERRED_SCHEDULER_EXPECTED_SHA PROBIGA_DEFERRED_SCHEDULER_CODE_ROOT=$DEFERRED_SCHEDULER_CODE_ROOT PROBIGA_ADMIN_AUTH_ENABLED=true QMT_ANNOUNCEMENT_CHECKPOINT_DIR=$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PROBIGA_EXPECTED_GIT_SHA=$revision PROBIGA_BUILD_COMMIT_SHA=$revision PROBIGA_CODE_ROOT=$code_root PROBIGA_EXPECTED_ADATA_SHA=$adata_sha PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha PROBIGA_ADATA_SOURCE_DIR=$adata_source PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha PYTHONPATH=$adata_source:$code_root $RELEASE_VENV_ROOT/$revision/bin/python -P -m uvicorn server.api.main:app --app-dir $code_root --host 127.0.0.1 --port 8000 --workers 2 --limit-concurrency 64 --backlog 256 --limit-max-requests 400 --limit-max-requests-jitter 100 --timeout-keep-alive 5" \
    'Environment=API_EMBEDDED_SCHEDULER_ENABLED=false' \
    'Environment=PROBIGA_IN_APP_DEPLOY_ENABLED=0' \
    'Environment=PROBIGA_DEPLOYMENT_MODE=production' \
    "Environment=PROBIGA_STRATEGY_GOVERNANCE_MODE=$STRATEGY_GOVERNANCE_MODE" \
    'Environment=PROBIGA_STRATEGY_GOVERNANCE_BASE_SCHEMA_READY=true' \
    "Environment=PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA=$DEFERRED_SCHEDULER_EXPECTED_SHA" \
    "Environment=PROBIGA_DEFERRED_SCHEDULER_CODE_ROOT=$DEFERRED_SCHEDULER_CODE_ROOT" \
    'Environment=PROBIGA_ADMIN_AUTH_ENABLED=true' \
    "Environment=QMT_ANNOUNCEMENT_CHECKPOINT_DIR=$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" \
    "Environment=PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT" \
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
    "WorkingDirectory=$code_root" \
    "ExecStart=/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin API_EMBEDDED_SCHEDULER_ENABLED=false API_SCHEDULER_MAX_CONCURRENT_TASKS=2 PROBIGA_DEPLOYMENT_MODE=production PROBIGA_STRATEGY_GOVERNANCE_MODE=$STRATEGY_GOVERNANCE_MODE PROBIGA_STRATEGY_GOVERNANCE_BASE_SCHEMA_READY=true PROBIGA_SCHEDULER_EXECUTOR_ROLE=linux_standalone QMT_ANNOUNCEMENT_CHECKPOINT_DIR=$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PROBIGA_EXPECTED_GIT_SHA=$revision PROBIGA_BUILD_COMMIT_SHA=$revision PROBIGA_CODE_ROOT=$code_root PROBIGA_EXPECTED_ADATA_SHA=$adata_sha PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha PROBIGA_ADATA_SOURCE_DIR=$adata_source PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha PYTHONPATH=$adata_source:$code_root $RELEASE_VENV_ROOT/$revision/bin/python -P $code_root/tools/run_scheduler_daemon.py" \
    'Restart=on-failure' \
    'RestartSec=5s' \
    'Environment=API_EMBEDDED_SCHEDULER_ENABLED=false' \
    'Environment=PROBIGA_DEPLOYMENT_MODE=production' \
    "Environment=PROBIGA_STRATEGY_GOVERNANCE_MODE=$STRATEGY_GOVERNANCE_MODE" \
    'Environment=PROBIGA_STRATEGY_GOVERNANCE_BASE_SCHEMA_READY=true' \
    'Environment=PROBIGA_SCHEDULER_EXECUTOR_ROLE=linux_standalone' \
    'Environment=API_SCHEDULER_MAX_CONCURRENT_TASKS=2' \
    "Environment=QMT_ANNOUNCEMENT_CHECKPOINT_DIR=$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" \
    "Environment=PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT" \
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
    "ExecStart=/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PROBIGA_DEPLOYMENT_MODE=production PROBIGA_EXPECTED_GIT_SHA=$revision PROBIGA_CODE_ROOT=$code_root PROBIGA_EXPECTED_ADATA_SHA=$adata_sha PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha PROBIGA_ADATA_SOURCE_DIR=$adata_source PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha PYTHONPATH=$adata_source:$code_root $RELEASE_VENV_ROOT/$revision/bin/python -P $code_root/tools/run_ai_recommendation_worker.py --once" \
    'Environment=GIT_OPTIONAL_LOCKS=0' \
    'Environment=PYTHONDONTWRITEBYTECODE=1' \
    'Environment=PYTHONSAFEPATH=1' \
    'Environment=PROBIGA_DEPLOYMENT_MODE=production' \
    "Environment=PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT" \
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
    adapter_registry_seal_sha="$(/usr/bin/cat -- \
      "$venv_path/.adapter-registry-seal.sha256")" || return 1
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
  CUTOVER_STEP=remove_database_writer_guard_inventory
  read -r main_record scheduler_record ai_service_record ai_timer_record \
    < <(database_writer_guard_inventory) || return 1
  CUTOVER_STEP=verify_database_writer_guard_marker_before_removal
  controlled_guard_assert_marker "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  CUTOVER_STEP=sync_database_writer_restore_journal_before_removal
  controlled_guard_sync_activation_journal "$EXPECTED_SHA" "$main_record" \
    "$scheduler_record" "$ai_service_record" "$ai_timer_record" || return 1
  CUTOVER_STEP=verify_database_writer_guard_dropins_before_removal
  for dropin in "${DATABASE_WRITER_GUARD_DROPINS[@]}"; do
    assert_database_writer_guard_dropin_file "$dropin" || return 1
  done
  assert_database_writer_guard_dropins_loaded || return 1
  CUTOVER_STEP=remove_database_writer_guard_marker
  if ! sudo rm -f -- "$DATABASE_WRITER_GUARD_FILE" || \
    ! sudo sync -f "$DATABASE_WRITER_GUARD_DIR"; then
    restore_database_writer_guard_after_cleanup_failure || true
    return 1
  fi
  CUTOVER_STEP=verify_database_writer_restore_journal_after_removal
  if [ -e "$DATABASE_WRITER_GUARD_FILE" ] || \
    [ -L "$DATABASE_WRITER_GUARD_FILE" ] || \
    ! controlled_guard_assert_restore_file "$EXPECTED_SHA" "$main_record" \
      "$scheduler_record" "$ai_service_record" "$ai_timer_record"; then
    restore_database_writer_guard_after_cleanup_failure || true
    return 1
  fi
  CUTOVER_STEP=verify_database_writer_guard_dropins_after_removal
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
  CUTOVER_STEP=verify_database_writer_boundary_after_guard_removal
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
  # Nginx may retain an open-file-cache entry for the stable symlink path even
  # after the link is atomically replaced.  Reload workers without dropping
  # connections so every subsequent exact-byte probe resolves the new tree.
  /usr/sbin/nginx -t
  systemctl reload nginx
  systemctl is-active --quiet nginx
}
assert_nginx_static_matches_checkout() {
  local checkout_root="${1:-$REPOSITORY_ROOT}"
  local asset
  local attempt
  local matched
  local response
  test -L "$STATIC_RELEASE_LINK" || return 1
  test "$(readlink -f "$STATIC_RELEASE_LINK")" = "$checkout_root" || \
    return 1
  for asset in js/app.js css/style.css; do
    response="$(mktemp)" || return 1
    matched=0
    for attempt in $(seq 1 15); do
      if curl --fail --silent --show-error \
          -H 'Cache-Control: no-cache' \
          "http://127.0.0.1/static/$asset" > "$response" && \
        cmp --silent "$checkout_root/server/static/$asset" "$response"; then
        matched=1
        break
      fi
      sleep 1
    done
    if [ "$matched" -ne 1 ]; then
      rm -f "$response" || true
      echo "Nginx did not serve the expected static asset after reload: $asset" >&2
      return 1
    fi
    rm -f "$response" || return 1
  done
  return 0
}
write_admin_auth_header_file() {
  local output_file="$1"
  test -f "$output_file" || return 1
  test ! -L "$output_file" || return 1
  test "$(stat -c '%U:%G' -- "$output_file")" = \
    "$SERVICE_USER:$SERVICE_USER" || return 1
  test "$(stat -c '%a' -- "$output_file")" = 600 || return 1
  (
    cd "$PREPARED_CODE_ROOT" || return 1
    sudo -u "$SERVICE_USER" /usr/bin/env -i \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
      PROBIGA_DEPLOYMENT_MODE=production \
      PROBIGA_ADMIN_AUTH_ENABLED=true \
      "PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P - \
      "$output_file" <<'PY'
from pathlib import Path
import sys

from server.common.config import get_admin_auth_config

config = get_admin_auth_config()
token = str(config.get("token") or "")
if config.get("enabled") is not True or not token:
    raise SystemExit(2)
try:
    token.encode("ascii")
except UnicodeEncodeError:
    raise SystemExit(2)
if any(ord(character) < 33 or ord(character) > 126 for character in token):
    raise SystemExit(2)
Path(sys.argv[1]).write_text(
    "X-ProBigA-Admin-Token: " + token,
    encoding="ascii",
)
PY
  ) || return 1
  chown root:root -- "$output_file" || return 1
  chmod 0600 -- "$output_file" || return 1
  test "$(stat -c '%U:%G' -- "$output_file")" = root:root || return 1
  test "$(stat -c '%a' -- "$output_file")" = 600 || return 1
  test "$(stat -c '%s' -- "$output_file")" -gt 28 || return 1
  test "$(stat -c '%s' -- "$output_file")" -le 2048 || return 1
  grep -q '^X-ProBigA-Admin-Token: [!-~][!-~]*$' "$output_file" || return 1
}
verify_account_login_api_and_page_smoke() {
  local expected_sha="$1"
  local status_response login_response login_request static_response
  local login_http_code
  [[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  status_response="$(mktemp)" || return 1
  login_response="$(mktemp)" || {
    rm -f -- "$status_response"
    return 1
  }
  login_request="$(mktemp)" || {
    rm -f -- "$status_response" "$login_response"
    return 1
  }
  static_response="$(mktemp)" || {
    rm -f -- "$status_response" "$login_response" "$login_request"
    return 1
  }
  chmod 0600 "$status_response" "$login_response" "$login_request" \
    "$static_response" || {
    rm -f -- "$status_response" "$login_response" "$login_request" \
      "$static_response"
    return 1
  }
  if ! "$BOOTSTRAP_PYTHON" -I - "$login_request" <<'PY'
import json
import secrets
import sys
from pathlib import Path

Path(sys.argv[1]).write_text(
    json.dumps({
        "username": "__release_probe_" + secrets.token_hex(16),
        "password": secrets.token_urlsafe(32),
    }, separators=(",", ":")),
    encoding="utf-8",
)
PY
  then
    rm -f -- "$status_response" "$login_response" "$login_request" \
      "$static_response"
    return 1
  fi
  if ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --output "$status_response" http://127.0.0.1/api/auth/status; then
    cat "$status_response" >&2
    rm -f -- "$status_response" "$login_response" "$login_request" \
      "$static_response"
    return 1
  fi
  if ! login_http_code="$(curl --silent --show-error --retry 3 \
      --retry-connrefused --retry-delay 1 \
      --header 'Accept: application/json' \
      --header 'Content-Type: application/json' \
      --data-binary @"$login_request" --output "$login_response" \
      --write-out '%{http_code}' http://127.0.0.1/api/auth/login)" || \
    [ "$login_http_code" != 401 ]; then
    echo "Account login negative-path smoke returned HTTP $login_http_code" >&2
    cat "$login_response" >&2
    rm -f -- "$status_response" "$login_response" "$login_request" \
      "$static_response"
    return 1
  fi
  if ! "$BOOTSTRAP_PYTHON" -I - "$status_response" "$login_response" \
      "$expected_sha" <<'PY'
import json
import sys
from pathlib import Path

status_path, login_path = map(Path, sys.argv[1:3])
expected_sha = sys.argv[3]

def fail(message):
    print(f"account_login_api_smoke invalid={message}", file=sys.stderr)
    raise SystemExit(2)

def read(path):
    try:
        if path.stat().st_size > 1024 * 1024:
            fail(f"response_too_large:{path.name}")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        fail(f"json:{path.name}:{type(exc).__name__}")
    if not isinstance(payload, dict):
        fail(f"payload_type:{path.name}")
    return payload

status = read(status_path)
login = read(login_path)
if not (
    status.get("status") == "ok"
    and status.get("required") is True
    and status.get("authenticated") is False
    and status.get("user_initialized") is True
    and type(status.get("user_count")) is int
    and status["user_count"] >= 1
    and status.get("registration_open") is False
):
    fail("status_or_initialized_account")
if not (
    login.get("status") == "error"
    and login.get("error") == "invalid_credentials"
    and login.get("authenticated") is not True
):
    fail("negative_login_contract")
print(
    "account_login_api_smoke status=PASS "
    f"build={expected_sha} initialized_users={status['user_count']}"
)
PY
  then
    rm -f -- "$status_response" "$login_response" "$login_request" \
      "$static_response"
    return 1
  fi
  if ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --output "$static_response" http://127.0.0.1/login || \
    ! cmp --silent "$PREPARED_CODE_ROOT/server/static/login.html" \
      "$static_response" || \
    ! grep -F -- 'login.js?v=1' "$static_response" >/dev/null || \
    ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --output "$static_response" http://127.0.0.1/static/js/login.js || \
    ! cmp --silent "$PREPARED_CODE_ROOT/server/static/js/login.js" \
      "$static_response" || \
    ! grep -F -- "fetch('/api/auth/' + mode" "$static_response" >/dev/null || \
    ! grep -F -- "fetch('/api/auth/status'" "$static_response" >/dev/null; then
    echo "Account login page/static release smoke failed" >&2
    rm -f -- "$status_response" "$login_response" "$login_request" \
      "$static_response"
    return 1
  fi
  rm -f -- "$status_response" "$login_response" "$login_request" \
    "$static_response"
  echo "Account login API and page smoke passed"
}
verify_strategy_governance_api_and_page_smoke() {
  local expected_sha="$1"
  local expected_trade_date="$2"
  local governance_response index_response app_response admin_header
  [[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$expected_trade_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  governance_response="$(mktemp)" || return 1
  index_response="$(mktemp)" || {
    rm -f -- "$governance_response"
    return 1
  }
  app_response="$(mktemp)" || {
    rm -f -- "$governance_response" "$index_response"
    return 1
  }
  admin_header="$(mktemp)" || {
    rm -f -- "$governance_response" "$index_response" "$app_response"
    return 1
  }
  chown "$SERVICE_USER:$SERVICE_USER" "$admin_header" || {
    rm -f -- "$governance_response" "$index_response" "$app_response" \
      "$admin_header"
    return 1
  }
  chmod 0600 "$governance_response" "$index_response" "$app_response" \
    "$admin_header" || {
    rm -f -- "$governance_response" "$index_response" "$app_response" \
      "$admin_header"
    return 1
  }
  if ! write_admin_auth_header_file "$admin_header"; then
    rm -f -- "$governance_response" "$index_response" "$app_response" \
      "$admin_header"
    return 1
  fi
  if ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --header @"$admin_header" \
      --output "$governance_response" \
      "http://127.0.0.1/api/strategy-center/governance?trade_date=$expected_trade_date"; then
    cat "$governance_response" >&2
    rm -f -- "$governance_response" "$index_response" "$app_response" \
      "$admin_header"
    return 1
  fi
  if ! "$BOOTSTRAP_PYTHON" -I - "$governance_response" \
      "$expected_sha" "$expected_trade_date" <<'PY'
import json
import re
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

path = Path(sys.argv[1])
expected_sha, expected_trade_date = sys.argv[2:]

def unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value

def fail(message):
    print(f"strategy_governance_api_smoke invalid={message}", file=sys.stderr)
    raise SystemExit(2)

def require_hash(value, length):
    return re.fullmatch(rf"[0-9a-f]{{{length}}}", str(value or "")) is not None

try:
    if path.stat().st_size > 8 * 1024 * 1024:
        fail("response_too_large")
    payload = json.loads(
        path.read_text(encoding="utf-8"), object_pairs_hook=unique_object,
    )
except Exception as exc:
    fail(f"json:{type(exc).__name__}")
if not isinstance(payload, dict):
    fail("payload_type")
run_uid = str(payload.get("run_uid") or "")
result_hash = str(payload.get("canonical_result_hash") or "")
if not (
    payload.get("status") == "ok"
    and payload.get("result_mode") == "CANONICAL_PERSISTED"
    and payload.get("is_canonical") is True
    and payload.get("ranking_response_bounded") is True
    and payload.get("trade_date") == expected_trade_date
    and payload.get("build_commit_sha") == expected_sha
    and payload.get("decision_contract_version")
    == "strategy-governance-decision.v7"
    and require_hash(run_uid, 32)
    and require_hash(result_hash, 64)
    and require_hash(payload.get("decision_hash"), 64)
):
    fail("canonical_identity")

authority_fields = {
    "automatic_real_order_submission",
    "real_order_authority",
    "real_order_submission_enabled",
    "real_order_submission",
    "real_orders_allowed",
    "real_order_allowed",
    "automatic_real_order_authority",
    "real_trading_enabled",
    "order_authority",
}
def is_authority_field(key):
    return key in authority_fields or key.endswith(
        ("_automatic_real_order_submission", "_real_order_authority")
    )
def verify_authority(value, location="$"):
    if isinstance(value, dict):
        for key, item in value.items():
            if is_authority_field(key) and item is not False:
                fail(f"authority:{location}.{key}")
            verify_authority(item, f"{location}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            verify_authority(item, f"{location}[{index}]")
verify_authority(payload)

summary = payload.get("summary")
pools = payload.get("pools")
if not isinstance(summary, dict) or not isinstance(pools, dict):
    fail("summary_or_pools_type")
if set(pools) != {"observation", "confirmation", "tradable"}:
    fail("pool_names")
for name in ("observation", "confirmation", "tradable"):
    rows = pools.get(name)
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        fail(f"pool_rows:{name}")
    if summary.get(f"{name}_count") != len(rows):
        fail(f"pool_count:{name}")

rankings = payload.get("ranking_pages")
if not isinstance(rankings, dict) or set(rankings) != {"strategy", "combination"}:
    fail("ranking_pages")
for name, entity_type, collection_key, summary_key in (
    ("strategy", "STRATEGY", "strategies", "strategy_count"),
    ("combination", "COMBINATION", "combinations", "combination_count"),
):
    metadata = rankings.get(name)
    rows = payload.get(collection_key)
    total = summary.get(summary_key)
    if not (
        isinstance(metadata, dict)
        and isinstance(rows, list)
        and isinstance(total, int)
        and not isinstance(total, bool)
        and metadata.get("schema") == "probiga.governance-ranking-page.v1"
        and metadata.get("run_uid") == run_uid
        and metadata.get("canonical_result_hash") == result_hash
        and metadata.get("trade_date") == expected_trade_date
        and metadata.get("entity_type") == entity_type
        and metadata.get("total_count") == total
        and metadata.get("unfiltered_total_count") == total
        and metadata.get("offset") == 0
        and metadata.get("limit") == 50
        and len(rows) == min(50, total)
        and require_hash(metadata.get("page_hash"), 64)
    ):
        fail(f"ranking_identity:{name}")

allocations = payload.get("allocations")
if not isinstance(allocations, list) or not allocations:
    fail("allocations")
weights = []
try:
    for row in allocations:
        if not isinstance(row, dict):
            fail("allocation_row")
        raw = row.get("simulated_weight_pct")
        if isinstance(raw, bool):
            fail("allocation_bool")
        weight = Decimal(str(raw))
        if not weight.is_finite() or weight < 0:
            fail("allocation_weight")
        weights.append(weight)
except (InvalidOperation, TypeError, ValueError):
    fail("allocation_weight")
cash_count = sum(row.get("target_type") == "CASH" for row in allocations)
non_cash_count = sum(row.get("target_type") != "CASH" for row in allocations)
if sum(weights) != Decimal("100") or cash_count != 1:
    fail("allocation_conservation")
if summary.get("allocation_count") != non_cash_count:
    fail("allocation_count")
print(
    "strategy_governance_api_smoke status=PASS "
    f"trade_date={expected_trade_date} run_uid={run_uid} "
    f"canonical_result_hash={result_hash}"
)
PY
  then
    rm -f -- "$governance_response" "$index_response" "$app_response" \
      "$admin_header"
    return 1
  fi
  if ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --header @"$admin_header" \
      --output "$index_response" http://127.0.0.1/ || \
    ! grep -F -- 'data-tab="strategy-center"' "$index_response" >/dev/null || \
    ! grep -F -- 'id="tab-strategy-center"' "$index_response" >/dev/null || \
    ! grep -F -- '动态策略竞技场' "$index_response" >/dev/null; then
    echo "Strategy governance page entry smoke failed" >&2
    rm -f -- "$governance_response" "$index_response" "$app_response" \
      "$admin_header"
    return 1
  fi
  if ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --output "$app_response" http://127.0.0.1/static/js/app.js || \
    ! grep -F -- '/api/strategy-center/governance' "$app_response" >/dev/null || \
    ! grep -F -- '真实下单权限：关闭' "$app_response" >/dev/null; then
    echo "Strategy governance page application smoke failed" >&2
    rm -f -- "$governance_response" "$index_response" "$app_response" \
      "$admin_header"
    return 1
  fi
  rm -f -- "$governance_response" "$index_response" "$app_response" \
    "$admin_header"
  echo "Strategy governance API and page smoke passed"
}
verify_strategy_pool_api_and_page_smoke() {
  local expected_sha="$1"
  local expected_trade_date="$2"
  local exact_response latest_response context_response static_response
  local admin_header
  [[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$expected_trade_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  exact_response="$(mktemp)" || return 1
  latest_response="$(mktemp)" || {
    rm -f -- "$exact_response"
    return 1
  }
  context_response="$(mktemp)" || {
    rm -f -- "$exact_response" "$latest_response"
    return 1
  }
  static_response="$(mktemp)" || {
    rm -f -- "$exact_response" "$latest_response" "$context_response"
    return 1
  }
  admin_header="$(mktemp)" || {
    rm -f -- "$exact_response" "$latest_response" "$context_response" \
      "$static_response"
    return 1
  }
  chown "$SERVICE_USER:$SERVICE_USER" "$admin_header" || {
    rm -f -- "$exact_response" "$latest_response" "$context_response" \
      "$static_response" "$admin_header"
    return 1
  }
  chmod 0600 "$exact_response" "$latest_response" "$context_response" \
    "$static_response" "$admin_header" || {
    rm -f -- "$exact_response" "$latest_response" "$context_response" \
      "$static_response" "$admin_header"
    return 1
  }
  if ! write_admin_auth_header_file "$admin_header" || \
    ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --header @"$admin_header" --output "$exact_response" \
      "http://127.0.0.1/api/v3/stock-pool?trade_date=$expected_trade_date" || \
    ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --header @"$admin_header" --output "$latest_response" \
      "http://127.0.0.1/api/v3/stock-pool?before_session_date=$expected_trade_date" || \
    ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --header @"$admin_header" --output "$context_response" \
      "http://127.0.0.1/api/v3/context?trade_date=$expected_trade_date"; then
    cat "$exact_response" "$latest_response" "$context_response" >&2
    rm -f -- "$exact_response" "$latest_response" "$context_response" \
      "$static_response" "$admin_header"
    return 1
  fi
  if ! "$BOOTSTRAP_PYTHON" -I - "$exact_response" "$latest_response" \
      "$context_response" "$expected_sha" "$expected_trade_date" <<'PY'
import json
import re
import sys
from pathlib import Path

exact_path, latest_path, context_path = map(Path, sys.argv[1:4])
expected_sha, expected_trade_date = sys.argv[4:]

def fail(message):
    print(f"strategy_pool_api_smoke invalid={message}", file=sys.stderr)
    raise SystemExit(2)

def unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value

def read_envelope(path):
    try:
        if path.stat().st_size > 16 * 1024 * 1024:
            fail(f"response_too_large:{path.name}")
        envelope = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique_object,
        )
    except Exception as exc:
        fail(f"json:{path.name}:{type(exc).__name__}")
    if not isinstance(envelope, dict):
        fail(f"envelope_type:{path.name}")
    if envelope.get("code_commit_sha") != expected_sha:
        fail(f"code_identity:{path.name}")
    payload = envelope.get("data")
    if not isinstance(payload, dict):
        fail(f"payload_type:{path.name}")
    return payload

def valid_pool(pool):
    items = pool.get("items")
    summary = pool.get("summary")
    status = str(pool.get("pool_status") or "").upper()
    if not (
        isinstance(pool.get("run_uid"), str)
        and bool(pool["run_uid"])
        and pool.get("pool_readable") is True
        and pool.get("run_status") == "COMPLETED"
        and pool.get("decision_integrity_verified") is True
        and status in {"READY", "EMPTY"}
        and re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}",
            str(pool.get("decision_session_date") or ""),
        )
        and re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}",
            str(pool.get("trade_date") or ""),
        )
        and isinstance(items, list)
        and all(isinstance(item, dict) for item in items)
        and isinstance(summary, dict)
    ):
        return False
    stock_count = summary.get("stock_count")
    candidate_count = summary.get("strategy_candidate_count")
    if (
        type(stock_count) is not int
        or stock_count != len(items)
        or type(candidate_count) is not int
        or candidate_count < 0
        or candidate_count != sum(
            item.get("is_strategy_candidate") is True for item in items
        )
    ):
        return False
    return (
        status == "READY" and candidate_count > 0
    ) or (
        status == "EMPTY" and candidate_count == 0
    )

def unavailable_pool(pool):
    return (
        pool.get("run_uid") is None
        and pool.get("pool_status") == "UNAVAILABLE"
        and pool.get("pool_readable") is False
        and pool.get("decision_integrity_verified") is False
        and pool.get("items") == []
    )

exact = read_envelope(exact_path)
latest = read_envelope(latest_path)
context = read_envelope(context_path)
if valid_pool(exact):
    exact_session = str(exact.get("decision_session_date") or "")
    if exact_session == expected_trade_date:
        if context.get("run_uid") != exact.get("run_uid"):
            fail("exact_context_run_uid")
        selected = exact
        mode = "EXACT_COMPLETED"
    elif exact_session < expected_trade_date:
        if not (
            exact.get("is_as_of_fallback") is True
            and exact.get("requested_trade_date") == expected_trade_date
            and exact.get("trade_date") == exact_session
            and exact.get("historical_read_only") is False
            and context.get("run_uid") == exact.get("run_uid")
            and context.get("requested_date") == expected_trade_date
            and context.get("decision_session_date") == expected_trade_date
            and context.get("data_date") == exact_session
            and context.get("is_as_of_fallback") is True
            and context.get("historical_read_only") is False
        ):
            fail("latest_as_of_contract")
        selected = exact
        mode = "LATEST_COMPLETED_AS_OF"
    else:
        fail("exact_session_date")
else:
    if not unavailable_pool(exact):
        fail("exact_unreadable_contract")
    if (
        context.get("decision_integrity_verified") is True
        and str(context.get("data_status") or "") == "READY"
        and str(context.get("decision_status") or "")
        in {"CANDIDATE_AVAILABLE", "EMPTY"}
    ):
        fail("context_ready_but_exact_pool_missing")
    if valid_pool(latest):
        if str(latest.get("decision_session_date") or "") >= expected_trade_date:
            fail("historical_pool_not_strictly_older")
        if not (
            latest.get("before_session_date") == expected_trade_date
            and latest.get("requested_trade_date") == expected_trade_date
            and latest.get("is_historical_fallback") is True
            and latest.get("historical_read_only") is True
            and latest.get("historical_fallback_status")
            == "HISTORICAL_READ_ONLY"
            and latest.get("historical_fallback_session_date")
            == latest.get("decision_session_date")
        ):
            fail("historical_pool_boundary_contract")
        selected = latest
        mode = "HISTORICAL_READ_ONLY"
    else:
        if not unavailable_pool(latest):
            fail("historical_pool_unavailable_contract")
        selected = latest
        mode = "UNAVAILABLE_NO_VERIFIED_POOL"
print(
    "strategy_pool_api_smoke status=PASS "
    f"mode={mode} requested_trade_date={expected_trade_date} "
    f"decision_session_date={selected.get('decision_session_date')} "
    f"run_uid={selected.get('run_uid')} "
    f"pool_status={selected.get('pool_status')}"
)
PY
  then
    rm -f -- "$exact_response" "$latest_response" "$context_response" \
      "$static_response" "$admin_header"
    return 1
  fi
  if ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --output "$static_response" http://127.0.0.1/static/trading-v3.html || \
    ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --output "$static_response" http://127.0.0.1/static/js/trading-v3.js; then
    echo "Strategy pool iframe/static release smoke failed" >&2
    rm -f -- "$exact_response" "$latest_response" "$context_response" \
      "$static_response" "$admin_header"
    return 1
  fi
  rm -f -- "$exact_response" "$latest_response" "$context_response" \
    "$static_response" "$admin_header"
  echo "Strategy pool API and real iframe page smoke passed"
}
verify_today_strategy_daily_result_smoke() {
  local expected_sha="$1"
  local expected_trade_date="$2"
  local response admin_header
  [[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  [[ "$expected_trade_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  response="$(mktemp)" || return 1
  admin_header="$(mktemp)" || {
    rm -f -- "$response"
    return 1
  }
  chown "$SERVICE_USER:$SERVICE_USER" "$admin_header" || {
    rm -f -- "$response" "$admin_header"
    return 1
  }
  chmod 0600 "$response" "$admin_header" || {
    rm -f -- "$response" "$admin_header"
    return 1
  }
  if ! write_admin_auth_header_file "$admin_header" || \
    ! curl --fail-with-body --silent --show-error --retry 15 \
      --retry-all-errors --retry-delay 2 --retry-connrefused \
      --header @"$admin_header" --output "$response" \
      "http://127.0.0.1/api/v3/daily-result?trade_date=$expected_trade_date&force=true"; then
    cat "$response" >&2
    rm -f -- "$response" "$admin_header"
    return 1
  fi
  if ! "$BOOTSTRAP_PYTHON" -I - "$response" "$expected_sha" \
      "$expected_trade_date" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
expected_sha, expected_trade_date = sys.argv[2:]

def fail(message):
    print(f"today_strategy_daily_result_smoke invalid={message}", file=sys.stderr)
    raise SystemExit(2)

try:
    if path.stat().st_size > 16 * 1024 * 1024:
        fail("response_too_large")
    envelope = json.loads(path.read_text(encoding="utf-8"))
except Exception as exc:
    fail(f"json:{type(exc).__name__}")

data = envelope.get("data") if isinstance(envelope, dict) else None
acceptance = data.get("acceptance") if isinstance(data, dict) else None
build = data.get("build_identity") if isinstance(data, dict) else None
valid = (
    envelope.get("status") == "ok"
    and envelope.get("code_commit_sha") == expected_sha
    and isinstance(data, dict)
    and data.get("schema") == "probiga.trading-v3.daily-result.v1"
    and data.get("delivery_status") == "COMPLETED"
    and data.get("reason_code") == "EXACT_DAILY_RESULT_VERIFIED"
    and data.get("requested_trade_date") == expected_trade_date
    and data.get("authoritative_closed_trade_date") == expected_trade_date
    and data.get("decision_session_date") == expected_trade_date
    and data.get("data_trade_date") == expected_trade_date
    and isinstance(data.get("run_uid"), str)
    and bool(data["run_uid"])
    and isinstance(acceptance, dict)
    and acceptance.get("accepted") is True
    and isinstance(build, dict)
    and build.get("api_build_sha") == expected_sha
    and build.get("canonical_pool_build_sha") == expected_sha
    and build.get("all_match") is True
    and data.get("automatic_real_order_submission") is False
    and data.get("real_order_authority") is False
)
if not valid:
    fail("exact_daily_result_not_accepted")
print(
    "today_strategy_daily_result_smoke status=PASS "
    f"trade_date={expected_trade_date} run_uid={data['run_uid']}"
)
PY
  then
    rm -f -- "$response" "$admin_header"
    return 1
  fi
  rm -f -- "$response" "$admin_header"
  echo "Today strategy daily-result API smoke passed"
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
QMT_EDGE_DEPLOYMENT_ATTEMPT_ID="$(
  "$BOOTSTRAP_PYTHON" -I -c 'import secrets; print(secrets.token_hex(16))'
)"
[[ "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID" =~ ^[0-9a-f]{32}$ ]]
readonly QMT_EDGE_DEPLOYMENT_ATTEMPT_ID
CUTOVER_STARTED=0
DEFERRED_DB_CUTOVER_STARTED=0
CUTOVER_BASE_SCHEMA_STARTED=0
CUTOVER_BASE_SCHEMA_APPLIED=0
DEFERRED_RELEASE_WRITER_FENCE_STARTED=0
DEFERRED_SCHEDULER_EXPECTED_SHA=""
DEFERRED_SCHEDULER_CODE_ROOT=""
CUTOVER_STEP=preparation
API_STOPPED=0
DEPLOY_SUCCEEDED=0
NEW_VENV_LINK=0
STAGING_WORKTREE=""
PREPARED_CODE_ROOT="$CODE_RELEASE_ROOT/$EXPECTED_SHA"
CODE_VALIDATION_ROOT=""
NEW_CODE_RELEASE=0
SCHEDULER_UNIT_TOUCHED=0
PRE_CUTOVER_SCHEDULER_STOPPED=0
QMT_EDGE_RECOVERABLE_HANDOFF_ATTEMPTED=0
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
QMT_ANNOUNCEMENT_TASK_OLD_SOURCE=""
QMT_ANNOUNCEMENT_TASK_NEW_SOURCE=""
GOVERNANCE_TASK_TOUCHED=0
QMT_ANNOUNCEMENT_TASK_TOUCHED=0
GOVERNANCE_TRADE_DATE=""
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
  [ -z "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE" ] || \
    rm -f -- "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"
  [ -z "$QMT_ANNOUNCEMENT_TASK_NEW_SOURCE" ] || \
    rm -f -- "$QMT_ANNOUNCEMENT_TASK_NEW_SOURCE"
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
strict_stopped_dropin_environment_value() {
  local name="$1"
  local -a values=()
  mapfile -t values < <(
    sed -n "s|^Environment=$name=||p" "$PREVIOUS_DROPIN"
  ) || return 1
  test "${#values[@]}" -eq 1 || return 1
  test -n "${values[0]}" || return 1
  printf '%s\n' "${values[0]}"
}
strict_stopped_dropin_execstart() {
  local -a values=()
  mapfile -t values < <(sed -n 's/^ExecStart=\(.\+\)$/\1/p' "$PREVIOUS_DROPIN") || \
    return 1
  test "${#values[@]}" -eq 1 || return 1
  case "${values[0]}" in
    '/usr/bin/env -i '*) ;;
    *) return 1 ;;
  esac
  printf '%s\n' "${values[0]}"
}
strict_stopped_dropin_venv() {
  local execstart="$1"
  local candidate_root
  local candidate_venv
  local -a matching_venvs=()
  for candidate_root in "$RELEASE_VENV_ROOT" "$LEGACY_RELEASE_VENV_ROOT"; do
    candidate_venv="$(printf '%s\n' "$execstart" | sed -n \
      "s|.* \($candidate_root/[0-9a-f]\{40\}\)/bin/python -P -m uvicorn server.api.main:app .*|\1|p")"
    if [ -n "$candidate_venv" ]; then
      matching_venvs+=("$candidate_venv")
    fi
  done
  test "${#matching_venvs[@]}" -eq 1 || return 1
  printf '%s\n' "${matching_venvs[0]}"
}
assert_stopped_previous_runtime_process_state() {
  local main_active
  local main_pid
  local scheduler_active
  local scheduler_load
  local scheduler_pid
  local scheduler_unit_file
  main_active="$(systemctl show -p ActiveState --value "$MAIN_SERVICE")" || \
    return 1
  main_pid="$(systemctl show -p MainPID --value "$MAIN_SERVICE")" || return 1
  case "$main_active" in inactive|failed) ;; *) return 1 ;; esac
  test "$main_pid" = 0 || return 1
  scheduler_load="$(systemctl show -p LoadState --value probiga-scheduler)" || \
    return 1
  scheduler_active="$(systemctl show \
    -p ActiveState --value probiga-scheduler)" || return 1
  scheduler_pid="$(systemctl show -p MainPID --value probiga-scheduler)" || \
    return 1
  scheduler_unit_file="$(systemctl show \
    -p UnitFileState --value probiga-scheduler)" || return 1
  test "$scheduler_load" = loaded || return 1
  test "$scheduler_active" = inactive || return 1
  test "$scheduler_pid" = 0 || return 1
  case "$scheduler_unit_file" in enabled|disabled) ;; *) return 1 ;; esac
  return 0
}
assert_previous_stopped_main_restored() {
  test "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 || return 1
  test "$(systemctl show -p ActiveState --value "$MAIN_SERVICE")" = \
    inactive || return 1
  test "$(systemctl show -p MainPID --value "$MAIN_SERVICE")" = 0 || return 1
  test "$(systemctl show -p UnitFileState --value "$MAIN_SERVICE")" = \
    "$PREVIOUS_MAIN_UNIT_FILE_STATE" || return 1
  controlled_guard_assert_file "$MAIN_RELEASE_DROPIN" 644 || return 1
  cmp --silent "$MAIN_RELEASE_DROPIN" "$PREVIOUS_DROPIN" || return 1
  return 0
}
verify_previous_main_health_or_stopped() {
  local health_path="$1"
  local retry_count="$2"
  local retry_delay="$3"
  case "$health_path" in /api/health|/api/health/runtime) ;; *) return 1 ;; esac
  case "$retry_count" in 3|15) ;; *) return 1 ;; esac
  case "$retry_delay" in 1|2) ;; *) return 1 ;; esac
  if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
    assert_previous_stopped_main_restored || return 1
    return 0
  fi
  systemctl is-active --quiet "$MAIN_SERVICE" || return 1
  curl --fail --silent --show-error --retry "$retry_count" \
    --retry-all-errors --retry-delay "$retry_delay" --retry-connrefused \
    "http://127.0.0.1$health_path" >/dev/null
}
PREVIOUS_MAIN_PID="$(systemctl show "$MAIN_SERVICE" --property=MainPID --value)"
case "$PREVIOUS_MAIN_PID" in
  0)
    test "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 || {
      echo "active probiga service has no main PID" >&2
      exit 2
    }
    assert_stopped_previous_runtime_process_state || {
      echo "stopped production writer state is incomplete or unsafe" >&2
      exit 2
    }
    if [ "$PREVIOUS_DROPIN_PRESENT" -ne 1 ] || \
      ! controlled_guard_assert_file "$MAIN_RELEASE_DROPIN" 644 || \
      ! cmp --silent "$MAIN_RELEASE_DROPIN" "$PREVIOUS_DROPIN" || \
      [ "${#PREVIOUS_LEGACY_MAIN_DROPINS[@]}" -ne 0 ]; then
      echo "stopped probiga service has no safe immutable runtime definition" >&2
      exit 2
    fi
    ;;
  ''|*[!0-9]*)
    echo "probiga service exposed an invalid main PID" >&2
    exit 2
    ;;
  *)
    if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ] || \
      [ "$PREVIOUS_MAIN_OBSERVED_ACTIVE_STATE" != active ]; then
      echo "stopped probiga service unexpectedly retained a main PID" >&2
      exit 2
    fi
    ;;
esac
runtime_environment_value() {
  local name="$1"
  if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
    return 0
  fi
  tr '\0' '\n' < "/proc/$PREVIOUS_MAIN_PID/environ" \
    | sed -n "s|^$name=||p" \
    | tail -n 1
}
STOPPED_PREVIOUS_EXECSTART=""
if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
  PREVIOUS_RELEASE_REVISION="$(strict_stopped_dropin_environment_value \
    PROBIGA_EXPECTED_GIT_SHA)"
  STOPPED_PREVIOUS_EXECSTART="$(strict_stopped_dropin_execstart)"
  PREVIOUS_VENV="$(strict_stopped_dropin_venv \
    "$STOPPED_PREVIOUS_EXECSTART")"
else
  PREVIOUS_RELEASE_REVISION="$(runtime_environment_value \
    PROBIGA_EXPECTED_GIT_SHA)"
  PREVIOUS_VENV=""
fi
if [ -n "$PREVIOUS_RELEASE_REVISION" ]; then
  [[ "$PREVIOUS_RELEASE_REVISION" =~ ^[0-9a-f]{40}$ ]]
fi
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
  if [ -z "$PREVIOUS_VENV" ] && \
    [ "$PREVIOUS_MAIN_WAS_STOPPED" -ne 1 ]; then
    for candidate_root in "$RELEASE_VENV_ROOT" "$LEGACY_RELEASE_VENV_ROOT"; do
      if [ -L "$candidate_root/$PREVIOUS_RELEASE_REVISION" ]; then
        PREVIOUS_VENV="$candidate_root/$PREVIOUS_RELEASE_REVISION"
        break
      fi
    done
  fi
  if [ -z "$PREVIOUS_VENV" ] && \
    [ "$PREVIOUS_MAIN_WAS_STOPPED" -ne 1 ]; then
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
  if [ -z "$PREVIOUS_VENV" ] && \
    [ "$PREVIOUS_MAIN_WAS_STOPPED" -ne 1 ]; then
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
  if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
    test "$(basename "$PREVIOUS_VENV")" = "$PREVIOUS_RELEASE_REVISION"
  fi
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
  if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
    test -f "$PREVIOUS_VENV/.probiga.gitsha"
    test ! -L "$PREVIOUS_VENV/.probiga.gitsha"
    test "$(cat "$PREVIOUS_VENV/.probiga.gitsha")" = \
      "$PREVIOUS_RELEASE_REVISION"
    test -f "$PREVIOUS_VENV/.requirements.input"
    test -f "$PREVIOUS_VENV/.requirements.input.sha256"
    test -f "$PREVIOUS_VENV/.requirements.freeze.sha256"
    test "$(sha256sum "$PREVIOUS_VENV/.requirements.input" | cut -d' ' -f1)" = \
      "$PREVIOUS_INPUT_LOCK_SHA256"
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
  if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
    controlled_guard_assert_immutable_venv_tree "$PREVIOUS_VENV_TARGET"
  fi
fi
if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
  PREVIOUS_CODE_ROOT="$(strict_stopped_dropin_environment_value \
    PROBIGA_CODE_ROOT)"
else
  PREVIOUS_CODE_ROOT="$(dropin_environment_value PROBIGA_CODE_ROOT)"
fi
if [ -z "$PREVIOUS_CODE_ROOT" ] && \
  [ "$PREVIOUS_MAIN_WAS_STOPPED" -ne 1 ]; then
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
    if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
      test "$(stat -c '%U:%G' "$PREVIOUS_CODE_ROOT")" = root:root
      test -z "$(find -P "$PREVIOUS_CODE_ROOT" -xdev \
        \( ! -user root -o -perm /022 \) -print -quit)"
    fi
    assert_service_cannot_write_release_paths "$PREVIOUS_CODE_ROOT"
    ;;
  *)
    echo "previous code root escaped immutable release storage" >&2
    exit 2
    ;;
esac
if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
  PREVIOUS_ADATA_SHA="$(strict_stopped_dropin_environment_value \
    PROBIGA_EXPECTED_ADATA_SHA)"
  PREVIOUS_ADATA_TREE_SHA256="$(strict_stopped_dropin_environment_value \
    PROBIGA_EXPECTED_ADATA_TREE_SHA256)"
  PREVIOUS_ADATA_SOURCE="$(strict_stopped_dropin_environment_value \
    PROBIGA_ADATA_SOURCE_DIR)"
else
  PREVIOUS_ADATA_SHA="$(dropin_environment_value PROBIGA_EXPECTED_ADATA_SHA)"
  PREVIOUS_ADATA_TREE_SHA256="$(dropin_environment_value PROBIGA_EXPECTED_ADATA_TREE_SHA256)"
  PREVIOUS_ADATA_SOURCE="$(dropin_environment_value PROBIGA_ADATA_SOURCE_DIR)"
fi
if [ -z "$PREVIOUS_ADATA_SHA" ] && \
  [ "$PREVIOUS_MAIN_WAS_STOPPED" -ne 1 ]; then
  PREVIOUS_ADATA_SHA="$(runtime_environment_value PROBIGA_EXPECTED_ADATA_SHA)"
fi
if [ -z "$PREVIOUS_ADATA_TREE_SHA256" ] && \
  [ "$PREVIOUS_MAIN_WAS_STOPPED" -ne 1 ]; then
  PREVIOUS_ADATA_TREE_SHA256="$(runtime_environment_value PROBIGA_EXPECTED_ADATA_TREE_SHA256)"
fi
if [ -z "$PREVIOUS_ADATA_SOURCE" ] && \
  [ "$PREVIOUS_MAIN_WAS_STOPPED" -ne 1 ]; then
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
if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
  PREVIOUS_RELEASE_TREE_SHA256="$(strict_stopped_dropin_environment_value \
    PROBIGA_RELEASE_TREE_SHA256)"
  PREVIOUS_ADAPTER_REGISTRY_SEAL_SHA256="$(
    strict_stopped_dropin_environment_value \
      PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256
  )"
  PREVIOUS_BUILD_COMMIT_SHA="$(strict_stopped_dropin_environment_value \
    PROBIGA_BUILD_COMMIT_SHA)"
  test "$(strict_stopped_dropin_environment_value \
    API_EMBEDDED_SCHEDULER_ENABLED)" = false
  test "$(strict_stopped_dropin_environment_value \
    PROBIGA_DEPLOYMENT_MODE)" = production
  test "$PREVIOUS_BUILD_COMMIT_SHA" = "$PREVIOUS_RELEASE_REVISION"
  [[ "$PREVIOUS_RELEASE_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]]
  [[ "$PREVIOUS_ADAPTER_REGISTRY_SEAL_SHA256" =~ ^[0-9a-f]{64}$ ]]
  test -f "$PREVIOUS_VENV/.adata.gitsha"
  test -f "$PREVIOUS_VENV/.adata.tree.sha256"
  test -f "$PREVIOUS_VENV/.release-tree.sha256"
  test -f "$PREVIOUS_VENV/.adapter-registry-seal.sha256"
  test "$(cat "$PREVIOUS_VENV/.adata.gitsha")" = "$PREVIOUS_ADATA_SHA"
  test "$(cat "$PREVIOUS_VENV/.adata.tree.sha256")" = \
    "$PREVIOUS_ADATA_TREE_SHA256"
  test "$(cat "$PREVIOUS_VENV/.release-tree.sha256")" = \
    "$PREVIOUS_RELEASE_TREE_SHA256"
  test "$(cat "$PREVIOUS_VENV/.adapter-registry-seal.sha256")" = \
    "$PREVIOUS_ADAPTER_REGISTRY_SEAL_SHA256"
  PREVIOUS_RELEASE_TREE_OID="$(git -C "$PREVIOUS_CODE_ROOT" rev-parse \
    "${PREVIOUS_RELEASE_REVISION}^{tree}")"
  PREVIOUS_COMPUTED_RELEASE_TREE_SHA256="$(
    printf '{"kind":"git-tree","tree":"%s"}' "$PREVIOUS_RELEASE_TREE_OID" \
      | sha256sum | cut -d' ' -f1
  )"
  test "$PREVIOUS_COMPUTED_RELEASE_TREE_SHA256" = \
    "$PREVIOUS_RELEASE_TREE_SHA256"
  test "$(grep -c \
    '^ADAPTER_REGISTRY_SEAL_SHA256=[0-9a-f]\{64\}$' \
    "$PREVIOUS_CODE_ROOT/deploy/production_release.env")" -eq 1
  test "$(sed -n 's/^ADAPTER_REGISTRY_SEAL_SHA256=//p' \
    "$PREVIOUS_CODE_ROOT/deploy/production_release.env")" = \
    "$PREVIOUS_ADAPTER_REGISTRY_SEAL_SHA256"
  for stopped_execstart_identity in \
    "API_EMBEDDED_SCHEDULER_ENABLED=false" \
    "PROBIGA_DEPLOYMENT_MODE=production" \
    "PROBIGA_EXPECTED_GIT_SHA=$PREVIOUS_RELEASE_REVISION" \
    "PROBIGA_BUILD_COMMIT_SHA=$PREVIOUS_RELEASE_REVISION" \
    "PROBIGA_CODE_ROOT=$PREVIOUS_CODE_ROOT" \
    "PROBIGA_EXPECTED_ADATA_SHA=$PREVIOUS_ADATA_SHA" \
    "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$PREVIOUS_ADATA_TREE_SHA256" \
    "PROBIGA_ADATA_SOURCE_DIR=$PREVIOUS_ADATA_SOURCE" \
    "PROBIGA_RELEASE_TREE_SHA256=$PREVIOUS_RELEASE_TREE_SHA256" \
    "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$PREVIOUS_ADAPTER_REGISTRY_SEAL_SHA256"; do
    stopped_execstart_identity_name="${stopped_execstart_identity%%=*}"
    test "$(printf '%s\n' " $STOPPED_PREVIOUS_EXECSTART " | grep -oF \
      " $stopped_execstart_identity_name=" | wc -l)" -eq 1
    case " $STOPPED_PREVIOUS_EXECSTART " in
      *" $stopped_execstart_identity "*) ;;
      *)
        echo "stopped probiga ExecStart identity is incomplete" >&2
        exit 2
        ;;
    esac
  done
  case " $STOPPED_PREVIOUS_EXECSTART " in
    *" $PREVIOUS_VENV/bin/python -P -m uvicorn server.api.main:app --app-dir $PREVIOUS_CODE_ROOT --host 127.0.0.1 --port 8000 "*) ;;
    *) echo "stopped probiga ExecStart is not the sealed API runtime" >&2; exit 2 ;;
  esac
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
if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
  if [ "$PREVIOUS_MAIN_NEEDS_FAILED_RESET" -eq 1 ]; then
    sudo systemctl reset-failed "$MAIN_SERVICE"
  fi
  test "$(systemctl show -p ActiveState --value "$MAIN_SERVICE")" = inactive
  assert_stopped_previous_runtime_process_state
fi
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
      "$RELEASE_ARTIFACT_ROOT"/.wheelhouse-cache-*) \
        chmod -R u+rwX "$TRUSTED_WHEELHOUSE" 2>/dev/null || true
        rm -rf -- "$TRUSTED_WHEELHOUSE"
        ;;
      "$RELEASE_ARTIFACT_ROOT"/wheelhouse-cache-*)
        # A content-addressed, root-owned cache is a durable release input.
        # It is revalidated before every use and must survive this release.
        ;;
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
  [ -z "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE" ] || \
    rm -f -- "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"
  [ -z "$QMT_ANNOUNCEMENT_TASK_NEW_SOURCE" ] || \
    rm -f -- "$QMT_ANNOUNCEMENT_TASK_NEW_SOURCE"
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
  if ! "$venv_path/bin/python" -I -m pip check >/dev/null; then
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
  local requirements_input="$CODE_VALIDATION_ROOT/deploy/production_requirements.in"
  local platform_input="$CODE_VALIDATION_ROOT/requirements-platform.txt"
  test -f "$requirements_input" || return 1
  test ! -L "$requirements_input" || return 1
  test -f "$platform_input" || return 1
  test ! -L "$platform_input" || return 1
  "$BOOTSTRAP_PYTHON" -I - "$lock_file" "$requirements_input" \
    "$platform_input" <<'PY'
import hashlib
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
requirements_input_path = Path(sys.argv[2])
platform_input_path = Path(sys.argv[3])
text = path.read_text(encoding="utf-8")
requirements_input = requirements_input_path.read_bytes()
platform_input = platform_input_path.read_bytes()
if "\r" in text or not text.endswith("\n"):
    raise SystemExit(2)
requirements_input_text = requirements_input.decode("utf-8")
platform_input.decode("utf-8")
if b"\r" in requirements_input or not requirements_input.endswith(b"\n"):
    raise SystemExit(2)
if b"\r" in platform_input or not platform_input.endswith(b"\n"):
    raise SystemExit(2)
lines = text.splitlines()
expected_header = [
    "# PROBIGA_PRODUCTION_REQUIREMENTS_LOCK_VERSION=2",
    "# SOURCE=deploy/production_requirements.in",
    "# REQUIREMENTS_INPUT_SHA256="
    + hashlib.sha256(requirements_input).hexdigest(),
    "# PLATFORM_SOURCE=requirements-platform.txt",
    "# PLATFORM_REQUIREMENTS_SHA256="
    + hashlib.sha256(platform_input).hexdigest(),
    "# TARGET=cp314-manylinux_2_28_x86_64",
    "# STATUS=READY",
    "--only-binary=:all:",
]
if lines[:8] != expected_header:
    raise SystemExit(2)
if [
    line
    for line in requirements_input_text.splitlines()
    if line.startswith("-r ")
] != ["-r ../requirements-platform.txt"]:
    raise SystemExit(2)
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

verify_sealed_wheelhouse() {
  local actual_files
  local expected_files
  local manifest_entries
  local manifest_file="$2"
  local unsafe_name
  local unsafe_path
  local wheel_file
  local wheel_sha
  local wheelhouse="$1"
  test -d "$wheelhouse" || return 1
  test ! -L "$wheelhouse" || return 1
  test "$(readlink -f "$wheelhouse")" = "$wheelhouse" || return 1
  test -f "$manifest_file" || return 1
  test ! -L "$manifest_file" || return 1
  test -f "$wheelhouse/.probiga-wheel-manifest" || return 1
  test ! -L "$wheelhouse/.probiga-wheel-manifest" || return 1
  cmp --silent "$manifest_file" \
    "$wheelhouse/.probiga-wheel-manifest" || return 1
  test "$(stat -c '%U:%G' "$wheelhouse")" = root:root || return 1
  test "$(stat -c '%a' "$wheelhouse")" = 555 || return 1
  unsafe_path="$(find -P "$wheelhouse" -xdev -mindepth 1 -maxdepth 1 \
    \( ! -type f -o ! -user root -o ! -group root -o -perm /222 \
       -o ! -links 1 \) -print -quit)" || return 1
  test -z "$unsafe_path" || return 1
  unsafe_name="$(find -P "$wheelhouse" -xdev -mindepth 1 -maxdepth 1 \
    -type f ! -name '.probiga-wheel-manifest' -printf '%f\n' | \
    grep -Ev '^[A-Za-z0-9_.+-]+\.whl$' || true)"
  test -z "$unsafe_name" || return 1
  manifest_entries="$(mktemp)" || return 1
  expected_files="$(mktemp)" || {
    rm -f -- "$manifest_entries"
    return 1
  }
  actual_files="$(mktemp)" || {
    rm -f -- "$manifest_entries" "$expected_files"
    return 1
  }
  if ! grep -E '^[0-9a-f]{64}  [A-Za-z0-9_.+-]+\.whl$' \
      "$manifest_file" > "$manifest_entries" || \
    ! awk '{print $2}' "$manifest_entries" | LC_ALL=C sort > \
      "$expected_files" || \
    ! find -P "$wheelhouse" -mindepth 1 -maxdepth 1 -type f \
      -name '*.whl' -printf '%f\n' | LC_ALL=C sort > "$actual_files" || \
    ! cmp --silent "$expected_files" "$actual_files"; then
    rm -f -- "$manifest_entries" "$expected_files" "$actual_files"
    return 1
  fi
  while read -r wheel_sha wheel_file; do
    test "$(sha256sum "$wheelhouse/$wheel_file" | cut -d' ' -f1)" = \
      "$wheel_sha" || {
        rm -f -- "$manifest_entries" "$expected_files" "$actual_files"
        return 1
      }
  done < "$manifest_entries"
  rm -f -- "$manifest_entries" "$expected_files" "$actual_files" || \
    return 1
  sudo -u "$SERVICE_USER" test ! -w "$wheelhouse" || return 1
  sudo -u "$BUILD_USER" test ! -w "$wheelhouse" || return 1
  return 0
}

seal_wheelhouse_cache() {
  local manifest_file="$2"
  local unsafe_name
  local unsafe_path
  local wheelhouse="$1"
  test -d "$wheelhouse" || return 1
  test ! -L "$wheelhouse" || return 1
  unsafe_path="$(find -P "$wheelhouse" -xdev -mindepth 1 -maxdepth 1 \
    \( ! -type f -o ! -links 1 \) -print -quit)" || return 1
  test -z "$unsafe_path" || return 1
  unsafe_name="$(find -P "$wheelhouse" -xdev -mindepth 1 -maxdepth 1 \
    -type f -printf '%f\n' | \
    grep -Ev '^[A-Za-z0-9_.+-]+\.whl$' || true)"
  test -z "$unsafe_name" || return 1
  install -o root -g root -m 0444 "$manifest_file" \
    "$wheelhouse/.probiga-wheel-manifest" || return 1
  chown -R root:root "$wheelhouse" || return 1
  find -P "$wheelhouse" -xdev -mindepth 1 -maxdepth 1 -type f \
    -exec chmod 0444 {} + || return 1
  chmod 0555 "$wheelhouse" || return 1
  verify_sealed_wheelhouse "$wheelhouse" "$manifest_file"
}

prepare_trusted_wheelhouse() {
  local actual_files
  local cache_name
  local cache_path
  local expected_files
  local manifest_entries
  local wheelhouse_build
  local wheel_file
  local wheel_sha
  local artifact_root="$RELEASE_ARTIFACT_ROOT"
  [[ "$EXPECTED_INPUT_LOCK_SHA256" =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ "$EXPECTED_WHEEL_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ ]] || return 1
  test ! -L "$artifact_root" || return 1
  install -d -o root -g root -m 0755 "$artifact_root" || return 1
  test "$(readlink -f "$artifact_root")" = "$artifact_root" || return 1
  cache_name="wheelhouse-cache-static-$EXPECTED_INPUT_LOCK_SHA256"
  cache_name="$cache_name-$EXPECTED_WHEEL_MANIFEST_SHA256"
  test "${#cache_name}" -le 247 || return 1
  cache_path="$artifact_root/$cache_name"
  test "$(dirname "$cache_path")" = "$artifact_root" || return 1
  if [ -e "$cache_path" ] || [ -L "$cache_path" ]; then
    TRUSTED_WHEELHOUSE="$cache_path"
    verify_sealed_wheelhouse "$TRUSTED_WHEELHOUSE" \
      "$TRUSTED_WHEEL_MANIFEST" || return 1
    echo "Reused verified dependency wheel cache: input_lock=$EXPECTED_INPUT_LOCK_SHA256" >&2
    return 0
  fi
  wheelhouse_build="$(mktemp -d \
    "$artifact_root/.$cache_name.XXXXXX")" || \
    return 1
  TRUSTED_WHEELHOUSE="$wheelhouse_build"
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
    /usr/bin/timeout --signal=TERM --kill-after=10s \
      "$DEPENDENCY_DOWNLOAD_TIMEOUT" \
      "$BOOTSTRAP_PYTHON" -I -m pip download \
      --require-hashes --only-binary=:all: --no-deps \
      --dest "$TRUSTED_WHEELHOUSE" -r "$RESOLVED_LOCK" || return 1
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
  seal_wheelhouse_cache "$TRUSTED_WHEELHOUSE" \
    "$TRUSTED_WHEEL_MANIFEST" || return 1
  mv -T -- "$TRUSTED_WHEELHOUSE" "$cache_path" || return 1
  TRUSTED_WHEELHOUSE="$cache_path"
  verify_sealed_wheelhouse "$TRUSTED_WHEELHOUSE" \
    "$TRUSTED_WHEEL_MANIFEST" || return 1
  return 0
}

prepare_ci_resolved_wheelhouse() {
  local actual_files
  local artifact_root="$RELEASE_ARTIFACT_ROOT"
  local cache_name
  local cache_path
  local wheelhouse_build
  local wheel_file
  local wheel_sha
  [[ "$EXPECTED_INPUT_LOCK_SHA256" =~ ^[0-9a-f]{64}$ ]] || return 1
  test ! -L "$artifact_root" || return 1
  install -d -o root -g root -m 0755 "$artifact_root" || return 1
  test "$(readlink -f "$artifact_root")" = "$artifact_root" || return 1
  cache_name="wheelhouse-cache-ci-$EXPECTED_INPUT_LOCK_SHA256"
  test "${#cache_name}" -le 247 || return 1
  cache_path="$artifact_root/$cache_name"
  test "$(dirname "$cache_path")" = "$artifact_root" || return 1
  if [ -e "$cache_path" ] || [ -L "$cache_path" ]; then
    TRUSTED_WHEELHOUSE="$cache_path"
    test -d "$TRUSTED_WHEELHOUSE" || return 1
    test ! -L "$TRUSTED_WHEELHOUSE" || return 1
    test "$(readlink -f "$TRUSTED_WHEELHOUSE")" = \
      "$TRUSTED_WHEELHOUSE" || return 1
    test -f "$TRUSTED_WHEELHOUSE/.probiga-wheel-manifest" || return 1
    test ! -L "$TRUSTED_WHEELHOUSE/.probiga-wheel-manifest" || return 1
    test "$(stat -c '%U:%G' \
      "$TRUSTED_WHEELHOUSE/.probiga-wheel-manifest")" = root:root || return 1
    test $((8#$(stat -c '%a' \
      "$TRUSTED_WHEELHOUSE/.probiga-wheel-manifest") & 8#222)) -eq 0 || \
      return 1
    TRUSTED_WHEEL_MANIFEST="$(mktemp)" || return 1
    install -o root -g root -m 0600 \
      "$TRUSTED_WHEELHOUSE/.probiga-wheel-manifest" \
      "$TRUSTED_WHEEL_MANIFEST" || return 1
    grep -Fx PROBIGA_RUNTIME_WHEEL_MANIFEST_VERSION=1 \
      "$TRUSTED_WHEEL_MANIFEST" >/dev/null || return 1
    grep -Fx TARGET=cp314-manylinux_2_28_x86_64 \
      "$TRUSTED_WHEEL_MANIFEST" >/dev/null || return 1
    grep -Fx SOURCE=ci-resolved-freeze-v1 \
      "$TRUSTED_WHEEL_MANIFEST" >/dev/null || return 1
    if grep -Ev '^(PROBIGA_RUNTIME_WHEEL_MANIFEST_VERSION=1|TARGET=cp314-manylinux_2_28_x86_64|SOURCE=ci-resolved-freeze-v1|[0-9a-f]{64}  [A-Za-z0-9_.+-]+\.whl)$' \
        "$TRUSTED_WHEEL_MANIFEST" >/dev/null; then
      return 1
    fi
    EXPECTED_WHEEL_MANIFEST_SHA256="$(sha256sum \
      "$TRUSTED_WHEEL_MANIFEST" | cut -d' ' -f1)"
    [[ "$EXPECTED_WHEEL_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ ]] || return 1
    verify_sealed_wheelhouse "$TRUSTED_WHEELHOUSE" \
      "$TRUSTED_WHEEL_MANIFEST" || return 1
    echo "Reused verified dependency wheel cache: input_lock=$EXPECTED_INPUT_LOCK_SHA256" >&2
    return 0
  fi
  wheelhouse_build="$(mktemp -d \
    "$artifact_root/.$cache_name.XXXXXX")" || return 1
  TRUSTED_WHEELHOUSE="$wheelhouse_build"
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
    /usr/bin/timeout --signal=TERM --kill-after=10s \
      "$DEPENDENCY_DOWNLOAD_TIMEOUT" \
      "$BOOTSTRAP_PYTHON" -I -m pip download \
      --only-binary=:all: --no-deps \
      --dest "$TRUSTED_WHEELHOUSE" -r "$RESOLVED_LOCK" || return 1
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
    TARGET=cp314-manylinux_2_28_x86_64 \
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
  seal_wheelhouse_cache "$TRUSTED_WHEELHOUSE" \
    "$TRUSTED_WHEEL_MANIFEST" || return 1
  mv -T -- "$TRUSTED_WHEELHOUSE" "$cache_path" || return 1
  TRUSTED_WHEELHOUSE="$cache_path"
  verify_sealed_wheelhouse "$TRUSTED_WHEELHOUSE" \
    "$TRUSTED_WHEEL_MANIFEST" || return 1
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
  else
    validate_hashed_requirements_lock "$RESOLVED_LOCK"
    TRUSTED_WHEEL_MANIFEST="$(mktemp)"
    printf '%s' "$TRUSTED_WHEEL_MANIFEST_B64" | base64 -d > \
      "$TRUSTED_WHEEL_MANIFEST"
    test "$(sha256sum "$TRUSTED_WHEEL_MANIFEST" | cut -d' ' -f1)" = \
      "$EXPECTED_WHEEL_MANIFEST_SHA256"
    grep -Fx PROBIGA_TRUSTED_WHEEL_MANIFEST_VERSION=1 \
      "$TRUSTED_WHEEL_MANIFEST" >/dev/null
    grep -Fx TARGET=cp314-manylinux_2_28_x86_64 \
      "$TRUSTED_WHEEL_MANIFEST" >/dev/null
    grep -Fx STATUS=READY "$TRUSTED_WHEEL_MANIFEST" >/dev/null
  fi
  # Both artifact modes use the isolated non-login build account to download
  # wheels. It needs read-only access to this validated, non-secret lock file.
  chmod 0444 "$RESOLVED_LOCK"
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
    else
      prepare_trusted_wheelhouse
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
    case "$TRUSTED_WHEELHOUSE" in
      "$RELEASE_ARTIFACT_ROOT"/.wheelhouse-cache-*)
        chmod -R u+rwX "$TRUSTED_WHEELHOUSE"
        rm -rf -- "$TRUSTED_WHEELHOUSE"
        ;;
      "$RELEASE_ARTIFACT_ROOT"/wheelhouse-cache-*)
        # Keep the verified content-addressed cache for later commit SHAs.
        ;;
      *)
        echo "refusing unsafe wheelhouse cleanup: $TRUSTED_WHEELHOUSE" >&2
        return 2
        ;;
    esac
  fi
  TRUSTED_WHEELHOUSE=""
  rm -rf "$ADATA_BUILD_SOURCE" "$ADATA_WHEEL_DIR"
  ADATA_BUILD_SOURCE=""
  ADATA_WHEEL_DIR=""
}
prepare_release() {
  if [ "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB ]; then
    capture_deferred_scheduler_identity
  fi
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
      "PYTHONPATH=$ADATA_SOURCE:$CODE_VALIDATION_ROOT" \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P -m \
      server.common.release_manifest write \
      --root "$CODE_VALIDATION_ROOT" \
      --release-id "$EXPECTED_SHA" \
      --source-tree-hash "$EXPECTED_RELEASE_TREE_SHA256" \
      --built-at "$DEPLOY_STARTED_AT" \
      --input-lock-sha256 "$EXPECTED_INPUT_LOCK_SHA256" \
      --wheel-manifest-sha256 "$EXPECTED_WHEEL_MANIFEST_SHA256" \
      --adata-sha "$EXPECTED_ADATA_SHA" \
      --adata-tree-sha256 "$EXPECTED_ADATA_TREE_SHA256" \
      --adapter-registry-seal-sha256 \
        "$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256"
  )
  test -r "$CODE_VALIDATION_ROOT/probiga.release.json"
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
  grep -Fx \
    "Environment=PROBIGA_STRATEGY_GOVERNANCE_MODE=$STRATEGY_GOVERNANCE_MODE" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  if [ "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB ]; then
    grep -Fx \
      "Environment=PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA=$DEFERRED_SCHEDULER_EXPECTED_SHA" \
      "$PREPARED_MAIN_DROPIN" >/dev/null
    grep -Fx \
      "Environment=PROBIGA_DEFERRED_SCHEDULER_CODE_ROOT=$DEFERRED_SCHEDULER_CODE_ROOT" \
      "$PREPARED_MAIN_DROPIN" >/dev/null
    grep -F -- \
      "PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA=$DEFERRED_SCHEDULER_EXPECTED_SHA" \
      "$PREPARED_MAIN_DROPIN" >/dev/null
    grep -F -- \
      "PROBIGA_DEFERRED_SCHEDULER_CODE_ROOT=$DEFERRED_SCHEDULER_CODE_ROOT" \
      "$PREPARED_MAIN_DROPIN" >/dev/null
  fi
  grep -Fx "Environment=PROBIGA_BUILD_COMMIT_SHA=$EXPECTED_SHA" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT" \
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
  grep -Fx 'Environment=PROBIGA_SCHEDULER_EXECUTOR_ROLE=linux_standalone' \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx 'Environment=API_SCHEDULER_MAX_CONCURRENT_TASKS=2' \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx \
    "Environment=PROBIGA_STRATEGY_GOVERNANCE_MODE=$STRATEGY_GOVERNANCE_MODE" \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_BUILD_COMMIT_SHA=$EXPECTED_SHA" \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT" \
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
    grep -Fx "Environment=PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT" \
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
prepared_active_runtime_matches_current_request() {
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
    "PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT"
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
  verify_venv_dependency_lock "$RELEASE_VENV_ROOT/$EXPECTED_SHA" || return 1
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
  grep -zFx -- 'PROBIGA_SCHEDULER_EXECUTOR_ROLE=linux_standalone' \
    "/proc/$scheduler_pid/environ" >/dev/null || return 1
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
  if [ "${RELEASE_DATA_VALIDATION_BLOCKING:-1}" -eq 1 ]; then
    run_prepared_python_tool \
      "$PREPARED_CODE_ROOT/tools/check_strategy_governance_health.py" \
      --compact --expected-build-sha "$EXPECTED_SHA" \
      --expected-scheduler-pid "$scheduler_pid" || return 1
  else
    echo "Post-start market-data validation skipped for code release" >&2
  fi
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
prepared_request_is_already_active() {
  finalized_receipt_matches_current_v2_request || return 1
  prepared_active_runtime_matches_current_request || return 1
  return 0
}
finalize_preserved_no_receipt_request() {
  local qmt_activation_output
  test "$DEPLOY_OPERATION" = deploy || return 1
  test "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 || return 1
  test "$V2_FORWARD_PRESERVED_NO_RECEIPT_SHA" = "$EXPECTED_SHA" || \
    return 1
  controlled_v2_assert_preserved_no_receipt_transaction "$EXPECTED_SHA" || \
    return 1
  prepared_active_runtime_matches_current_request || return 1
  ACTIVE_INPUT_LOCK_SHA256="$EXPECTED_INPUT_LOCK_SHA256"
  ACTIVE_RESOLVED_FREEZE_SHA256="$EXPECTED_RESOLVED_FREEZE_SHA256"
  ACTIVE_ADATA_SHA="$EXPECTED_ADATA_SHA"
  ACTIVE_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256"
  CUTOVER_STEP=resume_preserved_no_receipt_transaction
  activation_snapshot_set_phase "$EXPECTED_SHA" runtime-units-installed || \
    return 1
  CUTOVER_STEP=persist_deployed_receipt_pending
  persist_deployed_receipt_pending || return 1
  CUTOVER_STEP=finalize_activation_journal
  controlled_guard_finalize_successful_activation "$EXPECTED_SHA" \
    "$V2_FORWARD_PRESERVED_MAIN_RECORD" \
    "$V2_FORWARD_PRESERVED_SCHEDULER_RECORD" \
    "$V2_FORWARD_PRESERVED_AI_SERVICE_RECORD" \
    "$V2_FORWARD_PRESERVED_AI_TIMER_RECORD" || return 1
  CUTOVER_STEP=write_verified_activation_receipt
  publish_deployed_receipt_pending "$EXPECTED_SHA" || return 1
  CUTOVER_STEP=grant_qmt_windows_edge_activation
  qmt_activation_output="$(controlled_guard_run_qmt_activation_tool \
    "$PREPARED_CODE_ROOT" "$RELEASE_VENV_ROOT/$EXPECTED_SHA" \
    "$EXPECTED_SHA" --activation-grant-latest)" || return 1
  printf '%s\n' "$qmt_activation_output"
  printf '%s' "$qmt_activation_output" | \
    controlled_guard_validate_qmt_activation_json \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" "$EXPECTED_SHA" \
      activation-grant-latest || return 1
  DEPLOY_SUCCEEDED=1
  trap '' TERM INT HUP
  CUTOVER_STEP=remove_finalized_activation_journal
  activation_snapshot_remove_finalized_before_deploy || return 1
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
      PROBIGA_STRATEGY_GOVERNANCE_MODE="$STRATEGY_GOVERNANCE_MODE" \
      QMT_ANNOUNCEMENT_CHECKPOINT_DIR="$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" \
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
run_prepared_scheduler_tool() {
  local executor_role="$1"
  shift
  case "$executor_role" in
    linux_provider|linux_standalone) ;;
    *) return 2 ;;
  esac
  (
    cd "$PREPARED_CODE_ROOT" || return 1
    sudo -u "$SERVICE_USER" /usr/bin/env -i \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      GIT_OPTIONAL_LOCKS=0 \
      PYTHONDONTWRITEBYTECODE=1 \
      PYTHONSAFEPATH=1 \
      PROBIGA_DEPLOYMENT_MODE=production \
      PROBIGA_STRATEGY_GOVERNANCE_MODE="$STRATEGY_GOVERNANCE_MODE" \
      PROBIGA_SCHEDULER_EXECUTOR_ROLE="$executor_role" \
      QMT_ANNOUNCEMENT_CHECKPOINT_DIR="$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" \
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
start_release_data_readiness_observer() {
  # This observer is intentionally outside the activation transaction.  It
  # may take hours for both scheduler hosts to produce exact-build evidence,
  # so enqueue a hardened transient service and never make code rollback wait
  # for data completion.
  local parent_root=/var/lib/probiga
  local protected_env=/opt/ProBigA/.env
  local service_group
  local status_file
  local unit_name
  local observer_entrypoint
  local observer_python
  test "${DEPLOY_SUCCEEDED:-0}" -eq 1 || return 1
  [[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || return 1
  test "$PREPARED_CODE_ROOT" = "$CODE_RELEASE_ROOT/$EXPECTED_SHA" || return 1
  service_group="$(id -gn "$SERVICE_USER")" || return 1
  test -n "$service_group" || return 1
  status_file="$RELEASE_DATA_READINESS_STATUS_ROOT/$EXPECTED_SHA.json"
  unit_name="probiga-release-data-readiness-$EXPECTED_SHA.service"
  observer_entrypoint="$PREPARED_CODE_ROOT/tools/wait_release_data_readiness.py"
  observer_python="$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python"

  test -d "$parent_root" || return 1
  test ! -L "$parent_root" || return 1
  test "$(readlink -f -- "$parent_root")" = "$parent_root" || return 1
  test "$(stat -c '%U:%G' -- "$parent_root")" = root:root || return 1
  test "$(stat -c '%a' -- "$parent_root")" = 755 || return 1
  if [ -e "$RELEASE_DATA_READINESS_STATUS_ROOT" ] || \
    [ -L "$RELEASE_DATA_READINESS_STATUS_ROOT" ]; then
    test -d "$RELEASE_DATA_READINESS_STATUS_ROOT" || return 1
    test ! -L "$RELEASE_DATA_READINESS_STATUS_ROOT" || return 1
    test "$(readlink -f -- "$RELEASE_DATA_READINESS_STATUS_ROOT")" = \
      "$RELEASE_DATA_READINESS_STATUS_ROOT" || return 1
    test "$(stat -c '%U:%G' -- \
      "$RELEASE_DATA_READINESS_STATUS_ROOT")" = root:root || return 1
    test "$(stat -c '%a' -- \
      "$RELEASE_DATA_READINESS_STATUS_ROOT")" = 755 || return 1
  else
    install -d -o root -g root -m 0755 \
      "$RELEASE_DATA_READINESS_STATUS_ROOT" || return 1
  fi

  # The immutable checkout has no .env.  Prove that the existing production
  # secret file is readable only through the service group; it is loaded by
  # the observer process and is never copied into an argument, unit property,
  # public receipt, or deploy-user output.
  test -d /opt/ProBigA || return 1
  test ! -L /opt/ProBigA || return 1
  test "$(readlink -f -- /opt/ProBigA)" = /opt/ProBigA || return 1
  test "$(stat -c '%U:%G' -- /opt/ProBigA)" = root:root || return 1
  test "$(stat -c '%a' -- /opt/ProBigA)" = 755 || return 1
  test -f "$protected_env" || return 1
  test ! -L "$protected_env" || return 1
  test "$(stat -c '%U:%G' -- "$protected_env")" = \
    "root:$service_group" || return 1
  test "$(stat -c '%a' -- "$protected_env")" = 640 || return 1
  test "$(stat -c '%h' -- "$protected_env")" = 1 || return 1
  sudo -u "$SERVICE_USER" test -r "$protected_env" || return 1

  test -f "$observer_entrypoint" || return 1
  test ! -L "$observer_entrypoint" || return 1
  test "$(stat -c '%U:%G' -- "$observer_entrypoint")" = root:root || return 1
  sudo -u "$SERVICE_USER" test ! -w "$observer_entrypoint" || return 1
  test -x "$observer_python" || return 1
  test -x /usr/bin/systemd-run || return 1
  test -x /usr/bin/flock || return 1

  if [ -e "$status_file" ] || [ -L "$status_file" ]; then
    test -f "$status_file" || return 1
    test ! -L "$status_file" || return 1
    test "$(stat -c '%U:%G' -- "$status_file")" = \
      "$SERVICE_USER:$service_group" || return 1
    test "$(stat -c '%a' -- "$status_file")" = 644 || return 1
    test "$(stat -c '%h' -- "$status_file")" = 1 || return 1
  else
    install -o "$SERVICE_USER" -g "$service_group" -m 0644 \
      /dev/null "$status_file" || return 1
  fi

  # An idempotent same-build deployment may find the exact observer still
  # active.  Reuse it only after proving its service identity and immutable
  # entrypoint; otherwise a completed --collect unit is recreated below.
  if systemctl is-active --quiet "$unit_name"; then
    test "$(systemctl show -p User --value "$unit_name")" = \
      "$SERVICE_USER" || return 1
    systemctl show -p ExecStart --value "$unit_name" | \
      grep -F -- "$observer_python" >/dev/null || return 1
    systemctl show -p ExecStart --value "$unit_name" | \
      grep -F -- "$observer_entrypoint" >/dev/null || return 1
    systemctl show -p ExecStart --value "$unit_name" | \
      grep -F -- "$EXPECTED_SHA" >/dev/null || return 1
    return 0
  fi

  truncate -s 0 -- "$status_file" || return 1
  sync -f "$status_file" || return 1
  sync -f "$RELEASE_DATA_READINESS_STATUS_ROOT" || return 1

  /usr/bin/systemd-run \
    --unit="$unit_name" \
    --description="ProBigA exact-build release data readiness observer" \
    --quiet --no-block --collect --service-type=exec \
    --uid="$SERVICE_USER" --gid="$service_group" \
    --working-directory="$PREPARED_CODE_ROOT" \
    --property=Restart=always \
    --property=RestartSec=300 \
    --property='RestartPreventExitStatus=3 4' \
    --property=StartLimitIntervalSec=0 \
    --property=RuntimeMaxSec=21900 \
    --property=TimeoutStopSec=30 \
    --property=KillMode=mixed \
    --property=ProtectSystem=strict \
    --property="ReadWritePaths=$status_file" \
    --property="ReadOnlyPaths=$protected_env" \
    --property=ProtectHome=true \
    --property=NoNewPrivileges=true \
    --property=PrivateTmp=true \
    --property=PrivateDevices=true \
    --property=ProtectKernelTunables=true \
    --property=ProtectKernelModules=true \
    --property=ProtectKernelLogs=true \
    --property=ProtectControlGroups=true \
    --property=ProtectClock=true \
    --property=RestrictRealtime=true \
    --property=RestrictSUIDSGID=true \
    --property=LockPersonality=true \
    --property=CapabilityBoundingSet= \
    --property=AmbientCapabilities= \
    --property='RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6' \
    --property=UMask=0077 \
    --property=Nice=10 \
    --property=IOSchedulingClass=idle \
    /usr/bin/env -i \
      PATH=/usr/sbin:/usr/bin:/sbin:/bin \
      PYTHONDONTWRITEBYTECODE=1 \
      PYTHONSAFEPATH=1 \
      PROBIGA_DEPLOYMENT_MODE=production \
      PROBIGA_STRATEGY_GOVERNANCE_MODE="$STRATEGY_GOVERNANCE_MODE" \
      QMT_ANNOUNCEMENT_CHECKPOINT_DIR="$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" \
      PROBIGA_EXPECTED_GIT_SHA="$EXPECTED_SHA" \
      PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA" \
      PROBIGA_EXPECTED_ADATA_SHA="$EXPECTED_ADATA_SHA" \
      PROBIGA_EXPECTED_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256" \
      PROBIGA_ADATA_SOURCE_DIR="$ADATA_SOURCE" \
      PROBIGA_CODE_ROOT="$PREPARED_CODE_ROOT" \
      PROBIGA_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256" \
      PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" \
      "PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
      "$observer_python" -P "$observer_entrypoint" \
        --local-runtime \
        --expected-build-sha "$EXPECTED_SHA" \
        --timeout-seconds 21600 \
        --poll-seconds 60 \
        --status-file "$status_file" || return 1
  return 0
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
prepared_qmt_announcement_snapshot() {
  local action="$1"
  local entrypoint="$PREPARED_CODE_ROOT/tools/add_qmt_announcement_task.py"
  local snapshot="$2"
  case "$action" in restore|verify) ;;
    *) echo "prepared_qmt_announcement_snapshot invalid_action" >&2; return 1 ;;
  esac
  if [ "$PREPARED_CODE_ROOT" != "$CODE_RELEASE_ROOT/$EXPECTED_SHA" ] || \
    [ ! -d "$PREPARED_CODE_ROOT" ] || [ -L "$PREPARED_CODE_ROOT" ]; then
    echo "prepared_qmt_announcement_snapshot invalid_code_root" >&2
    return 1
  fi
  if [ ! -f "$entrypoint" ] || [ -L "$entrypoint" ] || \
    [ "$(stat -c '%U:%G' "$entrypoint")" != root:root ] || \
    ! sudo -u "$SERVICE_USER" test ! -w "$entrypoint"; then
    echo "prepared_qmt_announcement_snapshot invalid_entrypoint" >&2
    return 1
  fi
  case "$snapshot" in
    "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE")
      test "$action" = verify || return 1
      ;;
    "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT")
      controlled_guard_assert_file \
        "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA" 600 || return 1
      test "$(<"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA")" = \
        "$(sha256sum "$snapshot" | cut -d' ' -f1)" || return 1
      ;;
    "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT")
      test "$action" = verify || return 1
      activation_snapshot_validate_governance_new || return 1
      ;;
    *) echo "prepared_qmt_announcement_snapshot invalid_snapshot" >&2; return 1 ;;
  esac
  controlled_guard_assert_file "$snapshot" 600 || return 1
  test -s "$snapshot" || return 1
  run_prepared_python_tool "$entrypoint" \
    "--${action}-snapshot" - < "$snapshot" || return 1
  return 0
}
prepared_restore_and_verify_governance_snapshot() {
  # OLD snapshots predate additive scheduler columns.  The authenticated
  # incoming-release recovery helper compares and restores only their sealed
  # projection, so it remains valid after the fenced schema expansion.
  test "$1" = "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" || return 1
  controlled_guard_restore_and_verify_governance_snapshot "$EXPECTED_SHA" \
    "$1"
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
  controlled_guard_governance_contract_snapshot verify "$EXPECTED_SHA" \
    "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" rollback-governance || return 1
  controlled_guard_governance_contract_snapshot verify "$EXPECTED_SHA" \
    "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" rollback-qmt || return 1
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
    [ "$1" = --phase ] && \
    { [ "$2" = cutover ] || [ "$2" = resume ]; } && \
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
      PROBIGA_PREVIOUS_GIT_SHA="$PREVIOUS_RELEASE_REVISION" \
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
validate_initial_database_schema_preflight_json() {
  local python_bin="$1"
  local tool_status="$2"
  "$python_bin" -I -c \
    '
import json
import re
import sys

if (
    len(sys.argv) != 2
    or re.fullmatch(r"(?:0|[1-9][0-9]{0,2})", sys.argv[1]) is None
):
    raise SystemExit(2)
tool_status = int(sys.argv[1])
if tool_status > 255:
    raise SystemExit(2)

def strict_object(pairs):
    result = dict()
    for key, value in pairs:
        if key in result:
            raise SystemExit(2)
        result[key] = value
    return result

def emit_canonical(value):
    json.dump(
        value,
        sys.stdout,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )

payload = json.load(sys.stdin, object_pairs_hook=strict_object)
blocked_stage_reason_codes = dict((
    ("project_environment", "PREFLIGHT_PROJECT_ENVIRONMENT_BLOCKED"),
    ("database_boundary", "PREFLIGHT_DATABASE_BOUNDARY_BLOCKED"),
    ("database_root_execution", "PREFLIGHT_DATABASE_ROOT_EXECUTION_BLOCKED"),
    ("database_admin_credential", "PREFLIGHT_DATABASE_ADMIN_CREDENTIAL_BLOCKED"),
    (
        "database_migrator_credential",
        "PREFLIGHT_DATABASE_MIGRATOR_CREDENTIAL_BLOCKED",
    ),
    (
        "database_credential_separation",
        "PREFLIGHT_DATABASE_CREDENTIAL_SEPARATION_BLOCKED",
    ),
    ("database_tls_ca", "PREFLIGHT_DATABASE_TLS_CA_BLOCKED"),
    (
        "database_engine_construction",
        "PREFLIGHT_DATABASE_ENGINE_CONSTRUCTION_BLOCKED",
    ),
    (
        "database_runtime_connection",
        "PREFLIGHT_DATABASE_RUNTIME_CONNECTION_BLOCKED",
    ),
    ("database_runtime_state", "PREFLIGHT_DATABASE_RUNTIME_STATE_BLOCKED"),
    (
        "database_admin_connection",
        "PREFLIGHT_DATABASE_ADMIN_CONNECTION_BLOCKED",
    ),
    ("database_admin_state", "PREFLIGHT_DATABASE_ADMIN_STATE_BLOCKED"),
    (
        "database_migrator_connection",
        "PREFLIGHT_DATABASE_MIGRATOR_CONNECTION_BLOCKED",
    ),
    ("database_migrator_state", "PREFLIGHT_DATABASE_MIGRATOR_STATE_BLOCKED"),
    (
        "database_duty_separation",
        "PREFLIGHT_DATABASE_DUTY_SEPARATION_BLOCKED",
    ),
    ("dependency_imports", "PREFLIGHT_DEPENDENCY_IMPORTS_BLOCKED"),
    (
        "runtime_identity_transport_boundary",
        "PREFLIGHT_RUNTIME_IDENTITY_TRANSPORT_BOUNDARY_BLOCKED",
    ),
    ("runtime_schema_bundle", "PREFLIGHT_RUNTIME_SCHEMA_BUNDLE_BLOCKED"),
    (
        "scheduler_runtime_schema",
        "PREFLIGHT_SCHEDULER_RUNTIME_SCHEMA_BLOCKED",
    ),
    (
        "scheduler_task_history_schema",
        "PREFLIGHT_SCHEDULER_TASK_HISTORY_SCHEMA_BLOCKED",
    ),
    (
        "direct_acquisition_progress_schema",
        "PREFLIGHT_DIRECT_ACQUISITION_PROGRESS_SCHEMA_BLOCKED",
    ),
    ("qmt_reference_schema", "PREFLIGHT_QMT_REFERENCE_SCHEMA_BLOCKED"),
    ("v3_migration_plan", "PREFLIGHT_V3_MIGRATION_PLAN_BLOCKED"),
    ("qmt_attestation_schema", "PREFLIGHT_QMT_ATTESTATION_SCHEMA_BLOCKED"),
    (
        "qmt_history_coverage_schema",
        "PREFLIGHT_QMT_HISTORY_COVERAGE_SCHEMA_BLOCKED",
    ),
    (
        "strategy_governance_schema",
        "PREFLIGHT_STRATEGY_GOVERNANCE_SCHEMA_BLOCKED",
    ),
    ("dynamic_shadow_schema", "PREFLIGHT_DYNAMIC_SHADOW_SCHEMA_BLOCKED"),
    ("pit_fact_schema", "PREFLIGHT_PIT_FACT_SCHEMA_BLOCKED"),
    (
        "release_trigger_contract",
        "PREFLIGHT_RELEASE_TRIGGER_CONTRACT_BLOCKED",
    ),
    ("unclassified", "PREFLIGHT_UNCLASSIFIED_BLOCKED"),
))
blocked_fields = frozenset((
    "status", "phase", "reason", "diagnostic_schema",
    "preflight_substage", "reason_code", "global_trust_changed",
    "trust_restoration_verified", "restore_primary_verified",
    "restore_secondary_verified", "restore_fresh_admin_verified",
    "runtime_trust_off_verified", "runtime_privileges_changed",
    "automatic_real_order_submission",
))
if isinstance(payload, dict) and payload.get("status") == "blocked":
    substage = payload.get("preflight_substage")
    blocked_ok = (
        set(payload) == blocked_fields
        and payload.get("phase") == "preflight"
        and payload.get("reason")
        == "database schema preparation failed closed"
        and payload.get("diagnostic_schema")
        == "probiga.strategy-governance-preflight-diagnostic.v1"
        and isinstance(substage, str)
        and payload.get("reason_code")
        == blocked_stage_reason_codes.get(substage)
        and all(
            type(payload.get(name)) is bool
            for name in (
                "global_trust_changed", "trust_restoration_verified",
                "restore_primary_verified", "restore_secondary_verified",
                "restore_fresh_admin_verified", "runtime_trust_off_verified",
                "runtime_privileges_changed",
                "automatic_real_order_submission",
            )
        )
        and payload.get("global_trust_changed") is False
        and payload.get("runtime_privileges_changed") is False
        and payload.get("automatic_real_order_submission") is False
    )
    if not blocked_ok or tool_status != 2:
        raise SystemExit(2)
    emit_canonical(payload)
    raise SystemExit(0)
bundle = (
    payload.get("runtime_schema_bundle")
    if isinstance(payload, dict) else None
)
governance_recovery = (
    payload.get("governance_cutover_recovery")
    if isinstance(payload, dict) else None
)
expected_planners = [
    "ai_bridge",
    "analysis_output",
    "recommended_run_history",
    "sim_trade",
    "qmt_catalog",
    "qmt_audit",
]
expected_bundle_hash = (
    "61f9ddfb3179f30c9976a090fce00adb8613d4e38d698c6cfc954f957084845f"
)
plans = bundle.get("recovery_plans") if isinstance(bundle, dict) else None
contracts = bundle.get("contracts") if isinstance(bundle, dict) else None
permission_summary = (
    payload.get("runtime_grant_summary")
    if isinstance(payload, dict) else None
)
validator_names = (
    bundle.get("validator_names") if isinstance(bundle, dict) else None
)
def valid_hash(value):
    return (
        isinstance(value, str)
        and re.fullmatch(r"[0-9a-f]{64}", value) is not None
    )

def recovery_hashes_exact(plan):
    if not isinstance(plan, dict) or not valid_hash(plan.get("plan_sha256")):
        return False
    canonical = plan["plan_sha256"]
    recovery_bundle = plan.get("recovery_bundle_sha256")
    atomic = plan.get("atomic_plan_sha256")
    if recovery_bundle is not None and (
        not valid_hash(recovery_bundle) or recovery_bundle != canonical
    ):
        return False
    if atomic is not None and (
        not valid_hash(atomic)
        or (recovery_bundle is None and atomic != canonical)
    ):
        return False
    return True

recognized_runtime_contract = (
    isinstance(payload, dict)
    and payload.get("permission_audit_status")
    == "SKIPPED_BY_USER_AUTHORIZATION"
    and payload.get("permission_audit_verified") is False
    and payload.get("runtime_privilege_boundary_verified") is False
    and payload.get("runtime_least_privilege_verified") is False
    and payload.get("runtime_legacy_ddl_compatibility") is False
    and payload.get("runtime_current_user")
    == "probiga_runtime@127.0.0.1"
    and payload.get("runtime_session_user")
    == "probiga_runtime@127.0.0.1"
    and payload.get("runtime_tls_verified") is True
    and payload.get("runtime_grant_count") is None
    and payload.get("runtime_grant_contract_hash") == ""
    and isinstance(permission_summary, dict)
    and set(permission_summary) == {
        "permission_audit_status", "permission_audit_verified",
        "runtime_grant_count", "runtime_grant_contract_hash",
    }  # permission_summary_keys
    and permission_summary.get("permission_audit_status")
    == payload.get("permission_audit_status")
    and permission_summary.get("permission_audit_verified")
    is payload.get("permission_audit_verified")
    and permission_summary.get("runtime_grant_count")
    is payload.get("runtime_grant_count")
    and permission_summary.get("runtime_grant_contract_hash")
    == payload.get("runtime_grant_contract_hash")
)
contracts_exact = (
    isinstance(validator_names, list)
    and validator_names == list(dict.fromkeys(validator_names))
    and isinstance(contracts, dict)
    and set(contracts) == set(validator_names)
    and bundle.get("validator_count") == 33
    and len(validator_names) == 33
    and bundle.get("contract_count") == len(contracts)
    and all(
        isinstance(item, dict)
        and item.get("status") in {"READY", "MIGRATION_REQUIRED"}
        and item.get("read_only") is True
        for item in contracts.values()
    )
)
recovery_exact = (
    isinstance(bundle, dict)
    and bundle.get("recovery_planner_count") == 6
    and bundle.get("recovery_planner_names") == expected_planners
    and bundle.get("recovery_plan_count") == 6
    and isinstance(plans, dict)
    and set(plans) == set(expected_planners)
    and all(
        isinstance(plans.get(name), dict)
        and plans[name].get("status") == "PLANNED"
        and plans[name].get("read_only") is True
        and plans[name].get("ready_for_privileged_apply") is True
        and recovery_hashes_exact(plans[name])
        for name in expected_planners
    )
    and bundle.get("recovery_ready_for_privileged_apply") is True
)
governance_recovery_exact = (
    isinstance(governance_recovery, dict)
    and governance_recovery.get("schema")
    == "probiga.strategy-governance-cutover-recovery.v1"
    and governance_recovery.get("status")
    in {"CUTOVER_READY", "RESUME_REQUIRED", "SEALED"}
    and governance_recovery.get("read_only") is True
    and type(governance_recovery.get("full_migration_marker_present"))
    is bool
    and type(governance_recovery.get("full_migration_marker_hash_verified"))
    is bool
    and type(governance_recovery.get("expected_trigger_count")) is int
    and type(governance_recovery.get("installed_trigger_count")) is int
    and type(governance_recovery.get("missing_trigger_count")) is int
    and governance_recovery["expected_trigger_count"] >= 0
    and governance_recovery["installed_trigger_count"] >= 0
    and governance_recovery["missing_trigger_count"] >= 0
    and governance_recovery["installed_trigger_count"]
    + governance_recovery["missing_trigger_count"]
    == governance_recovery["expected_trigger_count"]
    and governance_recovery.get("full_migration_marker_hash_verified")
    is governance_recovery.get("full_migration_marker_present")
    and type(governance_recovery.get("resume_required")) is bool
    and governance_recovery.get("resume_required")
    is (
        governance_recovery.get("full_migration_marker_hash_verified")
        and governance_recovery["missing_trigger_count"] > 0
    )
    and governance_recovery.get("status")
    == (
        "RESUME_REQUIRED"
        if governance_recovery.get("resume_required")
        else (
            "SEALED"
            if governance_recovery.get("full_migration_marker_hash_verified")
            else "CUTOVER_READY"
        )
    )
)
ok = (
    isinstance(payload, dict)
    and payload.get("status") == "ok"
    and payload.get("phase") == "preflight"
    and recognized_runtime_contract
    and payload.get("routine_inventory_audit_status")
    == "SKIPPED_BY_USER_AUTHORIZATION"
    and payload.get("runtime_self_definer_routine_count") is None
    and payload.get("migrator_self_definer_routine_count") is None
    and payload.get("runtime_definer_routine_count") is None
    and payload.get("runtime_definer_routine_inventory_verified") is False
    and payload.get("runtime_definer_routine_inventory_complete") is False
    and payload.get("runtime_definer_routine_inventory_authority") == ""
    and payload.get("runtime_definer_routine_inventory_schemas") == []
    and payload.get("runtime_privileges_changed") is False
    and payload.get("global_trust_changed") is False
    and payload.get("trust_restoration_verified") is True
    and payload.get("automatic_real_order_submission") is False
    and isinstance(bundle, dict)
    and bundle.get("schema")
    == "probiga.production-runtime-schema-bundle.v1"
    and bundle.get("contract_hash") == expected_bundle_hash
    and bundle.get("migration_count") == 30
    and bundle.get("seed_count") == 3
    and bundle.get("trigger_installation_policy")
    == "FROZEN_RELEASE_BROKER_ONLY"
    and bundle.get("broker_owned_trigger_migration_names") == [
        "qmt_stock_catalog_truth",
        "qmt_trade_calendar",
        "market_field_capture",
        "auxiliary_runtime",
    ]
    and bundle.get("read_only") is True
    and contracts_exact
    and recovery_exact
    and governance_recovery_exact
    and type(bundle.get("migration_required")) is bool
    and bundle.get("migration_required")
    is (
        any(item.get("status") != "READY" for item in contracts.values())
        or not bundle.get("recovery_ready_for_privileged_apply")
    )
)
if not ok or tool_status != 0:
    raise SystemExit(2)
emit_canonical(payload)
' "$tool_status"
}
run_database_boundary_bootstrap() {
  local action="$1"
  local entrypoint="$PREPARED_CODE_ROOT/deploy/production_db_boundary_bootstrap.py"
  case "$action" in
    prepare|commit|rollback|verify) ;;
    *) echo "database boundary bootstrap action is not allowlisted" >&2; return 2 ;;
  esac
  test -f "$entrypoint"
  test ! -L "$entrypoint"
  test "$(stat -c '%F' -- "$entrypoint")" = "regular file"
  test "$(stat -c '%U:%G' -- "$entrypoint")" = root:root
  test "$(stat -c '%h' -- "$entrypoint")" = 1
  test $((8#$(stat -c '%a' -- "$entrypoint") & 8#022)) -eq 0
  sudo -u "$SERVICE_USER" test ! -w "$entrypoint"
  sudo -u probiga-deploy test ! -w "$entrypoint"
  /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/root LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
    "$BOOTSTRAP_PYTHON" -I "$entrypoint" "$action"
}
run_initial_database_schema_preflight() {
  local output
  local validated_output
  local tool_status=0
  local validation_status=0
  case "$STRATEGY_GOVERNANCE_MODE" in
    DEFERRED_DB) ;;
    REQUIRED)
      CUTOVER_STEP=prepare_production_database_boundary
      run_database_boundary_bootstrap prepare
      ;;
    *)
      echo "initial database schema preflight mode is unsupported" >&2
      return 2
      ;;
  esac
  CUTOVER_STEP=preflight_strategy_governance_database_schema
  output="$(
    run_prepared_database_migration_tool \
      "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_schema.py" \
      --phase preflight
  )" || tool_status=$?
  validated_output="$(printf '%s' "$output" \
    | validate_initial_database_schema_preflight_json \
        "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" \
        "$tool_status")" || validation_status=$?
  test "$validation_status" -eq 0 || return "$validation_status"
  output="$validated_output"
  printf '%s\n' "$output"
  test "$tool_status" -eq 0 || return "$tool_status"
}
select_fenced_strategy_governance_schema_phase() {
  local output
  local selected_phase
  local validated_output
  local tool_status=0
  local validation_status=0
  output="$(
    run_prepared_database_migration_tool \
      "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_schema.py" \
      --phase preflight
  )" || tool_status=$?
  validated_output="$(printf '%s' "$output" \
    | validate_initial_database_schema_preflight_json \
        "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" \
        "$tool_status")" || validation_status=$?
  test "$validation_status" -eq 0 || return "$validation_status"
  output="$validated_output"
  printf '%s\n' "$output"
  test "$tool_status" -eq 0 || return "$tool_status"
  selected_phase="$(printf '%s' "$output" \
    | "$BOOTSTRAP_PYTHON" -I -c \
      'import json,sys; p=json.load(sys.stdin); r=p["governance_cutover_recovery"]; print("resume" if r["resume_required"] is True else "cutover")')"
  case "$selected_phase" in
    cutover|resume) ;;
    *)
      echo "fenced governance schema phase is invalid" >&2
      return 2
      ;;
  esac
  FENCED_STRATEGY_GOVERNANCE_SCHEMA_PHASE="$selected_phase"
}
assert_deferred_scheduler_process_cmdline() {
  local scheduler_pid="$1"
  local expected_sha="$2"
  local expected_code_root="$3"
  local -a scheduler_argv=()
  case "$scheduler_pid" in ''|0|*[!0-9]*) return 1 ;; esac
  [[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
  test "$expected_code_root" = "$CODE_RELEASE_ROOT/$expected_sha" || return 1
  mapfile -d '' -t scheduler_argv < "/proc/$scheduler_pid/cmdline" || return 1
  test "${#scheduler_argv[@]}" -eq 3 || return 1
  test "${scheduler_argv[0]}" = \
    "$RELEASE_VENV_ROOT/$expected_sha/bin/python" || return 1
  test "${scheduler_argv[1]}" = -P || return 1
  test "${scheduler_argv[2]}" = \
    "$expected_code_root/tools/run_scheduler_daemon.py" || return 1
}
capture_deferred_scheduler_identity() {
  local main_pid
  local scheduler_active_state
  local scheduler_unit_file_state
  local scheduler_pid
  local observed_expected_sha
  local observed_build_sha
  local observed_code_root
  test "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB || return 1
  scheduler_active_state="$(systemctl show -p ActiveState --value \
    probiga-scheduler)" || return 1
  scheduler_unit_file_state="$(systemctl show -p UnitFileState --value \
    probiga-scheduler)" || return 1
  scheduler_pid="$(systemctl show -p MainPID --value probiga-scheduler)" || \
    return 1
  case "$scheduler_active_state:$scheduler_unit_file_state" in
    active:enabled)
      case "$scheduler_pid" in ''|0|*[!0-9]*) return 1 ;; esac
      observed_expected_sha="$(tr '\0' '\n' \
        < "/proc/$scheduler_pid/environ" | sed -n \
        's/^PROBIGA_EXPECTED_GIT_SHA=//p' | tail -n 1)"
      observed_build_sha="$(tr '\0' '\n' \
        < "/proc/$scheduler_pid/environ" | sed -n \
        's/^PROBIGA_BUILD_COMMIT_SHA=//p' | tail -n 1)"
      observed_code_root="$(tr '\0' '\n' \
        < "/proc/$scheduler_pid/environ" | sed -n \
        's/^PROBIGA_CODE_ROOT=//p' | tail -n 1)"
      [[ "$observed_expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
      test "$observed_expected_sha" != "0000000000000000000000000000000000000000" || \
        return 1
      test "$observed_build_sha" = "$observed_expected_sha" || return 1
      test "$observed_code_root" = \
        "$CODE_RELEASE_ROOT/$observed_expected_sha" || return 1
      assert_deferred_scheduler_process_cmdline "$scheduler_pid" \
        "$observed_expected_sha" "$observed_code_root" || return 1
      ;;
    inactive:disabled)
      test "$scheduler_pid" = 0 || return 1
      main_pid="$(systemctl show -p MainPID --value "$MAIN_SERVICE")" || \
        return 1
      case "$main_pid" in ''|0|*[!0-9]*) return 1 ;; esac
      observed_expected_sha="$(tr '\0' '\n' \
        < "/proc/$main_pid/environ" | sed -n \
        's/^PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA=//p' | tail -n 1)"
      observed_code_root="$(tr '\0' '\n' \
        < "/proc/$main_pid/environ" | sed -n \
        's/^PROBIGA_DEFERRED_SCHEDULER_CODE_ROOT=//p' | tail -n 1)"
      [[ "$observed_expected_sha" =~ ^[0-9a-f]{40}$ ]] || return 1
      test "$observed_code_root" = \
        "$CODE_RELEASE_ROOT/$observed_expected_sha" || return 1
      ;;
    *) return 1 ;;
  esac
  # Retain the sealed prior auxiliary identity for rollback and AI-worker
  # attestation only.  During DEFERRED_DB no scheduler process may use it.
  DEFERRED_SCHEDULER_EXPECTED_SHA="$observed_expected_sha"
  DEFERRED_SCHEDULER_CODE_ROOT="$observed_code_root"
}
fence_deferred_release_writers() {
  local writer_fence_result
  test "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB || return 1
  writer_fence_result="$(run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/add_trading_v3_tasks.py" \
    --deferred-release-fence-only)" || \
    return 1
  printf '%s\n' "$writer_fence_result"
  printf '%s' "$writer_fence_result" | "$BOOTSTRAP_PYTHON" -I -c \
    'import json,sys; p=json.load(sys.stdin); t=p.get("fenced_task_types") if isinstance(p,dict) else None; expected={"trading_v3_counterfactual_audit","trading_v3_continuous_calibration","trading_v3_close_decision","trading_v3_premarket_review"}; ok=isinstance(p,dict) and p.get("status")=="ok" and p.get("mode")=="deferred-release-fence-only" and p.get("writer_fence_active") is True and p.get("layer4_writers_enabled") is False and p.get("paper_buy_writers_enabled") is False and p.get("fenced_row_count")==4 and isinstance(t,list) and len(t)==4 and set(t)==expected and p.get("tasks")==[]; raise SystemExit(0 if ok else 2)'
}
assert_deferred_database_runtime() {
  local deferred_v3_endpoint
  local governance_response
  local health_response
  local admin_header
  local main_pid
  local scheduler_pid
  test "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB || return 1
  systemctl is-active --quiet "$MAIN_SERVICE" || return 1
  test "$(systemctl show -p ActiveState --value probiga-scheduler)" = \
    inactive || return 1
  test "$(systemctl show -p UnitFileState --value probiga-scheduler)" = \
    disabled || return 1
  main_pid="$(systemctl show -p MainPID --value "$MAIN_SERVICE")" || return 1
  scheduler_pid="$(systemctl show -p MainPID --value probiga-scheduler)" || \
    return 1
  case "$main_pid" in ''|0|*[!0-9]*) return 1 ;; esac
  test "$scheduler_pid" = 0 || return 1
  grep -zFx -- "PROBIGA_EXPECTED_GIT_SHA=$EXPECTED_SHA" \
    "/proc/$main_pid/environ" >/dev/null || return 1
  grep -zFx -- "PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
    "/proc/$main_pid/environ" >/dev/null || return 1
  grep -zFx -- 'PROBIGA_STRATEGY_GOVERNANCE_MODE=DEFERRED_DB' \
    "/proc/$main_pid/environ" >/dev/null || return 1
  grep -zFx -- 'PROBIGA_STRATEGY_GOVERNANCE_BASE_SCHEMA_READY=true' \
    "/proc/$main_pid/environ" >/dev/null || return 1
  grep -zFx -- \
    "PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA=$DEFERRED_SCHEDULER_EXPECTED_SHA" \
    "/proc/$main_pid/environ" >/dev/null || return 1
  grep -zFx -- \
    "PROBIGA_DEFERRED_SCHEDULER_CODE_ROOT=$DEFERRED_SCHEDULER_CODE_ROOT" \
    "/proc/$main_pid/environ" >/dev/null || return 1
  health_response="$(mktemp)" || return 1
  governance_response="$(mktemp)" || {
    rm -f -- "$health_response"
    return 1
  }
  admin_header="$(mktemp)" || {
    rm -f -- "$health_response" "$governance_response"
    return 1
  }
  chown "$SERVICE_USER:$SERVICE_USER" "$admin_header" || {
    rm -f -- "$health_response" "$governance_response" "$admin_header"
    return 1
  }
  chmod 0600 "$health_response" "$governance_response" "$admin_header" || {
    rm -f -- "$health_response" "$governance_response" "$admin_header"
    return 1
  }
  if ! write_admin_auth_header_file "$admin_header"; then
    rm -f -- "$health_response" "$governance_response" "$admin_header"
    return 1
  fi
  if ! curl --fail-with-body --silent --show-error --retry 45 \
      --retry-all-errors --retry-delay 2 --retry-max-time 120 \
      --retry-connrefused \
      --output "$health_response" http://127.0.0.1/api/health || \
    ! "$BOOTSTRAP_PYTHON" -I - "$health_response" "$EXPECTED_SHA" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
expected_sha = sys.argv[2]
expected_code_root = f"/opt/ProBigA-releases/{expected_sha}"
revision = payload.get("release_revision")
standalone = payload.get("standalone_scheduler")
identity = standalone.get("release_identity") if isinstance(standalone, dict) else None
valid = (
    isinstance(payload, dict)
    and payload.get("status") == "degraded"
    and payload.get("strategy_governance_mode") == "DEFERRED_DB"
    and payload.get("base_schema_ready") is True
    and payload.get("schema_ready") is False
    and payload.get("governance_ready") is False
    and payload.get("activation_enabled") is False
    and payload.get("automatic_real_order_submission") is False
    and payload.get("real_order_authority") is False
    and isinstance(revision, dict)
    and revision.get("expected_git_sha") == expected_sha
    and revision.get("matches_expected") is True
    and revision.get("code_worktree_clean") is True
    and isinstance(standalone, dict)
    and standalone.get("verified") is True
    and standalone.get("fenced") is True
    and standalone.get("active") is False
    and standalone.get("state") == "inactive"
    and standalone.get("enabled") is False
    and standalone.get("enablement_state") == "disabled"
    and standalone.get("pid") == 0
    and isinstance(identity, dict)
    and identity.get("ready") is True
    and identity.get("identity_mode") == "FENCED_DEFERRED"
    and identity.get("api_build_sha") == expected_sha
    and identity.get("expected_build_sha") == expected_sha
    and identity.get("expected_code_root") == expected_code_root
    and identity.get("observed_build_sha") is None
    and identity.get("observed_code_root") is None
    and identity.get("same_build_as_api") is None
)
raise SystemExit(0 if valid else 2)
PY
  then
    echo "deferred_runtime_gate_failed gate=health_contract" >&2
    cat "$health_response" >&2
    rm -f -- "$health_response" "$governance_response" "$admin_header"
    return 1
  fi
  if ! curl --fail-with-body --silent --show-error --retry 5 \
      --retry-all-errors --retry-delay 1 --retry-connrefused \
      --header @"$admin_header" \
      --output "$governance_response" \
      http://127.0.0.1/api/strategy-center/governance || \
    ! "$BOOTSTRAP_PYTHON" -I - "$governance_response" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
allocations = payload.get("allocations")
pools = payload.get("pools")
valid = (
    isinstance(payload, dict)
    and payload.get("status") == "degraded"
    and payload.get("strategy_governance_mode") == "DEFERRED_DB"
    and payload.get("base_schema_ready") is True
    and payload.get("result_mode") == "CANONICAL_UNAVAILABLE"
    and payload.get("activation_enabled") is False
    and payload.get("schema_ready") is False
    and payload.get("strategies") == []
    and payload.get("combinations") == []
    and pools == {"observation": [], "confirmation": [], "tradable": []}
    and isinstance(allocations, list)
    and len(allocations) == 1
    and allocations[0].get("target_type") == "CASH"
    and float(allocations[0].get("simulated_weight_pct")) == 100.0
    and payload.get("automatic_real_order_submission") is False
    and payload.get("real_order_authority") is False
)
raise SystemExit(0 if valid else 2)
PY
  then
    echo "deferred_runtime_gate_failed gate=governance_contract" >&2
    cat "$governance_response" >&2
    rm -f -- "$health_response" "$governance_response" "$admin_header"
    return 1
  fi
  for deferred_v3_endpoint in context readiness stock-pool; do
    if ! curl --fail-with-body --silent --show-error --retry 5 \
        --retry-all-errors --retry-delay 1 --retry-connrefused \
        --header @"$admin_header" \
        --output "$governance_response" \
        "http://127.0.0.1/api/v3/$deferred_v3_endpoint" || \
      ! "$BOOTSTRAP_PYTHON" -I - "$governance_response" \
        "$deferred_v3_endpoint" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
endpoint = sys.argv[2]
data = payload.get("data") if isinstance(payload, dict) else None
valid = (
    isinstance(data, dict)
    and data.get("strategy_governance_mode") == "DEFERRED_DB"
    and data.get("governance_deferred") is True
    and data.get("activation_enabled") is False
    and data.get("actionable_output_allowed") is False
)
if endpoint == "context":
    valid = (
        valid
        and payload.get("status") == "blocked"
        and data.get("decision_status") == "BLOCKED"
        and data.get("decision_scope") == "RESEARCH_ONLY"
        and data.get("paper_order_authority") == "NONE"
    )
elif endpoint == "readiness":
    valid = (
        valid
        and payload.get("status") == "blocked"
        and data.get("paper_ready") is False
        and data.get("paper_authority_ready") is False
        and data.get("execution_ready") is False
    )
elif endpoint == "stock-pool":
    items = data.get("items")
    valid = (
        valid
        and isinstance(items, list)
        and all(
            isinstance(item, dict)
            and item.get("actionability") == "RESEARCH_ONLY"
            and isinstance(item.get("action_plan"), dict)
            and item["action_plan"].get("actionability") == "RESEARCH_ONLY"
            and item["action_plan"].get("buy_range") is None
            and item["action_plan"].get("sell_range") is None
            and item["action_plan"].get("protective_stop") is None
            for item in items
        )
    )
else:
    valid = False
raise SystemExit(0 if valid else 2)
PY
    then
      echo "deferred_runtime_gate_failed gate=v3_deferred_contract endpoint=$deferred_v3_endpoint" >&2
      cat "$governance_response" >&2
      rm -f -- "$health_response" "$governance_response" "$admin_header"
      return 1
    fi
  done
  rm -f -- "$health_response" "$governance_response" "$admin_header" || \
    return 1
  curl --fail --silent --show-error --retry 3 --retry-all-errors \
    --retry-delay 1 --retry-connrefused \
    http://127.0.0.1/api/health/runtime >/dev/null || {
      echo "deferred_runtime_gate_failed gate=runtime_health" >&2
      return 1
    }
  run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_deferred_schema.py" \
    --verify >/dev/null || {
      echo "deferred_runtime_gate_failed gate=deferred_schema_verify" >&2
      return 1
    }
  run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
    --deferred-disable >/dev/null || {
      echo "deferred_runtime_gate_failed gate=governance_task_disabled" >&2
      return 1
    }
  fence_deferred_release_writers >/dev/null || {
    echo "deferred_runtime_gate_failed gate=deferred_writer_fence" >&2
    return 1
  }
  run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/verify_trading_v3_production.py" \
    --real-trading-closed-only >/dev/null || {
      echo "deferred_runtime_gate_failed gate=real_trading_closed" >&2
      return 1
    }
  assert_nginx_static_matches_checkout "$PREPARED_CODE_ROOT" || {
    echo "deferred_runtime_gate_failed gate=static_release_identity" >&2
    return 1
  }
  verify_account_login_api_and_page_smoke "$EXPECTED_SHA" || {
    echo "deferred_runtime_gate_failed gate=account_login" >&2
    return 1
  }
  return 0
}
rollback_deferred_database_release() {
  local failed_status="${1:-2}"
  local preserve_same_sha_scheduler_fence=0
  local scheduler_safe_to_start=1
  local rollback_failed=0
  trap - ERR TERM INT HUP
  set +e
  echo "Deferred database release failed; restoring previous API/static runtime" >&2
  if [ "$PREVIOUS_SHA" = "$EXPECTED_SHA" ]; then
    # The current build's DEFERRED_DB contract requires the scheduler to stay
    # inactive.  Once a same-SHA repair has fenced a legacy scheduler, putting
    # that process back would be an unsafe rollback to a state this very build
    # rejects.  Keep the safe fence and verify the complete health contract.
    preserve_same_sha_scheduler_fence=1
    scheduler_safe_to_start=0
  fi
  systemctl stop "$MAIN_SERVICE" || rollback_failed=1
  systemctl stop probiga-scheduler || rollback_failed=1
  if [ "$GOVERNANCE_TASK_TOUCHED" -eq 1 ] || \
    [ "$CUTOVER_BASE_SCHEMA_STARTED" -eq 1 ]; then
    if ! run_prepared_python_tool \
      "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
      --deferred-disable >/dev/null; then
      scheduler_safe_to_start=0
      rollback_failed=1
    fi
  fi
  if [ "$DEFERRED_RELEASE_WRITER_FENCE_STARTED" -eq 1 ] && \
    ! fence_deferred_release_writers >/dev/null; then
    scheduler_safe_to_start=0
    rollback_failed=1
  fi
  if [ "$PREVIOUS_DROPIN_PRESENT" -eq 1 ]; then
    install -o root -g root -m 0644 "$PREVIOUS_DROPIN" \
      "$MAIN_RELEASE_DROPIN" || rollback_failed=1
  else
    rm -f -- "$MAIN_RELEASE_DROPIN" || rollback_failed=1
  fi
  systemctl daemon-reload || rollback_failed=1
  point_static_release_to_checkout "$PREVIOUS_CODE_ROOT" || rollback_failed=1
  if [ "$PREVIOUS_SCHEDULER_ENABLED" -eq 1 ] && \
    [ "$scheduler_safe_to_start" -eq 1 ]; then
    systemctl enable probiga-scheduler || rollback_failed=1
  else
    systemctl disable probiga-scheduler || rollback_failed=1
  fi
  if [ "$PREVIOUS_SCHEDULER_ACTIVE" -eq 1 ] && \
    [ "$scheduler_safe_to_start" -eq 1 ]; then
    systemctl start probiga-scheduler || rollback_failed=1
  else
    systemctl stop probiga-scheduler || rollback_failed=1
    if [ "$PREVIOUS_SCHEDULER_ACTIVE" -eq 1 ] && \
      [ "$preserve_same_sha_scheduler_fence" -ne 1 ]; then
      rollback_failed=1
    fi
  fi
  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    if [ "$PREVIOUS_AI_WORKER_SERVICE_ACTIVE" -eq 1 ]; then
      systemctl start "$AI_WORKER_SERVICE" || rollback_failed=1
    else
      systemctl stop "$AI_WORKER_SERVICE" || rollback_failed=1
    fi
    if [ "$PREVIOUS_AI_WORKER_TIMER_ACTIVE" -eq 1 ]; then
      systemctl start "$AI_WORKER_TIMER" || rollback_failed=1
    else
      systemctl stop "$AI_WORKER_TIMER" || rollback_failed=1
    fi
  fi
  if [ "$PREVIOUS_MAIN_ACTIVE_STATE" = active ]; then
    systemctl start "$MAIN_SERVICE" || rollback_failed=1
  fi
  if [ "$preserve_same_sha_scheduler_fence" -eq 1 ]; then
    assert_deferred_database_runtime || rollback_failed=1
  elif ! verify_previous_main_health_or_stopped /api/health/runtime 15 2 || \
    ! assert_nginx_static_matches_checkout "$PREVIOUS_CODE_ROOT"; then
    rollback_failed=1
  fi
  if [ "$PREVIOUS_SCHEDULER_ACTIVE" -eq 1 ] && \
    [ "$scheduler_safe_to_start" -eq 1 ]; then
    systemctl is-active --quiet probiga-scheduler || rollback_failed=1
    run_prepared_python_tool \
      "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
      --deferred-disable >/dev/null || rollback_failed=1
  else
    ! systemctl is-active --quiet probiga-scheduler || rollback_failed=1
  fi
  if [ "$PREVIOUS_SCHEDULER_ENABLED" -eq 1 ] && \
    [ "$scheduler_safe_to_start" -eq 1 ]; then
    systemctl is-enabled --quiet probiga-scheduler || rollback_failed=1
  else
    ! systemctl is-enabled --quiet probiga-scheduler || rollback_failed=1
  fi
  ACTIVE_INPUT_LOCK_SHA256="$PREVIOUS_INPUT_LOCK_SHA256"
  ACTIVE_RESOLVED_FREEZE_SHA256="$PREVIOUS_RESOLVED_FREEZE_SHA256"
  ACTIVE_ADATA_SHA="$PREVIOUS_ADATA_SHA"
  ACTIVE_ADATA_TREE_SHA256="$PREVIOUS_ADATA_TREE_SHA256"
  if [ "$rollback_failed" -eq 0 ]; then
    if [ "$preserve_same_sha_scheduler_fence" -eq 1 ]; then
      write_receipt ROLLED_FORWARD_DEFERRED_SCHEDULER_FENCED \
        "$PREVIOUS_SHA" || true
    elif [ "$CUTOVER_BASE_SCHEMA_STARTED" -eq 1 ]; then
      # MySQL DDL may auto-commit before a later statement fails. Keep all
      # additive results and record that the trigger boundary is still absent.
      write_receipt ROLLED_BACK_DEFERRED_SCHEMA_RETAINED "$PREVIOUS_SHA" || true
    elif [ "$GOVERNANCE_TASK_TOUCHED" -eq 1 ]; then
      write_receipt ROLLED_BACK_GOVERNANCE_TASK_DISABLED "$PREVIOUS_SHA" || true
    else
      write_receipt ROLLED_BACK "$PREVIOUS_SHA" || true
    fi
  else
    write_receipt ROLLBACK_FAILED "$PREVIOUS_SHA" || true
  fi
  exit "$failed_status"
}
deploy_deferred_database_release() {
  local scheduler_pid
  local observed_scheduler_expected_sha
  local observed_scheduler_build_sha
  local observed_scheduler_code_root
  local schema_result
  local schema_status
  test "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB
  test "$PREVIOUS_MAIN_ACTIVE_STATE" = active
  case "$PREVIOUS_SCHEDULER_ACTIVE:$PREVIOUS_SCHEDULER_ENABLED" in
    1:1|0:0) ;;
    *) return 1 ;;
  esac
  scheduler_pid="$(systemctl show -p MainPID --value probiga-scheduler)"
  if [ "$PREVIOUS_SCHEDULER_ACTIVE" -eq 1 ]; then
    case "$scheduler_pid" in ''|0|*[!0-9]*) return 1 ;; esac
    observed_scheduler_expected_sha="$(tr '\0' '\n' \
      < "/proc/$scheduler_pid/environ" | sed -n \
      's/^PROBIGA_EXPECTED_GIT_SHA=//p' | tail -n 1)"
    observed_scheduler_build_sha="$(tr '\0' '\n' \
      < "/proc/$scheduler_pid/environ" | sed -n \
      's/^PROBIGA_BUILD_COMMIT_SHA=//p' | tail -n 1)"
    observed_scheduler_code_root="$(tr '\0' '\n' \
      < "/proc/$scheduler_pid/environ" | sed -n \
      's/^PROBIGA_CODE_ROOT=//p' | tail -n 1)"
    [[ "$observed_scheduler_expected_sha" =~ ^[0-9a-f]{40}$ ]]
    test "$observed_scheduler_build_sha" = \
      "$observed_scheduler_expected_sha"
    test "$observed_scheduler_code_root" = \
      "$CODE_RELEASE_ROOT/$observed_scheduler_expected_sha"
    assert_deferred_scheduler_process_cmdline "$scheduler_pid" \
      "$observed_scheduler_expected_sha" "$observed_scheduler_code_root"
  else
    test "$scheduler_pid" = 0
  fi
  if test "$PREVIOUS_SHA" = "$EXPECTED_SHA"; then
    DEFERRED_DB_CUTOVER_STARTED=$((1))
    CUTOVER_STARTED="$DEFERRED_DB_CUTOVER_STARTED"
    CUTOVER_STEP=fence_deferred_scheduler
    systemctl stop probiga-scheduler
    systemctl disable probiga-scheduler
    test "$(systemctl show -p ActiveState --value probiga-scheduler)" = \
      inactive
    test "$(systemctl show -p UnitFileState --value probiga-scheduler)" = \
      disabled
    test "$(systemctl show -p MainPID --value probiga-scheduler)" = 0
    test "$PREVIOUS_CODE_ROOT" = "$PREPARED_CODE_ROOT"
    cmp --silent "$PREVIOUS_DROPIN" "$PREPARED_MAIN_DROPIN"
    run_prepared_python_tool \
      "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_deferred_schema.py" \
      --verify >/dev/null
    run_prepared_python_tool \
      "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
      --deferred-disable >/dev/null
    assert_deferred_database_runtime
    ACTIVE_INPUT_LOCK_SHA256="$EXPECTED_INPUT_LOCK_SHA256"
    ACTIVE_RESOLVED_FREEZE_SHA256="$EXPECTED_RESOLVED_FREEZE_SHA256"
    ACTIVE_ADATA_SHA="$EXPECTED_ADATA_SHA"
    ACTIVE_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256"
    write_receipt DEPLOYED_CODE_ONLY_DEGRADED "$EXPECTED_SHA"
    DEPLOY_SUCCEEDED=1
    trap - ERR TERM INT HUP
    exit 0
  fi
  test -L "$STATIC_RELEASE_LINK"
  test "$(readlink -f "$STATIC_RELEASE_LINK")" = "$PREVIOUS_CODE_ROOT"
  test "${#PREVIOUS_LEGACY_MAIN_DROPINS[@]}" -eq 0
  GOVERNANCE_TASK_OLD_SOURCE="$(mktemp)"
  chown "$SERVICE_USER:$SERVICE_USER" "$GOVERNANCE_TASK_OLD_SOURCE"
  chmod 0600 "$GOVERNANCE_TASK_OLD_SOURCE"
  DEFERRED_DB_CUTOVER_STARTED=$((1))
  CUTOVER_STARTED="$DEFERRED_DB_CUTOVER_STARTED"
  CUTOVER_STEP=stop_deferred_database_writers
  API_STOPPED=1
  systemctl stop "$MAIN_SERVICE"
  ! systemctl is-active --quiet "$MAIN_SERVICE"
  systemctl stop probiga-scheduler
  ! systemctl is-active --quiet probiga-scheduler
  systemctl disable probiga-scheduler
  ! systemctl is-enabled --quiet probiga-scheduler
  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    systemctl stop "$AI_WORKER_TIMER"
    systemctl stop "$AI_WORKER_SERVICE"
  fi
  CUTOVER_STEP=disable_governance_task_for_deferred_database
  run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
    --deferred-disable --snapshot-file "$GOVERNANCE_TASK_OLD_SOURCE"
  GOVERNANCE_TASK_TOUCHED=1
  CUTOVER_STEP=fence_deferred_release_writers
  DEFERRED_RELEASE_WRITER_FENCE_STARTED=1
  fence_deferred_release_writers
  CUTOVER_STEP=prepare_deferred_governance_base_schema
  CUTOVER_BASE_SCHEMA_STARTED=1
  if schema_result="$(run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_deferred_schema.py" \
    --apply --writers-fenced)"; then
    schema_status=0
  else
    schema_status=$?
    # The tool emits a deliberately redacted JSON failure payload.  Print it
    # before ERR handling detaches the SSH transport so a failed production
    # migration is diagnosable without exposing SQL or credentials.
    printf '%s\n' "$schema_result" >&2
    printf 'deploy_failure phase=cutover cutover_step=%s status=%s\n' \
      "$CUTOVER_STEP" "$schema_status" >&2
    return "$schema_status"
  fi
  printf '%s\n' "$schema_result"
  printf '%s' "$schema_result" | "$BOOTSTRAP_PYTHON" -I -c \
    'import json,sys; p=json.load(sys.stdin); ok=isinstance(p,dict) and p.get("status")=="ok" and p.get("mode")=="DEFERRED_DB_BASE_SCHEMA" and p.get("schema_ready_without_triggers") is True and type(p.get("missing_trigger_count")) is int and p["missing_trigger_count"]>0 and p.get("database_triggers_installed") is False; raise SystemExit(0 if ok else 2)'
  CUTOVER_BASE_SCHEMA_APPLIED=1
  CUTOVER_STEP=install_deferred_main_runtime
  install -d -o root -g root -m 0755 "$(dirname "$MAIN_RELEASE_DROPIN")"
  install -o root -g root -m 0644 "$PREPARED_MAIN_DROPIN" \
    "$MAIN_RELEASE_DROPIN"
  sync -f /etc/systemd/system
  systemctl daemon-reload
  controlled_guard_assert_file "$MAIN_RELEASE_DROPIN" 644
  cmp --silent "$MAIN_RELEASE_DROPIN" "$PREPARED_MAIN_DROPIN"
  CUTOVER_STEP=verify_deferred_scheduler_fenced
  test "$(systemctl show -p ActiveState --value probiga-scheduler)" = \
    inactive
  test "$(systemctl show -p UnitFileState --value probiga-scheduler)" = \
    disabled
  test "$(systemctl show -p MainPID --value probiga-scheduler)" = 0
  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    if [ "$PREVIOUS_AI_WORKER_SERVICE_ACTIVE" -eq 1 ]; then
      systemctl start "$AI_WORKER_SERVICE"
    fi
    if [ "$PREVIOUS_AI_WORKER_TIMER_ACTIVE" -eq 1 ]; then
      systemctl start "$AI_WORKER_TIMER"
    fi
  fi
  CUTOVER_STEP=start_deferred_api
  systemctl start "$MAIN_SERVICE"
  systemctl is-active --quiet "$MAIN_SERVICE"
  CUTOVER_STEP=switch_deferred_static_release
  point_static_release_to_checkout "$PREPARED_CODE_ROOT"
  CUTOVER_STEP=verify_deferred_database_release
  assert_deferred_database_runtime
  ACTIVE_INPUT_LOCK_SHA256="$EXPECTED_INPUT_LOCK_SHA256"
  ACTIVE_RESOLVED_FREEZE_SHA256="$EXPECTED_RESOLVED_FREEZE_SHA256"
  ACTIVE_ADATA_SHA="$EXPECTED_ADATA_SHA"
  ACTIVE_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256"
  CUTOVER_STEP=write_deferred_database_receipt
  write_receipt DEPLOYED_CODE_ONLY_DEGRADED "$EXPECTED_SHA"
  DEPLOY_SUCCEEDED=1
  trap - ERR TERM INT HUP
  echo "Deployed $EXPECTED_SHA with deferred trigger boundary; governance remains locked"
  exit 0
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
  local failure_audit_sha=""
  local failure_phase=preparation
  local failure_step="${CUTOVER_STEP:-unknown}"
  if [ "${CUTOVER_STARTED:-0}" -eq 1 ]; then
    failure_phase=cutover
  fi
  # Freeze and seal the original ERR location before any rollback helper can
  # mutate CUTOVER_STEP, inspect a partial transaction, or take an early exit.
  failure_audit_sha="$(persist_deploy_failure_audit "$failure_phase" \
    "$failure_step" "$failed_line" "$failed_status" 2>/dev/null)" || \
    failure_audit_sha=unavailable
  emit_deploy_failure_checkpoint "$failure_phase" "$failure_step" \
    "$failed_line" "$failed_status" "$failure_audit_sha"
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
  local database_boundary_rollback_failed=0
  if [ "${DEFERRED_DB_CUTOVER_STARTED:-0}" -eq 1 ]; then
    rollback_deferred_database_release "$failed_status"
  fi
  if [ -f "$PREPARED_CODE_ROOT/deploy/production_db_boundary_bootstrap.py" ] && \
    ! run_database_boundary_bootstrap rollback >/dev/null 2>&1; then
    database_boundary_rollback_failed=1
  fi
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
    echo "Release preparation failed before the API stop" >&2
    if [ "${PRE_CUTOVER_SCHEDULER_STOPPED:-0}" -eq 1 ] && \
      [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
      if [ "$PREVIOUS_SCHEDULER_ACTIVE" -eq 1 ]; then
        sudo systemctl start probiga-scheduler || rollback_failed=1
        systemctl is-active --quiet probiga-scheduler || rollback_failed=1
      else
        sudo systemctl stop probiga-scheduler || rollback_failed=1
        ! systemctl is-active --quiet probiga-scheduler || rollback_failed=1
      fi
      PRE_CUTOVER_SCHEDULER_STOPPED=0
    fi
    if [ "$DATABASE_FORWARD_MIGRATION_STARTED" -eq 1 ]; then
      echo "Forward-only QMT schema preparation may remain installed; no database object will be rolled back or dropped" >&2
    fi
    current_sha="$(git -C "$PREVIOUS_CODE_ROOT" rev-parse HEAD 2>/dev/null)"
    ACTIVE_INPUT_LOCK_SHA256="$PREVIOUS_INPUT_LOCK_SHA256"
    ACTIVE_RESOLVED_FREEZE_SHA256="$PREVIOUS_RESOLVED_FREEZE_SHA256"
    ACTIVE_ADATA_SHA="$PREVIOUS_ADATA_SHA"
    ACTIVE_ADATA_TREE_SHA256="$PREVIOUS_ADATA_TREE_SHA256"
    verify_previous_main_health_or_stopped /api/health 3 1 || rollback_failed=1
    if [ "${QMT_EDGE_RECOVERABLE_HANDOFF_ATTEMPTED:-0}" -eq 1 ]; then
      if [ "$rollback_failed" -eq 0 ] && \
        [ "$database_boundary_rollback_failed" -eq 0 ] && \
        [ "$current_sha" = "$PREVIOUS_SHA" ] && \
        [ "$DATABASE_FORWARD_MIGRATION_STARTED" -eq 0 ]; then
        # Execute from the exact retained prior release. It independently
        # checks the original seal and the globally latest immutable context.
        if ! controlled_guard_run_qmt_activation_tool \
          "$PREVIOUS_CODE_ROOT" "$PREVIOUS_VENV" "$PREVIOUS_SHA" \
          --abort-precutover "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID" "$EXPECTED_SHA"; then
          echo "RECOVERY_BLOCKED: Windows pre-cutover abort proof failed" >&2
          rollback_failed=1
        fi
      else
        echo "RECOVERY_BLOCKED: prior runtime/schema was not proven unchanged; Windows remains fenced" >&2
        rollback_failed=1
      fi
    fi
    if [ "$rollback_failed" -ne 0 ] || \
      [ "$database_boundary_rollback_failed" -ne 0 ] || \
      [ "$current_sha" != "$PREVIOUS_SHA" ]; then
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
  if [ "$database_boundary_rollback_failed" -ne 0 ]; then
    rollback_failure "restore provisional database boundary transaction"
  fi

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
    if [ "$GOVERNANCE_TASK_TOUCHED" -eq 1 ] || \
      [ "$QMT_ANNOUNCEMENT_TASK_TOUCHED" -eq 1 ]; then
      if [ ! -s "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" ] || \
        [ ! -s "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" ]; then
        rollback_failure "scheduler task snapshots are missing"
        restoration_ready=0
      elif ! prepared_restore_and_verify_governance_snapshot \
        "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"; then
        rollback_failure "restore previous scheduler task set"
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
      if [ "$PREVIOUS_MAIN_WAS_STOPPED" -eq 1 ]; then
        sudo systemctl stop "$MAIN_SERVICE" || \
          rollback_failure "keep probiga stopped"
      else
        sudo systemctl start "$MAIN_SERVICE" || rollback_failure "start probiga"
      fi
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
    verify_previous_main_health_or_stopped /api/health 15 2 || \
      rollback_failure "verify previous API state"
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
trap 'rollback 143 "$LINENO"' TERM
trap 'rollback 130 "$LINENO"' INT
trap 'rollback 129 "$LINENO"' HUP
# PREPARE: all network, dependency, and release validation work happens while
# the old API remains active. This phase must not mutate the live checkout.
CUTOVER_STEP=prebuild_release_space
prebuild_reclaim_release_space
CUTOVER_STEP=prepare_release
prepare_release
# A deployment-only or read-model-only follow-up does not change the strategy
# runtime. Reuse the current completed canonical batch instead of spending a
# full strategy cycle solely to publish UI/API projections of that same batch.
GOVERNANCE_RESULT_BUILD_SHA="$EXPECTED_SHA"
GOVERNANCE_RESULT_TRADE_DATE=""
GOVERNANCE_PARENT_SHA=""
GOVERNANCE_CHANGED_PATHS=""
GOVERNANCE_DEPLOYMENT_ONLY=0
if GOVERNANCE_PARENT_SHA="$(git --git-dir="$CODE_GIT_CACHE" rev-parse \
    "${EXPECTED_SHA}^" 2>/dev/null)"; then
  GOVERNANCE_CHANGED_PATHS="$(git --git-dir="$CODE_GIT_CACHE" diff \
    --name-only --no-renames "$GOVERNANCE_PARENT_SHA" "$EXPECTED_SHA")"
  if [ -n "$GOVERNANCE_CHANGED_PATHS" ]; then
    GOVERNANCE_DEPLOYMENT_ONLY=1
    while IFS= read -r governance_changed_path; do
      case "$governance_changed_path" in
        tests/*|server/static/*|server/api/routers/hot_data.py|server/api/routers/holding_strategy.py|server/api/routers/trading_v3.py|server/common/canonical_decision_bridge.py) ;;
        *) GOVERNANCE_DEPLOYMENT_ONLY=0 ;;
      esac
    done <<< "$GOVERNANCE_CHANGED_PATHS"
  fi
fi
if [ "$GOVERNANCE_DEPLOYMENT_ONLY" -eq 1 ]; then
  printf 'strategy_governance reuse_current_completed release=%s\n' \
    "$EXPECTED_SHA" >&2
fi
# PREPARE DATABASE: every production mode runs the same full read-only schema
# plan and strictly validates its JSON while all existing writers remain online.
# REQUIRED first stages its recoverable credential boundary; DEFERRED_DB without
# that boundary fails here before its service-stop branch can be entered.
CUTOVER_STEP=initial_database_schema_preflight
run_initial_database_schema_preflight
if [ "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB ]; then
  deploy_deferred_database_release
fi
if [ "$PREVIOUS_SHA" = "$EXPECTED_SHA" ]; then
  CUTOVER_STEP=verify_production_database_boundary
  run_database_boundary_bootstrap verify
  if [ "$V2_FORWARD_PRESERVED_NO_RECEIPT_SHA" = "$EXPECTED_SHA" ]; then
    CUTOVER_STEP=finalize_preserved_no_receipt_request
    finalize_preserved_no_receipt_request
    trap - ERR TERM INT HUP
    if ! start_release_data_readiness_observer; then
      echo "Warning: release data readiness observer did not start" >&2
    fi
    exit 0
  fi
  if ! prepared_request_is_already_active; then
    echo "existing release SHA does not match the complete finalized request identity" >&2
    false
  fi
  CUTOVER_STEP=ensure_same_sha_qmt_windows_edge_activation
  QMT_EDGE_ACTIVATION_OUTPUT="$(controlled_guard_run_qmt_activation_tool \
    "$PREPARED_CODE_ROOT" "$RELEASE_VENV_ROOT/$EXPECTED_SHA" \
    "$EXPECTED_SHA" --activation-grant-latest)"
  printf '%s\n' "$QMT_EDGE_ACTIVATION_OUTPUT"
  printf '%s' "$QMT_EDGE_ACTIVATION_OUTPUT" | \
    controlled_guard_validate_qmt_activation_json \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" "$EXPECTED_SHA" \
      activation-grant-latest
  ACTIVE_INPUT_LOCK_SHA256="$EXPECTED_INPUT_LOCK_SHA256"
  ACTIVE_RESOLVED_FREEZE_SHA256="$EXPECTED_RESOLVED_FREEZE_SHA256"
  ACTIVE_ADATA_SHA="$EXPECTED_ADATA_SHA"
  ACTIVE_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256"
  CUTOVER_STEP=write_idempotent_deployed_receipt
  trap '' TERM INT HUP
  if ! write_receipt DEPLOYED "$EXPECTED_SHA"; then
    trap 'rollback 143 "$LINENO"' TERM
    trap 'rollback 130 "$LINENO"' INT
    trap 'rollback 129 "$LINENO"' HUP
    false
  fi
  DEPLOY_SUCCEEDED=1
  trap - ERR TERM INT HUP
  if ! start_release_data_readiness_observer; then
    echo "Warning: release data readiness observer did not start" >&2
  fi
  exit 0
fi
if [ "$RELEASE_DATA_VALIDATION_BLOCKING" -eq 1 ]; then
  CUTOVER_STEP=resolve_release_strategy_target_trade_date
  RELEASE_STRATEGY_TARGET_TRADE_DATE="$(run_prepared_python_tool -c \
    'from server.common.authoritative_market_clock import authoritative_closed_trade_date; from server.common.batch_db import create_batch_engine; from tools.env_config import load_project_env; load_project_env(); engine=create_batch_engine(future=True); value=authoritative_closed_trade_date(engine); engine.dispose(); print(value)')"
  [[ "$RELEASE_STRATEGY_TARGET_TRADE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]
  readonly RELEASE_STRATEGY_TARGET_TRADE_DATE
  CUTOVER_STEP=repair_release_stock_daily_flow
  RELEASE_FLOW_OUTPUT=""
  RELEASE_FLOW_STATUS=0
  if RELEASE_FLOW_OUTPUT="$(run_prepared_scheduler_tool linux_provider \
    "$PREPARED_CODE_ROOT/tools/repair_linux_recent_data_gaps.py" \
    --dataset stock_daily_flow --lookback-sessions 1 \
    --max-repairs-per-run 1 --expected-build-sha "$EXPECTED_SHA" \
    --state-file /var/lib/probiga/jobs/linux-recent-data-gap-repair-v1.json \
    --apply --json)"; then
    RELEASE_FLOW_STATUS=0
  else
    RELEASE_FLOW_STATUS=$?
  fi
  printf '%s\n' "$RELEASE_FLOW_OUTPUT"
  printf '%s' "$RELEASE_FLOW_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
    'import json,sys; p=json.load(sys.stdin); ok=int(sys.argv[1])==0 and p.get("schema")=="probiga.linux-recent-data-gap-repair-result.v1" and p.get("status")=="COMPLETE" and p.get("build_sha")==sys.argv[2] and p.get("datasets")==["stock_daily_flow"] and p.get("sessions")==[sys.argv[3]] and p.get("remaining_count")==0 and p.get("automatic_order_submission") is False; raise SystemExit(0 if ok else 2)' \
    "$RELEASE_FLOW_STATUS" "$EXPECTED_SHA" "$RELEASE_STRATEGY_TARGET_TRADE_DATE"

  # Keep the old API online until the exact-date core analysis inputs are ready.
  # Missing native upper-limit evidence remains fail-closed per stock and does
  # not block publication of the otherwise complete analysis pool.
  CUTOVER_STEP=wait_release_analysis_native_input
  run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/run_release_analysis_fast.py" \
    --readiness-only --wait-seconds 900 \
    --target-date "$RELEASE_STRATEGY_TARGET_TRADE_DATE" \
    --expected-build-sha "$EXPECTED_SHA"
  CUTOVER_STEP=publish_release_analysis_pool
  RELEASE_ANALYSIS_OUTPUT="$(run_prepared_scheduler_tool linux_standalone \
    "$PREPARED_CODE_ROOT/tools/run_release_analysis_fast.py" \
    --target-date "$RELEASE_STRATEGY_TARGET_TRADE_DATE" \
    --expected-build-sha "$EXPECTED_SHA")"
  printf '%s\n' "$RELEASE_ANALYSIS_OUTPUT"
  printf '%s' "$RELEASE_ANALYSIS_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
    'import json,re,sys; p=json.load(sys.stdin); ok=p.get("schema")=="probiga.release-analysis-fast-result.v1" and p.get("status")=="COMPLETE" and p.get("task_type")=="analysis_fast" and p.get("target_trade_date")==sys.argv[1] and p.get("build_sha")==sys.argv[2] and p.get("ready") is True and int(p.get("flow_rows") or 0)>=5000 and int(p.get("analysis_count") or 0)>=1000 and int(p.get("recommendation_count") or 0)>=0 and re.fullmatch(r"[0-9a-f]{32}",str(p.get("run_uid") or "")) and re.fullmatch(r"[0-9a-f]{64}",str(p.get("canonical_pool_sha256") or "")) and p.get("automatic_real_order_submission") is False and p.get("real_order_authority") is False; raise SystemExit(0 if ok else 2)' \
    "$RELEASE_STRATEGY_TARGET_TRADE_DATE" "$EXPECTED_SHA"
fi
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
QMT_ANNOUNCEMENT_TASK_OLD_SOURCE="$(mktemp)"
chown "$SERVICE_USER:$SERVICE_USER" "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"
chmod 0600 "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"
CUTOVER_STEP=capture_qmt_announcement_task_before_cutover
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_qmt_announcement_task.py" \
  --capture-snapshot "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"
test -s "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"
chown root:root "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"
chmod 0600 "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"
controlled_guard_assert_file "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE" 600
CUTOVER_STEP=preflight_qmt_announcement_rollback_channel
prepared_qmt_announcement_snapshot verify \
  "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"

# Append one per-broker hold before authorizing the exact Windows edge
# revision, then quiesce only the Linux scheduler while the old API remains
# online.  The updater must keep the Windows edge stopped until this exact
# attempt receives its post-activation grant.  The writer proof covers the
# updater's five-minute cadence, its bounded stop, the strict heartbeat expiry
# boundary and one final poll.
CUTOVER_STEP=request_qmt_windows_edge_quiescence_before_service_stop
if [ "$QMT_EDGE_RECOVERY_COMPATIBILITY_INSTALL" -eq 1 ]; then
QMT_EDGE_REQUEST_OUTPUT="$(controlled_guard_run_qmt_activation_tool \
  "$PREPARED_CODE_ROOT" "$RELEASE_VENV_ROOT/$EXPECTED_SHA" "$EXPECTED_SHA" \
  --request-compatibility-quiescence "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID")"
printf '%s\n' "$QMT_EDGE_REQUEST_OUTPUT"
printf '%s' "$QMT_EDGE_REQUEST_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
  'import json,sys; p=json.load(sys.stdin); ok=isinstance(p,dict) and p.get("mode")=="request-compatibility-quiescence" and p.get("compatibility_install") is True and p.get("database_writes") is True and p.get("build_sha")==sys.argv[1] and p.get("deployment_attempt_id")==sys.argv[2] and p.get("activation_granted") is False and p.get("status") in {"inserted","idempotent"}; raise SystemExit(0 if ok else 2)' \
  "$EXPECTED_SHA" "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID"
else
CUTOVER_STEP=request_qmt_windows_edge_forward_only_handoff
QMT_EDGE_FORWARD_REQUEST_OUTPUT="$(controlled_guard_run_qmt_activation_tool \
  "$PREPARED_CODE_ROOT" "$RELEASE_VENV_ROOT/$EXPECTED_SHA" "$EXPECTED_SHA" \
  --request-forward-quiescence "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID" "$PREVIOUS_SHA")"
printf '%s\n' "$QMT_EDGE_FORWARD_REQUEST_OUTPUT"
QMT_EDGE_HANDOFF_KIND="$(printf '%s' "$QMT_EDGE_FORWARD_REQUEST_OUTPUT" | \
  "$BOOTSTRAP_PYTHON" -I -c '
import json, sys
p = json.load(sys.stdin)
base = (
    isinstance(p, dict)
    and p.get("mode") == "request-forward-quiescence"
    and p.get("build_sha") == sys.argv[1]
    and p.get("prior_build_sha") == sys.argv[2]
    and p.get("deployment_attempt_id") == sys.argv[3]
    and p.get("activation_granted") is False
)
if not base:
    raise SystemExit(2)
if p.get("status") == "not_applicable":
    if p.get("database_writes") is not False or p.get("context") is not None:
        raise SystemExit(2)
    print("fresh")
else:
    c = p.get("context")
    valid = (
        ((p.get("status") == "inserted" and p.get("database_writes") is True)
         or (p.get("status") == "idempotent" and p.get("database_writes") is False))
        and isinstance(c, dict)
        and c.get("schema") == "probiga.qmt-edge-forward-only-supersession.v1"
        and c.get("protocol") == "probiga.qmt-edge-forward-only-supersession.v1"
        and c.get("scope") == "FORWARD_ONLY_SUPERSESSION"
        and c.get("build_sha") == sys.argv[1]
        and c.get("original_prior_build_sha") == sys.argv[2]
        and c.get("deployment_attempt_id") == sys.argv[3]
        and c.get("real_order") is False
    )
    if not valid:
        raise SystemExit(2)
    print("forward")
' "$EXPECTED_SHA" "$PREVIOUS_SHA" "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID")"
if [ "$QMT_EDGE_HANDOFF_KIND" = fresh ]; then
CUTOVER_STEP=request_qmt_windows_edge_fresh_prior_handoff
QMT_EDGE_RECOVERABLE_HANDOFF_ATTEMPTED=1
QMT_EDGE_REQUEST_OUTPUT="$(controlled_guard_run_qmt_activation_tool \
  "$PREVIOUS_CODE_ROOT" "$PREVIOUS_VENV" "$PREVIOUS_SHA" \
  --request-recoverable-quiescence "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID" "$EXPECTED_SHA")"
printf '%s\n' "$QMT_EDGE_REQUEST_OUTPUT"
printf '%s' "$QMT_EDGE_REQUEST_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
  'import json,sys; p=json.load(sys.stdin); c=p.get("context") if isinstance(p,dict) else None; ok=isinstance(c,dict) and p.get("mode")=="request-recoverable-quiescence" and p.get("activation_granted") is False and ((p.get("status")=="inserted" and p.get("database_writes") is True) or (p.get("status")=="idempotent" and p.get("database_writes") is False)) and c.get("build_sha")==sys.argv[1] and c.get("deployment_attempt_id")==sys.argv[2] and c.get("protocol")=="probiga.qmt-edge-precutover-recovery.v1" and c.get("prior_running") is True; raise SystemExit(0 if ok else 2)' \
  "$EXPECTED_SHA" "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID"
else
  # A forward-only context never permits prior resumption. Leave it pending
  # on failure; the ordinary writer fence, full activation checks, and final
  # grant below remain mandatory before the target Windows writer can run.
  test "$QMT_EDGE_HANDOFF_KIND" = forward
  QMT_EDGE_REQUEST_OUTPUT="$QMT_EDGE_FORWARD_REQUEST_OUTPUT"
fi
fi
CUTOVER_STEP=stop_linux_scheduler_before_writer_quiescence
if [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
  sudo systemctl stop probiga-scheduler
  ! systemctl is-active --quiet probiga-scheduler
  PRE_CUTOVER_SCHEDULER_STOPPED=1
fi
CUTOVER_STEP=verify_cross_host_writer_quiescence_before_api_stop
WRITER_QUIESCENCE_OUTPUT="$(run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/trading_v3_layer4_maintenance.py" \
  wait-writers --timeout-seconds 600 --poll-seconds 5)"
printf '%s\n' "$WRITER_QUIESCENCE_OUTPUT"
printf '%s' "$WRITER_QUIESCENCE_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
  'import json,sys; p=json.load(sys.stdin); ok=isinstance(p,dict) and p.get("status")=="ok" and p.get("ready") is True and p.get("live_writer_count")==0 and p.get("live_writers")==[]; raise SystemExit(0 if ok else 2)'
CUTOVER_STEP=migrate_probiga_job_log_legacy_modes_after_writer_quiescence
migrate_legacy_flow_progress apply
migrate_probiga_job_log_legacy_modes

# CUTOVER: persist the exact pre-cutover activation journal before the first
# stop/disable.  A completed journal is always present before any writer state
# changes; the marker and permanent drop-ins then make an interrupted fence
# recoverable without trusting caller-supplied state.
CUTOVER_STEP=persist_database_writer_restore_journal
persist_database_writer_restore_journal
CUTOVER_STARTED=1
CUTOVER_STEP=commit_production_database_boundary
run_database_boundary_bootstrap commit
DATABASE_GUARD_MIGRATION_UNVERIFIED=1
CUTOVER_STEP=install_database_writer_guard_dropins
install_database_writer_guard_dropins
CUTOVER_STEP=persist_database_writer_guard
persist_database_writer_guard
DATABASE_WRITER_GUARD_PERSISTED=1
CUTOVER_STEP=load_database_writer_guard_dropins
sudo systemctl daemon-reload
assert_database_writer_guard_dropins_loaded

# Fence future writer claims and immediately re-prove the pre-cutover result.
# The only service stopped so far is the Linux scheduler; the old API remains
# online until this final atomic database fence succeeds.
CUTOVER_STEP=writer_fence_before_api_stop
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
    tools/add_trading_v3_tasks.py --fence-only \
      --require-no-live-scheduler-writers \
      --writer-drain-timeout-seconds 0 \
      --writer-drain-poll-seconds 5
) || WRITER_FENCE_STATUS=$?
if [ "$WRITER_FENCE_STATUS" -ne 0 ]; then
  if [ "$WRITER_FENCE_STATUS" -eq 3 ]; then
    EXTERNAL_WRITER_BLOCKED=1
  fi
  false
fi

# Install the prevalidated runtime and governance schema/task, run the bounded
# daily close, then start and prove health/static.  The live checkout remains
# untouched.
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
! systemctl is-active --quiet "$MAIN_SERVICE"
! systemctl is-enabled --quiet "$MAIN_SERVICE"
if [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
  ! systemctl is-active --quiet probiga-scheduler
  ! systemctl is-enabled --quiet probiga-scheduler
fi
if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
  assert_ai_worker_writer_fence
fi
CUTOVER_STEP=select_strategy_governance_database_schema_phase
FENCED_STRATEGY_GOVERNANCE_SCHEMA_PHASE=""
select_fenced_strategy_governance_schema_phase
CUTOVER_STEP=prepare_strategy_governance_database_schema
DATABASE_FORWARD_MIGRATION_STARTED=1
if [ "$FENCED_STRATEGY_GOVERNANCE_SCHEMA_PHASE" = resume ]; then
  run_prepared_database_migration_tool \
    "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_schema.py" \
    --phase resume --writers-fenced
else
  test "$FENCED_STRATEGY_GOVERNANCE_SCHEMA_PHASE" = cutover
  run_prepared_database_migration_tool \
    "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_schema.py" \
    --phase cutover --writers-fenced
fi
CUTOVER_STEP=recover_strategy_governance_database_trust
run_prepared_database_migration_tool \
  "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_schema.py" \
  --phase recover
CUTOVER_STEP=stage_trading_v3_tasks_disabled
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_trading_v3_tasks.py" \
  --writer-fence
CUTOVER_STEP=install_qmt_announcement_task_disabled
if ! run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_qmt_announcement_task.py" --disabled; then
  QMT_ANNOUNCEMENT_TASK_TOUCHED=1
  false
fi
QMT_ANNOUNCEMENT_TASK_TOUCHED=1
CUTOVER_STEP=install_qmt_operations_tasks_disabled
if ! run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_qmt_operations_tasks.py" --disabled; then
  QMT_ANNOUNCEMENT_TASK_TOUCHED=1
  false
fi
QMT_ANNOUNCEMENT_TASK_TOUCHED=1
# The immutable QMT reference and attestation tables do not exist on a first
# deployment until the privileged schema cutover above has completed.  Keep
# all reads and captures of those tables after the cutover validation.
CUTOVER_STEP=verify_qmt_local_history_provenance_schema_after_cutover
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/migrate_qmt_local_history_provenance.py" \
  --check-via-primary
# The Windows updater stops the old edge before this schema cutover, consumes
# only the post-schema request above, starts the new-SHA daemon, and performs
# the native QMT capture.  Linux only polls read-only proofs; it never imports
# or invokes the Windows QMT runtime.
if [ "$QMT_EDGE_DEPLOY_BLOCKING" -eq 1 ]; then
CUTOVER_STEP=request_qmt_windows_edge_release_bootstrap
QMT_EDGE_REQUEST_OUTPUT="$(run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/run_qmt_windows_edge_release_bootstrap.py" \
  --request --expected-build-sha "$EXPECTED_SHA" --compact)"
printf '%s\n' "$QMT_EDGE_REQUEST_OUTPUT"
printf '%s' "$QMT_EDGE_REQUEST_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
  'import json,re,sys; p=json.load(sys.stdin); ok=isinstance(p,dict) and p.get("mode")=="request" and p.get("status") in {"inserted","idempotent"} and p.get("build_sha")==sys.argv[1] and p.get("database_writes") is True and re.fullmatch(r"qmt-edge-request-[0-9a-f]{40}",str(p.get("request_run_uid") or "")); raise SystemExit(0 if ok else 2)' \
  "$EXPECTED_SHA"
CUTOVER_STEP=wait_for_qmt_windows_edge_identity
QMT_EDGE_WAIT_DEADLINE=$((SECONDS + 900))
QMT_EDGE_IDENTITY_OUTPUT=""
while [ "$SECONDS" -lt "$QMT_EDGE_WAIT_DEADLINE" ]; do
  if QMT_EDGE_IDENTITY_OUTPUT="$(run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/check_qmt_windows_edge.py" \
    --identity-only --expected-build-sha "$EXPECTED_SHA" \
    --expected-poll-seconds 60 --compact)"; then
    break
  fi
  sleep 5
done
test -n "$QMT_EDGE_IDENTITY_OUTPUT"
printf '%s\n' "$QMT_EDGE_IDENTITY_OUTPUT"
printf '%s' "$QMT_EDGE_IDENTITY_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
  'import json,sys; p=json.load(sys.stdin); d=p.get("detail") if isinstance(p,dict) else None; c=d.get("current") if isinstance(d,dict) else None; ok=p.get("status")=="AVAILABLE" and p.get("mode")=="identity" and p.get("strategy_eligible") is True and p.get("database_writes") is False and isinstance(c,dict) and c.get("build_sha")==sys.argv[1] and c.get("executor_role")=="qmt_windows_edge" and c.get("instance_id")==f"{c.get('"'"'host_name'"'"')}-{c.get('"'"'pid'"'"')}" and d.get("expected_poll_seconds")==60 and d.get("errors")==[]; raise SystemExit(0 if ok else 2)' \
  "$EXPECTED_SHA"

CUTOVER_STEP=wait_for_qmt_windows_edge_release_bootstrap
QMT_EDGE_BOOTSTRAP_DEADLINE=$((SECONDS + 2700))
QMT_EDGE_BOOTSTRAP_OUTPUT=""
while [ "$SECONDS" -lt "$QMT_EDGE_BOOTSTRAP_DEADLINE" ]; do
  if QMT_EDGE_BOOTSTRAP_OUTPUT="$(run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/check_qmt_windows_edge.py" \
    --release-bootstrap-only --expected-build-sha "$EXPECTED_SHA" \
    --expected-poll-seconds 60 --compact)"; then
    break
  fi
  sleep 10
done
test -n "$QMT_EDGE_BOOTSTRAP_OUTPUT"
printf '%s\n' "$QMT_EDGE_BOOTSTRAP_OUTPUT"
printf '%s' "$QMT_EDGE_BOOTSTRAP_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
  'import json,re,sys; p=json.load(sys.stdin); d=p.get("detail") if isinstance(p,dict) else None; r=d.get("receipt") if isinstance(d,dict) else None; i=d.get("identity") if isinstance(d,dict) else None; c=i.get("current") if isinstance(i,dict) else None; ok=p.get("status")=="AVAILABLE" and p.get("mode")=="release-bootstrap" and p.get("strategy_eligible") is True and p.get("database_writes") is False and d.get("immutable_reference_verified") is True and d.get("receipt_count")==1 and d.get("errors")==[] and isinstance(r,dict) and r.get("build_sha")==sys.argv[1] and r.get("request_run_uid")==f"qmt-edge-request-{sys.argv[1]}" and r.get("scheduler_instance_id")==c.get("instance_id") and str(r.get("catalog_batch_id") or "").startswith(f"qmt_rel_{sys.argv[1]}_") and r.get("catalog_batch_id")==r.get("calendar_batch_id") and re.fullmatch(r"[0-9a-f]{64}",str(r.get("receipt_hash") or "")); raise SystemExit(0 if ok else 2)' \
  "$EXPECTED_SHA"
else
  CUTOVER_STEP=defer_qmt_windows_edge_release_bootstrap
  echo "QMT Windows edge bootstrap wait skipped; the pre-cutover request is scheduler-owned" >&2
fi
CUTOVER_STEP=read_strategy_governance_qmt_history_readiness_after_schema
if [ "$QMT_HISTORY_DEPLOY_BLOCKING" -eq 1 ]; then
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
CUTOVER_STEP=prepare_strategy_governance_qmt_history
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_qmt_history.py" \
  --expected-target-trade-date "$QMT_HISTORY_TARGET_TRADE_DATE" \
  --expected-start-date "$QMT_HISTORY_START_DATE" \
  --expected-end-date "$QMT_HISTORY_END_DATE" \
  --expected-session-window-sha256 \
    "$QMT_HISTORY_SESSION_WINDOW_SHA256"
else
  CUTOVER_STEP=resolve_strategy_governance_trade_date_without_history_scan
  if [ "$RELEASE_DATA_VALIDATION_BLOCKING" -eq 1 ]; then
    QMT_HISTORY_TARGET_TRADE_DATE="$(run_prepared_python_tool -c \
      'from server.common.authoritative_market_clock import authoritative_closed_trade_date; from server.common.batch_db import create_batch_engine; from tools.env_config import load_project_env; load_project_env(); engine=create_batch_engine(future=True); value=authoritative_closed_trade_date(engine); engine.dispose(); print(value)')"
    [[ "$QMT_HISTORY_TARGET_TRADE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]
  else
    QMT_HISTORY_TARGET_TRADE_DATE=""
  fi
  QMT_HISTORY_START_DATE=""
  QMT_HISTORY_END_DATE=""
  QMT_HISTORY_SESSION_WINDOW_SHA256=""
  echo "QMT history release scan skipped; data readiness remains scheduler-owned" >&2
fi
readonly QMT_HISTORY_TARGET_TRADE_DATE QMT_HISTORY_START_DATE \
  QMT_HISTORY_END_DATE QMT_HISTORY_SESSION_WINDOW_SHA256
if [ "$RELEASE_DATA_VALIDATION_BLOCKING" -eq 1 ]; then
  CUTOVER_STEP=validate_existing_qmt_announcement_full_market_batch
  QMT_ANNOUNCEMENT_RUN_OUTPUT=""
  QMT_ANNOUNCEMENT_RUN_STATUS=0
  QMT_ANNOUNCEMENT_DISPOSITION=""
  if QMT_ANNOUNCEMENT_RUN_OUTPUT="$(run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/sync_qmt_announcement_pit.py" \
    --validate-existing-complete-batch --window-days 30 \
    --expected-trade-date "$QMT_HISTORY_TARGET_TRADE_DATE")"; then
    QMT_ANNOUNCEMENT_RUN_STATUS=0
  else
    QMT_ANNOUNCEMENT_RUN_STATUS=$?
  fi
  printf '%s\n' "$QMT_ANNOUNCEMENT_RUN_OUTPUT"
  QMT_ANNOUNCEMENT_DISPOSITION="$(
    printf '%s' "$QMT_ANNOUNCEMENT_RUN_OUTPUT" | run_prepared_python_tool \
      "$PREPARED_CODE_ROOT/tools/sync_qmt_announcement_pit.py" \
      --validate-existing-result-exit "$QMT_ANNOUNCEMENT_RUN_STATUS" \
      --expected-trade-date "$QMT_HISTORY_TARGET_TRADE_DATE"
  )"
  case "$QMT_ANNOUNCEMENT_RUN_STATUS:$QMT_ANNOUNCEMENT_DISPOSITION" in
    0:complete) ;;
    2:data_blocked)
      echo "QMT announcement batch is not ready; deferring data catch-up until after code/service publication" >&2
      ;;
    *)
      printf 'QMT announcement validation invalid_result exit=%s disposition=%q\n' \
        "$QMT_ANNOUNCEMENT_RUN_STATUS" "$QMT_ANNOUNCEMENT_DISPOSITION" >&2
      false
      ;;
  esac
else
  CUTOVER_STEP=skip_qmt_announcement_data_validation
  echo "QMT announcement data validation skipped for code release" >&2
fi
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
if [ "$RELEASE_DATA_VALIDATION_BLOCKING" -eq 1 ]; then
  test "$QMT_HISTORY_TARGET_TRADE_DATE" = \
    "$RELEASE_STRATEGY_TARGET_TRADE_DATE"
fi
CUTOVER_STEP=run_strategy_governance
GOVERNANCE_RUN_OUTPUT=""
GOVERNANCE_RUN_STATUS=0
GOVERNANCE_HEALTH_DISPOSITION=skipped
GOVERNANCE_RUN_ARGS=()
if [ "$RELEASE_DATA_VALIDATION_BLOCKING" -eq 0 ]; then
  echo "Strategy batch execution skipped for code release" >&2
else
  GOVERNANCE_HEALTH_DISPOSITION=completed
  if [ "$GOVERNANCE_DEPLOYMENT_ONLY" -ne 1 ]; then
    GOVERNANCE_RUN_ARGS=(--expected-build-sha "$EXPECTED_SHA")
  fi
  if GOVERNANCE_RUN_OUTPUT="$(run_prepared_python_tool \
    "$PREPARED_CODE_ROOT/tools/run_strategy_governance_daily.py" \
    "${GOVERNANCE_RUN_ARGS[@]}")"; then
    GOVERNANCE_RUN_STATUS=0
  else
    GOVERNANCE_RUN_STATUS=$?
  fi
  printf '%s\n' "$GOVERNANCE_RUN_OUTPUT"
  GOVERNANCE_JSON_STATUS=""
  if ! GOVERNANCE_JSON_STATUS="$(
    printf '%s' "$GOVERNANCE_RUN_OUTPUT" | run_prepared_python_tool \
      "$PREPARED_CODE_ROOT/tools/run_strategy_governance_daily.py" \
      --validate-result-exit "$GOVERNANCE_RUN_STATUS" \
      "${GOVERNANCE_RUN_ARGS[@]}"
  )"; then
    printf 'strategy_governance invalid_result exit=%s\n' \
      "$GOVERNANCE_RUN_STATUS" >&2
    false
  fi
  if [ "$GOVERNANCE_DEPLOYMENT_ONLY" -eq 1 ]; then
    GOVERNANCE_RESULT_BUILD_SHA="$(
      printf '%s' "$GOVERNANCE_RUN_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
        'import json,re,sys; p=json.load(sys.stdin); c=p.get("current_run") if isinstance(p,dict) else None; v=(c.get("build_commit_sha") if isinstance(c,dict) else p.get("build_commit_sha")) or ""; print(v) if re.fullmatch(r"[0-9a-f]{40}",str(v)) else sys.exit(2)'
    )"
    GOVERNANCE_RESULT_TRADE_DATE="$(
      printf '%s' "$GOVERNANCE_RUN_OUTPUT" | "$BOOTSTRAP_PYTHON" -I -c \
        'import json,re,sys; p=json.load(sys.stdin); c=p.get("current_run") if isinstance(p,dict) else None; v=(c.get("trade_date") if isinstance(c,dict) else p.get("trade_date")) or ""; print(v) if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}",str(v)) else sys.exit(2)'
    )"
    printf 'strategy_governance reused_completed build=%s release=%s\n' \
      "$GOVERNANCE_RESULT_BUILD_SHA" "$EXPECTED_SHA" >&2
  fi
  case "$GOVERNANCE_RUN_STATUS:$GOVERNANCE_JSON_STATUS" in
    0:completed|0:not_due) ;;
    2:not_ready)
      echo "Strategy governance input is not ready; refusing deployment before code/service publication" >&2
      false
      ;;
    3:integrity_error)
      echo "Strategy governance integrity check failed; refusing deployment" >&2
      false
      ;;
    4:program_error)
      echo "Strategy governance program failed; refusing deployment" >&2
      false
      ;;
    *)
      printf 'strategy_governance invalid_result exit=%s json_status=%q\n' \
        "$GOVERNANCE_RUN_STATUS" "$GOVERNANCE_JSON_STATUS" >&2
      false
      ;;
  esac
fi
readonly GOVERNANCE_RESULT_BUILD_SHA GOVERNANCE_RESULT_TRADE_DATE
CUTOVER_STEP=enable_strategy_governance_task
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
  --schema-prepared
CUTOVER_STEP=normalize_daily_strategy_pipeline_schedule
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/ensure_quality_gate.py" \
  --task-type analysis_upper_evidence_prepare \
  --task-type analysis_fast
CUTOVER_STEP=enable_qmt_announcement_task
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_qmt_announcement_task.py"
CUTOVER_STEP=enable_qmt_operations_tasks
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_qmt_operations_tasks.py"
GOVERNANCE_TASK_NEW_SOURCE="$(mktemp)"
chown "$SERVICE_USER:$SERVICE_USER" "$GOVERNANCE_TASK_NEW_SOURCE"
chmod 0600 "$GOVERNANCE_TASK_NEW_SOURCE"
CUTOVER_STEP=capture_strategy_governance_task_after_enable
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" \
  --capture-snapshot "$GOVERNANCE_TASK_NEW_SOURCE"
activation_snapshot_install_governance_new "$GOVERNANCE_TASK_NEW_SOURCE"
QMT_ANNOUNCEMENT_TASK_NEW_SOURCE="$(mktemp)"
chown "$SERVICE_USER:$SERVICE_USER" "$QMT_ANNOUNCEMENT_TASK_NEW_SOURCE"
chmod 0600 "$QMT_ANNOUNCEMENT_TASK_NEW_SOURCE"
CUTOVER_STEP=capture_qmt_announcement_task_after_enable
run_prepared_python_tool \
  "$PREPARED_CODE_ROOT/tools/add_qmt_announcement_task.py" \
  --capture-snapshot "$QMT_ANNOUNCEMENT_TASK_NEW_SOURCE"
activation_snapshot_install_qmt_announcement_new \
  "$QMT_ANNOUNCEMENT_TASK_NEW_SOURCE"
prepared_governance_snapshot verify "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
prepared_qmt_announcement_snapshot verify \
  "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT"
CUTOVER_STEP=record_strategy_governance_trade_date
GOVERNANCE_TRADE_DATE="${GOVERNANCE_RESULT_TRADE_DATE:-$QMT_HISTORY_TARGET_TRADE_DATE}"
CUTOVER_STEP=verify_strategy_governance_before_start
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
grep -zFx -- "PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT" \
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
test "${MAIN_CMDLINE[11]}" = --workers
test "${MAIN_CMDLINE[12]}" = 2
test "${MAIN_CMDLINE[13]}" = --limit-concurrency
test "${MAIN_CMDLINE[14]}" = 64
test "${MAIN_CMDLINE[15]}" = --backlog
test "${MAIN_CMDLINE[16]}" = 256
test "${MAIN_CMDLINE[17]}" = --limit-max-requests
test "${MAIN_CMDLINE[18]}" = 400
test "${MAIN_CMDLINE[19]}" = --limit-max-requests-jitter
test "${MAIN_CMDLINE[20]}" = 100
test "${MAIN_CMDLINE[21]}" = --timeout-keep-alive
test "${MAIN_CMDLINE[22]}" = 5
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
grep -zFx -- 'PROBIGA_SCHEDULER_EXECUTOR_ROLE=linux_standalone' \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- 'PROBIGA_STRATEGY_GOVERNANCE_MODE=REQUIRED' \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_EXPECTED_GIT_SHA=$EXPECTED_SHA" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_BUILD_COMMIT_SHA=$EXPECTED_SHA" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
  "/proc/$SCHEDULER_MAIN_PID/environ" >/dev/null
grep -zFx -- "PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT" \
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
CUTOVER_STEP=wait_for_first_scheduler_heartbeat
SCHEDULER_HEARTBEAT_READY=0
for _heartbeat_attempt in $(seq 1 30); do
  if run_prepared_python_tool \
      "$PREPARED_CODE_ROOT/tools/check_scheduler_runtime_heartbeat.py" \
      --expected-build-sha "$EXPECTED_SHA" \
      --expected-scheduler-pid "$SCHEDULER_MAIN_PID" >/dev/null; then
    SCHEDULER_HEARTBEAT_READY=1
    break
  fi
  if [ "$_heartbeat_attempt" -lt 30 ]; then
    sleep 2
  fi
done
test "$SCHEDULER_HEARTBEAT_READY" -eq 1
CUTOVER_STEP=record_strategy_governance_scheduler_heartbeat
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
  "$PREPARED_CODE_ROOT/tools/ensure_quality_gate.py"
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
  --validate-required-task-contracts
if ! prune_release_venvs "$EXPECTED_SHA" "$PREVIOUS_RELEASE_REVISION"; then
  echo "Warning: release venv cleanup failed before final verification" >&2
fi
if ! prune_code_releases "$PREPARED_CODE_ROOT" "$PREVIOUS_CODE_ROOT"; then
  echo "Warning: immutable code release cleanup failed before final verification" >&2
fi
if ! prune_release_temp_files; then
  echo "Warning: release temp cleanup failed before final verification" >&2
fi
CUTOVER_STEP=verify_post_prune_release_identity
release_identity_check 1 "$PREPARED_CODE_ROOT"
CUTOVER_STEP=verify_post_prune_health
HEALTH_RESPONSE="$(mktemp)"
if ! curl --fail-with-body --silent --show-error --retry 15 \
  --retry-all-errors --retry-delay 2 --retry-connrefused \
  --output "$HEALTH_RESPONSE" http://127.0.0.1/api/health; then
  cat "$HEALTH_RESPONSE" >&2
  rm -f "$HEALTH_RESPONSE"
  HEALTH_RESPONSE=""
  false
fi
cat "$HEALTH_RESPONSE"
rm -f "$HEALTH_RESPONSE"
HEALTH_RESPONSE=""
curl --fail-with-body --silent --show-error --retry 15 \
  --retry-all-errors --retry-delay 2 --retry-connrefused \
  http://127.0.0.1/api/health/runtime >/dev/null
CUTOVER_STEP=verify_account_login_api_and_page_smoke
verify_account_login_api_and_page_smoke "$EXPECTED_SHA"
if [ "$RELEASE_DATA_VALIDATION_BLOCKING" -eq 1 ]; then
  CUTOVER_STEP=verify_strategy_governance_api_and_page_smoke
  if [ "$GOVERNANCE_HEALTH_DISPOSITION" = completed ]; then
    verify_strategy_governance_api_and_page_smoke \
      "$GOVERNANCE_RESULT_BUILD_SHA" "$GOVERNANCE_TRADE_DATE"
  else
    test "$GOVERNANCE_HEALTH_DISPOSITION" = input_not_ready
    echo "Strategy governance canonical API smoke deferred until release catch-up completes" >&2
  fi
  CUTOVER_STEP=verify_strategy_pool_api_and_page_smoke
  verify_strategy_pool_api_and_page_smoke \
    "$EXPECTED_SHA" "$GOVERNANCE_TRADE_DATE"
  CUTOVER_STEP=verify_today_strategy_daily_result_smoke
  verify_today_strategy_daily_result_smoke \
    "$EXPECTED_SHA" "$GOVERNANCE_TRADE_DATE"
else
  CUTOVER_STEP=skip_market_data_api_smokes
  echo "Strategy batch and pool content smokes skipped for code release" >&2
fi
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
CUTOVER_STEP=grant_qmt_windows_edge_activation
QMT_EDGE_ACTIVATION_OUTPUT="$(controlled_guard_run_qmt_activation_tool \
  "$PREPARED_CODE_ROOT" "$RELEASE_VENV_ROOT/$EXPECTED_SHA" \
  "$EXPECTED_SHA" --activation-grant \
  "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID")"
printf '%s\n' "$QMT_EDGE_ACTIVATION_OUTPUT"
printf '%s' "$QMT_EDGE_ACTIVATION_OUTPUT" | \
  controlled_guard_validate_qmt_activation_json \
    "$BOOTSTRAP_PYTHON" "$EXPECTED_SHA" activation-grant \
    "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID"
DEPLOY_SUCCEEDED=1
trap '' TERM INT HUP
CUTOVER_STEP=remove_finalized_activation_journal
activation_snapshot_remove_finalized_before_deploy
trap - ERR TERM INT HUP
if ! start_release_data_readiness_observer; then
  echo "Warning: release data readiness observer did not start" >&2
fi
df -h / >&2
