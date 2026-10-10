#!/usr/bin/env bash
# Audited Trading V3 Layer-4 production activation.
#
# Schema migration belongs exclusively to production_deploy.sh, whose fenced
# database boundary owns the remote-TLS migrator and trigger-administrator
# credentials.  This script only activates the already migrated Shadow writer
# tasks.  It never registers/pins a model or enables a real-order route.  Any
# uncertainty leaves the Layer-4 task fence active.
set -Eeuo pipefail
umask 077

EXPECTED_SHA="${PROBIGA_EXPECTED_GIT_SHA:-}"
PHASE="${PROBIGA_MAINTENANCE_PHASE:-}"
ACK="${PROBIGA_MAINTENANCE_ACK:-}"
ALLOW_RESUME="${PROBIGA_MAINTENANCE_ALLOW_RESUME:-false}"
RUN_ID="${PROBIGA_MAINTENANCE_RUN_ID:-}"
ACTOR="${PROBIGA_MAINTENANCE_ACTOR:-}"
BOOTSTRAP_PYTHON=/usr/bin/python3.14
RECEIPT_ROOT=/var/lib/probiga/maintenance-receipts
CODE_RELEASE_ROOT=/opt/ProBigA-releases
CURRENT_RELEASE_LINK=/opt/ProBigA-current
RELEASE_VENV_ROOT=/var/lib/probiga/release-venvs
ADATA_RUNTIME_ROOT=/var/lib/probiga/release-sources/adata
DEPLOY_LOCK_ROOT=/run/probiga
DEPLOY_LOCK_FILE="$DEPLOY_LOCK_ROOT/production-deploy.lock"
ROOT="$CODE_RELEASE_ROOT/$EXPECTED_SHA"
HEALTH_ATTEMPT_TIMEOUT_SECONDS=120
HEALTH_RETRY_MAX_SECONDS=360

die() {
  echo "Layer-4 maintenance blocked: $1" >&2
  # Returning lets `set -E` route every post-trap failure through the common
  # recovery handler.  An explicit `exit` would bypass the ERR trap and could
  # leave services running after a late authority check failed.
  return 2
}

[[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || die "invalid expected Git SHA"
[[ "$RUN_ID" =~ ^[0-9]{1,30}$ ]] || die "invalid GitHub run id"
[[ "$ACTOR" =~ ^[A-Za-z0-9-]{1,64}$ ]] || die "invalid GitHub actor"
test "$PHASE" = activate || \
  die "Layer-4 schema migration is owned by the production deploy pipeline"
test "$ALLOW_RESUME" = false || die "activation cannot request migration resume"
test "$ACK" = I_CONFIRM_LAYER4_SHADOW_WRITERS_ACTIVATION || \
  die "Shadow-writer activation acknowledgement missing"
test "${PROBIGA_DEPLOYMENT_MODE:-}" = production || \
  die "production deployment mode is required"
test "${EUID:-$(id -u)}" -eq 0 || \
  die "Layer-4 maintenance must run through the root maintenance broker"
test -x "$BOOTSTRAP_PYTHON" || die "pinned bootstrap Python is missing"
test "$(stat -c '%U' "$BOOTSTRAP_PYTHON")" = root || \
  die "bootstrap Python is not root-owned"

test ! -L "$DEPLOY_LOCK_ROOT" || die "deploy lock root must not be a symlink"
install -d -o root -g root -m 0700 "$DEPLOY_LOCK_ROOT"
test "$(readlink -f "$DEPLOY_LOCK_ROOT")" = "$DEPLOY_LOCK_ROOT" || \
  die "deploy lock root is not canonical"
test ! -L "$DEPLOY_LOCK_FILE" || die "deploy lock file must not be a symlink"
touch "$DEPLOY_LOCK_FILE"
chown root:root "$DEPLOY_LOCK_FILE"
chmod 0600 "$DEPLOY_LOCK_FILE"
test "$(stat -c '%U:%G:%a' "$DEPLOY_LOCK_FILE")" = root:root:600 || \
  die "deploy lock file ownership or mode is unsafe"
exec 9>"$DEPLOY_LOCK_FILE"
if ! flock -n 9; then
  die "another deploy or maintenance run holds the remote lock"
fi
test ! -L "$CODE_RELEASE_ROOT" || die "code release root must not be a symlink"
test "$(readlink -f "$CODE_RELEASE_ROOT")" = "$CODE_RELEASE_ROOT" || \
  die "code release root is not canonical"
test ! -L "$ROOT" && test -d "$ROOT" || \
  die "expected immutable code release is missing or linked"
test "$(readlink -f "$ROOT")" = "$ROOT" || \
  die "expected immutable code release is not canonical"
test -L "$CURRENT_RELEASE_LINK" || die "current release link is missing"
test "$(readlink -f "$CURRENT_RELEASE_LINK")" = "$ROOT" || \
  die "current release does not select the expected SHA"
cd "$ROOT"

STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RECEIPT_ID="layer4-${PHASE}-${EXPECTED_SHA}-${RUN_ID}"
RUN_DIR="$(mktemp -d /tmp/probiga-layer4-maintenance.XXXXXX)"
LOCK_IPC_DIR="$RUN_DIR/maintenance-lock-ipc"
READY_FILE="$LOCK_IPC_DIR/maintenance-lock.ready.json"
RELEASE_FILE="$LOCK_IPC_DIR/maintenance-lock.release"
LOCK_LOG="$RUN_DIR/maintenance-lock.log"
LOCK_PID=""
SERVICES_STOPPED=0
ACTIVATION_STARTED=0
WRITER_EXECUTION_BLOCKED_ON_FAILURE=0
FINAL_STATUS=STARTED
FAILURE_DETAIL=""

cleanup_run_dir() {
  rm -rf -- "$RUN_DIR"
}
trap cleanup_run_dir EXIT

write_receipt() {
  local status="$1"
  local detail="${2:-}"
  local temporary="$RUN_DIR/receipt.json"
  RECEIPT_STATUS="$status" RECEIPT_DETAIL="$detail" \
  RECEIPT_ID="$RECEIPT_ID" RECEIPT_STARTED_AT="$STARTED_AT" \
  RECEIPT_ENDED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  RECEIPT_PHASE="$PHASE" RECEIPT_SHA="$EXPECTED_SHA" \
  RECEIPT_RUN_ID="$RUN_ID" RECEIPT_ACTOR="$ACTOR" \
  RECEIPT_ACTIVATION_STARTED="$ACTIVATION_STARTED" \
  RECEIPT_WRITER_EXECUTION_BLOCKED_ON_FAILURE="$WRITER_EXECUTION_BLOCKED_ON_FAILURE" \
  "$BOOTSTRAP_PYTHON" -I - <<'PY' > "$temporary"
import json, os
print(json.dumps({
    "schema_version": "probiga.layer4-activation-receipt.v2",
    "receipt_id": os.environ["RECEIPT_ID"],
    "status": os.environ["RECEIPT_STATUS"],
    "detail": os.environ["RECEIPT_DETAIL"][:500],
    "phase": os.environ["RECEIPT_PHASE"],
    "expected_git_sha": os.environ["RECEIPT_SHA"],
    "github_run_id": os.environ["RECEIPT_RUN_ID"],
    "github_actor": os.environ["RECEIPT_ACTOR"],
    "started_at": os.environ["RECEIPT_STARTED_AT"],
    "ended_at": os.environ["RECEIPT_ENDED_AT"],
    "activation_started": os.environ["RECEIPT_ACTIVATION_STARTED"] == "1",
    "writer_execution_blocked_on_failure": (
        os.environ["RECEIPT_WRITER_EXECUTION_BLOCKED_ON_FAILURE"] == "1"
    ),
    "model_gate_modified": False,
    "order_authority": False,
}, ensure_ascii=False, sort_keys=True))
PY
  sudo mkdir -p "$RECEIPT_ROOT"
  sudo chown root:root "$RECEIPT_ROOT"
  sudo chmod 0700 "$RECEIPT_ROOT"
  sudo install -o root -g root -m 0600 "$temporary" \
    "$RECEIPT_ROOT/$RECEIPT_ID.json"
  sha256sum "$temporary" | awk '{print $1}' \
    > "$RUN_DIR/receipt.sha256"
}

read_deep_health() {
  # Production health proves database-backed release, authentication, schema,
  # component and scheduler contracts.  The authoritative MySQL boundary is a
  # TLS connection behind the fixed loopback tunnel, so a healthy deep probe
  # can legitimately exceed the old 20-second ceiling while that tunnel is
  # congested.  Keep every proof, but bound both each attempt and the complete
  # retry window so maintenance can neither false-fail nor hang indefinitely.
  curl --fail --silent --show-error \
    --connect-timeout 10 --max-time "$HEALTH_ATTEMPT_TIMEOUT_SECONDS" \
    --retry 2 --retry-all-errors --retry-delay 2 \
    --retry-max-time "$HEALTH_RETRY_MAX_SECONDS" --retry-connrefused \
    http://127.0.0.1/api/health
}

HEALTH_JSON="$(read_deep_health)"
mapfile -t RELEASE_IDENTITY < <(
  HEALTH_JSON="$HEALTH_JSON" EXPECTED_SHA="$EXPECTED_SHA" \
    "$BOOTSTRAP_PYTHON" -I - <<'PY'
import json, os, re
p = json.loads(os.environ["HEALTH_JSON"])
r = p.get("release_revision") or {}
a = p.get("adata_release_revision") or {}
s = p.get("scheduler_runtime") or {}
standalone = p.get("standalone_scheduler") or {}
heartbeat = p.get("standalone_scheduler_heartbeat") or {}
heartbeat_detail = heartbeat.get("detail") or {}
current_scheduler = heartbeat_detail.get("current") or {}
expected = os.environ["EXPECTED_SHA"]
assert p.get("status") == "ok"
assert r.get("deployment_mode") == "production"
assert r.get("expected_git_sha") == expected
assert r.get("actual_git_sha") == expected
assert r.get("matches_expected") is True
assert r.get("code_worktree_clean") is True
assert a.get("verified") is True and a.get("read_only") is True
assert s.get("embedded_scheduler_enabled") is False
assert s.get("embedded_scheduler_running") is False
assert standalone.get("active") is True and standalone.get("enabled") is True
assert heartbeat.get("ready") is True
values = (
    a.get("expected_git_sha"),
    a.get("expected_tree_sha256"),
    current_scheduler.get("instance_id"),
)
assert a.get("actual_git_sha") == values[0]
assert a.get("actual_tree_sha256") == values[1]
assert re.fullmatch(r"[0-9a-f]{40}", str(values[0] or ""))
assert re.fullmatch(r"[0-9a-f]{64}", str(values[1] or ""))
assert re.fullmatch(r"[A-Za-z0-9_.:-]{1,255}", str(values[2] or ""))
for value in values:
    print(value)
PY
)
test "${#RELEASE_IDENTITY[@]}" -eq 3 || die "active release identity failed"
ADATA_SHA="${RELEASE_IDENTITY[0]}"
ADATA_TREE_SHA256="${RELEASE_IDENTITY[1]}"
SCHEDULER_INSTANCE_ID="${RELEASE_IDENTITY[2]}"
test ! -L "$ADATA_RUNTIME_ROOT" || die "adata release root must not be a symlink"
test "$(readlink -f "$ADATA_RUNTIME_ROOT")" = "$ADATA_RUNTIME_ROOT" || \
  die "adata release root is not canonical"
ADATA_SOURCE="$ADATA_RUNTIME_ROOT/$ADATA_SHA-$ADATA_TREE_SHA256"
test ! -L "$ADATA_SOURCE" && test -d "$ADATA_SOURCE" || \
  die "expected immutable adata release is missing or linked"
test "$(readlink -f "$ADATA_SOURCE")" = "$ADATA_SOURCE" || \
  die "expected immutable adata release is not canonical"
test "$(stat -c '%U:%G' "$ADATA_SOURCE")" = root:root || \
  die "expected immutable adata release is not root-owned"
test "$(cat "$ADATA_SOURCE/.probiga-adata.gitsha")" = "$ADATA_SHA" || \
  die "adata release Git marker differs"
test "$(cat "$ADATA_SOURCE/.probiga-adata.tree.sha256")" = \
  "$ADATA_TREE_SHA256" || die "adata release tree marker differs"
test "$(git -C "$ROOT" rev-parse HEAD)" = "$EXPECTED_SHA" || \
  die "active code release SHA differs"
RELEASE_VENV="$RELEASE_VENV_ROOT/$EXPECTED_SHA"
test -L "$RELEASE_VENV" || die "release venv is not SHA-addressed"
RELEASE_VENV_TARGET="$(readlink -f "$RELEASE_VENV")"
case "$RELEASE_VENV_TARGET" in
  "$RELEASE_VENV_ROOT/build-$EXPECTED_SHA-"*) ;;
  *) die "release venv escaped its immutable root" ;;
esac
test "$(cat "$RELEASE_VENV/.probiga.gitsha")" = "$EXPECTED_SHA" || \
  die "release venv Git marker differs"
test "$(cat "$RELEASE_VENV/.adata.gitsha")" = "$ADATA_SHA" || \
  die "release venv adata marker differs"
test "$(cat "$RELEASE_VENV/.adata.tree.sha256")" = "$ADATA_TREE_SHA256" || \
  die "release venv adata tree marker differs"
SERVICE_USER="$(systemctl show -p User --value probiga)"
test -n "$SERVICE_USER" && test "$SERVICE_USER" != root || \
  die "service user is invalid"
SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
test -n "$SERVICE_GROUP" || die "service group is invalid"
chown root:"$SERVICE_GROUP" "$RUN_DIR"
chmod 0710 "$RUN_DIR"
test "$(stat -c '%U:%G:%a' "$RUN_DIR")" = "root:$SERVICE_GROUP:710" || \
  die "maintenance run directory ownership or mode is unsafe"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0700 "$LOCK_IPC_DIR"
test ! -L "$LOCK_IPC_DIR" || die "maintenance lock IPC directory is linked"
test "$(readlink -f "$LOCK_IPC_DIR")" = "$LOCK_IPC_DIR" || \
  die "maintenance lock IPC directory is not canonical"
test "$(stat -c '%U:%G:%a' "$LOCK_IPC_DIR")" = \
  "$SERVICE_USER:$SERVICE_GROUP:700" || \
  die "maintenance lock IPC directory ownership or mode is unsafe"
test "$(systemctl is-enabled probiga)" = enabled || \
  die "production API service must be enabled before maintenance"
test "$(systemctl is-enabled probiga-scheduler)" = enabled || \
  die "production scheduler service must be enabled before maintenance"
sudo -u "$SERVICE_USER" test ! -w "$ADATA_SOURCE" || \
  die "service user can mutate the immutable adata release"
for unit in probiga probiga-scheduler; do
  main_pid="$(systemctl show -p MainPID --value "$unit")"
  [[ "$main_pid" =~ ^[1-9][0-9]*$ ]] || die "$unit has no live MainPID"
  active_argv0="$(tr '\0' '\n' < "/proc/$main_pid/cmdline" | sed -n '1p')"
  test "$active_argv0" = "$RELEASE_VENV/bin/python" || \
    die "$unit is not running the pinned release interpreter"
  grep -zFx -- "PROBIGA_CODE_ROOT=$ROOT" \
    "/proc/$main_pid/environ" >/dev/null || \
    die "$unit is not bound to the active code release"
  grep -zFx -- "PYTHONPATH=$ADATA_SOURCE:$ROOT" \
    "/proc/$main_pid/environ" >/dev/null || \
    die "$unit PYTHONPATH is not sealed adata plus active code"
done

run_release_python() {
  local entrypoint="$1"
  local service_home
  shift
  case "$entrypoint" in
    tools/*.py) ;;
    *) die "maintenance entrypoint escaped the active release" ;;
  esac
  test -f "$ROOT/$entrypoint" || die "maintenance entrypoint is missing"
  service_home="$(getent passwd "$SERVICE_USER" | cut -d: -f6)"
  sudo -u "$SERVICE_USER" env -i \
    PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    HOME="$service_home" LANG=C.UTF-8 PYTHONUTF8=1 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
    PROBIGA_DEPLOYMENT_MODE=production \
    PROBIGA_CODE_ROOT="$ROOT" \
    PROBIGA_EXPECTED_GIT_SHA="$EXPECTED_SHA" \
    PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA" \
    PROBIGA_EXPECTED_ADATA_SHA="$ADATA_SHA" \
    PROBIGA_EXPECTED_ADATA_TREE_SHA256="$ADATA_TREE_SHA256" \
    PROBIGA_ADATA_SOURCE_DIR="$ADATA_SOURCE" \
    PYTHONPATH="$ADATA_SOURCE:$ROOT" \
    "$RELEASE_VENV/bin/python" -P "$ROOT/$entrypoint" "$@"
}

release_maintenance_lock() {
  if [ -n "$LOCK_PID" ]; then
    touch "$RELEASE_FILE"
    wait "$LOCK_PID" || true
    LOCK_PID=""
  fi
}

failure_recovery() {
  local failed_status="${1:-$?}"
  local recovery_fence_succeeded=0
  trap - ERR TERM INT
  set +e
  if [ -z "$FAILURE_DETAIL" ]; then
    FAILURE_DETAIL="maintenance failed with exit $failed_status"
  fi
  # Keep the exclusion lock held until the durable task fence has been
  # attempted.  Releasing first would open a scheduler race during recovery
  # from a partially applied activation.
  if run_release_python tools/add_trading_v3_tasks.py --fence-only \
      > "$RUN_DIR/recovery-fence.json" 2>&1 && \
    run_release_python tools/trading_v3_layer4_maintenance.py task-state \
      --expected fenced > "$RUN_DIR/recovery-fence-state.json" 2>&1; then
    recovery_fence_succeeded=1
    WRITER_EXECUTION_BLOCKED_ON_FAILURE=1
  elif sudo systemctl disable --now probiga-scheduler >/dev/null 2>&1 && \
    test "$(systemctl show -p ActiveState --value probiga-scheduler)" = inactive && \
    test "$(systemctl is-enabled probiga-scheduler)" = disabled; then
    WRITER_EXECUTION_BLOCKED_ON_FAILURE=1
  fi
  release_maintenance_lock
  if [ "$SERVICES_STOPPED" -eq 1 ] && \
    [ "$recovery_fence_succeeded" -eq 1 ] && \
    sudo systemctl enable --now probiga >/dev/null 2>&1 && \
    sudo systemctl enable --now probiga-scheduler >/dev/null 2>&1 && \
    test "$(systemctl is-active probiga)" = active && \
    test "$(systemctl is-active probiga-scheduler)" = active; then
    SERVICES_STOPPED=0
    FINAL_STATUS=BLOCKED_FENCED
  elif [ "$SERVICES_STOPPED" -eq 1 ]; then
    sudo systemctl disable --now probiga-scheduler >/dev/null 2>&1
    sudo systemctl disable --now probiga >/dev/null 2>&1
    if test "$(systemctl show -p ActiveState --value probiga-scheduler)" = inactive && \
      test "$(systemctl is-enabled probiga-scheduler)" = disabled; then
      WRITER_EXECUTION_BLOCKED_ON_FAILURE=1
    fi
    FINAL_STATUS=SERVICE_RECOVERY_REQUIRED
  elif [ "$WRITER_EXECUTION_BLOCKED_ON_FAILURE" -ne 1 ]; then
    FINAL_STATUS=SERVICE_RECOVERY_REQUIRED
  elif [ "$recovery_fence_succeeded" -ne 1 ]; then
    FINAL_STATUS=TASK_FENCE_RECOVERY_REQUIRED
  else
    FINAL_STATUS=BLOCKED_FENCED
  fi
  write_receipt "$FINAL_STATUS" "$FAILURE_DETAIL" || true
  echo "Layer-4 maintenance failed; inspect recovery receipt: $RECEIPT_ROOT/$RECEIPT_ID.json" >&2
  exit "$failed_status"
}
trap 'failure_recovery $?' ERR
trap 'failure_recovery 143' TERM
trap 'failure_recovery 130' INT

write_receipt STARTED "active immutable release verified"

# Reject a fresh Windows/remote scheduler before any task or service mutation.
# The caller must quiesce that endpoint explicitly; maintenance never assumes
# that a remote heartbeat is stale or safe to ignore.
if ! run_release_python tools/trading_v3_layer4_maintenance.py \
  assert-exclusive-writer \
  --expected-instance-id "$SCHEDULER_INSTANCE_ID" \
  > "$RUN_DIR/writer-topology.json"; then
  FAILURE_DETAIL="$(head -c 500 "$RUN_DIR/writer-topology.json" | tr '\n' ' ')"
  false
fi

# This is the only mutation allowed before the schema backup.  --fence-only
# executes one UPDATE transaction and cannot upsert definitions or add columns.
run_release_python tools/add_trading_v3_tasks.py --fence-only \
  > "$RUN_DIR/fence-only.json"
run_release_python tools/trading_v3_layer4_maintenance.py task-state \
  --expected fenced > "$RUN_DIR/pre-activation-fence-state.json"

sudo systemctl disable --now probiga-scheduler
sudo systemctl disable --now probiga
SERVICES_STOPPED=1
test "$(systemctl show -p ActiveState --value probiga-scheduler)" = inactive
test "$(systemctl show -p ActiveState --value probiga)" = inactive
test "$(systemctl show -p MainPID --value probiga-scheduler)" = 0
test "$(systemctl show -p MainPID --value probiga)" = 0
assert_unit_cgroup_empty() {
  local control_group="$1"
  local process_id
  test -n "$control_group" || return 0
  test "$control_group" != / || die "refusing to inspect root cgroup"
  test -d "/sys/fs/cgroup$control_group" || return 0
  process_id="$(find "/sys/fs/cgroup$control_group" -name cgroup.procs \
    -type f -exec cat {} + | sed -n '1p')"
  test -z "$process_id" || die "service cgroup still has process $process_id"
}
assert_unit_cgroup_empty "$(systemctl show -p ControlGroup --value probiga)"
assert_unit_cgroup_empty \
  "$(systemctl show -p ControlGroup --value probiga-scheduler)"
for trigger in probiga-scheduler.timer probiga-scheduler.path \
  probiga-scheduler.socket; do
  load_state="$(systemctl show -p LoadState --value "$trigger")"
  if [ "$load_state" != not-found ]; then
    case "$(systemctl show -p ActiveState --value "$trigger")" in
      inactive|failed) ;;
      *) die "scheduler activation unit remains active: $trigger" ;;
    esac
  fi
done

if ! run_release_python tools/trading_v3_layer4_maintenance.py wait-writers \
  --timeout-seconds 150 --poll-seconds 5 > "$RUN_DIR/writer-drain.json"; then
  FAILURE_DETAIL="$(head -c 500 "$RUN_DIR/writer-drain.json" | tr '\n' ' ')"
  false
fi

run_release_python tools/trading_v3_layer4_maintenance.py hold-lock \
  --ready-file "$READY_FILE" --release-file "$RELEASE_FILE" \
  --timeout-seconds 30 --max-hold-seconds 3600 --parent-pid $$ \
  > "$LOCK_LOG" 2>&1 &
LOCK_PID=$!
for _attempt in $(seq 1 80); do
  test -f "$READY_FILE" && break
  kill -0 "$LOCK_PID" 2>/dev/null || die "maintenance DB lock exited early"
  sleep 0.25
done
test -f "$READY_FILE" || die "maintenance DB lock was not acquired"

# Re-check after the DB lock closes the compliant-writer race.
run_release_python tools/trading_v3_layer4_maintenance.py wait-writers \
  --timeout-seconds 0 --poll-seconds 1 > "$RUN_DIR/writer-recheck.json"

run_release_python tools/trading_v3_layer4_maintenance.py \
  verify-migrations > "$RUN_DIR/migration-verify.json"
ACTIVATION_STARTED=1
run_release_python tools/add_trading_v3_tasks.py --activate-layer4 \
  > "$RUN_DIR/task-activate.json"
run_release_python tools/trading_v3_layer4_maintenance.py task-state \
  --expected enabled > "$RUN_DIR/task-state.json"
FINAL_STATUS=SHADOW_WRITERS_ACTIVATED

release_maintenance_lock
sudo systemctl enable probiga
sudo systemctl start probiga
sudo systemctl enable probiga-scheduler
sudo systemctl start probiga-scheduler
FINAL_HEALTH_JSON="$(read_deep_health)"
printf '%s\n' "$FINAL_HEALTH_JSON" > "$RUN_DIR/final-health.json"
HEALTH_JSON="$FINAL_HEALTH_JSON" EXPECTED_SHA="$EXPECTED_SHA" \
  "$BOOTSTRAP_PYTHON" -I - <<'PY'
import json, os
p = json.loads(os.environ["HEALTH_JSON"])
r = p.get("release_revision") or {}
s = p.get("scheduler_runtime") or {}
standalone = p.get("standalone_scheduler") or {}
assert p.get("status") == "ok"
assert r.get("deployment_mode") == "production"
assert r.get("expected_git_sha") == os.environ["EXPECTED_SHA"]
assert r.get("actual_git_sha") == os.environ["EXPECTED_SHA"]
assert r.get("matches_expected") is True
assert r.get("code_worktree_clean") is True
assert s.get("embedded_scheduler_enabled") is False
assert s.get("embedded_scheduler_running") is False
assert standalone.get("active") is True and standalone.get("enabled") is True
PY
test "$(systemctl show -p ActiveState --value probiga)" = active
test "$(systemctl show -p ActiveState --value probiga-scheduler)" = active
test "$(systemctl is-enabled probiga-scheduler)" = enabled

WRITER_READY=0
for _attempt in $(seq 1 30); do
  if run_release_python tools/trading_v3_layer4_maintenance.py \
    assert-exclusive-writer \
    --expected-instance-id "$SCHEDULER_INSTANCE_ID" \
    > "$RUN_DIR/final-writer-topology.json"; then
    WRITER_READY=1
    break
  fi
  sleep 2
done
if [ "$WRITER_READY" -ne 1 ]; then
  FAILURE_DETAIL="$(head -c 500 "$RUN_DIR/final-writer-topology.json" | tr '\n' ' ')"
  false
fi

SERVICES_STOPPED=0
write_receipt "$FINAL_STATUS" \
  "activation completed; schema and model/order gates unchanged"
trap - ERR TERM INT
echo "Layer-4 maintenance completed: $FINAL_STATUS"
echo "Receipt: $RECEIPT_ROOT/$RECEIPT_ID.json"
echo "Receipt SHA256: $(cat "$RUN_DIR/receipt.sha256")"
