#!/usr/bin/env bash
# Audited, forward-only Trading V3 Layer-4 production maintenance.
#
# This script is invoked only by the protected workflow_dispatch job.  It never
# performs a down migration, never registers/pins a model, and never enables a
# real-order route.  Any uncertainty leaves the Layer-4 task fence active.
set -Eeuo pipefail
umask 077

EXPECTED_SHA="${PROBIGA_EXPECTED_GIT_SHA:-}"
PHASE="${PROBIGA_MAINTENANCE_PHASE:-}"
ACK="${PROBIGA_MAINTENANCE_ACK:-}"
ALLOW_RESUME="${PROBIGA_MAINTENANCE_ALLOW_RESUME:-false}"
RUN_ID="${PROBIGA_MAINTENANCE_RUN_ID:-}"
ACTOR="${PROBIGA_MAINTENANCE_ACTOR:-}"
BOOTSTRAP_PYTHON=/usr/bin/python3.14
MYSQL_BIN=/usr/bin/mysql
MYSQLDUMP_BIN=/usr/bin/mysqldump
RECEIPT_ROOT=/var/lib/probiga/maintenance-receipts
BACKUP_ROOT=/var/backups/probiga/layer4
CODE_RELEASE_ROOT=/opt/ProBigA-releases
CURRENT_RELEASE_LINK=/opt/ProBigA-current
RELEASE_VENV_ROOT=/var/lib/probiga/release-venvs
DEPLOY_LOCK_ROOT=/run/probiga
DEPLOY_LOCK_FILE="$DEPLOY_LOCK_ROOT/production-deploy.lock"
ROOT="$CODE_RELEASE_ROOT/$EXPECTED_SHA"

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
case "$PHASE" in
  migrate)
    if [ "$ALLOW_RESUME" = true ]; then
      test "$ACK" = I_CONFIRM_LAYER4_FORWARD_RECOVERY || \
        die "forward-recovery acknowledgement missing"
    else
      test "$ACK" = I_CONFIRM_LAYER4_PRODUCTION_MIGRATION || \
        die "migration acknowledgement missing"
    fi
    ;;
  activate)
    test "$ALLOW_RESUME" = false || die "activate cannot request migration resume"
    test "$ACK" = I_CONFIRM_LAYER4_SHADOW_WRITERS_ACTIVATION || \
      die "Shadow-writer activation acknowledgement missing"
    ;;
  *) die "phase must be migrate or activate" ;;
esac
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
READY_FILE="$RUN_DIR/maintenance-lock.ready.json"
RELEASE_FILE="$RUN_DIR/maintenance-lock.release"
LOCK_LOG="$RUN_DIR/maintenance-lock.log"
LOCK_PID=""
SERVICES_STOPPED=0
APPLY_STARTED=0
MIGRATIONS_ACCEPTED=0
BACKUP_FILE=""
BACKUP_SHA256=""
BACKUP_BYTES=0
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
  RECEIPT_BACKUP_FILE="$BACKUP_FILE" \
  RECEIPT_BACKUP_SHA256="$BACKUP_SHA256" \
  RECEIPT_BACKUP_BYTES="$BACKUP_BYTES" \
  RECEIPT_APPLY_STARTED="$APPLY_STARTED" \
  RECEIPT_MIGRATIONS_ACCEPTED="$MIGRATIONS_ACCEPTED" \
  "$BOOTSTRAP_PYTHON" -I - <<'PY' > "$temporary"
import json, os
print(json.dumps({
    "schema_version": "probiga.layer4-maintenance-receipt.v1",
    "receipt_id": os.environ["RECEIPT_ID"],
    "status": os.environ["RECEIPT_STATUS"],
    "detail": os.environ["RECEIPT_DETAIL"][:500],
    "phase": os.environ["RECEIPT_PHASE"],
    "expected_git_sha": os.environ["RECEIPT_SHA"],
    "github_run_id": os.environ["RECEIPT_RUN_ID"],
    "github_actor": os.environ["RECEIPT_ACTOR"],
    "started_at": os.environ["RECEIPT_STARTED_AT"],
    "ended_at": os.environ["RECEIPT_ENDED_AT"],
    "backup": {
        "path": os.environ["RECEIPT_BACKUP_FILE"],
        "sha256": os.environ["RECEIPT_BACKUP_SHA256"],
        "bytes": int(os.environ["RECEIPT_BACKUP_BYTES"] or 0),
    },
    "apply_started": os.environ["RECEIPT_APPLY_STARTED"] == "1",
    "migrations_accepted": (
        os.environ["RECEIPT_MIGRATIONS_ACCEPTED"] == "1"
    ),
    "layer4_writer_fence_preserved_on_failure": True,
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

HEALTH_JSON="$(curl --fail --silent --show-error --max-time 20 \
  http://127.0.0.1/api/health)"
mapfile -t RELEASE_IDENTITY < <(
  HEALTH_JSON="$HEALTH_JSON" EXPECTED_SHA="$EXPECTED_SHA" \
    "$BOOTSTRAP_PYTHON" -I - <<'PY'
import json, os, re
p = json.loads(os.environ["HEALTH_JSON"])
r = p.get("release_revision") or {}
a = p.get("adata_release_revision") or {}
s = p.get("scheduler_runtime") or {}
standalone = p.get("standalone_scheduler") or {}
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
values = (
    a.get("expected_git_sha"),
    a.get("expected_tree_sha256"),
    a.get("source_dir"),
)
assert re.fullmatch(r"[0-9a-f]{40}", str(values[0] or ""))
assert re.fullmatch(r"[0-9a-f]{64}", str(values[1] or ""))
assert isinstance(values[2], str) and "\n" not in values[2]
for value in values:
    print(value)
PY
)
test "${#RELEASE_IDENTITY[@]}" -eq 3 || die "active release identity failed"
ADATA_SHA="${RELEASE_IDENTITY[0]}"
ADATA_TREE_SHA256="${RELEASE_IDENTITY[1]}"
ADATA_SOURCE="${RELEASE_IDENTITY[2]}"
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
  trap - ERR TERM INT
  set +e
  FAILURE_DETAIL="maintenance failed with exit $failed_status"
  # Keep the exclusion lock held until the durable task fence has been
  # attempted.  Releasing first would open a manual-writer race during
  # recovery from a partially applied forward migration.
  run_release_python tools/add_trading_v3_tasks.py --fence-only \
    > "$RUN_DIR/recovery-fence.json" 2>&1
  release_maintenance_lock
  if [ "$SERVICES_STOPPED" -eq 1 ]; then
    sudo systemctl disable --now probiga-scheduler >/dev/null 2>&1
    sudo systemctl disable --now probiga >/dev/null 2>&1
    FINAL_STATUS=FORWARD_RECOVERY_REQUIRED
  else
    FINAL_STATUS=BLOCKED_FENCED
  fi
  write_receipt "$FINAL_STATUS" "$FAILURE_DETAIL" || true
  echo "Layer-4 maintenance failed; writers remain fenced. Receipt: $RECEIPT_ROOT/$RECEIPT_ID.json" >&2
  exit "$failed_status"
}
trap 'failure_recovery $?' ERR
trap 'failure_recovery 143' TERM
trap 'failure_recovery 130' INT

write_receipt STARTED "active immutable release verified"

# This is the only mutation allowed before the schema backup.  --fence-only
# executes one UPDATE transaction and cannot upsert definitions or add columns.
run_release_python tools/add_trading_v3_tasks.py --fence-only \
  > "$RUN_DIR/fence-only.json"

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

run_release_python tools/trading_v3_layer4_maintenance.py wait-writers \
  --timeout-seconds 150 --poll-seconds 5 > "$RUN_DIR/writer-drain.json"

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

test -x "$MYSQL_BIN" || die "Oracle MySQL client is missing"
test -x "$MYSQLDUMP_BIN" || die "Oracle mysqldump client is missing"
sudo -u "$SERVICE_USER" test ! -w "$MYSQL_BIN"
sudo -u "$SERVICE_USER" test ! -w "$MYSQLDUMP_BIN"

dba_scalar() {
  local sql="$1"
  sudo -n "$MYSQL_BIN" --protocol=socket --batch --raw \
    --skip-column-names probiga --execute "$sql"
}

dba_audit() {
  local identity trx_count mdl_count schema_lock calibration_lock maintenance_lock
  identity="$(dba_scalar \
    "SELECT CONCAT_WS('|', VERSION(), @@version_comment, DATABASE());")"
  case "$identity" in
    8.4.11*"|MySQL "*"|probiga") ;;
    *) die "DBA connection is not exact Oracle MySQL 8.4.11/probiga" ;;
  esac
  trx_count="$(dba_scalar \
    "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_mysql_thread_id <> CONNECTION_ID();")"
  test "$trx_count" = 0 || die "active InnoDB transactions remain"
  mdl_count="$(dba_scalar "SELECT COUNT(*) FROM performance_schema.metadata_locks WHERE object_schema='probiga' AND object_name IN ('st_decision_run_v3','st_horizon_model_artifact_v3','st_horizon_forecast_contract_v3','st_horizon_outcome_v3','st_shadow_release_v3','st_calibration_gate_v3','st_counterfactual_learning_run_v3') AND (lock_status='PENDING' OR (lock_status='GRANTED' AND lock_duration='TRANSACTION'));" )"
  test "$mdl_count" = 0 || die "blocking target-table metadata locks remain"
  schema_lock="$(dba_scalar \
    "SELECT COALESCE(IS_USED_LOCK('probiga:trading_v3_schema'),0);")"
  calibration_lock="$(dba_scalar \
    "SELECT COALESCE(IS_USED_LOCK('probiga:trading_v3:continuous_calibration'),0);")"
  maintenance_lock="$(dba_scalar \
    "SELECT COALESCE(IS_USED_LOCK('probiga:trading_v3:maintenance'),0);")"
  test "$schema_lock" = 0 || die "V3 schema lock is already held"
  test "$calibration_lock" = 0 || die "continuous-calibration lock is already held"
  test "$maintenance_lock" -gt 0 || die "maintenance exclusion lock is not held"
}

dba_audit

if [ "$PHASE" = migrate ]; then
  version_text="$(sudo -n "$MYSQLDUMP_BIN" --version)"
  case "$version_text" in
    *8.4.11*MySQL*) ;;
    *) die "mysqldump is not exact Oracle MySQL 8.4.11" ;;
  esac
  case "${version_text,,}" in
    *mariadb*|*percona*) die "non-Oracle mysqldump is forbidden" ;;
  esac
  sudo mkdir -p "$BACKUP_ROOT"
  sudo chown root:root "$BACKUP_ROOT"
  sudo chmod 0700 "$BACKUP_ROOT"
  BACKUP_FILE="$BACKUP_ROOT/${RECEIPT_ID}-schema.sql"
  sudo -n "$MYSQLDUMP_BIN" --protocol=socket \
    --default-character-set=utf8mb4 --no-data --routines --events --triggers \
    --single-transaction --skip-lock-tables --skip-column-statistics \
    --set-gtid-purged=OFF --result-file="$BACKUP_FILE" probiga
  sudo test -s "$BACKUP_FILE"
  sudo grep -Fq 'CREATE TABLE `st_decision_run_v3`' "$BACKUP_FILE"
  sudo grep -Fq 'CREATE TABLE `schema_migration_v3`' "$BACKUP_FILE"
  if sudo grep -Eq '^INSERT INTO ' "$BACKUP_FILE"; then
    die "schema-only backup unexpectedly contains row inserts"
  fi
  BACKUP_SHA256="$(sudo sha256sum "$BACKUP_FILE" | awk '{print $1}')"
  BACKUP_BYTES="$(sudo stat -c '%s' "$BACKUP_FILE")"
  [[ "$BACKUP_SHA256" =~ ^[0-9a-f]{64}$ ]]
  test "$BACKUP_BYTES" -gt 0

  PLAN_ARGS=(migration-plan)
  if [ "$ALLOW_RESUME" = true ]; then
    PLAN_ARGS+=(--allow-resume)
  fi
  run_release_python tools/trading_v3_layer4_maintenance.py \
    "${PLAN_ARGS[@]}" > "$RUN_DIR/migration-plan.json"
  dba_audit
  APPLY_STARTED=1
  run_release_python tools/migrate_trading_v3.py \
    > "$RUN_DIR/migration-apply.json"
  run_release_python tools/migrate_trading_v3.py --dry-run \
    > "$RUN_DIR/migration-replay.json"
  run_release_python tools/trading_v3_layer4_maintenance.py \
    verify-migrations > "$RUN_DIR/migration-verify.json"
  MIGRATIONS_ACCEPTED=1
  dba_audit
  run_release_python tools/add_trading_v3_tasks.py --writer-fence \
    > "$RUN_DIR/task-stage-fenced.json"
  run_release_python tools/trading_v3_layer4_maintenance.py task-state \
    --expected fenced > "$RUN_DIR/task-state.json"
  FINAL_STATUS=MIGRATED_FENCED
else
  run_release_python tools/trading_v3_layer4_maintenance.py \
    verify-migrations > "$RUN_DIR/migration-verify.json"
  run_release_python tools/add_trading_v3_tasks.py --activate-layer4 \
    > "$RUN_DIR/task-activate.json"
  run_release_python tools/trading_v3_layer4_maintenance.py task-state \
    --expected enabled > "$RUN_DIR/task-state.json"
  FINAL_STATUS=SHADOW_WRITERS_ACTIVATED
fi

release_maintenance_lock
sudo systemctl enable probiga
sudo systemctl start probiga
sudo systemctl enable probiga-scheduler
sudo systemctl start probiga-scheduler
FINAL_HEALTH_JSON="$(curl --fail --silent --show-error --retry 20 \
  --retry-delay 2 --retry-connrefused http://127.0.0.1/api/health)"
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

for _attempt in $(seq 1 30); do
  fresh_count="$(dba_scalar "SELECT COUNT(*) FROM st_scheduler_runtime WHERE poll_seconds > 0 AND TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) <= 2 * poll_seconds;")"
  standalone_count="$(dba_scalar "SELECT COUNT(*) FROM st_scheduler_runtime WHERE mode='standalone' AND poll_seconds > 0 AND TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) BETWEEN 0 AND 2 * poll_seconds;")"
  if [ "$fresh_count" = 1 ] && [ "$standalone_count" = 1 ]; then
    break
  fi
  sleep 2
done
test "$fresh_count" = 1 && test "$standalone_count" = 1 || \
  die "unique fresh standalone scheduler authority was not established"

SERVICES_STOPPED=0
write_receipt "$FINAL_STATUS" \
  "forward-only maintenance completed; model/order gates unchanged"
trap - ERR TERM INT
echo "Layer-4 maintenance completed: $FINAL_STATUS"
echo "Receipt: $RECEIPT_ROOT/$RECEIPT_ID.json"
echo "Receipt SHA256: $(cat "$RUN_DIR/receipt.sha256")"
