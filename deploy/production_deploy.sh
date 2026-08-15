#!/usr/bin/env bash
# Production deployment logic invoked by the pinned GitHub SSH action.
# Inputs are passed explicitly through the action env allowlist.
set -Eeuo pipefail
cd /opt/ProBigA
DEPLOY_LOCK_DIR=/opt/ProBigA/.probiga_deploy_lock
if ! mkdir "$DEPLOY_LOCK_DIR"; then
  echo "Another production deployment holds the remote lock" >&2
  exit 2
fi
release_lock() {
  rmdir "$DEPLOY_LOCK_DIR" 2>/dev/null || true
}
trap release_lock EXIT
: "${EXPECTED_SHA:?EXPECTED_SHA is required}"
: "${RESOLVED_REQUIREMENTS_B64:?RESOLVED_REQUIREMENTS_B64 is required}"
: "${EXPECTED_REQUIREMENTS_SHA256:?EXPECTED_REQUIREMENTS_SHA256 is required}"
: "${EXPECTED_ADATA_SHA:?EXPECTED_ADATA_SHA is required}"
: "${EXPECTED_ADATA_TREE_SHA256:?EXPECTED_ADATA_TREE_SHA256 is required}"
[[ "$EXPECTED_ADATA_SHA" =~ ^[0-9a-f]{40}$ ]]
[[ "$EXPECTED_ADATA_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]]
PREVIOUS_SHA="$(git rev-parse HEAD)"
DEPLOY_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RECEIPT_ID="${EXPECTED_SHA}-$(date -u +%Y%m%dT%H%M%SZ)"
RECEIPT_DIR=/var/lib/probiga/deploy-receipts
sudo mkdir -p "$RECEIPT_DIR"
sudo chown root:root "$RECEIPT_DIR"
sudo chmod 0700 "$RECEIPT_DIR"
write_receipt() {
  local status="$1"
  local active_sha="$2"
  local ended_at
  local receipt_tmp
  ended_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  receipt_tmp="$(sudo mktemp \
    "$RECEIPT_DIR/.${RECEIPT_ID}.XXXXXX")" || return 1
  if ! printf '{"schema_version":"probiga.deploy-receipt.v3","status":"%s","expected_sha":"%s","previous_sha":"%s","active_sha":"%s","expected_requirements_sha256":"%s","previous_requirements_sha256":"%s","active_requirements_sha256":"%s","expected_adata_sha":"%s","expected_adata_tree_sha256":"%s","previous_adata_sha":"%s","previous_adata_tree_sha256":"%s","active_adata_sha":"%s","active_adata_tree_sha256":"%s","started_at":"%s","ended_at":"%s"}\n' \
    "$status" "$EXPECTED_SHA" "$PREVIOUS_SHA" "$active_sha" \
    "$EXPECTED_REQUIREMENTS_SHA256" \
    "${PREVIOUS_REQUIREMENTS_SHA256:-}" \
    "${ACTIVE_REQUIREMENTS_SHA256:-}" "$EXPECTED_ADATA_SHA" \
    "$EXPECTED_ADATA_TREE_SHA256" "${PREVIOUS_ADATA_SHA:-}" \
    "${PREVIOUS_ADATA_TREE_SHA256:-}" "${ACTIVE_ADATA_SHA:-}" \
    "${ACTIVE_ADATA_TREE_SHA256:-}" \
    "$DEPLOY_STARTED_AT" "$ended_at" \
    | sudo tee "$receipt_tmp" >/dev/null; then
    sudo rm -f "$receipt_tmp"
    return 1
  fi
  if ! sudo chmod 0600 "$receipt_tmp" || \
    ! sudo mv -f "$receipt_tmp" "$RECEIPT_DIR/$RECEIPT_ID.json"; then
    sudo rm -f "$receipt_tmp"
    return 1
  fi
}
SERVICE_USER="$(systemctl show -p User --value probiga)"
test -n "$SERVICE_USER"
test "$SERVICE_USER" != root
sudo -u "$SERVICE_USER" test ! -w /opt/ProBigA
REPOSITORY_ROOT=/opt/ProBigA
if ! sudo git config --system --get-all safe.directory \
  | grep -Fxq "$REPOSITORY_ROOT"; then
  sudo git config --system --add safe.directory "$REPOSITORY_ROOT"
fi
LEGACY_ADATA_REPOSITORY=/opt/ProBigA/adata
if [ -d "$LEGACY_ADATA_REPOSITORY/.git" ] && \
  ! sudo git config --system --get-all safe.directory \
    | grep -Fxq "$LEGACY_ADATA_REPOSITORY"; then
  sudo git config --system --add safe.directory "$LEGACY_ADATA_REPOSITORY"
fi
LEGACY_STATE_DIR="$RECEIPT_DIR/legacy-state-$RECEIPT_ID"
preserve_tracked_worktree_state() {
  local repository="$1"
  local label="$2"
  local status_file
  local patch_file
  local manifest_file
  local tracked_status
  tracked_status="$(git -C "$repository" status \
    --porcelain --untracked-files=no)"
  if [ -z "$tracked_status" ]; then
    return 0
  fi
  sudo mkdir -p "$LEGACY_STATE_DIR"
  sudo chown root:root "$LEGACY_STATE_DIR"
  sudo chmod 0700 "$LEGACY_STATE_DIR"
  status_file="$LEGACY_STATE_DIR/$label.status"
  patch_file="$LEGACY_STATE_DIR/$label.patch"
  manifest_file="$LEGACY_STATE_DIR/$label.sha256"
  printf '%s\n' "$tracked_status" | sudo tee "$status_file" >/dev/null
  git -C "$repository" diff --binary --full-index HEAD -- \
    | sudo tee "$patch_file" >/dev/null
  (
    cd "$LEGACY_STATE_DIR"
    sha256sum "$label.status" "$label.patch"
  ) | sudo tee "$manifest_file" >/dev/null
  sudo chown root:root "$status_file" "$patch_file" "$manifest_file"
  sudo chmod 0600 "$status_file" "$patch_file" "$manifest_file"
  echo "Preserved tracked legacy $label changes in $LEGACY_STATE_DIR" >&2
}
preserve_tracked_worktree_state "$REPOSITORY_ROOT" repository
if [ -d "$LEGACY_ADATA_REPOSITORY/.git" ]; then
  preserve_tracked_worktree_state "$LEGACY_ADATA_REPOSITORY" adata
fi
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
quarantine_unsafe_untracked_release_files
RELEASE_VENV_ROOT=/opt/ProBigA/.release_venvs
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
    case "$active_state" in
      inactive|failed) ;;
      *)
        echo "scheduler activation unit is active: $trigger_unit ($active_state)" >&2
        return 2
        ;;
    esac
    unit_file_state="$(systemctl show -p UnitFileState --value "$trigger_unit")" || \
      return 2
    case "$unit_file_state" in
      disabled|masked|static|'') ;;
      *)
        echo "scheduler activation unit is enabled: $trigger_unit ($unit_file_state)" >&2
        return 2
        ;;
    esac
  done
}
assert_service_cannot_write_release_paths() {
  local writable_path
  local writable_root_file
  writable_path="$(sudo -u "$SERVICE_USER" find \
    .git .github deploy server biz integrations tools scripts \
    strategies versions artifacts/trading_v4 artifacts/trading_v5 \
    artifacts/trading_v6 requirements-platform.txt .gitattributes \
    .gitignore -writable -print -quit 2>/dev/null || true)"
  if [ -n "$writable_path" ]; then
    echo "service account can modify protected release paths: $writable_path" >&2
    return 2
  fi
  writable_root_file="$(sudo -u "$SERVICE_USER" find . -maxdepth 1 \
    -type f \( -name '*.py' -o -name '*.pyw' -o -name '*.pyc' \
    -o -name '*.pyd' -o -name '*.so' \) -writable -print -quit \
    2>/dev/null || true)"
  if [ -n "$writable_root_file" ]; then
    echo "service account can modify root executable code: $writable_root_file" >&2
    return 2
  fi
}
seal_release_checkout() {
  declare -A tracked_directories=()
  local directory
  local entry
  local git_mode
  local tracked_path
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
  find .git -type f -exec chown root:root -- {} + \
    -exec chmod 0444 -- {} +
  find .git -type d -exec chown root:root -- {} + \
    -exec chmod 0555 -- {} +
}
assert_scheduler_triggers_quiescent
write_dropin() {
  local revision="$1"
  local adata_sha="$2"
  local adata_tree_sha="$3"
  local adata_source="$4"
  printf '%s\n' \
    '[Service]' \
    'WorkingDirectory=/opt/ProBigA' \
    'ExecStart=' \
    "ExecStart=$RELEASE_VENV_ROOT/$revision/bin/python -m uvicorn server.api.main:app --host 127.0.0.1 --port 8000" \
    'Environment=API_EMBEDDED_SCHEDULER_ENABLED=true' \
    'Environment=PROBIGA_IN_APP_DEPLOY_ENABLED=0' \
    'Environment=PROBIGA_DEPLOYMENT_MODE=production' \
    'Environment=PROBIGA_ADMIN_AUTH_ENABLED=true' \
    'Environment=GIT_OPTIONAL_LOCKS=0' \
    'Environment=PYTHONDONTWRITEBYTECODE=1' \
    "Environment=PROBIGA_EXPECTED_GIT_SHA=$revision" \
    "Environment=PROBIGA_EXPECTED_ADATA_SHA=$adata_sha" \
    "Environment=PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha" \
    "Environment=PROBIGA_ADATA_SOURCE_DIR=$adata_source" \
    "Environment=PYTHONPATH=$adata_source:/opt/ProBigA" \
    | sudo tee /etc/systemd/system/probiga.service.d/scheduler.conf >/dev/null
}
BOOTSTRAP_PYTHON=/usr/bin/python3.14
test -x "$BOOTSTRAP_PYTHON"
test "$(stat -c '%U' "$BOOTSTRAP_PYTHON")" = root
sudo -u "$SERVICE_USER" test ! -w "$BOOTSTRAP_PYTHON"
test "$($BOOTSTRAP_PYTHON -I -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" = "3.14"
PREVIOUS_DROPIN="$(mktemp)"
PREVIOUS_DROPIN_PRESENT=0
if sudo test -f /etc/systemd/system/probiga.service.d/scheduler.conf; then
  sudo cat /etc/systemd/system/probiga.service.d/scheduler.conf > "$PREVIOUS_DROPIN"
  PREVIOUS_DROPIN_PRESENT=1
fi
dropin_environment_value() {
  local name="$1"
  sed -n "s|^Environment=$name=||p" "$PREVIOUS_DROPIN" | tail -n 1
}
PREVIOUS_RELEASE_REVISION="$(sed -n \
  "s|^ExecStart=$RELEASE_VENV_ROOT/\([0-9a-f]\{40\}\)/bin/python .*|\1|p" \
  "$PREVIOUS_DROPIN" | tail -n 1)"
PREVIOUS_REQUIREMENTS_SHA256=""
if [ -n "$PREVIOUS_RELEASE_REVISION" ]; then
  test "$PREVIOUS_RELEASE_REVISION" = "$PREVIOUS_SHA"
  PREVIOUS_VENV="$RELEASE_VENV_ROOT/$PREVIOUS_RELEASE_REVISION"
  test -L "$PREVIOUS_VENV"
  PREVIOUS_VENV_TARGET="$(readlink -f "$PREVIOUS_VENV")"
  case "$PREVIOUS_VENV_TARGET" in
    "$RELEASE_VENV_ROOT"/build-*) ;;
    *) echo "previous release venv escaped its immutable root" >&2; exit 2 ;;
  esac
  test -x "$PREVIOUS_VENV/bin/python"
  PREVIOUS_REQUIREMENTS_SHA256="$(cat \
    "$PREVIOUS_VENV/.requirements.sha256")"
  [[ "$PREVIOUS_REQUIREMENTS_SHA256" =~ ^[0-9a-f]{64}$ ]]
  PREVIOUS_LOCK_SNAPSHOT="$(mktemp)"
  PYTHONDONTWRITEBYTECODE=1 "$PREVIOUS_VENV/bin/python" \
    -m pip freeze --all --exclude-editable \
    | awk 'tolower($0) !~ /^adata([[:space:]]|==|@)/' \
    | LC_ALL=C sort > "$PREVIOUS_LOCK_SNAPSHOT"
  test "$(sha256sum "$PREVIOUS_LOCK_SNAPSHOT" | cut -d' ' -f1)" = \
    "$PREVIOUS_REQUIREMENTS_SHA256"
  rm -f "$PREVIOUS_LOCK_SNAPSHOT"
  assert_service_cannot_write_tree "$PREVIOUS_VENV_TARGET" \
    "previous release virtual environment"
fi
PREVIOUS_ADATA_SHA="$(dropin_environment_value PROBIGA_EXPECTED_ADATA_SHA)"
PREVIOUS_ADATA_TREE_SHA256="$(dropin_environment_value PROBIGA_EXPECTED_ADATA_TREE_SHA256)"
PREVIOUS_ADATA_SOURCE="$(dropin_environment_value PROBIGA_ADATA_SOURCE_DIR)"
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
  test -d /opt/ProBigA/adata/.git
  PREVIOUS_ADATA_SHA="$(git -C /opt/ProBigA/adata rev-parse HEAD)"
  [[ "$PREVIOUS_ADATA_SHA" =~ ^[0-9a-f]{40}$ ]]
  PREVIOUS_ADATA_TREE_SHA256=""
  PREVIOUS_ADATA_SOURCE=/opt/ProBigA/adata
fi
SCHEDULER_UNIT_PRESENT=0
PREVIOUS_SCHEDULER_ACTIVE=0
PREVIOUS_SCHEDULER_ENABLED=0
if systemctl list-unit-files probiga-scheduler.service --no-legend \
  | grep -q '^probiga-scheduler.service'; then
  SCHEDULER_UNIT_PRESENT=1
  systemctl is-active --quiet probiga-scheduler && PREVIOUS_SCHEDULER_ACTIVE=1 || true
  systemctl is-enabled --quiet probiga-scheduler && PREVIOUS_SCHEDULER_ENABLED=1 || true
fi
ADATA_REPOSITORY_URL=https://github.com/1nchaos/adata.git
ADATA_GIT_CACHE=/opt/ProBigA/.release_sources/adata.git
ADATA_RUNTIME_ROOT=/var/lib/probiga/release-sources/adata
mkdir -p "$(dirname "$ADATA_GIT_CACHE")"
if [ ! -d "$ADATA_GIT_CACHE" ]; then
  ADATA_CACHE_BUILD="$(mktemp -d /opt/ProBigA/.release_sources/adata-git.XXXXXX)"
  if git -C "$LEGACY_ADATA_REPOSITORY" cat-file -e \
    "${EXPECTED_ADATA_SHA}^{commit}"; then
    git clone --mirror "$LEGACY_ADATA_REPOSITORY" \
      "$ADATA_CACHE_BUILD/repository.git"
    git --git-dir="$ADATA_CACHE_BUILD/repository.git" remote set-url origin \
      "$ADATA_REPOSITORY_URL"
  else
    git -c http.lowSpeedLimit=1024 -c http.lowSpeedTime=30 clone --mirror \
      "$ADATA_REPOSITORY_URL" "$ADATA_CACHE_BUILD/repository.git"
  fi
  mv "$ADATA_CACHE_BUILD/repository.git" "$ADATA_GIT_CACHE"
  rmdir "$ADATA_CACHE_BUILD"
fi
test "$(git --git-dir="$ADATA_GIT_CACHE" remote get-url origin)" = "$ADATA_REPOSITORY_URL"
if ! git --git-dir="$ADATA_GIT_CACHE" cat-file -e \
  "${EXPECTED_ADATA_SHA}^{commit}"; then
  git -c http.lowSpeedLimit=1024 -c http.lowSpeedTime=30 \
    --git-dir="$ADATA_GIT_CACHE" fetch --no-tags origin \
    "$EXPECTED_ADATA_SHA"
fi
test "$(git --git-dir="$ADATA_GIT_CACHE" rev-parse "${EXPECTED_ADATA_SHA}^{commit}")" = \
  "$EXPECTED_ADATA_SHA"
rollback() {
  local failed_status="${1:-$?}"
  local rollback_failed=0
  local current_sha=""
  local observed_scheduler_active=0
  local observed_scheduler_enabled=0
  local restoration_ready=1
  local service_active_state=""
  local services_quiescent=1
  trap - ERR TERM INT
  set +e
  echo "Deployment failed; rolling back to $PREVIOUS_SHA" >&2

  rollback_failure() {
    echo "Rollback step failed: $1" >&2
    rollback_failed=1
  }

  if [ "$services_quiescent" -eq 1 ] && \
    [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
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
            "probiga-scheduler remained $service_active_state before checkout"
          services_quiescent=0
          ;;
      esac
    fi
  fi
  sudo systemctl stop probiga || rollback_failure "stop probiga"
  if ! service_active_state="$(systemctl show -p ActiveState --value \
    probiga)"; then
    rollback_failure "inspect probiga stop state"
    services_quiescent=0
  else
    case "$service_active_state" in
      inactive|failed) ;;
      *)
        rollback_failure \
          "probiga remained $service_active_state before checkout"
        services_quiescent=0
        ;;
    esac
  fi
  if [ "$services_quiescent" -eq 1 ]; then
    if ! git checkout --detach "$PREVIOUS_SHA"; then
      rollback_failure "checkout previous Git revision"
      restoration_ready=0
    fi
    if [ "$restoration_ready" -eq 1 ]; then
      if [ "$PREVIOUS_DROPIN_PRESENT" -eq 1 ]; then
        if ! sudo cp "$PREVIOUS_DROPIN" \
          /etc/systemd/system/probiga.service.d/scheduler.conf; then
          rollback_failure "restore previous probiga drop-in"
          restoration_ready=0
        fi
      elif ! sudo rm -f \
        /etc/systemd/system/probiga.service.d/scheduler.conf; then
        rollback_failure "remove release probiga drop-in"
        restoration_ready=0
      fi
    fi
    if [ "$restoration_ready" -eq 1 ] && \
      ! sudo systemctl daemon-reload; then
      rollback_failure "systemd daemon-reload"
      restoration_ready=0
    fi
    if [ "$restoration_ready" -eq 1 ]; then
      sudo systemctl start probiga || rollback_failure "start probiga"
    else
      rollback_failure "probiga restart skipped after unsafe restore state"
    fi
  else
    rollback_failure "services were not quiescent; code restoration skipped"
    restoration_ready=0
  fi
  if [ "$restoration_ready" -eq 1 ] && \
    [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
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
  sudo systemctl is-active --quiet probiga || \
    rollback_failure "verify probiga is active"
  curl --fail --silent --show-error --retry 15 --retry-all-errors \
    --retry-delay 2 --retry-connrefused \
    http://127.0.0.1/api/health >/dev/null || \
    rollback_failure "verify previous API health"
  current_sha="$(git rev-parse HEAD 2>/dev/null)"
  if [ "$current_sha" != "$PREVIOUS_SHA" ]; then
    rollback_failure "verify previous Git revision"
  fi
  if [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
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
  fi
  assert_scheduler_triggers_quiescent || \
    rollback_failure "verify scheduler activation units remain quiescent"

  if [ "$rollback_failed" -ne 0 ]; then
    ACTIVE_REQUIREMENTS_SHA256=""
    ACTIVE_ADATA_SHA=""
    ACTIVE_ADATA_TREE_SHA256=""
    echo "Rollback verification failed" >&2
    write_receipt "ROLLBACK_FAILED" "$current_sha" || true
  else
    ACTIVE_REQUIREMENTS_SHA256="$PREVIOUS_REQUIREMENTS_SHA256"
    ACTIVE_ADATA_SHA="$PREVIOUS_ADATA_SHA"
    ACTIVE_ADATA_TREE_SHA256="$PREVIOUS_ADATA_TREE_SHA256"
    write_receipt "ROLLED_BACK" "$PREVIOUS_SHA" || true
  fi
  exit "$failed_status"
}
trap 'rollback $?' ERR
trap 'rollback 143' TERM
trap 'rollback 130' INT
mkdir -p "$RELEASE_VENV_ROOT"
sudo -u "$SERVICE_USER" test ! -w "$RELEASE_VENV_ROOT"
if [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
  sudo systemctl stop probiga-scheduler
  ! systemctl is-active --quiet probiga-scheduler
  sudo systemctl disable probiga-scheduler
fi
sudo systemctl stop probiga
git cat-file -e "${EXPECTED_SHA}^{commit}"
git checkout --detach --force "$EXPECTED_SHA"
find server biz integrations tools scripts strategies versions \
  -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete
seal_release_checkout
assert_service_cannot_write_release_paths
test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"
if [ -n "$PREVIOUS_ADATA_TREE_SHA256" ]; then
  $BOOTSTRAP_PYTHON -I server/common/adata_release.py verify \
    --source "$PREVIOUS_ADATA_SOURCE" --git-sha "$PREVIOUS_ADATA_SHA" \
    --tree-sha256 "$PREVIOUS_ADATA_TREE_SHA256"
fi
RESOLVED_LOCK="$(mktemp)"
printf '%s' "$RESOLVED_REQUIREMENTS_B64" | base64 -d > "$RESOLVED_LOCK"
test "$(sha256sum "$RESOLVED_LOCK" | cut -d' ' -f1)" = \
  "$EXPECTED_REQUIREMENTS_SHA256"
verify_venv_dependency_lock() {
  local venv_path="$1"
  local observed_lock
  observed_lock="$(mktemp)"
  "$venv_path/bin/python" -m pip freeze --all --exclude-editable \
    | awk 'tolower($0) !~ /^adata([[:space:]]|==|@)/' \
    | LC_ALL=C sort > "$observed_lock"
  test "$(sha256sum "$observed_lock" | cut -d' ' -f1)" = \
    "$EXPECTED_REQUIREMENTS_SHA256"
  rm -f "$observed_lock"
}
sudo mkdir -p "$ADATA_RUNTIME_ROOT"
sudo chown root:root "$ADATA_RUNTIME_ROOT" "$(dirname "$ADATA_RUNTIME_ROOT")"
sudo chmod 0755 "$ADATA_RUNTIME_ROOT" "$(dirname "$ADATA_RUNTIME_ROOT")"
ADATA_SOURCE="$ADATA_RUNTIME_ROOT/$EXPECTED_ADATA_SHA-$EXPECTED_ADATA_TREE_SHA256"
if [ ! -d "$ADATA_SOURCE" ]; then
  ADATA_SOURCE_BUILD="$(mktemp -d)"
  git --git-dir="$ADATA_GIT_CACHE" archive "$EXPECTED_ADATA_SHA" \
    | tar -xf - -C "$ADATA_SOURCE_BUILD"
  SEAL_JSON="$($BOOTSTRAP_PYTHON -I server/common/adata_release.py seal \
    --source "$ADATA_SOURCE_BUILD" --git-sha "$EXPECTED_ADATA_SHA")"
  SEALED_TREE_SHA="$(printf '%s' "$SEAL_JSON" | $BOOTSTRAP_PYTHON -I -c \
    'import json,sys; print(json.load(sys.stdin)["tree_sha256"])')"
  test "$SEALED_TREE_SHA" = "$EXPECTED_ADATA_TREE_SHA256"
  chmod -R a-w "$ADATA_SOURCE_BUILD"
  sudo mv "$ADATA_SOURCE_BUILD" "$ADATA_SOURCE"
  sudo chown -R root:root "$ADATA_SOURCE"
fi
sudo -u "$SERVICE_USER" test ! -w "$ADATA_RUNTIME_ROOT"
sudo -u "$SERVICE_USER" test ! -w "$(dirname "$ADATA_RUNTIME_ROOT")"
sudo -u "$SERVICE_USER" test ! -w "$(dirname "$(dirname "$ADATA_RUNTIME_ROOT")")"
sudo -u "$SERVICE_USER" test ! -w "$ADATA_SOURCE"
$BOOTSTRAP_PYTHON -I server/common/adata_release.py verify \
  --source "$ADATA_SOURCE" --git-sha "$EXPECTED_ADATA_SHA" \
  --tree-sha256 "$EXPECTED_ADATA_TREE_SHA256"
if [ -e "$RELEASE_VENV_ROOT/$EXPECTED_SHA" ]; then
  test -L "$RELEASE_VENV_ROOT/$EXPECTED_SHA"
  EXPECTED_VENV_TARGET="$(readlink -f "$RELEASE_VENV_ROOT/$EXPECTED_SHA")"
  case "$EXPECTED_VENV_TARGET" in
    "$RELEASE_VENV_ROOT"/build-*) ;;
    *) echo "release venv target escaped its immutable root" >&2; exit 2 ;;
  esac
  sudo -u "$SERVICE_USER" test ! -w "$EXPECTED_VENV_TARGET"
  test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.requirements.sha256")" = \
    "$EXPECTED_REQUIREMENTS_SHA256"
  test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.adata.gitsha")" = \
    "$EXPECTED_ADATA_SHA"
  test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.adata.tree.sha256")" = \
    "$EXPECTED_ADATA_TREE_SHA256"
  verify_venv_dependency_lock "$RELEASE_VENV_ROOT/$EXPECTED_SHA"
  assert_service_cannot_write_tree "$EXPECTED_VENV_TARGET" \
    "reused release virtual environment"
else
  EXPECTED_BUILD="$RELEASE_VENV_ROOT/build-$EXPECTED_SHA-$RANDOM"
  $BOOTSTRAP_PYTHON -I -m venv "$EXPECTED_BUILD"
  "$EXPECTED_BUILD/bin/python" -m pip install -r "$RESOLVED_LOCK" --quiet
  ADATA_BUILD_SOURCE="$(mktemp -d)"
  ADATA_WHEEL_DIR="$(mktemp -d)"
  git --git-dir="$ADATA_GIT_CACHE" archive "$EXPECTED_ADATA_SHA" \
    | tar -xf - -C "$ADATA_BUILD_SOURCE"
  "$EXPECTED_BUILD/bin/python" -m pip wheel --no-deps \
    --wheel-dir "$ADATA_WHEEL_DIR" "$ADATA_BUILD_SOURCE" --quiet
  mapfile -t ADATA_WHEELS < <(find "$ADATA_WHEEL_DIR" -maxdepth 1 \
    -type f -name '*.whl' -print)
  test "${#ADATA_WHEELS[@]}" -eq 1
  "$EXPECTED_BUILD/bin/python" -m pip install --no-deps \
    "${ADATA_WHEELS[0]}" --quiet
  printf '%s\n' "$EXPECTED_REQUIREMENTS_SHA256" \
    > "$EXPECTED_BUILD/.requirements.sha256"
  printf '%s\n' "$EXPECTED_ADATA_SHA" > "$EXPECTED_BUILD/.adata.gitsha"
  printf '%s\n' "$EXPECTED_ADATA_TREE_SHA256" \
    > "$EXPECTED_BUILD/.adata.tree.sha256"
  verify_venv_dependency_lock "$EXPECTED_BUILD"
  chmod -R a-w "$EXPECTED_BUILD"
  assert_service_cannot_write_tree "$EXPECTED_BUILD" \
    "new release virtual environment"
  rm -rf "$ADATA_BUILD_SOURCE" "$ADATA_WHEEL_DIR"
  ln -s "$EXPECTED_BUILD" "$RELEASE_VENV_ROOT/$EXPECTED_SHA"
fi
"$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" \
  tools/validate_production_release_boundary.py \
  --require-git-anchor --expected-git-sha "$EXPECTED_SHA"
# The Windows QMT bridge only runs registered task types. Refuse activation
# unless the four delivery tasks were provisioned explicitly and match this
# release; production deployment itself remains read-only with respect to DB.
sudo -u "$SERVICE_USER" env PYTHONDONTWRITEBYTECODE=1 \
  "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" \
  tools/ensure_quality_gate.py --validate-review-delivery
rm -f "$RESOLVED_LOCK"
sudo mkdir -p /etc/systemd/system/probiga.service.d
write_dropin "$EXPECTED_SHA" "$EXPECTED_ADATA_SHA" \
  "$EXPECTED_ADATA_TREE_SHA256" "$ADATA_SOURCE"
sudo systemctl daemon-reload
sudo systemctl restart probiga
curl --fail --silent --show-error --retry 15 --retry-all-errors \
  --retry-delay 2 --retry-connrefused \
  http://127.0.0.1/api/health >/dev/null
sudo systemctl is-active --quiet probiga
if [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
  ! systemctl is-active --quiet probiga-scheduler
  ! systemctl is-enabled --quiet probiga-scheduler
fi
assert_scheduler_triggers_quiescent
test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"
sudo -u "$SERVICE_USER" env \
  "PYTHONPATH=$ADATA_SOURCE:/opt/ProBigA" \
  "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" \
  tools/ensure_quality_gate.py \
  --task-type analysis_premarket_external
ACTIVE_REQUIREMENTS_SHA256="$EXPECTED_REQUIREMENTS_SHA256"
ACTIVE_ADATA_SHA="$EXPECTED_ADATA_SHA"
ACTIVE_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256"
write_receipt "DEPLOYED" "$EXPECTED_SHA"
trap - ERR TERM INT
rm -f "$PREVIOUS_DROPIN"
