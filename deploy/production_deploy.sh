#!/usr/bin/env bash
# Production deployment logic invoked by the pinned GitHub SSH action.
# Inputs are passed explicitly through the action env allowlist.
# ERR inheritance catches failures inside preparation helpers. rollback()
# fences child shells by BASHPID so a subshell cannot perform system rollback.
set -Eeuo pipefail
umask 022
REPOSITORY_ROOT=/opt/ProBigA
CODE_GIT_CACHE=/var/lib/probiga/release-sources/probiga.git
CODE_RELEASE_ROOT=/opt/ProBigA-releases
RELEASE_VENV_ROOT=/var/lib/probiga/release-venvs
LEGACY_RELEASE_VENV_ROOT=/opt/ProBigA/.release_venvs
DEPLOY_LOCK_ROOT=/run/probiga
DEPLOY_LOCK_FILE="$DEPLOY_LOCK_ROOT/production-deploy.lock"
REQUIRED_DEPLOY_PROTOCOL=probiga-production-deploy-v2
if [ "${EUID:-$(id -u)}" -ne 0 ]; then
  echo "production deploy engine must run through the root broker" >&2
  exit 2
fi
if [ "${PROBIGA_DEPLOY_PROTOCOL_VERSION:-}" != "$REQUIRED_DEPLOY_PROTOCOL" ]; then
  echo "production deploy broker protocol mismatch; install the new root broker out of band" >&2
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
: "${EXPECTED_SHA:?EXPECTED_SHA is required}"
: "${RESOLVED_REQUIREMENTS_B64:?RESOLVED_REQUIREMENTS_B64 is required}"
: "${EXPECTED_REQUIREMENTS_SHA256:?EXPECTED_REQUIREMENTS_SHA256 is required}"
: "${EXPECTED_ADATA_SHA:?EXPECTED_ADATA_SHA is required}"
: "${EXPECTED_ADATA_TREE_SHA256:?EXPECTED_ADATA_TREE_SHA256 is required}"
[[ "$EXPECTED_ADATA_SHA" =~ ^[0-9a-f]{40}$ ]]
[[ "$EXPECTED_ADATA_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]]
LEGACY_LIVE_SHA="$(git rev-parse HEAD)"
PREVIOUS_SHA="$LEGACY_LIVE_SHA"
DEPLOY_MAIN_BASHPID="$BASHPID"
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
precutover_failure() {
  local failed_status="$1"
  local failed_line="$2"
  if [ "$BASHPID" != "$DEPLOY_MAIN_BASHPID" ]; then
    trap - ERR TERM INT
    exit "$failed_status"
  fi
  trap - ERR TERM INT
  set +e
  printf 'deploy_failure phase=preflight line=%s status=%s\n' \
    "$failed_line" "$failed_status" >&2
  write_receipt "PREFLIGHT_FAILED" "$PREVIOUS_SHA" || true
  exit "$failed_status"
}
trap 'precutover_failure "$?" "$LINENO"' ERR
MAIN_SERVICE=probiga
SERVICE_USER="$(systemctl show -p User --value "$MAIN_SERVICE")"
test -n "$SERVICE_USER"
test "$SERVICE_USER" != root
sudo -u "$SERVICE_USER" test ! -w /opt/ProBigA
AI_WORKER_SERVICE=probiga-ai-recommendation-worker.service
AI_WORKER_TIMER=probiga-ai-recommendation-worker.timer
AI_WORKER_DROPIN=/etc/systemd/system/probiga-ai-recommendation-worker.service.d/release-runtime.conf
SCHEDULER_UNIT=/etc/systemd/system/probiga-scheduler.service
MAIN_RELEASE_DROPIN=/etc/systemd/system/probiga.service.d/scheduler.conf
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
  local output_file="$6"
  printf '%s\n' \
    '[Service]' \
    'WorkingDirectory=/opt/ProBigA' \
    'ExecStart=' \
    "ExecStart=/usr/bin/env API_EMBEDDED_SCHEDULER_ENABLED=false PROBIGA_IN_APP_DEPLOY_ENABLED=0 PROBIGA_DEPLOYMENT_MODE=production PROBIGA_ADMIN_AUTH_ENABLED=true GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PROBIGA_EXPECTED_GIT_SHA=$revision PROBIGA_BUILD_COMMIT_SHA=$revision PROBIGA_CODE_ROOT=$code_root PROBIGA_EXPECTED_ADATA_SHA=$adata_sha PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha PROBIGA_ADATA_SOURCE_DIR=$adata_source PYTHONPATH=$adata_source:$code_root $RELEASE_VENV_ROOT/$revision/bin/python -P -m uvicorn server.api.main:app --host 127.0.0.1 --port 8000" \
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
    "Environment=PYTHONPATH=$adata_source:$code_root" \
    > "$output_file"
}
write_scheduler_dropin() {
  local revision="$1"
  local code_root="$2"
  local adata_sha="$3"
  local adata_tree_sha="$4"
  local adata_source="$5"
  local output_file="$6"
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
    "ExecStart=/usr/bin/env API_EMBEDDED_SCHEDULER_ENABLED=false PROBIGA_DEPLOYMENT_MODE=production GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PROBIGA_EXPECTED_GIT_SHA=$revision PROBIGA_BUILD_COMMIT_SHA=$revision PROBIGA_CODE_ROOT=$code_root PROBIGA_EXPECTED_ADATA_SHA=$adata_sha PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha PROBIGA_ADATA_SOURCE_DIR=$adata_source PYTHONPATH=$adata_source:$code_root $RELEASE_VENV_ROOT/$revision/bin/python -P $code_root/tools/run_scheduler_daemon.py" \
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
  local output_file="$6"
  printf '%s\n' \
    '[Service]' \
    "User=$SERVICE_USER" \
    "Group=$SERVICE_USER" \
    'WorkingDirectory=/opt/ProBigA' \
    'ExecStart=' \
    "ExecStart=/usr/bin/env GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PROBIGA_DEPLOYMENT_MODE=production PROBIGA_EXPECTED_GIT_SHA=$revision PROBIGA_CODE_ROOT=$code_root PROBIGA_EXPECTED_ADATA_SHA=$adata_sha PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha PROBIGA_ADATA_SOURCE_DIR=$adata_source PYTHONPATH=$adata_source:$code_root $RELEASE_VENV_ROOT/$revision/bin/python -P $code_root/tools/run_ai_recommendation_worker.py --once" \
    'Environment=GIT_OPTIONAL_LOCKS=0' \
    'Environment=PYTHONDONTWRITEBYTECODE=1' \
    'Environment=PYTHONSAFEPATH=1' \
    'Environment=PROBIGA_DEPLOYMENT_MODE=production' \
    "Environment=PROBIGA_EXPECTED_GIT_SHA=$revision" \
    "Environment=PROBIGA_CODE_ROOT=$code_root" \
    "Environment=PROBIGA_EXPECTED_ADATA_SHA=$adata_sha" \
    "Environment=PROBIGA_EXPECTED_ADATA_TREE_SHA256=$adata_tree_sha" \
    "Environment=PROBIGA_ADATA_SOURCE_DIR=$adata_source" \
    "Environment=PYTHONPATH=$adata_source:$code_root" \
    > "$output_file"
}
assert_ai_worker_runtime() {
  local revision="$1"
  local venv_path="${2:-$RELEASE_VENV_ROOT/$revision}"
  local code_root="${3:-$CODE_RELEASE_ROOT/$revision}"
  test "$(systemctl show -p User --value "$AI_WORKER_SERVICE")" = \
    "$SERVICE_USER"
  test "$(systemctl show -p Group --value "$AI_WORKER_SERVICE")" = \
    "$SERVICE_USER"
  test "$(systemctl show -p WorkingDirectory --value "$AI_WORKER_SERVICE")" = \
    /opt/ProBigA
  systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
    | grep -F -- 'PYTHONDONTWRITEBYTECODE=1' >/dev/null
  systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
    | grep -F -- 'PYTHONSAFEPATH=1' >/dev/null
  systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
    | grep -F -- "$venv_path/bin/python" >/dev/null
  systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
    | grep -F -- ' -P ' >/dev/null
  systemctl show -p ExecStart --value "$AI_WORKER_SERVICE" \
    | grep -F -- "$code_root/tools/run_ai_recommendation_worker.py --once" >/dev/null
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
  test -L "$STATIC_RELEASE_LINK"
  test "$(readlink -f "$STATIC_RELEASE_LINK")" = "$checkout_root"
  for asset in js/app.js css/style.css; do
    response="$(mktemp)"
    if ! curl --fail --silent --show-error \
      -H 'Cache-Control: no-cache' \
      "http://127.0.0.1/static/$asset" > "$response"; then
      rm -f "$response"
      return 1
    fi
    if ! cmp --silent "$checkout_root/server/static/$asset" "$response"; then
      rm -f "$response"
      echo "Nginx served stale static asset: $asset" >&2
      return 1
    fi
    rm -f "$response"
  done
}
release_identity_check() {
  local require_clean="$1"
  local checkout_root="${2:-$REPOSITORY_ROOT}"
  (
  cd "$checkout_root"
  sudo -u "$SERVICE_USER" env \
    GIT_OPTIONAL_LOCKS=0 \
    GIT_CONFIG_COUNT=1 \
    GIT_CONFIG_KEY_0=safe.directory \
    GIT_CONFIG_VALUE_0="$checkout_root" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONSAFEPATH=1 \
    PROBIGA_DEPLOYMENT_MODE=production \
    PROBIGA_EXPECTED_GIT_SHA="$EXPECTED_SHA" \
    PROBIGA_EXPECTED_ADATA_SHA="$EXPECTED_ADATA_SHA" \
    PROBIGA_EXPECTED_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256" \
    PROBIGA_ADATA_SOURCE_DIR="$ADATA_SOURCE" \
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
    echo "running probiga service did not expose a valid main PID" >&2
    exit 2
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
PREVIOUS_REQUIREMENTS_SHA256=""
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
    test "$PREVIOUS_SHA" = "$LEGACY_LIVE_SHA"
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
EXTERNAL_WRITER_BLOCKED=0
AI_WORKER_UNIT_PRESENT=0
PREVIOUS_AI_WORKER_TIMER_ACTIVE=0
PREVIOUS_AI_WORKER_TIMER_ENABLED=0
AI_WORKER_SERVICE_LOAD="$(systemctl show -p LoadState --value \
  "$AI_WORKER_SERVICE")"
AI_WORKER_TIMER_LOAD="$(systemctl show -p LoadState --value \
  "$AI_WORKER_TIMER")"
if [ "$AI_WORKER_SERVICE_LOAD" != not-found ] || \
  [ "$AI_WORKER_TIMER_LOAD" != not-found ]; then
  test "$AI_WORKER_SERVICE_LOAD" != not-found
  test "$AI_WORKER_TIMER_LOAD" != not-found
  AI_WORKER_UNIT_PRESENT=1
  systemctl is-active --quiet "$AI_WORKER_TIMER" && \
    PREVIOUS_AI_WORKER_TIMER_ACTIVE=1 || true
  systemctl is-enabled --quiet "$AI_WORKER_TIMER" && \
    PREVIOUS_AI_WORKER_TIMER_ENABLED=1 || true
fi
CODE_REPOSITORY_URL=git@github.com:MingMG/probiga.git
ADATA_REPOSITORY_URL=https://github.com/1nchaos/adata.git
ADATA_GIT_CACHE=/var/lib/probiga/release-sources/adata.git
LEGACY_ADATA_GIT_CACHE=/opt/ProBigA/.release_sources/adata.git
ADATA_RUNTIME_ROOT=/var/lib/probiga/release-sources/adata
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
  [ -z "$HEALTH_RESPONSE" ] || rm -f -- "$HEALTH_RESPONSE"
  if [ -n "$ADATA_SOURCE_BUILD" ]; then
    case "$ADATA_SOURCE_BUILD" in
      "$ADATA_RUNTIME_ROOT"/.build-*) rm -rf -- "$ADATA_SOURCE_BUILD" ;;
    esac
  fi
  for temp_dir in "$ADATA_BUILD_SOURCE" "$ADATA_WHEEL_DIR"; do
    case "$temp_dir" in
      /tmp/tmp.*) rm -rf -- "$temp_dir" ;;
    esac
  done
  if [ -n "$ADATA_CACHE_BUILD" ]; then
    case "$ADATA_CACHE_BUILD" in
      /var/lib/probiga/release-sources/adata-git.*) \
        rm -rf -- "$ADATA_CACHE_BUILD" ;;
    esac
  fi
  if [ "$DEPLOY_SUCCEEDED" -ne 1 ] && [ -n "$EXPECTED_BUILD" ]; then
    if path_is_runtime_referenced "$RELEASE_VENV_ROOT/$EXPECTED_SHA" || \
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
  [ -z "$PREPARED_MAIN_DROPIN" ] || rm -f -- "$PREPARED_MAIN_DROPIN"
  [ -z "$PREPARED_SCHEDULER_DROPIN" ] || \
    rm -f -- "$PREPARED_SCHEDULER_DROPIN"
  [ -z "$PREPARED_AI_WORKER_DROPIN" ] || \
    rm -f -- "$PREPARED_AI_WORKER_DROPIN"
}
verify_venv_dependency_lock() {
  local venv_path="$1"
  local observed_lock
  local observed_sha
  observed_lock="$(mktemp)"
  if ! "$venv_path/bin/python" -m pip freeze --all --exclude-editable \
    | awk 'tolower($0) !~ /^adata([[:space:]]|==|@)/' \
    | LC_ALL=C sort > "$observed_lock"; then
    rm -f "$observed_lock"
    return 2
  fi
  observed_sha="$(sha256sum "$observed_lock" | cut -d' ' -f1)"
  rm -f "$observed_lock"
  test "$observed_sha" = "$EXPECTED_REQUIREMENTS_SHA256"
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
  test ! -L "$CODE_GIT_CACHE"
  test -d "$CODE_GIT_CACHE"
  test "$(git --git-dir="$CODE_GIT_CACHE" rev-parse --is-bare-repository)" = true
  test "$(git --git-dir="$CODE_GIT_CACHE" remote get-url origin)" = \
    "$CODE_REPOSITORY_URL"
  git --git-dir="$CODE_GIT_CACHE" cat-file -e "${EXPECTED_SHA}^{commit}"
  test "$(git --git-dir="$CODE_GIT_CACHE" rev-parse "$EXPECTED_SHA^{commit}")" = \
    "$EXPECTED_SHA"
  chown -R root:root "$CODE_GIT_CACHE"
  chmod -R u+rwX,go+rX,go-w "$CODE_GIT_CACHE"
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
  local adata_seed=""
  local seal_json
  local sealed_tree_sha
  test ! -L "$(dirname "$ADATA_GIT_CACHE")"
  test ! -L "$ADATA_RUNTIME_ROOT"
  install -d -o root -g root -m 0755 "$(dirname "$ADATA_GIT_CACHE")"
  test ! -L "$ADATA_GIT_CACHE"
  if [ ! -d "$ADATA_GIT_CACHE" ]; then
    ADATA_CACHE_BUILD="$(mktemp -d \
      "$(dirname "$ADATA_GIT_CACHE")/adata-git.XXXXXX")"
    if [ -d "$LEGACY_ADATA_GIT_CACHE" ] && \
      git --git-dir="$LEGACY_ADATA_GIT_CACHE" cat-file -e \
        "${EXPECTED_ADATA_SHA}^{commit}"; then
      adata_seed="$LEGACY_ADATA_GIT_CACHE"
    elif [ -d "$LEGACY_ADATA_REPOSITORY/.git" ] && \
      git -C "$LEGACY_ADATA_REPOSITORY" cat-file -e \
        "${EXPECTED_ADATA_SHA}^{commit}"; then
      adata_seed="$LEGACY_ADATA_REPOSITORY"
    fi
    if [ -n "$adata_seed" ]; then
      git clone --mirror --no-hardlinks "$adata_seed" \
        "$ADATA_CACHE_BUILD/repository.git"
    else
      git -c http.lowSpeedLimit=1024 -c http.lowSpeedTime=30 clone --mirror \
        "$ADATA_REPOSITORY_URL" "$ADATA_CACHE_BUILD/repository.git"
    fi
    git --git-dir="$ADATA_CACHE_BUILD/repository.git" remote set-url origin \
      "$ADATA_REPOSITORY_URL"
    mv "$ADATA_CACHE_BUILD/repository.git" "$ADATA_GIT_CACHE"
    rmdir "$ADATA_CACHE_BUILD"
    ADATA_CACHE_BUILD=""
  fi
  test "$(git --git-dir="$ADATA_GIT_CACHE" rev-parse --is-bare-repository)" = true
  test "$(git --git-dir="$ADATA_GIT_CACHE" remote get-url origin)" = \
    "$ADATA_REPOSITORY_URL"
  if ! git --git-dir="$ADATA_GIT_CACHE" cat-file -e \
    "${EXPECTED_ADATA_SHA}^{commit}"; then
    git -c http.lowSpeedLimit=1024 -c http.lowSpeedTime=30 \
      --git-dir="$ADATA_GIT_CACHE" fetch --no-tags origin \
      "$EXPECTED_ADATA_SHA"
  fi
  test "$(git --git-dir="$ADATA_GIT_CACHE" rev-parse \
    "${EXPECTED_ADATA_SHA}^{commit}")" = "$EXPECTED_ADATA_SHA"
  install -d -o root -g root -m 0755 "$ADATA_RUNTIME_ROOT"
  test "$(readlink -f "$ADATA_RUNTIME_ROOT")" = "$ADATA_RUNTIME_ROOT"
  ADATA_SOURCE="$ADATA_RUNTIME_ROOT/$EXPECTED_ADATA_SHA-$EXPECTED_ADATA_TREE_SHA256"
  test ! -L "$ADATA_SOURCE"
  if [ ! -d "$ADATA_SOURCE" ]; then
    ADATA_SOURCE_BUILD="$(mktemp -d \
      "$ADATA_RUNTIME_ROOT/.build-$EXPECTED_ADATA_SHA.XXXXXX")"
    git --git-dir="$ADATA_GIT_CACHE" archive "$EXPECTED_ADATA_SHA" \
      | tar -xf - -C "$ADATA_SOURCE_BUILD"
    seal_json="$("$BOOTSTRAP_PYTHON" -I \
      "$CODE_VALIDATION_ROOT/server/common/adata_release.py" seal \
      --source "$ADATA_SOURCE_BUILD" --git-sha "$EXPECTED_ADATA_SHA")"
    sealed_tree_sha="$(printf '%s' "$seal_json" | "$BOOTSTRAP_PYTHON" -I -c \
      'import json,sys; print(json.load(sys.stdin)["tree_sha256"])')"
    test "$sealed_tree_sha" = "$EXPECTED_ADATA_TREE_SHA256"
    chown -R root:root "$ADATA_SOURCE_BUILD"
    chmod -R a+rX,a-w "$ADATA_SOURCE_BUILD"
    mv "$ADATA_SOURCE_BUILD" "$ADATA_SOURCE"
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
  test ! -L "$RELEASE_VENV_ROOT"
  install -d -o root -g root -m 0755 "$RELEASE_VENV_ROOT"
  test "$(readlink -f "$RELEASE_VENV_ROOT")" = "$RELEASE_VENV_ROOT"
  RESOLVED_LOCK="$(mktemp)"
  printf '%s' "$RESOLVED_REQUIREMENTS_B64" | base64 -d > "$RESOLVED_LOCK"
  test "$(sha256sum "$RESOLVED_LOCK" | cut -d' ' -f1)" = \
    "$EXPECTED_REQUIREMENTS_SHA256"
  if [ -e "$RELEASE_VENV_ROOT/$EXPECTED_SHA" ]; then
    test -L "$RELEASE_VENV_ROOT/$EXPECTED_SHA"
    EXPECTED_VENV_TARGET="$(readlink -f "$RELEASE_VENV_ROOT/$EXPECTED_SHA")"
    case "$EXPECTED_VENV_TARGET" in
      "$RELEASE_VENV_ROOT"/build-*) ;;
      *) echo "release venv target escaped its immutable root" >&2; return 2 ;;
    esac
    test "$(dirname "$EXPECTED_VENV_TARGET")" = "$RELEASE_VENV_ROOT"
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.requirements.sha256")" = \
      "$EXPECTED_REQUIREMENTS_SHA256"
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.probiga.gitsha")" = \
      "$EXPECTED_SHA"
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.adata.gitsha")" = \
      "$EXPECTED_ADATA_SHA"
    test "$(cat "$RELEASE_VENV_ROOT/$EXPECTED_SHA/.adata.tree.sha256")" = \
      "$EXPECTED_ADATA_TREE_SHA256"
    verify_venv_dependency_lock "$RELEASE_VENV_ROOT/$EXPECTED_SHA"
    assert_service_cannot_write_tree "$EXPECTED_VENV_TARGET" \
      "reused release virtual environment"
  else
    EXPECTED_BUILD="$RELEASE_VENV_ROOT/build-$EXPECTED_SHA-$RANDOM"
    "$BOOTSTRAP_PYTHON" -I -m venv "$EXPECTED_BUILD"
    "$EXPECTED_BUILD/bin/python" -m pip install -r "$RESOLVED_LOCK" --quiet
    ADATA_BUILD_SOURCE="$(mktemp -d)"
    ADATA_WHEEL_DIR="$(mktemp -d)"
    git --git-dir="$ADATA_GIT_CACHE" archive "$EXPECTED_ADATA_SHA" \
      | tar -xf - -C "$ADATA_BUILD_SOURCE"
    "$EXPECTED_BUILD/bin/python" -m pip wheel --no-deps \
      --wheel-dir "$ADATA_WHEEL_DIR" "$ADATA_BUILD_SOURCE" --quiet
    mapfile -t adata_wheels < <(find "$ADATA_WHEEL_DIR" -maxdepth 1 \
      -type f -name '*.whl' -print)
    test "${#adata_wheels[@]}" -eq 1
    "$EXPECTED_BUILD/bin/python" -m pip install --no-deps \
      "${adata_wheels[0]}" --quiet
    printf '%s\n' "$EXPECTED_REQUIREMENTS_SHA256" \
      > "$EXPECTED_BUILD/.requirements.sha256"
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
  assert_service_cannot_write_release_paths "$CODE_VALIDATION_ROOT"
  (
    cd "$CODE_VALIDATION_ROOT"
    GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
      "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P \
      tools/validate_production_release_boundary.py \
      --require-git-anchor --expected-git-sha "$EXPECTED_SHA"
    sudo -u "$SERVICE_USER" env GIT_OPTIONAL_LOCKS=0 \
      GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory \
      GIT_CONFIG_VALUE_0="$CODE_VALIDATION_ROOT" \
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
    "$PREPARED_MAIN_DROPIN"
  chmod 0600 "$PREPARED_MAIN_DROPIN"
  grep -Fx 'Environment=API_EMBEDDED_SCHEDULER_ENABLED=false' \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_BUILD_COMMIT_SHA=$EXPECTED_SHA" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -Fx "Environment=PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -F -- 'PYTHONSAFEPATH=1' "$PREPARED_MAIN_DROPIN" >/dev/null
  grep -F -- "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python -P " \
    "$PREPARED_MAIN_DROPIN" >/dev/null
  PREPARED_SCHEDULER_DROPIN="$(mktemp)"
  write_scheduler_dropin "$EXPECTED_SHA" "$PREPARED_CODE_ROOT" \
    "$EXPECTED_ADATA_SHA" "$EXPECTED_ADATA_TREE_SHA256" "$ADATA_SOURCE" \
    "$PREPARED_SCHEDULER_DROPIN"
  chmod 0600 "$PREPARED_SCHEDULER_DROPIN"
  grep -Fx 'Environment=API_EMBEDDED_SCHEDULER_ENABLED=false' \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_BUILD_COMMIT_SHA=$EXPECTED_SHA" \
    "$PREPARED_SCHEDULER_DROPIN" >/dev/null
  grep -Fx "Environment=PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
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
      "$PREPARED_AI_WORKER_DROPIN"
    chmod 0600 "$PREPARED_AI_WORKER_DROPIN"
    grep -Fx "Environment=PROBIGA_CODE_ROOT=$PREPARED_CODE_ROOT" \
      "$PREPARED_AI_WORKER_DROPIN" >/dev/null
    grep -F -- "$PREPARED_CODE_ROOT/tools/run_ai_recommendation_worker.py" \
      "$PREPARED_AI_WORKER_DROPIN" >/dev/null
    grep -F -- 'PYTHONSAFEPATH=1' "$PREPARED_AI_WORKER_DROPIN" >/dev/null
  fi
}
install_prepared_dropins() {
  test -s "$PREPARED_MAIN_DROPIN"
  sudo install -d -o root -g root -m 0755 \
    "$(dirname "$MAIN_RELEASE_DROPIN")"
  for legacy_main_dropin in "${LEGACY_MAIN_OVERRIDE_DROPINS[@]}"; do
    sudo rm -f "$legacy_main_dropin"
  done
  sudo install -o root -g root -m 0644 "$PREPARED_MAIN_DROPIN" \
    "$MAIN_RELEASE_DROPIN"
  test -s "$PREPARED_SCHEDULER_DROPIN"
  sudo install -d -o root -g root -m 0755 \
    "$(dirname "$SCHEDULER_UNIT")"
  SCHEDULER_UNIT_TOUCHED=1
  sudo install -o root -g root -m 0644 "$PREPARED_SCHEDULER_DROPIN" \
    "$SCHEDULER_UNIT"
  for legacy_scheduler_dropin in "${LEGACY_SCHEDULER_OVERRIDE_DROPINS[@]}"; do
    sudo rm -f "$legacy_scheduler_dropin"
  done
  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    test -s "$PREPARED_AI_WORKER_DROPIN"
    sudo install -d -o root -g root -m 0755 \
      "$(dirname "$AI_WORKER_DROPIN")"
    sudo install -o root -g root -m 0644 "$PREPARED_AI_WORKER_DROPIN" \
      "$AI_WORKER_DROPIN"
  fi
}
rollback() {
  local failed_status="${1:-$?}"
  local failed_line="${2:-0}"
  if [ "$BASHPID" != "$DEPLOY_MAIN_BASHPID" ]; then
    trap - ERR TERM INT
    exit "$failed_status"
  fi
  local rollback_failed=0
  local current_sha=""
  local observed_scheduler_active=0
  local observed_scheduler_enabled=0
  local restoration_ready=1
  local service_active_state=""
  local services_quiescent=1
  trap - ERR TERM INT
  set +e
  if [ "$CUTOVER_STARTED" -eq 1 ]; then
    printf 'deploy_failure phase=cutover cutover_step=%s line=%s status=%s\n' \
      "$CUTOVER_STEP" "$failed_line" "$failed_status" >&2
  else
    printf 'deploy_failure phase=preparation step=%s line=%s status=%s\n' \
      "$CUTOVER_STEP" "$failed_line" "$failed_status" >&2
  fi
  if [ "$CUTOVER_STARTED" -eq 0 ]; then
    echo "Release preparation failed; the running services were not stopped" >&2
    current_sha="$(git -C "$PREVIOUS_CODE_ROOT" rev-parse HEAD 2>/dev/null)"
    ACTIVE_REQUIREMENTS_SHA256="$PREVIOUS_REQUIREMENTS_SHA256"
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

  rollback_failure() {
    echo "Rollback step failed: $1" >&2
    rollback_failed=1
  }

  if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
    sudo systemctl stop "$AI_WORKER_TIMER" || \
      rollback_failure "stop AI recommendation worker timer"
    sudo systemctl stop "$AI_WORKER_SERVICE" || \
      rollback_failure "stop AI recommendation worker"
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
      [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ]; then
      sudo systemctl stop "$MAIN_SERVICE" || \
        rollback_failure "keep probiga stopped after external writer block"
    elif [ "$restoration_ready" -eq 1 ]; then
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
    if [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ]; then
      sudo systemctl disable probiga-scheduler || \
        rollback_failure "disable scheduler after external writer block"
      sudo systemctl stop probiga-scheduler || \
        rollback_failure "keep scheduler stopped after external writer block"
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
    if [ "$PREVIOUS_AI_WORKER_TIMER_ENABLED" -eq 1 ]; then
      sudo systemctl enable "$AI_WORKER_TIMER" || \
        rollback_failure "enable AI recommendation worker timer"
    else
      sudo systemctl disable "$AI_WORKER_TIMER" || \
        rollback_failure "disable AI recommendation worker timer"
    fi
    if [ "$PREVIOUS_AI_WORKER_TIMER_ACTIVE" -eq 1 ]; then
      sudo systemctl start "$AI_WORKER_TIMER" || \
        rollback_failure "start AI recommendation worker timer"
    else
      sudo systemctl stop "$AI_WORKER_TIMER" || \
        rollback_failure "keep AI recommendation worker timer stopped"
    fi
    if [ "$PREVIOUS_AI_WORKER_DROPIN_PRESENT" -eq 1 ] && \
      [ -n "$PREVIOUS_RELEASE_REVISION" ]; then
      assert_ai_worker_runtime "$PREVIOUS_RELEASE_REVISION" \
        "$PREVIOUS_VENV" "$PREVIOUS_CODE_ROOT" || \
        rollback_failure "verify previous AI recommendation worker runtime"
    fi
  fi
  if [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ]; then
    if systemctl is-active --quiet "$MAIN_SERVICE"; then
      rollback_failure "probiga restarted after external writer block"
    fi
    if systemctl is-active --quiet probiga-scheduler; then
      rollback_failure \
        "probiga-scheduler restarted after external writer block"
    fi
    if systemctl is-enabled --quiet probiga-scheduler; then
      rollback_failure \
        "probiga-scheduler remained enabled after external writer block"
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
    [ "$EXTERNAL_WRITER_BLOCKED" -eq 0 ]; then
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

  if [ "$rollback_failed" -ne 0 ]; then
    ACTIVE_REQUIREMENTS_SHA256=""
    ACTIVE_ADATA_SHA=""
    ACTIVE_ADATA_TREE_SHA256=""
    echo "Rollback verification failed" >&2
    write_receipt "ROLLBACK_FAILED" "$current_sha" || true
  elif [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ]; then
    ACTIVE_REQUIREMENTS_SHA256="$PREVIOUS_REQUIREMENTS_SHA256"
    ACTIVE_ADATA_SHA="$PREVIOUS_ADATA_SHA"
    ACTIVE_ADATA_TREE_SHA256="$PREVIOUS_ADATA_TREE_SHA256"
    write_receipt "BLOCKED_EXTERNAL_WRITER" "$PREVIOUS_SHA" || true
  else
    ACTIVE_REQUIREMENTS_SHA256="$PREVIOUS_REQUIREMENTS_SHA256"
    ACTIVE_ADATA_SHA="$PREVIOUS_ADATA_SHA"
    ACTIVE_ADATA_TREE_SHA256="$PREVIOUS_ADATA_TREE_SHA256"
    write_receipt "ROLLED_BACK" "$PREVIOUS_SHA" || true
  fi
  exit "$failed_status"
}
trap 'rollback "$?" "$LINENO"' ERR
trap 'rollback 143' TERM
trap 'rollback 130' INT
# PREPARE: all network, dependency, and release validation work happens while
# the old API remains active. This phase must not mutate the live checkout.
CUTOVER_STEP=prepare_release
prepare_release

# CUTOVER: only quiesce writers, install the prevalidated runtime definition,
# reload systemd, start, and prove health/static. The live checkout is untouched.
CUTOVER_STARTED=1
CUTOVER_STEP=stop_auxiliary_writers
if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
  sudo systemctl stop "$AI_WORKER_TIMER"
  sudo systemctl stop "$AI_WORKER_SERVICE"
  ! systemctl is-active --quiet "$AI_WORKER_SERVICE"
fi
CUTOVER_STEP=stop_scheduler
if [ "$SCHEDULER_UNIT_PRESENT" -eq 1 ]; then
  sudo systemctl stop probiga-scheduler
  ! systemctl is-active --quiet probiga-scheduler
  sudo systemctl disable probiga-scheduler
fi
CUTOVER_STEP=stop_api
API_STOPPED=1
sudo systemctl stop "$MAIN_SERVICE"
# Persist the Layer-4 writer fence while both scheduler implementations are
# stopped. Activation is a separate, schema-gated maintenance operation.
CUTOVER_STEP=writer_fence
WRITER_FENCE_STATUS=0
(
  cd "$PREPARED_CODE_ROOT"
  sudo -u "$SERVICE_USER" env GIT_OPTIONAL_LOCKS=0 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 \
    PROBIGA_DEPLOYMENT_MODE=production \
    PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA" \
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
CUTOVER_STEP=daemon_reload
sudo systemctl daemon-reload
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
    "$MAIN_MARKET_RADAR_DROPIN"|"$MAIN_SERVICE_USER_DROPIN") ;;
    *)
      printf 'main_identity unexpected_dropin=%q\n' \
        "$main_dropin_path" >&2
      false
      ;;
  esac
done
SCHEDULER_DROPIN_PATHS="$(systemctl show probiga-scheduler \
  --property=DropInPaths --value)"
EXPECTED_SCHEDULER_DROPIN_PATHS=""
if sudo test -f "$SCHEDULER_LIMITS_DROPIN"; then
  # This root-owned operational drop-in supplies production resource/runtime
  # limits.  It is the only permitted drop-in; the live process checks below
  # independently prove code, revision, adata, interpreter and script identity.
  EXPECTED_SCHEDULER_DROPIN_PATHS="$SCHEDULER_LIMITS_DROPIN"
fi
if [ "$SCHEDULER_DROPIN_PATHS" != "$EXPECTED_SCHEDULER_DROPIN_PATHS" ]; then
  printf 'scheduler_identity unexpected_dropins=%q\n' \
    "$SCHEDULER_DROPIN_PATHS" >&2
  false
fi
CUTOVER_STEP=start_api
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
point_static_release_to_checkout "$PREPARED_CODE_ROOT"
assert_nginx_static_matches_checkout "$PREPARED_CODE_ROOT"
systemctl is-active --quiet probiga-scheduler
systemctl is-enabled --quiet probiga-scheduler
if [ "$AI_WORKER_UNIT_PRESENT" -eq 1 ]; then
  if [ "$PREVIOUS_AI_WORKER_TIMER_ENABLED" -eq 1 ]; then
    sudo systemctl enable "$AI_WORKER_TIMER"
  else
    sudo systemctl disable "$AI_WORKER_TIMER"
  fi
  if [ "$PREVIOUS_AI_WORKER_TIMER_ACTIVE" -eq 1 ]; then
    sudo systemctl start "$AI_WORKER_TIMER"
  else
    sudo systemctl stop "$AI_WORKER_TIMER"
  fi
  assert_ai_worker_runtime "$EXPECTED_SHA" \
    "$RELEASE_VENV_ROOT/$EXPECTED_SHA" "$PREPARED_CODE_ROOT"
  if [ "$PREVIOUS_AI_WORKER_TIMER_ENABLED" -eq 1 ]; then
    systemctl is-enabled --quiet "$AI_WORKER_TIMER"
  else
    ! systemctl is-enabled --quiet "$AI_WORKER_TIMER"
  fi
  if [ "$PREVIOUS_AI_WORKER_TIMER_ACTIVE" -eq 1 ]; then
    systemctl is-active --quiet "$AI_WORKER_TIMER"
  else
    ! systemctl is-active --quiet "$AI_WORKER_TIMER"
  fi
fi
assert_scheduler_triggers_quiescent
sudo -u "$SERVICE_USER" env PYTHONSAFEPATH=1 \
  "PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" \
  "$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P \
  "$PREPARED_CODE_ROOT/tools/ensure_quality_gate.py" \
  --task-type analysis_premarket_external
ACTIVE_REQUIREMENTS_SHA256="$EXPECTED_REQUIREMENTS_SHA256"
ACTIVE_ADATA_SHA="$EXPECTED_ADATA_SHA"
ACTIVE_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256"
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
write_receipt "DEPLOYED" "$EXPECTED_SHA"
DEPLOY_SUCCEEDED=1
trap - ERR TERM INT
