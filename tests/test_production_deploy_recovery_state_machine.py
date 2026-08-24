from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _bash() -> str | None:
    discovered = shutil.which("bash")
    if discovered:
        return discovered
    git_bash = Path(r"C:\Program Files\Git\bin\bash.exe")
    return str(git_bash) if git_bash.is_file() else None


def _shell_function_bodies(source: str) -> dict[str, str]:
    bodies: dict[str, str] = {}
    header = re.compile(r"(?m)^([A-Za-z_][A-Za-z0-9_]*)\(\) \{\s*$")
    for match in header.finditer(source):
        depth = 1
        cursor = match.end()
        while depth and cursor < len(source):
            if source.startswith("\n}", cursor):
                depth -= 1
                if depth == 0:
                    bodies[match.group(1)] = source[match.end() : cursor + 1]
                    break
            cursor += 1
    return bodies


def _function(name: str, body: str) -> str:
    return f"{name}() {{\n{body}\n}}\n"


def test_activation_snapshot_fault_is_retryable_and_restores_exact_old_set(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable recovery state test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    names = (
        "controlled_guard_assert_file",
        "activation_snapshot_assert_container",
        "activation_snapshot_validate_release_identity",
        "activation_snapshot_phase",
        "activation_snapshot_set_phase",
        "activation_snapshot_validate",
        "activation_snapshot_phase_unchecked",
        "activation_snapshot_append_new_record",
        "activation_snapshot_validate_new",
        "activation_snapshot_create",
        "activation_snapshot_assert_old_set",
        "activation_snapshot_restore_old_set",
        "activation_snapshot_assert_new_set",
        "activation_snapshot_restore_new_set",
    )
    shell_functions = "".join(_function(name, bodies[name]) for name in names)
    # The production code insists on root ownership.  This executable unit test
    # keeps the same ownership equality checks but substitutes the actual test
    # process owner and removes only chown/install owner switches.
    shell_functions = shell_functions.replace(
        "root:root", '"$TEST_OWNER"'
    ).replace(
        "chown \"$TEST_OWNER\"", "true"
    ).replace(
        "install -d -o root -g root -m", "install -d -m"
    ).replace(
        "install -o root -g root -m", "install -m"
    ).replace(
        "sync -f /etc/systemd/system", 'sync -f "$TEST_ROOT"'
    ).replace(
        "sync -f /opt", 'sync -f "$TEST_ROOT"'
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_MANIFEST" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_MANIFEST" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_PHASE" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_PHASE" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_STATE" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_STATE" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_STATE_SHA" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_STATE_SHA" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" 600',
        '"$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_GOVERNANCE_OLD_SHA" 600',
        '"$ACTIVATION_GOVERNANCE_OLD_SHA" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_RELEASE_IDENTITY" 600',
        '"$ACTIVATION_RELEASE_IDENTITY" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_RELEASE_IDENTITY_SHA" 600',
        '"$ACTIVATION_RELEASE_IDENTITY_SHA" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$DATABASE_WRITER_RESTORE_FILE" 600',
        '"$DATABASE_WRITER_RESTORE_FILE" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '= 700 ||', '= "$TEST_DIR_MODE" ||'
    ).replace(
        "-exec sync -f {} \\;", "-exec true {} \\;"
    )
    root = tmp_path.as_posix()
    harness = f"""
set -u
TEST_ROOT={root!r}
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) SIMULATE_SIGKILL=1 ;;
  *) SIMULATE_SIGKILL=0 ;;
esac
TEST_OWNER="$(stat -c '%U:%G' "$TEST_ROOT")"
DATABASE_WRITER_GUARD_DIR="$TEST_ROOT/guards"
ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/activation-unit-transaction"
ACTIVATION_UNIT_SNAPSHOT_MANIFEST="$ACTIVATION_UNIT_SNAPSHOT_DIR/manifest"
ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST="$ACTIVATION_UNIT_SNAPSHOT_DIR/new-manifest"
ACTIVATION_UNIT_SNAPSHOT_PHASE="$ACTIVATION_UNIT_SNAPSHOT_DIR/phase"
ACTIVATION_UNIT_SNAPSHOT_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state"
ACTIVATION_UNIT_SNAPSHOT_STATE_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state.sha256"
ACTIVATION_GOVERNANCE_OLD_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/governance-task-old.json"
ACTIVATION_GOVERNANCE_OLD_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/governance-task-old.sha256"
ACTIVATION_RELEASE_IDENTITY="$ACTIVATION_UNIT_SNAPSHOT_DIR/release-identity"
ACTIVATION_RELEASE_IDENTITY_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/release-identity.sha256"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/restore"
GOVERNANCE_TASK_OLD_SOURCE="$TEST_ROOT/governance-old.json"
STATIC_RELEASE_LINK="$TEST_ROOT/static"
UNIT_A="$TEST_ROOT/probiga.service.conf"
UNIT_B="$TEST_ROOT/probiga-scheduler.service"
MAIN_RELEASE_DROPIN="$UNIT_A"
SCHEDULER_UNIT="$UNIT_B"
AI_WORKER_DROPIN="$TEST_ROOT/ai.conf"
AI_WORKER_UNIT_PRESENT=0
CODE_RELEASE_ROOT="$TEST_ROOT/releases"
PREPARED_MAIN_DROPIN="$TEST_ROOT/new-main.conf"
PREPARED_SCHEDULER_DROPIN="$TEST_ROOT/new-scheduler.conf"
PREPARED_AI_WORKER_DROPIN="$TEST_ROOT/new-ai.conf"
ACTIVATION_UNIT_PATHS=("$UNIT_A" "$UNIT_B")
EXPECTED_SHA={'a' * 40}
PREVIOUS_RELEASE_REVISION={'b' * 40}
EXPECTED_RELEASE_TREE_SHA256={'c' * 64}
EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256={'d' * 64}
mkdir -p "$DATABASE_WRITER_GUARD_DIR"
chmod 700 "$DATABASE_WRITER_GUARD_DIR"
printf 'old-main\n' > "$UNIT_A"
printf 'old-scheduler\n' > "$UNIT_B"
printf 'new-main\n' > "$PREPARED_MAIN_DROPIN"
printf 'new-scheduler\n' > "$PREPARED_SCHEDULER_DROPIN"
printf '[]\n' > "$GOVERNANCE_TASK_OLD_SOURCE"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$EXPECTED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=not-found,not-found,not-found \
  ai_timer_unit=not-found,not-found,not-found \
  > "$DATABASE_WRITER_RESTORE_FILE"
chmod 644 "$UNIT_A" "$UNIT_B"
chmod 644 "$PREPARED_MAIN_DROPIN" "$PREPARED_SCHEDULER_DROPIN"
chmod 600 "$DATABASE_WRITER_RESTORE_FILE" "$GOVERNANCE_TASK_OLD_SOURCE"
TEST_DIR_MODE="$(stat -c '%a' "$TEST_ROOT")"
TEST_SECURE_FILE_MODE="$(stat -c '%a' "$DATABASE_WRITER_RESTORE_FILE")"
test_install() {{
  local directory=0
  local mode=
  while [ "$#" -gt 0 ]; do
    case "$1" in
      -d) directory=1; shift ;;
      -m) mode="$2"; shift 2 ;;
      *) break ;;
    esac
  done
  if [ "$directory" -eq 1 ]; then
    mkdir -p "$1" || return 1
    chmod "$mode" "$1" || return 1
  else
    cp "$1" "$2" || return 1
    chmod "$mode" "$2" || return 1
  fi
}}
install() {{ test_install "$@"; }}
sync() {{ return 0; }}
{shell_functions}
# Git Bash filesystem metadata calls are orders of magnitude slower than Linux.
# Ownership/mode contracts have dedicated static tests. Keep all persistent
# content hashes here while avoiding repeated Windows stat calls in each retry.
activation_snapshot_assert_container() {{
  test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  test -f "$ACTIVATION_UNIT_SNAPSHOT_MANIFEST" || return 1
  test -f "$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST" || return 1
  test -f "$ACTIVATION_UNIT_SNAPSHOT_PHASE" || return 1
  test "$(<"$ACTIVATION_UNIT_SNAPSHOT_STATE_SHA")" = \
    "$(sha256sum "$ACTIVATION_UNIT_SNAPSHOT_STATE" | cut -d' ' -f1)" || return 1
  test "$(<"$ACTIVATION_GOVERNANCE_OLD_SHA")" = \
    "$(sha256sum "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" | cut -d' ' -f1)" || return 1
  test "$(<"$ACTIVATION_RELEASE_IDENTITY_SHA")" = \
    "$(sha256sum "$ACTIVATION_RELEASE_IDENTITY" | cut -d' ' -f1)" || return 1
  return 0
}}
# Use a lightweight phase writer so this test exercises recovery rather than
# fsync latency.
activation_snapshot_set_phase() {{
  printf '%s\n' "$2" > "$ACTIVATION_UNIT_SNAPSHOT_PHASE" || return 1
  return 0
}}
activation_snapshot_create || exit 20
cmp "$GOVERNANCE_TASK_OLD_SOURCE" "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" || \
  exit 31
cp "$ACTIVATION_RELEASE_IDENTITY" "$TEST_ROOT/release-identity.good"
printf 'tampered\n' >> "$ACTIVATION_RELEASE_IDENTITY"
if activation_snapshot_validate "$EXPECTED_SHA" >/dev/null 2>&1; then
  echo 'tampered release identity unexpectedly validated' >&2
  exit 32
fi
cp "$TEST_ROOT/release-identity.good" "$ACTIVATION_RELEASE_IDENTITY"
activation_snapshot_set_phase "$EXPECTED_SHA" runtime-units-installing || exit 21
# A real SIGKILL can leave a mixed unit set.  Recovery must converge that
# observable state back to the sealed old set before it does anything else.
if [ "$SIMULATE_SIGKILL" -eq 1 ]; then
  # MSYS2/Git Bash process emulation can destabilize the hosting Windows
  # interpreter after a real KILL.  Reproduce the exact persistent mixed
  # state on Windows; Linux production CI still exercises the real signal.
  printf 'sigkill-new-main\n' > "$UNIT_A"
  rm -f "$UNIT_B"
  SIGKILL_STATUS=137
else
  set +e
  (
    printf 'sigkill-new-main\n' > "$UNIT_A"
    rm -f "$UNIT_B"
    kill -KILL "$BASHPID"
  )
  SIGKILL_STATUS=$?
  set -e
fi
test "$SIGKILL_STATUS" -eq 137 || exit 24
activation_snapshot_restore_old_set "$EXPECTED_SHA" || exit 25
activation_snapshot_assert_old_set "$EXPECTED_SHA" || exit 26
activation_snapshot_set_phase "$EXPECTED_SHA" runtime-units-installing || exit 27
printf 'new-main\n' > "$UNIT_A"
rm -f "$UNIT_B"
install() {{
  case "${{@: -1}}" in
    "$UNIT_B") return 91 ;;
    *) test_install "$@" ;;
  esac
}}
if activation_snapshot_restore_old_set "$EXPECTED_SHA"; then
  echo 'fault injection unexpectedly succeeded' >&2
  exit 10
fi
test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR"
test -f "$ACTIVATION_UNIT_SNAPSHOT_MANIFEST"
test "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = restoring-old
install() {{ test_install "$@"; }}
activation_snapshot_restore_old_set "$EXPECTED_SHA" || exit 22
activation_snapshot_assert_old_set "$EXPECTED_SHA" || exit 23
test "$(<"$UNIT_A")" = old-main
test "$(<"$UNIT_B")" = old-scheduler
test "$(stat -c '%a' "$UNIT_A")" = 644
test "$(stat -c '%a' "$UNIT_B")" = 644
test "$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")" = old-set-restored
activation_snapshot_set_phase "$EXPECTED_SHA" new-runtime-verified || exit 28
printf 'mixed-main\n' > "$UNIT_A"
rm -f "$UNIT_B"
install() {{
  case "${{@: -1}}" in
    "$UNIT_B") return 92 ;;
    *) test_install "$@" ;;
  esac
}}
if activation_snapshot_restore_new_set "$EXPECTED_SHA"; then
  echo 'new-set fault injection unexpectedly succeeded' >&2
  exit 13
fi
test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR"
install() {{ test_install "$@"; }}
activation_snapshot_restore_new_set "$EXPECTED_SHA" || exit 29
activation_snapshot_assert_new_set "$EXPECTED_SHA" || exit 30
test "$(<"$UNIT_A")" = new-main
test "$(<"$UNIT_B")" = new-scheduler
"""
    assert "MINGW*|MSYS*|CYGWIN*) SIMULATE_SIGKILL=1 ;;" in harness
    assert 'kill -KILL "$BASHPID"' in harness
    harness_path = tmp_path / "activation-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    numbered = "\n".join(
        f"{index + 1:04d}: {line}" for index, line in enumerate(harness.splitlines())
    )
    assert completed.returncode == 0, (
        (completed.stdout or "") + (completed.stderr or "") + numbered
    )


def test_rollback_receipt_state_accepts_canonical_poststart_receipt(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable receipt-state regression")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = _shell_function_bodies(source)[
        "activation_snapshot_validate_rollback_receipt_state"
    ]
    pending = (tmp_path / "deployed-receipt-pending.json").as_posix()
    pending_sha = (tmp_path / "deployed-receipt-pending.sha256").as_posix()
    expected_sha = "a" * 40
    harness = f"""
set -u
ACTIVATION_RECEIPT_PENDING={pending!r}
ACTIVATION_RECEIPT_PENDING_SHA={pending_sha!r}
activation_snapshot_validate_receipt_pending() {{
  test -f "$ACTIVATION_RECEIPT_PENDING" || return 1
  test ! -L "$ACTIVATION_RECEIPT_PENDING" || return 1
  test "$(<"$ACTIVATION_RECEIPT_PENDING")" = "$1" || return 1
  if [ -e "$ACTIVATION_RECEIPT_PENDING_SHA" ] || \
    [ -L "$ACTIVATION_RECEIPT_PENDING_SHA" ]; then
    test -f "$ACTIVATION_RECEIPT_PENDING_SHA" || return 1
    test ! -L "$ACTIVATION_RECEIPT_PENDING_SHA" || return 1
    test "$(<"$ACTIVATION_RECEIPT_PENDING_SHA")" = sealed || return 1
  fi
}}
activation_snapshot_validate_rollback_receipt_state() {{
{body}
}}
activation_snapshot_validate_rollback_receipt_state {expected_sha} prepared || exit 20
printf '%s\n' {expected_sha} > "$ACTIVATION_RECEIPT_PENDING"
activation_snapshot_validate_rollback_receipt_state \
  {expected_sha} runtime-units-installed || exit 21
printf '%s\n' sealed > "$ACTIVATION_RECEIPT_PENDING_SHA"
for phase in runtime-units-installed restoring-old old-set-restored \
    old-runtime-verified; do
  activation_snapshot_validate_rollback_receipt_state \
    {expected_sha} "$phase" || exit 22
done
for phase in prepared runtime-units-installing; do
  if activation_snapshot_validate_rollback_receipt_state \
      {expected_sha} "$phase"; then
    echo "pre-start phase $phase accepted a pending receipt" >&2
    exit 23
  fi
done
printf '%s\n' changed > "$ACTIVATION_RECEIPT_PENDING_SHA"
if activation_snapshot_validate_rollback_receipt_state \
    {expected_sha} old-runtime-verified; then
  echo 'changed pending receipt hash unexpectedly validated' >&2
  exit 24
fi
rm -f "$ACTIVATION_RECEIPT_PENDING"
printf '%s\n' sealed > "$ACTIVATION_RECEIPT_PENDING_SHA"
if activation_snapshot_validate_rollback_receipt_state \
    {expected_sha} runtime-units-installed; then
  echo 'orphan pending receipt hash unexpectedly validated' >&2
  exit 25
fi
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_pending_receipt_json_is_recoverable_without_redundant_checksum(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable receipt validator test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    validator = _function(
        "activation_snapshot_validate_receipt_pending",
        _shell_function_bodies(source)[
            "activation_snapshot_validate_receipt_pending"
        ],
    ).replace('/usr/bin/python3.14 -I -', '"$TEST_PYTHON" -I -')
    expected_sha = "a" * 40
    receipt = tmp_path / "deployed-receipt-pending.json"
    receipt_sha = tmp_path / "deployed-receipt-pending.sha256"
    receipt.write_text(
        json.dumps(
            {
                "schema_version": "probiga.deploy-receipt.v4",
                "status": "DEPLOYED",
                "expected_sha": expected_sha,
                "active_sha": expected_sha,
            }
        ),
        encoding="utf-8",
    )
    python_executable = Path(shutil.which("python") or sys.executable).as_posix()
    harness = f"""
set -u
TEST_PYTHON={python_executable!r}
ACTIVATION_RECEIPT_PENDING={receipt.as_posix()!r}
ACTIVATION_RECEIPT_PENDING_SHA={receipt_sha.as_posix()!r}
controlled_guard_assert_file() {{
  test -f "$1" || return 1
  test ! -L "$1" || return 1
}}
{validator}
activation_snapshot_validate_receipt_pending {expected_sha} || exit 20
sha256sum "$ACTIVATION_RECEIPT_PENDING" | cut -d' ' -f1 \
  > "$ACTIVATION_RECEIPT_PENDING_SHA"
activation_snapshot_validate_receipt_pending {expected_sha} || exit 21
printf '%064d\n' 0 > "$ACTIVATION_RECEIPT_PENDING_SHA"
if activation_snapshot_validate_receipt_pending {expected_sha}; then
  exit 22
fi
rm -f "$ACTIVATION_RECEIPT_PENDING"
if activation_snapshot_validate_receipt_pending {expected_sha}; then
  exit 23
fi
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_governance_snapshot_is_recoverable_without_redundant_checksum(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the governance snapshot validator test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    validator = _function(
        "activation_snapshot_validate_governance_new",
        _shell_function_bodies(source)["activation_snapshot_validate_governance_new"],
    )
    snapshot = tmp_path / "governance-task-new.json"
    snapshot_sha = tmp_path / "governance-task-new.sha256"
    snapshot.write_text('{"tasks":[]}', encoding="utf-8")
    harness = f"""
set -u
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT={snapshot.as_posix()!r}
ACTIVATION_GOVERNANCE_NEW_SHA={snapshot_sha.as_posix()!r}
controlled_guard_assert_file() {{
  test -f "$1" || return 1
  test ! -L "$1" || return 1
}}
{validator}
activation_snapshot_validate_governance_new || exit 20
sha256sum "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" | cut -d' ' -f1 \
  > "$ACTIVATION_GOVERNANCE_NEW_SHA"
activation_snapshot_validate_governance_new || exit 21
printf '%064d\n' 0 > "$ACTIVATION_GOVERNANCE_NEW_SHA"
if activation_snapshot_validate_governance_new; then
  exit 22
fi
rm -f "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
if activation_snapshot_validate_governance_new; then
  exit 23
fi
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_missing_guard_is_recreated_only_for_retryable_recovery_phases() -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable missing-guard regression")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = _shell_function_bodies(source)[
        "activation_snapshot_allows_missing_guard_for_recovery"
    ]
    harness = f"""
set -u
activation_snapshot_allows_missing_guard_for_recovery() {{
{body}
}}
    for phase in prepared runtime-units-installed restoring-old old-set-restored \
        old-runtime-verified; do
  activation_snapshot_allows_missing_guard_for_recovery "$phase" || exit 20
done
    for phase in runtime-units-installing new-runtime-verified \
        finalized; do
  if activation_snapshot_allows_missing_guard_for_recovery "$phase"; then
    echo "unsafe missing-guard phase $phase was accepted" >&2
    exit 21
  fi
done
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize(
    ("phase", "has_new_pair", "guard_present", "same_target"),
    (
        ("prepared", False, True, False),
        ("prepared", False, False, False),
        ("runtime-units-installing", False, True, False),
        ("runtime-units-installed", True, False, False),
        ("runtime-units-installed", True, False, True),
        ("old-runtime-verified", True, True, False),
    ),
)
def test_interrupted_recovery_restores_governance_before_old_runtime(
    tmp_path: Path,
    phase: str,
    has_new_pair: bool,
    guard_present: bool,
    same_target: bool,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable post-start recovery test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    recovery = _function(
        "controlled_v2_rollback_only_recovery",
        _shell_function_bodies(source)["controlled_v2_rollback_only_recovery"],
    )
    root = tmp_path.as_posix()
    guarded_sha = "a" * 40
    old_sha = "b" * 40
    expected_sha = guarded_sha if same_target else "c" * 40
    new_pair_setup = ""
    if has_new_pair:
        new_pair_setup = """
printf 'new\n' > "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
printf 'sealed\n' > "$ACTIVATION_GOVERNANCE_NEW_SHA"
"""
    guard_setup = ': > "$DATABASE_WRITER_GUARD_FILE"' if guard_present else ""
    expected_trace = [] if phase == "old-runtime-verified" else ["ready"]
    if has_new_pair:
        expected_trace.append("validate-new")
    if not guard_present:
        expected_trace.append("recreate")
    expected_trace.extend(("install-fence", "reload", "fence", "boundary"))
    if phase != "old-runtime-verified":
        expected_trace.append("restore-governance")
    expected_trace.extend(
        (
            "restore-old-set",
            "reload",
            "assert-old-set",
            "boundary",
            "capture-old",
            "cleanup",
            "restore-writers",
            "verify-old",
            "phase-old-runtime-verified",
            "remove-journal",
        )
    )
    expected_trace_text = "\n".join(expected_trace) + "\n"
    harness = f"""
set -u
TEST_ROOT={root!r}
EXPECTED_SHA={expected_sha}
GUARDED_SHA={guarded_sha}
OLD_SHA={old_sha}
DEPLOY_OPERATION=deploy
DEPLOY_ARTIFACT_MODE=ci-resolved-freeze-v1
RELEASE_VENV_ROOT="$TEST_ROOT/venvs"
DATABASE_WRITER_GUARD_DIR="$TEST_ROOT/guards"
DATABASE_WRITER_GUARD_FILE="$DATABASE_WRITER_GUARD_DIR/guard"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/restore"
ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/transaction"
ACTIVATION_UNIT_SNAPSHOT_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state"
ACTIVATION_GOVERNANCE_OLD_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/old.json"
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/new.json"
ACTIVATION_GOVERNANCE_NEW_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/new.sha256"
ACTIVATION_RECEIPT_PENDING="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.json"
ACTIVATION_RECEIPT_PENDING_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.sha256"
DB_STATE="$TEST_ROOT/db-state"
PHASE_STATE="$TEST_ROOT/phase-state"
TRANSACTION_PHASE="$TEST_ROOT/transaction-phase"
TRACE="$TEST_ROOT/trace"
FENCED=0
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR" "$RELEASE_VENV_ROOT"
: > "$RELEASE_VENV_ROOT/$GUARDED_SHA"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$GUARDED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=loaded,inactive,static \
  ai_timer_unit=loaded,inactive,disabled \
  > "$ACTIVATION_UNIT_SNAPSHOT_STATE"
printf 'old\n' > "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"
{new_pair_setup}
printf 'restore\n' > "$DATABASE_WRITER_RESTORE_FILE"
{guard_setup}
printf '%s\n' {('old' if phase == 'old-runtime-verified' else 'new')!r} > "$DB_STATE"
printf '%s\n' {phase!r} > "$TRANSACTION_PHASE"
: > "$TRACE"
activation_snapshot_recorded_release() {{ printf '%s\n' "$GUARDED_SHA"; }}
activation_snapshot_old_release() {{
  test "$1" = "$GUARDED_SHA" || return 1
  printf '%s\n' "$OLD_SHA"
}}
activation_snapshot_phase() {{ printf '%s\n' "$(<"$TRANSACTION_PHASE")"; }}
activation_snapshot_validate() {{
  test "$1" = "$GUARDED_SHA" || return 1
  test "$(<"$TRANSACTION_PHASE")" = old-runtime-verified || return 1
}}
controlled_guard_assert_governance_restore_runtime() {{
  test "$1" = "$GUARDED_SHA" || return 1
  printf 'ready\n' >> "$TRACE"
}}
activation_snapshot_validate_governance_new() {{
  test "$(<"$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT")" = new || return 1
  test "$(<"$ACTIVATION_GOVERNANCE_NEW_SHA")" = sealed || return 1
  printf 'validate-new\n' >> "$TRACE"
}}
activation_snapshot_validate_rollback_receipt_state() {{ return 0; }}
controlled_guard_assert_directory() {{ test -d "$DATABASE_WRITER_GUARD_DIR"; }}
controlled_guard_assert_file() {{ test -f "$1"; }}
controlled_guard_assert_state_record() {{ return 0; }}
controlled_guard_assert_restore_file() {{ test -f "$DATABASE_WRITER_RESTORE_FILE"; }}
controlled_guard_assert_marker() {{ test -f "$DATABASE_WRITER_GUARD_FILE"; }}
activation_snapshot_allows_missing_guard_for_recovery() {{
  case "$1" in
    prepared|runtime-units-installed|restoring-old|old-set-restored|old-runtime-verified) ;;
    *) return 1 ;;
  esac
}}
controlled_guard_recreate_file() {{
  : > "$DATABASE_WRITER_GUARD_FILE"
  printf 'recreate\n' >> "$TRACE"
}}
controlled_guard_install_dropins() {{ printf 'install-fence\n' >> "$TRACE"; }}
systemctl() {{
  test "$1:${{2:-}}" = daemon-reload: || return 1
  printf 'reload\n' >> "$TRACE"
}}
controlled_guard_force_all_writers_fenced() {{
  FENCED=1
  printf 'fence\n' >> "$TRACE"
  kill -HUP "$BASHPID"
}}
controlled_guard_assert_boundary() {{
  test "$FENCED" -eq 1 || return 1
  test -f "$DATABASE_WRITER_GUARD_FILE" || return 1
  printf 'boundary\n' >> "$TRACE"
}}
controlled_guard_refence_after_restore_failure() {{
  printf 'unexpected-refence\n' >> "$TRACE"
  return 1
}}
controlled_guard_restore_and_verify_governance_snapshot() {{
  test "$1" = "$GUARDED_SHA" || return 1
  test "$2" = "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" || return 1
  test "$FENCED" -eq 1 || return 1
  case "$(<"$DB_STATE")" in new|old) ;; *) return 1 ;; esac
  printf 'old\n' > "$DB_STATE"
  printf 'restore-governance\n' >> "$TRACE"
  kill -TERM "$BASHPID"
}}
activation_snapshot_restore_old_set() {{
  test "$(<"$DB_STATE")" = old || return 1
  printf 'old-set-restored\n' > "$PHASE_STATE"
  printf 'restore-old-set\n' >> "$TRACE"
}}
activation_snapshot_assert_old_set() {{
  test "$(<"$PHASE_STATE")" = old-set-restored || return 1
  printf 'assert-old-set\n' >> "$TRACE"
}}
controlled_guard_capture_current_governance_snapshot() {{
  test "$1:$2" = "$GUARDED_SHA:$OLD_SHA" || return 1
  test "$(<"$DB_STATE")" = old || return 1
  printf 'capture-old\n' >> "$TRACE"
  kill -INT "$BASHPID"
}}
controlled_guard_cleanup() {{
  test "$(<"$DB_STATE")" = old || return 1
  rm -f "$DATABASE_WRITER_GUARD_FILE"
  printf 'cleanup\n' >> "$TRACE"
}}
controlled_guard_restore_previous_writer_states() {{
  test "$(<"$DB_STATE")" = old || return 1
  printf 'restore-writers\n' >> "$TRACE"
}}
controlled_guard_verify_restored_runtime() {{
  test "$3" = "$OLD_SHA" || return 1
  test "$6" = rollback-only || return 1
  test "$(<"$DB_STATE")" = old || return 1
  printf 'verify-old\n' >> "$TRACE"
}}
activation_snapshot_set_phase() {{
  test "$1:$2" = "$GUARDED_SHA:old-runtime-verified" || return 1
  printf 'old-runtime-verified\n' > "$TRANSACTION_PHASE"
  printf 'phase-old-runtime-verified\n' >> "$TRACE"
  return 91
}}
sync() {{ return 0; }}
controlled_guard_write_restore_file() {{ return 1; }}
activation_snapshot_remove_old_runtime_verified() {{
  rm -rf "$ACTIVATION_UNIT_SNAPSHOT_DIR"
  printf 'remove-journal\n' >> "$TRACE"
}}
{recovery}
trap '' TERM INT HUP
controlled_v2_rollback_only_recovery || exit 30
test "$(<"$DB_STATE")" = old || exit 31
test ! -e "$DATABASE_WRITER_GUARD_FILE" || exit 32
test ! -e "$DATABASE_WRITER_RESTORE_FILE" || exit 33
test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || exit 34
cat > "$TEST_ROOT/expected-trace" <<'EOF'
{expected_trace_text}EOF
cmp "$TEST_ROOT/expected-trace" "$TRACE" || exit 35
"""
    harness_path = tmp_path / "poststart-recovery-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_old_runtime_verified_cleanup_fault_stays_online_and_retry_converges(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable old-finalize recovery test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    recovery = _function(
        "controlled_v2_rollback_only_recovery",
        _shell_function_bodies(source)["controlled_v2_rollback_only_recovery"],
    )
    root = tmp_path.as_posix()
    guarded_sha = "a" * 40
    old_sha = "b" * 40
    expected_trace = """assert-old-set
capture-old
verify-old
cleanup-fault
assert-old-set
capture-old
verify-old
remove-journal
"""
    harness = f"""
set -u
TEST_ROOT={root!r}
EXPECTED_SHA={'c' * 40}
GUARDED_SHA={guarded_sha}
OLD_SHA={old_sha}
DEPLOY_OPERATION=deploy
DEPLOY_ARTIFACT_MODE=ci-resolved-freeze-v1
RELEASE_VENV_ROOT="$TEST_ROOT/venvs"
DATABASE_WRITER_GUARD_DIR="$TEST_ROOT/guards"
DATABASE_WRITER_GUARD_FILE="$DATABASE_WRITER_GUARD_DIR/guard"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/restore"
ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/transaction"
ACTIVATION_UNIT_SNAPSHOT_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state"
ACTIVATION_GOVERNANCE_OLD_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/old.json"
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/new.json"
ACTIVATION_GOVERNANCE_NEW_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/new.sha256"
ACTIVATION_RECEIPT_PENDING="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.json"
ACTIVATION_RECEIPT_PENDING_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.sha256"
FAULT_ONCE="$TEST_ROOT/cleanup-failed-once"
SERVICE_STATE="$TEST_ROOT/service-state"
TRACE="$TEST_ROOT/trace"
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR" "$RELEASE_VENV_ROOT"
: > "$RELEASE_VENV_ROOT/$GUARDED_SHA"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$GUARDED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=loaded,inactive,static \
  ai_timer_unit=loaded,inactive,disabled \
  > "$ACTIVATION_UNIT_SNAPSHOT_STATE"
printf 'old\n' > "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"
printf 'restore\n' > "$DATABASE_WRITER_RESTORE_FILE"
printf 'active\n' > "$SERVICE_STATE"
: > "$TRACE"
activation_snapshot_recorded_release() {{ printf '%s\n' "$GUARDED_SHA"; }}
activation_snapshot_old_release() {{ printf '%s\n' "$OLD_SHA"; }}
activation_snapshot_phase() {{ printf 'old-runtime-verified\n'; }}
activation_snapshot_validate_rollback_receipt_state() {{ return 0; }}
controlled_guard_assert_directory() {{ test -d "$DATABASE_WRITER_GUARD_DIR"; }}
controlled_guard_assert_file() {{ test -f "$1"; }}
controlled_guard_assert_state_record() {{ return 0; }}
controlled_guard_assert_restore_file() {{ test -f "$DATABASE_WRITER_RESTORE_FILE"; }}
activation_snapshot_assert_old_set() {{
  test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR" || return 1
  printf 'assert-old-set\n' >> "$TRACE"
}}
controlled_guard_capture_current_governance_snapshot() {{
  test "$1:$2" = "$GUARDED_SHA:$OLD_SHA" || return 1
  printf 'capture-old\n' >> "$TRACE"
}}
controlled_guard_verify_restored_runtime() {{
  test "$3:$6" = "$OLD_SHA:rollback-only" || return 1
  test "$(<"$SERVICE_STATE")" = active || return 1
  printf 'verify-old\n' >> "$TRACE"
}}
activation_snapshot_remove_old_runtime_verified() {{
  if [ ! -e "$FAULT_ONCE" ]; then
    : > "$FAULT_ONCE"
    printf 'cleanup-fault\n' >> "$TRACE"
    return 99
  fi
  rm -rf "$ACTIVATION_UNIT_SNAPSHOT_DIR"
  printf 'remove-journal\n' >> "$TRACE"
}}
controlled_guard_refence_after_restore_failure() {{
  printf 'stopped\n' > "$SERVICE_STATE"
  printf 'unexpected-refence\n' >> "$TRACE"
  return 1
}}
controlled_guard_recreate_file() {{
  printf 'unexpected-recreate\n' >> "$TRACE"
  return 1
}}
sync() {{ return 0; }}
{recovery}
if controlled_v2_rollback_only_recovery; then
  exit 30
fi
test "$(<"$SERVICE_STATE")" = active || exit 31
test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR" || exit 32
test ! -e "$DATABASE_WRITER_RESTORE_FILE" || exit 33
controlled_v2_rollback_only_recovery || exit 34
test "$(<"$SERVICE_STATE")" = active || exit 35
test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || exit 36
cat > "$TEST_ROOT/expected-trace" <<'EOF'
{expected_trace}EOF
cmp "$TEST_ROOT/expected-trace" "$TRACE" || exit 37
"""
    harness_path = tmp_path / "old-finalize-recovery-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize(
    ("phase", "expected_begin_writes"),
    (
        ("runtime-units-installed", 1),
        ("restoring-old", 1),
        ("restoring-new-no-receipt", 0),
    ),
)
def test_forward_no_receipt_recovery_restarts_exact_new_runtime_and_retires(
    tmp_path: Path,
    phase: str,
    expected_begin_writes: int,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable forward-preserve test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    recovery = _function(
        "controlled_v2_forward_preserve_no_receipt_recovery",
        _shell_function_bodies(source)[
            "controlled_v2_forward_preserve_no_receipt_recovery"
        ],
    )
    root = tmp_path.as_posix()
    guarded_sha = "a" * 40
    old_sha = "b" * 40
    harness = f"""
set -u
TEST_ROOT={root!r}
EXPECTED_SHA={'c' * 40}
GUARDED_SHA={guarded_sha}
OLD_SHA={old_sha}
DEPLOY_OPERATION=deploy
DEPLOY_ARTIFACT_MODE=ci-resolved-freeze-v1
DATABASE_WRITER_GUARD_DIR="$TEST_ROOT/guards"
DATABASE_WRITER_GUARD_FILE="$DATABASE_WRITER_GUARD_DIR/guard"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/restore"
ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/transaction"
ACTIVATION_UNIT_SNAPSHOT_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state"
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/new.json"
ACTIVATION_RECEIPT_PENDING="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.json"
ACTIVATION_RECEIPT_PENDING_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.sha256"
PHASE_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/phase"
DB_STATE="$TEST_ROOT/governance-state"
TRACE="$TEST_ROOT/trace"
V2_RECOVERY_STEP=not-started
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$GUARDED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=loaded,inactive,static \
  ai_timer_unit=loaded,inactive,disabled \
  > "$ACTIVATION_UNIT_SNAPSHOT_STATE"
printf 'new\n' > "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
printf 'old-or-partial\n' > "$DB_STATE"
printf '%s\n' {phase!r} > "$PHASE_STATE"
printf 'restore\n' > "$DATABASE_WRITER_RESTORE_FILE"
printf 'guard\n' > "$DATABASE_WRITER_GUARD_FILE"
: > "$TRACE"
activation_snapshot_recorded_release() {{ printf '%s\n' "$GUARDED_SHA"; }}
activation_snapshot_old_release() {{ printf '%s\n' "$OLD_SHA"; }}
activation_snapshot_phase() {{ printf '%s\n' "$(<"$PHASE_STATE")"; }}
activation_snapshot_validate() {{
  test "$1" = "$GUARDED_SHA" || return 1
  test "$(<"$PHASE_STATE")" = new-runtime-preserved-no-receipt || return 1
  printf 'revalidate-commit\n' >> "$TRACE"
}}
activation_snapshot_validate_new() {{ printf 'validate-new\n' >> "$TRACE"; }}
activation_snapshot_validate_governance_new() {{ printf 'validate-snapshot\n' >> "$TRACE"; }}
activation_snapshot_assert_pending_receipt_absent() {{
  test ! -e "$ACTIVATION_RECEIPT_PENDING" || return 1
  test ! -e "$ACTIVATION_RECEIPT_PENDING_SHA" || return 1
  printf 'receipt-absent\n' >> "$TRACE"
}}
controlled_guard_assert_governance_restore_runtime() {{
  test "$1" = "$GUARDED_SHA" || return 1
  printf 'sealed-runtime\n' >> "$TRACE"
}}
controlled_guard_assert_directory() {{ test -d "$DATABASE_WRITER_GUARD_DIR"; }}
controlled_guard_assert_file() {{ test -f "$1"; }}
controlled_guard_assert_state_record() {{ return 0; }}
controlled_guard_assert_restore_file() {{
  test -f "$DATABASE_WRITER_RESTORE_FILE" || return 1
  printf 'assert-restore\n' >> "$TRACE"
}}
controlled_guard_assert_marker() {{ test -f "$DATABASE_WRITER_GUARD_FILE"; }}
controlled_guard_recreate_file() {{ printf 'unexpected-recreate\n' >> "$TRACE"; return 1; }}
controlled_guard_install_dropins() {{ printf 'install-fence\n' >> "$TRACE"; }}
systemctl() {{ test "$1" = daemon-reload || return 1; printf 'reload\n' >> "$TRACE"; }}
controlled_guard_force_all_writers_fenced() {{ printf 'fence\n' >> "$TRACE"; }}
controlled_guard_assert_boundary() {{
  test -f "$DATABASE_WRITER_GUARD_FILE" || return 1
  printf 'boundary\n' >> "$TRACE"
}}
controlled_guard_refence_after_restore_failure() {{
  printf 'unexpected-refence\n' >> "$TRACE"
  return 1
}}
controlled_guard_governance_contract_snapshot() {{
  test "$2:$3" = \
    "$GUARDED_SHA:$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
  test "$(<"$PHASE_STATE")" = restoring-new-no-receipt || return 1
  case "$1" in
    restore)
      test -f "$DATABASE_WRITER_GUARD_FILE" || return 1
      printf 'new\n' > "$DB_STATE"
      printf 'restore-live-new\n' >> "$TRACE"
      ;;
    verify)
      test "$(<"$DB_STATE")" = new || return 1
      printf 'verify-live-new\n' >> "$TRACE"
      ;;
    *) return 1 ;;
  esac
}}
activation_snapshot_set_phase() {{
  test "$1" = "$GUARDED_SHA" || return 1
  case "$2" in
    restoring-new-no-receipt|new-runtime-preserved-no-receipt) ;;
    *) return 1 ;;
  esac
  printf '%s\n' "$2" > "$PHASE_STATE"
  printf 'phase-%s\n' "$2" >> "$TRACE"
  if [ "$2" = new-runtime-preserved-no-receipt ]; then
    return 91
  fi
}}
activation_snapshot_restore_new_set() {{ printf 'restore-new\n' >> "$TRACE"; }}
activation_snapshot_assert_new_set() {{ printf 'assert-new\n' >> "$TRACE"; }}
controlled_guard_cleanup() {{
  test "$(<"$DB_STATE")" = new || return 1
  rm -f "$DATABASE_WRITER_GUARD_FILE"
  printf 'remove-fence\n' >> "$TRACE"
}}
controlled_guard_verify_restored_runtime() {{
  test "$3" = "$GUARDED_SHA" || return 1
  case "$6" in
    full)
      test "$1:$2:$4:$5" = \
        "loaded,inactive,disabled:loaded,inactive,disabled:loaded,inactive,static:loaded,inactive,disabled" || \
        return 1
      test -f "$DATABASE_WRITER_GUARD_FILE" || return 1
      test "$(<"$DB_STATE")" = new || return 1
      printf 'verify-gates-fenced\n' >> "$TRACE"
      ;;
    rollback-only)
      test "$1:$2:$4:$5" = \
        "loaded,active,enabled:loaded,active,enabled:loaded,inactive,static:loaded,inactive,disabled" || \
        return 1
      test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
      printf 'verify-runtime\n' >> "$TRACE"
      ;;
    *) return 1 ;;
  esac
}}
controlled_guard_assert_dropin_contract() {{ return 0; }}
activation_snapshot_remove_new_runtime_preserved_no_receipt() {{
  test "$(<"$PHASE_STATE")" = new-runtime-preserved-no-receipt || return 1
  test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
  test ! -e "$ACTIVATION_RECEIPT_PENDING" || return 1
  test ! -e "$ACTIVATION_RECEIPT_PENDING_SHA" || return 1
  rm -rf "$ACTIVATION_UNIT_SNAPSHOT_DIR"
  printf 'retire-no-receipt\n' >> "$TRACE"
}}
publish_deployed_receipt_pending() {{ printf 'unexpected-publish\n' >> "$TRACE"; return 1; }}
sync() {{ return 0; }}
{recovery}
trap '' TERM INT HUP
controlled_v2_forward_preserve_no_receipt_recovery || exit 30
test "$V2_RECOVERY_STEP" = complete || exit 31
test ! -e "$DATABASE_WRITER_GUARD_FILE" || exit 32
test ! -e "$DATABASE_WRITER_RESTORE_FILE" || exit 33
test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || exit 34
test ! -e "$ACTIVATION_RECEIPT_PENDING" || exit 35
test ! -e "$ACTIVATION_RECEIPT_PENDING_SHA" || exit 36
! grep -q '^unexpected-' "$TRACE" || exit 37
test "$(grep -c '^phase-restoring-new-no-receipt$' "$TRACE")" -eq \
  {expected_begin_writes} || exit 38
test "$(grep -c '^phase-new-runtime-preserved-no-receipt$' "$TRACE")" -eq 1 || exit 39
test "$(grep -c '^verify-gates-fenced$' "$TRACE")" -eq 1 || exit 40
test "$(grep -c '^verify-runtime$' "$TRACE")" -eq 1 || exit 41
test "$(grep -c '^retire-no-receipt$' "$TRACE")" -eq 1 || exit 42
test "$(grep -c '^revalidate-commit$' "$TRACE")" -eq 1 || exit 43
test "$(grep -c '^restore-live-new$' "$TRACE")" -eq 1 || exit 44
test "$(grep -c '^verify-live-new$' "$TRACE")" -eq 3 || exit 45
"""
    harness_path = tmp_path / "forward-preserve-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_forward_contract_restore_failure_stays_durably_fenced(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable forward failure test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    recovery = _function(
        "controlled_v2_forward_preserve_no_receipt_recovery",
        _shell_function_bodies(source)[
            "controlled_v2_forward_preserve_no_receipt_recovery"
        ],
    )
    root = tmp_path.as_posix()
    guarded_sha = "a" * 40
    harness = f"""
set -u
TEST_ROOT={root!r}
EXPECTED_SHA={'c' * 40}
GUARDED_SHA={guarded_sha}
DEPLOY_OPERATION=deploy
DEPLOY_ARTIFACT_MODE=ci-resolved-freeze-v1
DATABASE_WRITER_GUARD_DIR="$TEST_ROOT/guards"
DATABASE_WRITER_GUARD_FILE="$DATABASE_WRITER_GUARD_DIR/guard"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/restore"
ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/transaction"
ACTIVATION_UNIT_SNAPSHOT_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state"
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/new.json"
ACTIVATION_RECEIPT_PENDING="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.json"
ACTIVATION_RECEIPT_PENDING_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.sha256"
PHASE_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/phase"
TRACE="$TEST_ROOT/trace"
V2_RECOVERY_STEP=not-started
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$GUARDED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=loaded,inactive,static \
  ai_timer_unit=loaded,inactive,disabled \
  > "$ACTIVATION_UNIT_SNAPSHOT_STATE"
printf 'new\n' > "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
printf 'runtime-units-installed\n' > "$PHASE_STATE"
printf 'restore\n' > "$DATABASE_WRITER_RESTORE_FILE"
printf 'guard\n' > "$DATABASE_WRITER_GUARD_FILE"
: > "$TRACE"
activation_snapshot_recorded_release() {{ printf '%s\n' "$GUARDED_SHA"; }}
activation_snapshot_old_release() {{ printf '%s\n' "{'b' * 40}"; }}
activation_snapshot_phase() {{ printf '%s\n' "$(<"$PHASE_STATE")"; }}
activation_snapshot_validate_new() {{ return 0; }}
activation_snapshot_validate_governance_new() {{ return 0; }}
activation_snapshot_assert_pending_receipt_absent() {{ return 0; }}
controlled_guard_assert_governance_restore_runtime() {{ return 0; }}
controlled_guard_assert_directory() {{ return 0; }}
controlled_guard_assert_file() {{ test -f "$1"; }}
controlled_guard_assert_state_record() {{ return 0; }}
controlled_guard_assert_restore_file() {{ test -f "$DATABASE_WRITER_RESTORE_FILE"; }}
controlled_guard_assert_marker() {{ test -f "$DATABASE_WRITER_GUARD_FILE"; }}
controlled_guard_recreate_file() {{ return 1; }}
controlled_guard_install_dropins() {{ printf 'install-fence\n' >> "$TRACE"; }}
systemctl() {{ test "$1" = daemon-reload || return 1; }}
controlled_guard_force_all_writers_fenced() {{ printf 'fence\n' >> "$TRACE"; }}
controlled_guard_assert_boundary() {{ test -f "$DATABASE_WRITER_GUARD_FILE"; }}
activation_snapshot_set_phase() {{
  test "$1:$2" = "$GUARDED_SHA:restoring-new-no-receipt" || return 1
  printf '%s\n' "$2" > "$PHASE_STATE"
  printf 'phase-restoring-new\n' >> "$TRACE"
}}
controlled_guard_governance_contract_snapshot() {{
  test "$1:$2:$3" = \
    "restore:$GUARDED_SHA:$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
  test "$(<"$PHASE_STATE")" = restoring-new-no-receipt || return 1
  test -f "$DATABASE_WRITER_GUARD_FILE" || return 1
  printf 'contract-restore-failed\n' >> "$TRACE"
  return 1
}}
controlled_guard_refence_after_restore_failure() {{
  test "$(<"$PHASE_STATE")" = restoring-new-no-receipt || return 1
  : > "$DATABASE_WRITER_GUARD_FILE"
  printf 'refenced\n' >> "$TRACE"
}}
{recovery}
if controlled_v2_forward_preserve_no_receipt_recovery; then exit 30; fi
test "$V2_RECOVERY_STEP" = forward-restore-governance-fenced || exit 31
test "$(<"$PHASE_STATE")" = restoring-new-no-receipt || exit 32
test -f "$DATABASE_WRITER_GUARD_FILE" || exit 33
test -f "$DATABASE_WRITER_RESTORE_FILE" || exit 34
test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR" || exit 35
test "$(grep -c '^phase-restoring-new$' "$TRACE")" -eq 1 || exit 36
test "$(grep -c '^contract-restore-failed$' "$TRACE")" -eq 1 || exit 37
test "$(grep -c '^refenced$' "$TRACE")" -eq 1 || exit 38
test "$(grep -n -E 'phase-restoring-new|contract-restore-failed|refenced' \
  "$TRACE" | cut -d: -f2 | paste -sd, -)" = \
  phase-restoring-new,contract-restore-failed,refenced || exit 39
"""
    harness_path = tmp_path / "forward-contract-failure-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_forward_no_receipt_commit_retry_never_refences_verified_runtime(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable forward commit retry test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    recovery = _function(
        "controlled_v2_forward_preserve_no_receipt_recovery",
        _shell_function_bodies(source)[
            "controlled_v2_forward_preserve_no_receipt_recovery"
        ],
    )
    root = tmp_path.as_posix()
    guarded_sha = "a" * 40
    harness = f"""
set -u
TEST_ROOT={root!r}
EXPECTED_SHA={'c' * 40}
GUARDED_SHA={guarded_sha}
DEPLOY_OPERATION=deploy
DEPLOY_ARTIFACT_MODE=ci-resolved-freeze-v1
DATABASE_WRITER_GUARD_DIR="$TEST_ROOT/guards"
DATABASE_WRITER_GUARD_FILE="$DATABASE_WRITER_GUARD_DIR/guard"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/restore"
ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/transaction"
ACTIVATION_UNIT_SNAPSHOT_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state"
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/new.json"
ACTIVATION_RECEIPT_PENDING="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.json"
ACTIVATION_RECEIPT_PENDING_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.sha256"
TRACE="$TEST_ROOT/trace"
V2_RECOVERY_STEP=not-started
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$GUARDED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=loaded,inactive,static \
  ai_timer_unit=loaded,inactive,disabled \
  > "$ACTIVATION_UNIT_SNAPSHOT_STATE"
printf 'new\n' > "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
printf 'restore\n' > "$DATABASE_WRITER_RESTORE_FILE"
: > "$TRACE"
activation_snapshot_recorded_release() {{ printf '%s\n' "$GUARDED_SHA"; }}
activation_snapshot_old_release() {{ printf '%s\n' "{'b' * 40}"; }}
activation_snapshot_phase() {{ printf 'new-runtime-preserved-no-receipt\n'; }}
activation_snapshot_validate_new() {{ return 0; }}
activation_snapshot_validate_governance_new() {{ return 0; }}
activation_snapshot_assert_pending_receipt_absent() {{
  test ! -e "$ACTIVATION_RECEIPT_PENDING" -a ! -e "$ACTIVATION_RECEIPT_PENDING_SHA"
}}
controlled_guard_assert_governance_restore_runtime() {{ return 0; }}
controlled_guard_assert_directory() {{ return 0; }}
controlled_guard_assert_file() {{ return 0; }}
controlled_guard_assert_state_record() {{ return 0; }}
controlled_guard_assert_restore_file() {{ test -f "$DATABASE_WRITER_RESTORE_FILE"; }}
activation_snapshot_assert_new_set() {{ printf 'assert-new\n' >> "$TRACE"; }}
controlled_guard_assert_dropin_contract() {{ printf 'dropins\n' >> "$TRACE"; }}
controlled_guard_verify_restored_runtime() {{
  test "$3:$6" = "$GUARDED_SHA:rollback-only" || return 1
  printf 'verify-online\n' >> "$TRACE"
}}
controlled_guard_governance_contract_snapshot() {{
  test "$1:$2:$3" = \
    "verify:$GUARDED_SHA:$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
  printf 'verify-governance\n' >> "$TRACE"
}}
controlled_guard_recreate_file() {{ printf 'unexpected-refence\n' >> "$TRACE"; return 1; }}
controlled_guard_install_dropins() {{ printf 'unexpected-refence\n' >> "$TRACE"; return 1; }}
controlled_guard_force_all_writers_fenced() {{ printf 'unexpected-refence\n' >> "$TRACE"; return 1; }}
controlled_guard_refence_after_restore_failure() {{ printf 'unexpected-refence\n' >> "$TRACE"; return 1; }}
activation_snapshot_remove_new_runtime_preserved_no_receipt() {{
  test ! -e "$DATABASE_WRITER_RESTORE_FILE" || return 1
  rm -rf "$ACTIVATION_UNIT_SNAPSHOT_DIR"
  printf 'retire\n' >> "$TRACE"
}}
sync() {{ return 0; }}
{recovery}
controlled_v2_forward_preserve_no_receipt_recovery || exit 30
test "$V2_RECOVERY_STEP" = complete || exit 31
test ! -e "$DATABASE_WRITER_RESTORE_FILE" || exit 32
test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || exit 33
! grep -q '^unexpected-refence$' "$TRACE" || exit 34
test "$(grep -c '^verify-online$' "$TRACE")" -eq 1 || exit 35
test "$(grep -c '^retire$' "$TRACE")" -eq 1 || exit 36
"""
    harness_path = tmp_path / "forward-commit-retry-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize("phase", ("runtime-units-installed", "restoring-old"))
def test_rollback_entry_prefers_sealed_new_target_without_live_new_match(
    tmp_path: Path,
    phase: str,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable recovery router test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    recovery = _function(
        "controlled_v2_rollback_only_recovery",
        _shell_function_bodies(source)["controlled_v2_rollback_only_recovery"],
    )
    root = tmp_path.as_posix()
    guarded_sha = "a" * 40
    harness = f"""
set -u
TEST_ROOT={root!r}
EXPECTED_SHA={'c' * 40}
GUARDED_SHA={guarded_sha}
OLD_SHA={'b' * 40}
DEPLOY_OPERATION=deploy
DEPLOY_ARTIFACT_MODE=ci-resolved-freeze-v1
RELEASE_VENV_ROOT="$TEST_ROOT/venvs"
DATABASE_WRITER_GUARD_DIR="$TEST_ROOT/guards"
DATABASE_WRITER_GUARD_FILE="$DATABASE_WRITER_GUARD_DIR/guard"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/restore"
ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/transaction"
ACTIVATION_UNIT_SNAPSHOT_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state"
ACTIVATION_GOVERNANCE_OLD_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/old.json"
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/new.json"
ACTIVATION_GOVERNANCE_NEW_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/new.sha256"
ACTIVATION_RECEIPT_PENDING="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.json"
ACTIVATION_RECEIPT_PENDING_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/receipt.sha256"
TRACE="$TEST_ROOT/trace"
V2_RECOVERY_STEP=not-started
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR" "$RELEASE_VENV_ROOT"
: > "$RELEASE_VENV_ROOT/$GUARDED_SHA"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$GUARDED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=loaded,inactive,static \
  ai_timer_unit=loaded,inactive,disabled \
  > "$ACTIVATION_UNIT_SNAPSHOT_STATE"
printf 'old\n' > "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"
printf 'new\n' > "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
printf 'sealed\n' > "$ACTIVATION_GOVERNANCE_NEW_SHA"
printf 'restore\n' > "$DATABASE_WRITER_RESTORE_FILE"
printf 'guard\n' > "$DATABASE_WRITER_GUARD_FILE"
: > "$TRACE"
activation_snapshot_recorded_release() {{ printf '%s\n' "$GUARDED_SHA"; }}
activation_snapshot_old_release() {{ printf '%s\n' "$OLD_SHA"; }}
activation_snapshot_phase() {{ printf '%s\n' {phase!r}; }}
controlled_guard_assert_governance_restore_runtime() {{ printf 'sealed-runtime\n' >> "$TRACE"; }}
activation_snapshot_validate_governance_new() {{ printf 'validate-new-snapshot\n' >> "$TRACE"; }}
activation_snapshot_validate_rollback_receipt_state() {{ return 0; }}
controlled_guard_assert_directory() {{ return 0; }}
controlled_guard_assert_file() {{ test -f "$1"; }}
controlled_guard_assert_state_record() {{ return 0; }}
controlled_guard_assert_restore_file() {{ test -f "$DATABASE_WRITER_RESTORE_FILE"; }}
controlled_guard_assert_marker() {{ test -f "$DATABASE_WRITER_GUARD_FILE"; }}
controlled_guard_install_dropins() {{ printf 'install-fence\n' >> "$TRACE"; }}
systemctl() {{ test "$1" = daemon-reload || return 1; printf 'reload\n' >> "$TRACE"; }}
controlled_guard_force_all_writers_fenced() {{ printf 'fence\n' >> "$TRACE"; }}
controlled_guard_assert_boundary() {{ printf 'boundary\n' >> "$TRACE"; }}
controlled_guard_refence_after_restore_failure() {{ printf 'unexpected-refence\n' >> "$TRACE"; return 1; }}
activation_snapshot_assert_pending_receipt_absent() {{ printf 'receipt-absent\n' >> "$TRACE"; }}
activation_snapshot_validate_new() {{ printf 'validate-new-units\n' >> "$TRACE"; }}
controlled_guard_governance_snapshot() {{
  printf 'unexpected-live-governance\n' >> "$TRACE"
  return 1
}}
controlled_v2_forward_preserve_no_receipt_recovery() {{
  printf 'preserve-new\n' >> "$TRACE"
  rm -f "$DATABASE_WRITER_GUARD_FILE" "$DATABASE_WRITER_RESTORE_FILE"
  rm -rf "$ACTIVATION_UNIT_SNAPSHOT_DIR"
}}
controlled_guard_restore_and_verify_governance_snapshot() {{ printf 'unexpected-restore-old\n' >> "$TRACE"; return 1; }}
activation_snapshot_restore_old_set() {{ printf 'unexpected-restore-old\n' >> "$TRACE"; return 1; }}
{recovery}
controlled_v2_rollback_only_recovery || exit 30
test "$V2_RECOVERY_STEP" = forward-preserve || exit 31
! grep -q '^unexpected-' "$TRACE" || exit 32
test "$(grep -c '^validate-new-snapshot$' "$TRACE")" -eq 2 || exit 33
test "$(grep -c '^preserve-new$' "$TRACE")" -eq 1 || exit 34
"""
    harness_path = tmp_path / "new-priority-router-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize("artifact", ("receipt", "receipt_sha"))
def test_forward_no_receipt_phase_rejects_any_pending_receipt_artifact(
    tmp_path: Path,
    artifact: str,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the receipt-absence validator test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = _shell_function_bodies(source)[
        "activation_snapshot_assert_pending_receipt_absent"
    ]
    receipt = (tmp_path / "receipt.json").as_posix()
    receipt_sha = (tmp_path / "receipt.sha256").as_posix()
    target = receipt if artifact == "receipt" else receipt_sha
    harness = f"""
set -u
ACTIVATION_RECEIPT_PENDING={receipt!r}
ACTIVATION_RECEIPT_PENDING_SHA={receipt_sha!r}
activation_snapshot_assert_pending_receipt_absent() {{
{body}
}}
activation_snapshot_assert_pending_receipt_absent || exit 20
: > {target!r}
if activation_snapshot_assert_pending_receipt_absent; then
  exit 21
fi
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_verified_transaction_retire_is_logically_atomic_before_cleanup(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable transaction-retire test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    retire = _function(
        "activation_snapshot_retire_verified_transaction",
        _shell_function_bodies(source)[
            "activation_snapshot_retire_verified_transaction"
        ],
    )
    root = tmp_path.as_posix()
    harness = f"""
set -Eeuo pipefail
DATABASE_WRITER_GUARD_DIR={root!r}/guards
ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/activation-unit-transaction"
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR"
printf 'sealed\n' > "$ACTIVATION_UNIT_SNAPSHOT_DIR/manifest"
sync() {{ return 99; }}
rm() {{
  if [ "$1:${{2:-}}" = -rf:-- ] && \
    [[ "${{3:-}}" = "$DATABASE_WRITER_GUARD_DIR"/.activation-unit-transaction.retired.* ]]; then
    return 98
  fi
  command rm "$@"
}}
{retire}
exec 2>&-
activation_snapshot_retire_verified_transaction || exit 30
test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || exit 31
test ! -L "$ACTIVATION_UNIT_SNAPSHOT_DIR" || exit 32
mapfile -t retired < <(find "$DATABASE_WRITER_GUARD_DIR" -maxdepth 1 \
  -type d -name '.activation-unit-transaction.retired.*' -print)
test "${{#retired[@]}}" -eq 1 || exit 33
test "$(<"${{retired[0]}}/manifest")" = sealed || exit 34
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_exact_live_request_noop_gate_is_strict_and_read_only(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable idempotent deploy test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    gate = _function(
        "prepared_request_is_already_active",
        _shell_function_bodies(source)["prepared_request_is_already_active"],
    )
    runtime_units = _function(
        "assert_prepared_runtime_units_still_current",
        _shell_function_bodies(source)[
            "assert_prepared_runtime_units_still_current"
        ],
    )
    root = tmp_path.as_posix()
    sha = "a" * 40
    lock_sha = "b" * 64
    adata_sha = "c" * 40
    adata_tree_sha = "d" * 64
    release_tree_sha = "e" * 64
    adapter_sha = "f" * 64
    harness = f"""
set -u
TEST_ROOT={root!r}
EXPECTED_SHA={sha}
EXPECTED_INPUT_LOCK_SHA256={lock_sha}
EXPECTED_RESOLVED_FREEZE_SHA256={lock_sha}
EXPECTED_ADATA_SHA={adata_sha}
EXPECTED_ADATA_TREE_SHA256={adata_tree_sha}
EXPECTED_RELEASE_TREE_SHA256={release_tree_sha}
EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256={adapter_sha}
PREVIOUS_SHA="$EXPECTED_SHA"
PREVIOUS_INPUT_LOCK_SHA256="$EXPECTED_INPUT_LOCK_SHA256"
PREVIOUS_RESOLVED_FREEZE_SHA256="$EXPECTED_RESOLVED_FREEZE_SHA256"
PREVIOUS_ADATA_SHA="$EXPECTED_ADATA_SHA"
PREVIOUS_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256"
ADATA_SOURCE="$TEST_ROOT/adata"
PREVIOUS_ADATA_SOURCE="$ADATA_SOURCE"
PREPARED_CODE_ROOT="$TEST_ROOT/releases/$EXPECTED_SHA"
PREVIOUS_CODE_ROOT="$PREPARED_CODE_ROOT"
RELEASE_VENV_ROOT="$TEST_ROOT/venvs"
PREVIOUS_VENV="$RELEASE_VENV_ROOT/$EXPECTED_SHA"
PREVIOUS_DROPIN="$TEST_ROOT/main.previous"
PREPARED_MAIN_DROPIN="$TEST_ROOT/main.prepared"
MAIN_RELEASE_DROPIN="$TEST_ROOT/main.installed"
PREVIOUS_SCHEDULER_DROPIN="$TEST_ROOT/scheduler.previous"
PREPARED_SCHEDULER_DROPIN="$TEST_ROOT/scheduler.prepared"
SCHEDULER_UNIT="$TEST_ROOT/scheduler.installed"
PREVIOUS_AI_WORKER_DROPIN="$TEST_ROOT/ai.previous"
PREPARED_AI_WORKER_DROPIN=""
AI_WORKER_DROPIN="$TEST_ROOT/ai.installed"
PREVIOUS_DROPIN_PRESENT=1
PREVIOUS_SCHEDULER_DROPIN_PRESENT=1
PREVIOUS_AI_WORKER_DROPIN_PRESENT=0
PREVIOUS_MAIN_ACTIVE_STATE=active
PREVIOUS_MAIN_UNIT_FILE_STATE=enabled
SCHEDULER_UNIT_PRESENT=1
AI_WORKER_UNIT_PRESENT=0
MAIN_SERVICE=probiga
PREVIOUS_LEGACY_MAIN_DROPINS=()
PREVIOUS_LEGACY_SCHEDULER_DROPINS=()
LEGACY_MAIN_OVERRIDE_DROPINS=()
LEGACY_SCHEDULER_OVERRIDE_DROPINS=()
MAIN_LIMITS_DROPIN="$TEST_ROOT/main-limits.conf"
MAIN_MARKET_RADAR_DROPIN="$TEST_ROOT/main-market-radar.conf"
MAIN_SERVICE_USER_DROPIN="$TEST_ROOT/main-service-user.conf"
MAIN_DATABASE_WRITER_GUARD_DROPIN="$TEST_ROOT/main-writer-guard.conf"
SCHEDULER_DATABASE_WRITER_GUARD_DROPIN="$TEST_ROOT/scheduler-writer-guard.conf"
SCHEDULER_LIMITS_DROPIN="$TEST_ROOT/scheduler-limits.conf"
mkdir -p "$ADATA_SOURCE" "$PREPARED_CODE_ROOT" "$RELEASE_VENV_ROOT"
printf 'main\n' > "$PREVIOUS_DROPIN"
cp "$PREVIOUS_DROPIN" "$PREPARED_MAIN_DROPIN"
cp "$PREPARED_MAIN_DROPIN" "$MAIN_RELEASE_DROPIN"
printf 'scheduler\n' > "$PREVIOUS_SCHEDULER_DROPIN"
cp "$PREVIOUS_SCHEDULER_DROPIN" "$PREPARED_SCHEDULER_DROPIN"
cp "$PREPARED_SCHEDULER_DROPIN" "$SCHEDULER_UNIT"
TEST_PID=$$
NEED_DAEMON_RELOAD=no
ROTATE_PID_ON_HEALTH=0
HEALTH_STATUS=0
systemctl() {{
  case "$*" in
    "show -p ActiveState --value probiga") printf 'active\n' ;;
    "show -p UnitFileState --value probiga") printf 'enabled\n' ;;
    "show -p ActiveState --value probiga-scheduler") printf 'active\n' ;;
    "show -p UnitFileState --value probiga-scheduler") printf 'enabled\n' ;;
    "show -p MainPID --value probiga"|\
    "show -p MainPID --value probiga-scheduler") printf '%s\n' "$TEST_PID" ;;
    "show -p NeedDaemonReload --value probiga"|\
    "show -p NeedDaemonReload --value probiga-scheduler")
      printf '%s\n' "$NEED_DAEMON_RELOAD"
      ;;
    "show probiga --property=DropInPaths --value")
      printf '%s %s\n' "$MAIN_RELEASE_DROPIN" \
        "$MAIN_DATABASE_WRITER_GUARD_DROPIN"
      ;;
    "show probiga-scheduler --property=DropInPaths --value")
      printf '%s\n' "$SCHEDULER_DATABASE_WRITER_GUARD_DROPIN"
      ;;
    *) return 90 ;;
  esac
}}
sudo() {{ "$@"; }}
grep() {{ return 0; }}
curl() {{ return 0; }}
mapfile() {{
  local target="${{@: -1}}"
  case "$target" in
    main_cmdline)
      main_cmdline=("$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P -m \
        uvicorn server.api.main:app --app-dir "$PREPARED_CODE_ROOT")
      ;;
    scheduler_cmdline)
      scheduler_cmdline=("$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P \
        "$PREPARED_CODE_ROOT/tools/run_scheduler_daemon.py")
      ;;
    *) return 91 ;;
  esac
}}
assert_nginx_static_matches_checkout() {{ test "$1" = "$PREPARED_CODE_ROOT"; }}
assert_ai_worker_runtime() {{ return 92; }}
assert_ai_worker_previous_state_restored() {{ return 93; }}
assert_scheduler_triggers_quiescent() {{ return 0; }}
assert_database_writer_guard_dropins_loaded() {{ return 0; }}
controlled_guard_assert_file() {{ test -f "$1" && test ! -L "$1"; }}
finalized_receipt_matches_current_v2_request() {{ return 0; }}
run_prepared_python_tool() {{
  test "$1" = \
    "$PREPARED_CODE_ROOT/tools/check_strategy_governance_health.py" || return 94
  if [ "$ROTATE_PID_ON_HEALTH" -eq 1 ]; then
    TEST_PID=$((TEST_PID + 1))
  fi
  return "$HEALTH_STATUS"
}}
{runtime_units}
{gate}
prepared_request_is_already_active || exit 20
PREVIOUS_INPUT_LOCK_SHA256={'0' * 64}
if prepared_request_is_already_active; then
  exit 21
fi
PREVIOUS_INPUT_LOCK_SHA256="$EXPECTED_INPUT_LOCK_SHA256"
printf 'drifted\n' > "$MAIN_RELEASE_DROPIN"
if prepared_request_is_already_active; then
  exit 22
fi
cp "$PREPARED_MAIN_DROPIN" "$MAIN_RELEASE_DROPIN"
ROTATE_PID_ON_HEALTH=1
if prepared_request_is_already_active; then
  exit 23
fi
ROTATE_PID_ON_HEALTH=0
NEED_DAEMON_RELOAD=yes
if prepared_request_is_already_active; then
  exit 24
fi
NEED_DAEMON_RELOAD=no
HEALTH_STATUS=1
if prepared_request_is_already_active; then
  exit 25
fi
test ! -e "$TEST_ROOT/mutation" || exit 26
"""
    harness_path = tmp_path / "exact-live-request-noop-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_same_sha_request_identity_mismatch_fails_before_database_phase(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable idempotent deploy test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    prepare = source.index("CUTOVER_STEP=prepare_release")
    gate_start = source.index(
        'if [ "$PREVIOUS_SHA" = "$EXPECTED_SHA" ]; then', prepare
    )
    database = source.index("# PREPARE DATABASE:", gate_start)
    gate = source[gate_start:database]
    marker = (tmp_path / "database-reached").as_posix()
    sha = "a" * 40
    failed = subprocess.run(
        [
            bash,
            "-c",
            f"""
set -Eeuo pipefail
PREVIOUS_SHA={sha}
EXPECTED_SHA={sha}
prepared_request_is_already_active() {{ return 1; }}
{gate}
printf reached > {marker!r}
""",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert failed.returncode != 0
    assert "complete finalized request identity" in failed.stderr
    assert not (tmp_path / "database-reached").exists()

    continued = subprocess.run(
        [
            bash,
            "-c",
            f"""
set -Eeuo pipefail
PREVIOUS_SHA={'b' * 40}
EXPECTED_SHA={sha}
prepared_request_is_already_active() {{ return 1; }}
{gate}
printf reached > {marker!r}
""",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert continued.returncode == 0, continued.stdout + continued.stderr
    assert (tmp_path / "database-reached").read_text() == "reached"


def test_nginx_static_identity_fails_closed_inside_conditional_verifier(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable static identity test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    helper = _function(
        "assert_nginx_static_matches_checkout",
        _shell_function_bodies(source)["assert_nginx_static_matches_checkout"],
    )
    root = tmp_path.as_posix()
    harness = f"""
set -Eeuo pipefail
TEST_ROOT={root!r}
CHECKOUT="$TEST_ROOT/checkout"
WRONG_CHECKOUT="$TEST_ROOT/wrong-checkout"
STATIC_RELEASE_LINK="$TEST_ROOT/current"
mkdir -p "$CHECKOUT/server/static/js" "$CHECKOUT/server/static/css" \
  "$WRONG_CHECKOUT"
printf js > "$CHECKOUT/server/static/js/app.js"
printf css > "$CHECKOUT/server/static/css/style.css"
curl() {{
  case "${{@: -1}}" in
    */js/app.js) printf js ;;
    */css/style.css) printf css ;;
    *) return 90 ;;
  esac
}}
{helper}
if assert_nginx_static_matches_checkout "$CHECKOUT"; then
  exit 20
fi
ln -s "$WRONG_CHECKOUT" "$STATIC_RELEASE_LINK"
if assert_nginx_static_matches_checkout "$CHECKOUT"; then
  exit 21
fi
"""
    harness_path = tmp_path / "static-identity-conditional-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_closed_transport_enters_and_completes_failure_handler(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable transport regression")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    preamble = source[: source.index("umask 022")]
    assert preamble.index("set -Eeuo pipefail") < preamble.index("trap '' PIPE")
    pipe_trap = next(
        line for line in preamble.splitlines() if line.strip() == "trap '' PIPE"
    )
    bodies = _shell_function_bodies(source)
    detach = _function(
        "detach_failure_handler_from_transport",
        bodies["detach_failure_handler_from_transport"],
    )
    precutover = _function(
        "precutover_failure",
        bodies["precutover_failure"],
    )
    sentinel = tmp_path / "transport-handler-complete"
    harness = f"""#!/usr/bin/env bash
set -Eeuo pipefail
{pipe_trap}
SENTINEL="$1"
DEPLOY_MAIN_BASHPID="$BASHPID"
PREVIOUS_SHA={'a' * 40}
{detach}
write_receipt() {{
  test "$1" = PREFLIGHT_FAILED || return 1
  test "$2" = "$PREVIOUS_SHA" || return 1
  printf 'entered\n' > "$SENTINEL"
  kill -HUP "$BASHPID"
  printf 'hup\n' >> "$SENTINEL"
  kill -TERM "$BASHPID"
  printf 'term\n' >> "$SENTINEL"
  kill -INT "$BASHPID"
  printf 'int\n' >> "$SENTINEL"
}}
{precutover}
trap 'precutover_failure "$?" "$LINENO"' ERR
for ((index = 0; index < 10000; index++)); do
  printf 'transport-output-%s\n' "$index"
done
exit 90
"""
    harness_path = tmp_path / "transport-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    outer = r"""
set +e
bash "$1" "$2" 2>&1 | head -c 1 >/dev/null
producer_status="${PIPESTATUS[0]}"
test "$producer_status" -ne 0 || exit 80
test "$producer_status" -ne 141 || exit 81
test -f "$2" || exit 82
test "$(printf '%s,' $(<"$2"))" = 'entered,hup,term,int,' || exit 83
"""
    completed = subprocess.run(
        [bash, "-c", outer, "transport-test", harness_path.as_posix(), sentinel.as_posix()],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_exit_cleanup_retains_transaction_referenced_forward_venv(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable venv-retention regression")
    if os.name == "nt":
        pytest.skip("Windows Git Bash cannot faithfully model POSIX venv symlinks")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    cleanup = _function(
        "cleanup_prepare_artifacts",
        _shell_function_bodies(source)["cleanup_prepare_artifacts"],
    )
    root = tmp_path.as_posix()
    expected_sha = "a" * 40
    harness = f"""
set -u
TEST_ROOT={root!r}
EXPECTED_SHA={expected_sha}
RELEASE_VENV_ROOT="$TEST_ROOT/venvs"
EXPECTED_BUILD="$RELEASE_VENV_ROOT/build-$EXPECTED_SHA-test"
ACTIVATION_UNIT_SNAPSHOT_DIR="$TEST_ROOT/guards/transaction"
DEPLOY_SUCCEEDED=0
NEW_VENV_LINK=1
STAGING_WORKTREE=
RESOLVED_LOCK=
TRUSTED_WHEEL_MANIFEST=
TRUSTED_WHEELHOUSE=
HEALTH_RESPONSE=
ADATA_SOURCE_BUILD=
ADATA_BUILD_SOURCE=
ADATA_WHEEL_DIR=
ADATA_CACHE_BUILD=
PREVIOUS_DROPIN=
PREVIOUS_SCHEDULER_DROPIN=
PREVIOUS_AI_WORKER_DROPIN=
PREVIOUS_LEGACY_MAIN_DROPIN_DIR=
PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR=
PREVIOUS_LOCK_SNAPSHOT=
GOVERNANCE_TASK_OLD_SOURCE=
GOVERNANCE_TASK_NEW_SOURCE=
PREPARED_MAIN_DROPIN=
PREPARED_SCHEDULER_DROPIN=
PREPARED_AI_WORKER_DROPIN=
mkdir -p "$EXPECTED_BUILD/bin" "$ACTIVATION_UNIT_SNAPSHOT_DIR"
printf '#!/usr/bin/env bash\n' > "$EXPECTED_BUILD/bin/python"
ln -s "$EXPECTED_BUILD" "$RELEASE_VENV_ROOT/$EXPECTED_SHA"
cleanup_staging_worktree() {{ return 0; }}
path_is_runtime_referenced() {{ return 1; }}
{cleanup}
cleanup_prepare_artifacts || exit 20
test -L "$RELEASE_VENV_ROOT/$EXPECTED_SHA" || exit 21
test -d "$EXPECTED_BUILD" || exit 22
rm -rf "$ACTIVATION_UNIT_SNAPSHOT_DIR"
cleanup_prepare_artifacts || exit 23
test ! -e "$RELEASE_VENV_ROOT/$EXPECTED_SHA" || exit 24
test ! -L "$RELEASE_VENV_ROOT/$EXPECTED_SHA" || exit 25
test ! -e "$EXPECTED_BUILD" || exit 26
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize(
    ("phase", "has_restore", "same_target", "request_matches"),
    (
        ("new-runtime-verified", True, False, False),
        ("new-runtime-verified", False, True, True),
        ("finalized", False, False, False),
        ("finalized", False, True, False),
    ),
)
def test_forward_finalize_recovery_closes_every_receipt_window(
    tmp_path: Path,
    phase: str,
    has_restore: bool,
    same_target: bool,
    request_matches: bool,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable forward-finalize test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    forward = _function(
        "controlled_v2_forward_finalize_recovery",
        _shell_function_bodies(source)["controlled_v2_forward_finalize_recovery"],
    )
    root = tmp_path.as_posix()
    guarded_sha = "a" * 40
    expected_sha = guarded_sha if same_target else "b" * 40
    restore_setup = (
        'printf "restore\\n" > "$DATABASE_WRITER_RESTORE_FILE"'
        if has_restore
        else ""
    )
    expected_trace = [
        "validate",
        "validate-new",
        "validate-governance",
        "validate-receipt",
    ]
    if same_target:
        expected_trace.append("match-request")
    expected_trace.extend(
        ("assert-new", "dropin-contract", "verify-runtime", "verify-governance")
    )
    if has_restore:
        expected_trace.append("assert-restore")
    if phase == "new-runtime-verified":
        expected_trace.append("set-finalized")
    expected_trace.extend(("publish", "remove-journal"))
    expected_trace_text = "\n".join(expected_trace) + "\n"
    harness = f"""
set -u
TEST_ROOT={root!r}
EXPECTED_SHA={expected_sha}
GUARDED_SHA={guarded_sha}
REQUEST_MATCH={int(request_matches)}
DEPLOY_OPERATION=deploy
DEPLOY_ARTIFACT_MODE=ci-resolved-freeze-v1
DATABASE_WRITER_GUARD_DIR="$TEST_ROOT/guards"
DATABASE_WRITER_GUARD_FILE="$DATABASE_WRITER_GUARD_DIR/guard"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/restore"
ACTIVATION_UNIT_SNAPSHOT_DIR="$DATABASE_WRITER_GUARD_DIR/transaction"
ACTIVATION_UNIT_SNAPSHOT_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/writer-state"
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/new.json"
PHASE_STATE="$ACTIVATION_UNIT_SNAPSHOT_DIR/phase"
TRACE="$TEST_ROOT/trace"
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$GUARDED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=loaded,inactive,static \
  ai_timer_unit=loaded,inactive,disabled \
  > "$ACTIVATION_UNIT_SNAPSHOT_STATE"
printf 'new\n' > "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
printf '%s\n' {phase!r} > "$PHASE_STATE"
{restore_setup}
: > "$TRACE"
activation_snapshot_recorded_release() {{ printf '%s\n' "$GUARDED_SHA"; }}
activation_snapshot_phase() {{ printf '%s\n' "$(<"$PHASE_STATE")"; }}
activation_snapshot_validate() {{ printf 'validate\n' >> "$TRACE"; }}
activation_snapshot_validate_new() {{ printf 'validate-new\n' >> "$TRACE"; }}
activation_snapshot_validate_governance_new() {{
  printf 'validate-governance\n' >> "$TRACE"
}}
activation_snapshot_validate_receipt_pending() {{
  test "$1" = "$GUARDED_SHA" || return 1
  printf 'validate-receipt\n' >> "$TRACE"
}}
activation_snapshot_receipt_matches_current_v2_request() {{
  test "$1" = "$EXPECTED_SHA" || return 1
  printf 'match-request\n' >> "$TRACE"
  test "$REQUEST_MATCH" -eq 1
}}
activation_snapshot_assert_new_set() {{ printf 'assert-new\n' >> "$TRACE"; }}
controlled_guard_assert_file() {{ test -f "$1"; }}
controlled_guard_assert_state_record() {{ return 0; }}
controlled_guard_assert_dropin_contract() {{
  test "$1:$2:$3" = loaded:loaded:loaded || return 1
  printf 'dropin-contract\n' >> "$TRACE"
}}
controlled_guard_verify_restored_runtime() {{
  test "$1:$2:$3:$6" = \
    "loaded,active,enabled:loaded,active,enabled:$GUARDED_SHA:rollback-only" || \
    return 1
  printf 'verify-runtime\n' >> "$TRACE"
  kill -HUP "$BASHPID"
}}
controlled_guard_governance_contract_snapshot() {{
  test "$1:$2:$3" = \
    "verify:$GUARDED_SHA:$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
  printf 'verify-governance\n' >> "$TRACE"
}}
controlled_guard_assert_restore_file() {{
  test -f "$DATABASE_WRITER_RESTORE_FILE" || return 1
  printf 'assert-restore\n' >> "$TRACE"
}}
sync() {{ return 0; }}
activation_snapshot_set_phase() {{
  test "$1:$2" = "$GUARDED_SHA:finalized" || return 1
  printf 'finalized\n' > "$PHASE_STATE"
  printf 'set-finalized\n' >> "$TRACE"
}}
publish_deployed_receipt_pending() {{
  test "$1" = "$GUARDED_SHA" || return 1
  printf 'publish\n' >> "$TRACE"
}}
activation_snapshot_remove_finalized_before_deploy() {{
  test "$(<"$PHASE_STATE")" = finalized || return 1
  publish_deployed_receipt_pending "$GUARDED_SHA" || return 1
  rm -rf "$ACTIVATION_UNIT_SNAPSHOT_DIR"
  printf 'remove-journal\n' >> "$TRACE"
}}
{forward}
trap '' TERM INT HUP
controlled_v2_forward_finalize_recovery || exit 30
test "$V2_FORWARD_FINALIZED_SHA" = "$GUARDED_SHA" || exit 301
test "$V2_FORWARD_FINALIZED_REQUEST_MATCH" -eq "$REQUEST_MATCH" || exit 302
test ! -e "$DATABASE_WRITER_GUARD_FILE" || exit 31
test ! -L "$DATABASE_WRITER_GUARD_FILE" || exit 32
test ! -e "$DATABASE_WRITER_RESTORE_FILE" || exit 33
test ! -L "$DATABASE_WRITER_RESTORE_FILE" || exit 34
test ! -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" || exit 35
cat > "$TEST_ROOT/expected-trace" <<'EOF'
{expected_trace_text}EOF
cmp "$TEST_ROOT/expected-trace" "$TRACE" || exit 36
"""
    harness_path = tmp_path / "forward-finalize-recovery-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize(
    "mismatch_field",
    (
        None,
        "expected_sha",
        "active_sha",
        "expected_input_lock_sha256",
        "active_input_lock_sha256",
        "expected_resolved_freeze_sha256",
        "active_resolved_freeze_sha256",
        "expected_adata_sha",
        "active_adata_sha",
        "expected_adata_tree_sha256",
        "active_adata_tree_sha256",
    ),
)
def test_same_sha_forward_finalize_requires_exact_request_artifact_identity(
    tmp_path: Path,
    mismatch_field: str | None,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable receipt identity test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    helper = _function(
        "activation_snapshot_receipt_matches_current_v2_request",
        _shell_function_bodies(source)[
            "activation_snapshot_receipt_matches_current_v2_request"
        ],
    ).replace('/usr/bin/python3.14 -I -', '"$TEST_PYTHON" -I -')
    release_sha = "a" * 40
    input_lock_sha256 = "b" * 64
    adata_sha = "c" * 40
    adata_tree_sha256 = "d" * 64
    payload = {
        "schema_version": "probiga.deploy-receipt.v4",
        "status": "DEPLOYED",
        "expected_sha": release_sha,
        "active_sha": release_sha,
        "expected_input_lock_sha256": input_lock_sha256,
        "active_input_lock_sha256": input_lock_sha256,
        "expected_resolved_freeze_sha256": input_lock_sha256,
        "active_resolved_freeze_sha256": input_lock_sha256,
        "expected_adata_sha": adata_sha,
        "active_adata_sha": adata_sha,
        "expected_adata_tree_sha256": adata_tree_sha256,
        "active_adata_tree_sha256": adata_tree_sha256,
    }
    if mismatch_field is not None:
        payload[mismatch_field] = "e" * len(str(payload[mismatch_field]))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    python_executable = Path(shutil.which("python") or sys.executable).as_posix()
    harness = f"""
set -u
TEST_PYTHON={python_executable!r}
ACTIVATION_RECEIPT_PENDING={receipt.as_posix()!r}
EXPECTED_SHA={release_sha}
EXPECTED_INPUT_LOCK_SHA256={input_lock_sha256}
EXPECTED_ADATA_SHA={adata_sha}
EXPECTED_ADATA_TREE_SHA256={adata_tree_sha256}
activation_snapshot_validate_receipt_pending() {{
  test "$1" = "$EXPECTED_SHA" || return 1
}}
{helper}
activation_snapshot_receipt_matches_current_v2_request "$EXPECTED_SHA"
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    if mismatch_field is None:
        assert completed.returncode == 0, completed.stdout + completed.stderr
    else:
        assert completed.returncode != 0


def test_controlled_database_gate_deadline_kills_uncooperative_process_group(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable gate deadline test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    deadline = _function(
        "controlled_guard_run_service_gate_with_deadline",
        _shell_function_bodies(source)[
            "controlled_guard_run_service_gate_with_deadline"
        ],
    ).replace('/usr/bin/sudo -u "$service_user" ', "").replace(
        "test -x /usr/bin/sudo || return 1", "true"
    )
    pid_file = (tmp_path / "deadline-child.pid").as_posix()
    harness = f"""
set -u
CONTROLLED_DATABASE_GATE_KILL_AFTER=1s
CONTROLLED_DATABASE_GATE_TIMEOUT=1s
PID_FILE={pid_file!r}
{deadline}
started="$(date +%s)"
set +e
controlled_guard_run_service_gate_with_deadline test-user /usr/bin/bash -c \
  'trap "" TERM; sleep 30 & child=$!; printf "%s\\n" "$child" > "$1"; wait "$child"' \
  deadline-child "$PID_FILE"
status=$?
set -e
test "$status" -eq 1 || exit 20
elapsed=$(( $(date +%s) - started ))
test "$elapsed" -le 8 || exit 21
test -s "$PID_FILE" || exit 22
child="$(<"$PID_FILE")"
case "$child" in ''|*[!0-9]*) exit 23 ;; esac
for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  if ! kill -0 "$child" 2>/dev/null; then
    exit 0
  fi
  sleep 0.1
done
exit 24
"""
    harness_path = tmp_path / "gate-deadline-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_finalized_receipt_requires_content_hash_and_exact_request_identity(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable receipt identity test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    helper = _function(
        "finalized_receipt_matches_current_v2_request",
        _shell_function_bodies(source)[
            "finalized_receipt_matches_current_v2_request"
        ],
    ).replace('/usr/bin/python3.14 -I -', '"$TEST_PYTHON" -I -')
    helper = helper.replace(
        'test "$(readlink -f "$RECEIPT_DIR")" = "$RECEIPT_DIR" || return 1',
        "true",
    ).replace(
        'test "$(stat -c \'%U:%G\' "$RECEIPT_DIR")" = root:root || return 1',
        "true",
    ).replace(
        'test "$(stat -c \'%a\' "$RECEIPT_DIR")" = 700 || return 1',
        "true",
    )
    release_sha = "a" * 40
    input_lock_sha256 = "b" * 64
    freeze_sha256 = "c" * 64
    wheel_sha256 = "d" * 64
    adata_sha = "e" * 40
    adata_tree_sha256 = "f" * 64
    payload = {
        "schema_version": "probiga.deploy-receipt.v4",
        "status": "DEPLOYED",
        "expected_sha": release_sha,
        "active_sha": release_sha,
        "expected_input_lock_sha256": input_lock_sha256,
        "active_input_lock_sha256": input_lock_sha256,
        "expected_resolved_freeze_sha256": freeze_sha256,
        "active_resolved_freeze_sha256": freeze_sha256,
        "expected_wheel_manifest_sha256": wheel_sha256,
        "expected_adata_sha": adata_sha,
        "active_adata_sha": adata_sha,
        "expected_adata_tree_sha256": adata_tree_sha256,
        "active_adata_tree_sha256": adata_tree_sha256,
    }
    receipt_dir = tmp_path / "receipts"
    receipt_dir.mkdir()
    python_executable = Path(shutil.which("python") or sys.executable).as_posix()
    harness = f"""
set -u
TEST_PYTHON={python_executable!r}
RECEIPT_DIR={receipt_dir.as_posix()!r}
EXPECTED_SHA={release_sha}
EXPECTED_INPUT_LOCK_SHA256={input_lock_sha256}
EXPECTED_RESOLVED_FREEZE_SHA256={freeze_sha256}
EXPECTED_WHEEL_MANIFEST_SHA256={wheel_sha256}
EXPECTED_ADATA_SHA={adata_sha}
EXPECTED_ADATA_TREE_SHA256={adata_tree_sha256}
controlled_guard_assert_file() {{
  test -f "$1" || return 1
  test ! -L "$1" || return 1
}}
{helper}
finalized_receipt_matches_current_v2_request
"""

    def run_validator(candidate_payload: dict[str, str], *, valid_hash: bool) -> int:
        for existing in receipt_dir.iterdir():
            existing.unlink()
        content = json.dumps(candidate_payload, separators=(",", ":")).encode()
        digest = hashlib.sha256(content).hexdigest()
        if not valid_hash:
            digest = "0" * 64
        receipt = receipt_dir / f"{release_sha}-finalized-{digest}.json"
        receipt.write_bytes(content)
        completed = subprocess.run(
            [bash, "-c", harness],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
        return completed.returncode

    assert run_validator(payload, valid_hash=True) == 0
    mismatch = dict(payload)
    mismatch["active_input_lock_sha256"] = "0" * 64
    assert run_validator(mismatch, valid_hash=True) != 0
    assert run_validator(payload, valid_hash=False) != 0


@pytest.mark.parametrize("fault", ("rm", "sync", "finalized-phase"))
def test_verified_runtime_cleanup_fault_never_refences_live_writers(
    tmp_path: Path,
    fault: str,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable finalize fault test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    finalize = _function(
        "controlled_guard_finalize_successful_activation",
        _shell_function_bodies(source)[
            "controlled_guard_finalize_successful_activation"
        ],
    )
    root = tmp_path.as_posix()
    guarded_sha = "a" * 40
    harness = f"""
set -u
TEST_ROOT={root!r}
FAULT={fault!r}
GUARDED_SHA={guarded_sha}
DATABASE_WRITER_GUARD_DIR="$TEST_ROOT/guards"
DATABASE_WRITER_GUARD_FILE="$DATABASE_WRITER_GUARD_DIR/guard"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/restore"
PHASE_STATE="$TEST_ROOT/phase"
UNSAFE_MUTATION="$TEST_ROOT/unsafe-mutation"
mkdir -p "$DATABASE_WRITER_GUARD_DIR"
printf 'restore\n' > "$DATABASE_WRITER_RESTORE_FILE"
printf 'runtime-units-installed\n' > "$PHASE_STATE"
controlled_guard_sync_activation_journal() {{ return 0; }}
controlled_guard_assert_dropin_contract() {{ return 0; }}
systemctl() {{
  if [ "$1" != show ]; then
    : > "$UNSAFE_MUTATION"
    return 90
  fi
  case "$3:${{5:-}}" in
    LoadState:probiga|LoadState:probiga-scheduler) printf 'loaded\n' ;;
    ActiveState:probiga|ActiveState:probiga-scheduler) printf 'active\n' ;;
    UnitFileState:probiga|UnitFileState:probiga-scheduler) printf 'enabled\n' ;;
    *) return 91 ;;
  esac
}}
controlled_guard_apply_unit_state() {{ return 0; }}
curl() {{ return 0; }}
activation_snapshot_validate() {{ return 0; }}
activation_snapshot_validate_new() {{ return 0; }}
activation_snapshot_assert_new_set() {{ return 0; }}
activation_snapshot_set_phase() {{
  test "$1" = "$GUARDED_SHA" || return 1
  if [ "$2" = finalized ] && [ "$FAULT" = finalized-phase ]; then
    return 92
  fi
  printf '%s\n' "$2" > "$PHASE_STATE"
}}
controlled_guard_write_restore_file() {{ : > "$UNSAFE_MUTATION"; return 1; }}
controlled_guard_refence_after_restore_failure() {{
  : > "$UNSAFE_MUTATION"
  return 1
}}
rm() {{
  if [ "$FAULT" = rm ] && [ "${{*: -1}}" = "$DATABASE_WRITER_RESTORE_FILE" ]; then
    return 93
  fi
  command rm "$@"
}}
sync() {{
  if [ "$FAULT" = sync ] && [ "${{*: -1}}" = "$DATABASE_WRITER_GUARD_DIR" ]; then
    return 94
  fi
  return 0
}}
{finalize}
if controlled_guard_finalize_successful_activation "$GUARDED_SHA" \
    loaded,active,enabled loaded,active,enabled \
    loaded,inactive,static loaded,inactive,disabled; then
  echo "fault $FAULT unexpectedly finalized" >&2
  exit 20
fi
test "$(<"$PHASE_STATE")" = new-runtime-verified || exit 21
test ! -e "$UNSAFE_MUTATION" || exit 22
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_guard_state_helpers_do_not_lose_early_failures_under_if_not() -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable errexit regression")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    shell_functions = "".join(
        _function(name, bodies[name])
        for name in (
            "controlled_guard_apply_unit_state",
            "controlled_guard_restore_previous_writer_states",
        )
    )
    harness = f"""
set -u
systemctl() {{
  local operation="$1"
  shift
  if [ "$operation" = show ]; then
    local property="$2"
    local unit="${{@: -1}}"
    case "$property:$unit" in
      LoadState:*) printf 'loaded\n' ;;
      UnitFileState:*) printf 'enabled\n' ;;
      ActiveState:probiga) printf 'inactive\n' ;;
      ActiveState:*) printf 'active\n' ;;
      MainPID:*|ExecMainPID:*) printf '0\n' ;;
      *) return 88 ;;
    esac
    return 0
  fi
  return 0
}}
curl() {{ return 0; }}
{shell_functions}
if controlled_guard_apply_unit_state probiga loaded,active,enabled; then
  echo 'active-state mismatch was lost' >&2
  exit 11
fi
if controlled_guard_restore_previous_writer_states \
  loaded,active,enabled loaded,active,enabled \
  loaded,active,enabled loaded,active,enabled; then
  echo 'nested active-state mismatch was lost' >&2
  exit 12
fi
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_immutable_venv_tree_accepts_safe_links_and_rejects_writes_and_escape(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable venv seal regression")
    if os.name == "nt":
        pytest.skip("Windows cannot faithfully create and chmod POSIX venv symlinks")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = _shell_function_bodies(source)[
        "controlled_guard_assert_immutable_venv_tree"
    ]
    tree = (tmp_path / "build-old-runtime").as_posix()
    escape_tree = (tmp_path / "build-escape-runtime").as_posix()
    bootstrap = (tmp_path / "trusted-python3.14").as_posix()
    attacker_link = (tmp_path / "attacker-python").as_posix()
    body = body.replace(
        "local bootstrap_entry=/usr/bin/python3.14",
        'local bootstrap_entry="$TEST_BOOTSTRAP"',
    )
    body = body.replace(
        "local expected_owner=root",
        'local expected_owner="$TEST_OWNER_NAME"',
    )
    body = body.replace(
        "local expected_owner_group=root:root",
        'local expected_owner_group="$TEST_OWNER_GROUP"',
    )
    harness = f"""
set -u
TREE={tree!r}
ESCAPE_TREE={escape_tree!r}
TEST_BOOTSTRAP={bootstrap!r}
ATTACKER_LINK={attacker_link!r}
mkdir -p "$TREE/bin" "$TREE/lib"
printf '#!/usr/bin/env bash\nexit 0\n' > "$TEST_BOOTSTRAP"
printf '#!/usr/bin/env bash\nexit 0\n' > "$TREE/bin/python3.14"
ln -s python3.14 "$TREE/bin/python"
ln -s lib "$TREE/lib64"
chmod 0555 "$TEST_BOOTSTRAP" "$TREE" "$TREE/bin" "$TREE/lib" \
  "$TREE/bin/python3.14"
TEST_OWNER_NAME="$(stat -c '%U' "$TREE")"
TEST_OWNER_GROUP="$(stat -c '%U:%G' "$TREE")"
controlled_guard_assert_immutable_venv_tree() {{
{body}
}}
controlled_guard_assert_immutable_venv_tree "$TREE" || exit 20
chmod 0755 "$TREE"
printf 'writable\n' > "$TREE/writable"
chmod 0666 "$TREE/writable"
chmod 0555 "$TREE"
if controlled_guard_assert_immutable_venv_tree "$TREE"; then
  echo 'writable regular file unexpectedly accepted' >&2
  exit 21
fi
chmod 0755 "$TREE"
rm -f "$TREE/writable"
chmod 0555 "$TREE"
mkdir -p "$ESCAPE_TREE"
ln -s /tmp "$ESCAPE_TREE/only-link"
chmod 0555 "$ESCAPE_TREE"
if controlled_guard_assert_immutable_venv_tree "$ESCAPE_TREE"; then
  echo 'single escaping symlink unexpectedly accepted' >&2
  exit 22
fi
ln -s "$TEST_BOOTSTRAP" "$ATTACKER_LINK"
chmod 0755 "$TREE"
ln -s "$ATTACKER_LINK" "$TREE/external-chain"
chmod 0555 "$TREE"
if controlled_guard_assert_immutable_venv_tree "$TREE"; then
  echo 'external intermediate symlink unexpectedly accepted' >&2
  exit 23
fi
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_governance_snapshot_verification_rejects_any_row_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import add_strategy_governance_task as task_tool

    row = {
        "id": 17,
        "task_type": task_tool.TASK["task_type"],
        "script_path": task_tool.TASK["script_path"],
        "enabled": 1,
    }
    snapshot = tmp_path / "governance.json"
    task_tool._write_snapshot(snapshot, [row])
    monkeypatch.setattr(task_tool, "_require_unique_task", lambda _engine: [row])
    assert task_tool._verify_snapshot(object(), snapshot) == {
        "verified": True,
        "row_count": 1,
    }
    monkeypatch.setattr(
        task_tool,
        "_require_unique_task",
        lambda _engine: [{**row, "enabled": 0}],
    )
    with pytest.raises(RuntimeError, match="differs from sealed snapshot"):
        task_tool._verify_snapshot(object(), snapshot)


def test_ai_runtime_assertion_propagates_first_middle_and_last_bash_failures(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable AI runtime regression")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = _shell_function_bodies(source)["assert_ai_worker_runtime"]
    counter = (tmp_path / "systemctl-calls").as_posix()
    harness = f"""
set -uo pipefail
SERVICE_USER=probiga
AI_WORKER_SERVICE=probiga-ai-recommendation-worker.service
RELEASE_VENV_ROOT=/venv
CODE_RELEASE_ROOT=/code
COUNTER={counter!r}
FAIL_CALL=0
systemctl() {{
  local count property
  count="$(<"$COUNTER")"
  count=$((count + 1))
  printf '%s\n' "$count" > "$COUNTER"
  if [ "$count" -eq "$FAIL_CALL" ]; then
    return 83
  fi
  property="$3"
  case "$property" in
    User|Group) printf '%s\n' "$SERVICE_USER" ;;
    WorkingDirectory) printf '%s\n' /opt/ProBigA ;;
    ExecStart) printf '%s\n' '/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 /venv/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/bin/python -P /code/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/tools/run_ai_recommendation_worker.py --once' ;;
    *) return 84 ;;
  esac
}}
assert_ai_worker_runtime() {{
{body}
}}
for FAIL_CALL in 1 5 9; do
  printf '0\n' > "$COUNTER"
  if assert_ai_worker_runtime {'a' * 40}; then
    echo "AI runtime assertion lost failure at call $FAIL_CALL" >&2
    exit 20
  fi
  test "$(<"$COUNTER")" -eq "$FAIL_CALL" || exit 21
done
FAIL_CALL=0
printf '0\n' > "$COUNTER"
assert_ai_worker_runtime {'a' * 40} || exit 22
test "$(<"$COUNTER")" -eq 9 || exit 23
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_ai_runtime_assertion_accepts_legacy_env_only_for_previous_rollback(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable AI runtime regression")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = _shell_function_bodies(source)["assert_ai_worker_runtime"]
    old_sha = "a" * 40
    new_sha = "b" * 40
    harness = f"""
set -uo pipefail
SERVICE_USER=probiga
AI_WORKER_SERVICE=probiga-ai-recommendation-worker.service
RELEASE_VENV_ROOT=/venv
CODE_RELEASE_ROOT=/code
EXPECTED_SHA={new_sha}
EXECSTART='/usr/bin/env GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PROBIGA_DEPLOYMENT_MODE=production PROBIGA_EXPECTED_GIT_SHA={old_sha} PROBIGA_CODE_ROOT=/code/{old_sha} /venv/{old_sha}/bin/python -P /code/{old_sha}/tools/run_ai_recommendation_worker.py --once'
systemctl() {{
  local property="$3"
  case "$property" in
    User|Group) printf '%s\n' "$SERVICE_USER" ;;
    WorkingDirectory) printf '%s\n' /opt/ProBigA ;;
    ExecStart) printf '%s\n' "$EXECSTART" ;;
    *) return 84 ;;
  esac
}}
assert_ai_worker_runtime() {{
{body}
}}
if assert_ai_worker_runtime {old_sha}; then
  echo 'strict verification accepted a legacy environment' >&2
  exit 30
fi
assert_ai_worker_runtime {old_sha} /venv/{old_sha} /code/{old_sha} \
  legacy-rollback || exit 31
EXPECTED_SHA={old_sha}
if assert_ai_worker_runtime {old_sha} /venv/{old_sha} /code/{old_sha} \
    legacy-rollback; then
  echo 'legacy rollback accepted the forward target revision' >&2
  exit 32
fi
EXPECTED_SHA={new_sha}
EXECSTART='/usr/bin/env PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 /venv/{old_sha}/bin/python -P /code/{new_sha}/tools/run_ai_recommendation_worker.py --once'
if assert_ai_worker_runtime {old_sha} /venv/{old_sha} /code/{old_sha} \
    legacy-rollback; then
  echo 'legacy rollback accepted a different code root' >&2
  exit 33
fi
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
