from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import os
import re
import shutil
import socket
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
        '"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" 600',
        '"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" "$TEST_SECURE_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA" 600',
        '"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA" "$TEST_SECURE_FILE_MODE"',
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
ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/qmt-announcement-task-old.json"
ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/qmt-announcement-task-old.sha256"
ACTIVATION_RELEASE_IDENTITY="$ACTIVATION_UNIT_SNAPSHOT_DIR/release-identity"
ACTIVATION_RELEASE_IDENTITY_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/release-identity.sha256"
DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/restore"
GOVERNANCE_TASK_OLD_SOURCE="$TEST_ROOT/governance-old.json"
QMT_ANNOUNCEMENT_TASK_OLD_SOURCE="$TEST_ROOT/qmt-announcement-old.json"
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
printf '{{"rows":[]}}\n' > "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$EXPECTED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=not-found,not-found,not-found \
  ai_timer_unit=not-found,not-found,not-found \
  > "$DATABASE_WRITER_RESTORE_FILE"
chmod 644 "$UNIT_A" "$UNIT_B"
chmod 644 "$PREPARED_MAIN_DROPIN" "$PREPARED_SCHEDULER_DROPIN"
chmod 600 "$DATABASE_WRITER_RESTORE_FILE" "$GOVERNANCE_TASK_OLD_SOURCE" \
  "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE"
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
  test "$(<"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA")" = \
    "$(sha256sum "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" | cut -d' ' -f1)" || return 1
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
cmp "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE" \
  "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" || exit 33
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
    qmt_snapshot = tmp_path / "qmt-announcement-task-new.json"
    qmt_snapshot_sha = tmp_path / "qmt-announcement-task-new.sha256"
    snapshot.write_text('{"tasks":[]}', encoding="utf-8")
    qmt_snapshot.write_text('{"rows":[]}', encoding="utf-8")
    harness = f"""
set -u
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT={snapshot.as_posix()!r}
ACTIVATION_GOVERNANCE_NEW_SHA={snapshot_sha.as_posix()!r}
ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT={qmt_snapshot.as_posix()!r}
ACTIVATION_QMT_ANNOUNCEMENT_NEW_SHA={qmt_snapshot_sha.as_posix()!r}
controlled_guard_assert_file() {{
  test -f "$1" || return 1
  test ! -L "$1" || return 1
}}
{validator}
activation_snapshot_validate_governance_new || exit 20
sha256sum "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" | cut -d' ' -f1 \
  > "$ACTIVATION_GOVERNANCE_NEW_SHA"
sha256sum "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT" | cut -d' ' -f1 \
  > "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SHA"
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
ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT="$ACTIVATION_UNIT_SNAPSHOT_DIR/qmt-new.json"
ACTIVATION_QMT_ANNOUNCEMENT_NEW_SHA="$ACTIVATION_UNIT_SNAPSHOT_DIR/qmt-new.sha256"
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
    (
        "phase",
        "expected_begin_writes",
        "initial_db_state",
        "expected_contract_restores",
    ),
    (
        ("runtime-units-installed", 1, "old-or-partial", 1),
        ("restoring-old", 1, "old-or-partial", 1),
        ("restoring-new-no-receipt", 0, "old-or-partial", 1),
        ("restoring-new-no-receipt", 0, "new", 0),
    ),
)
def test_forward_no_receipt_recovery_restarts_exact_new_runtime_and_retires(
    tmp_path: Path,
    phase: str,
    expected_begin_writes: int,
    initial_db_state: str,
    expected_contract_restores: int,
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
CUTOVER_DEADLINE=9999999999
RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE=
RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH=
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$GUARDED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=loaded,inactive,static \
  ai_timer_unit=loaded,inactive,disabled \
  > "$ACTIVATION_UNIT_SNAPSHOT_STATE"
printf 'new\n' > "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
printf '%s\n' {initial_db_state!r} > "$DB_STATE"
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
controlled_guard_assert_activation_deadline() {{
  test "$1" = "$CUTOVER_DEADLINE"
}}
controlled_guard_install_recovery_cutover_dropins() {{
  test "$1" = "$CUTOVER_DEADLINE" || return 1
  printf 'install-cutover-deadline\n' >> "$TRACE"
}}
controlled_guard_remove_recovery_cutover_dropins() {{
  test "$1" = "$CUTOVER_DEADLINE" || return 1
  printf 'remove-cutover-deadline\n' >> "$TRACE"
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
  test "$6" = "$CUTOVER_DEADLINE" || return 1
  test "$(<"$DB_STATE")" = new || return 1
  rm -f "$DATABASE_WRITER_GUARD_FILE"
  printf 'remove-fence\n' >> "$TRACE"
}}
controlled_guard_verify_restored_runtime() {{
  test "$3" = "$GUARDED_SHA" || return 1
    case "$6" in
    full)
      test "${{7:-}}" = recover-input-readiness || return 1
      test "$1:$2:$4:$5" = \
        "loaded,inactive,disabled:loaded,inactive,disabled:loaded,inactive,static:loaded,inactive,disabled" || \
        return 1
      test -f "$DATABASE_WRITER_GUARD_FILE" || return 1
      test "$(<"$DB_STATE")" = new || return 1
      RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE=2026-08-21
      RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH="$CUTOVER_DEADLINE"
      printf 'verify-gates-fenced\n' >> "$TRACE"
      ;;
    rollback-only)
      test "$#" -eq 8 || return 1
      test "$7:$8" = "strict:$CUTOVER_DEADLINE" || return 1
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
test "$(grep -c '^install-cutover-deadline$' "$TRACE")" -eq 1 || exit 46
test "$(grep -c '^remove-cutover-deadline$' "$TRACE")" -eq 1 || exit 47
test "$(grep -c '^retire-no-receipt$' "$TRACE")" -eq 1 || exit 42
test "$(grep -c '^revalidate-commit$' "$TRACE")" -eq 1 || exit 43
test "$(grep -c '^restore-live-new$' "$TRACE")" -eq \
  {expected_contract_restores} || exit 44
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


@pytest.mark.parametrize(
    "failure_code",
    ("governance-recheck", "governance-health-final", "governance-date-final"),
)
def test_forward_recovery_gate_failure_refences_without_cleanup_or_start(
    tmp_path: Path,
    failure_code: str,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable forward gate failure test")
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
FAILURE_CODE={failure_code!r}
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
UNSAFE_CLEANUP="$TEST_ROOT/unsafe-cleanup"
UNSAFE_START="$TEST_ROOT/unsafe-start"
V2_RECOVERY_STEP=not-started
RESTORED_RUNTIME_FAILURE_CODE=
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR"
printf '%s\n' probiga.database-writer-restore.v1 \
  "release=$GUARDED_SHA" \
  main_unit=loaded,active,enabled \
  scheduler_unit=loaded,active,enabled \
  ai_service_unit=loaded,inactive,static \
  ai_timer_unit=loaded,inactive,disabled \
  > "$ACTIVATION_UNIT_SNAPSHOT_STATE"
printf 'new\n' > "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
printf 'restoring-new-no-receipt\n' > "$PHASE_STATE"
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
controlled_guard_assert_directory() {{ test -d "$DATABASE_WRITER_GUARD_DIR"; }}
controlled_guard_assert_file() {{ test -f "$1"; }}
controlled_guard_assert_state_record() {{ return 0; }}
controlled_guard_assert_restore_file() {{ test -f "$DATABASE_WRITER_RESTORE_FILE"; }}
controlled_guard_assert_marker() {{ test -f "$DATABASE_WRITER_GUARD_FILE"; }}
controlled_guard_recreate_file() {{ return 90; }}
controlled_guard_install_dropins() {{ printf 'install-fence\n' >> "$TRACE"; }}
systemctl() {{ test "$1" = daemon-reload || return 1; }}
controlled_guard_force_all_writers_fenced() {{ printf 'fence\n' >> "$TRACE"; }}
controlled_guard_assert_boundary() {{ test -f "$DATABASE_WRITER_GUARD_FILE"; }}
controlled_guard_governance_contract_snapshot() {{
  test "$1:$2:$3" = \
    "verify:$GUARDED_SHA:$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || return 1
  printf 'verify-governance\n' >> "$TRACE"
}}
activation_snapshot_restore_new_set() {{ printf 'restore-new\n' >> "$TRACE"; }}
activation_snapshot_assert_new_set() {{ printf 'assert-new\n' >> "$TRACE"; }}
controlled_guard_verify_restored_runtime() {{
  test "$3" = "$GUARDED_SHA" || return 1
  case "$6" in
    full)
      test "$7" = recover-input-readiness || return 1
      RESTORED_RUNTIME_FAILURE_CODE="$FAILURE_CODE"
      printf 'full-gate-%s\n' "$FAILURE_CODE" >> "$TRACE"
      return 1
      ;;
    rollback-only)
      : > "$UNSAFE_START"
      return 0
      ;;
    *) return 1 ;;
  esac
}}
controlled_guard_cleanup() {{ : > "$UNSAFE_CLEANUP"; return 0; }}
controlled_guard_refence_after_restore_failure() {{
  test -f "$DATABASE_WRITER_GUARD_FILE" || return 1
  printf 'refence\n' >> "$TRACE"
}}
{recovery}
if controlled_v2_forward_preserve_no_receipt_recovery; then exit 30; fi
test "$V2_RECOVERY_STEP" = \
  "forward-verify-gates-fenced-$FAILURE_CODE" || exit 31
test -f "$DATABASE_WRITER_GUARD_FILE" || exit 32
test -f "$DATABASE_WRITER_RESTORE_FILE" || exit 33
test -d "$ACTIVATION_UNIT_SNAPSHOT_DIR" || exit 34
test "$(<"$PHASE_STATE")" = restoring-new-no-receipt || exit 35
test ! -e "$UNSAFE_CLEANUP" || exit 36
test ! -e "$UNSAFE_START" || exit 37
test "$(grep -c '^refence$' "$TRACE")" -eq 1 || exit 38
test "$(grep -n -E 'full-gate-|refence' "$TRACE" | cut -d: -f2 | \
  paste -sd, -)" = "full-gate-$FAILURE_CODE,refence" || exit 39
"""
    harness_path = tmp_path / f"forward-gate-failure-{failure_code}.sh"
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


def test_final_authoritative_date_change_fails_with_bounded_code(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable final date test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    python_executable = Path(shutil.which("python") or sys.executable).as_posix()
    cutover_parser = _function(
        "controlled_guard_parse_governance_cutover_result",
        bodies["controlled_guard_parse_governance_cutover_result"],
    ).replace('/usr/bin/python3.14 -I -', '"$TEST_PYTHON" -I -')
    verifier_start = source.index("controlled_guard_verify_restored_runtime() {")
    quality_start = source.index(
        "  RESTORED_RUNTIME_FAILURE_CODE=premarket-task-ensure", verifier_start
    )
    date_start = source.index(
        '  if [ "$input_readiness_mode" = recover-input-readiness ]; then',
        quality_start,
    )
    date_end = source.index(
        '  RESTORED_RUNTIME_FAILURE_CODE=""', date_start
    )
    date_probe = source[date_start:date_end]
    root = tmp_path.as_posix()
    harness = f"""
set -u
TEST_ROOT={root!r}
TEST_PYTHON={python_executable!r}
ACTIVATION_UNIT_SNAPSHOT_DIR="$TEST_ROOT/transaction"
CONTROLLED_RECOVERY_CUTOVER_RESERVE_SECONDS=10800
input_readiness_mode=recover-input-readiness
service_user=probiga
code_root="$TEST_ROOT/code"
python_path=/guarded/python
governance_trade_date=2026-08-21
cutover_probe_code=embedded-recovery-owned-probe
governance_result_file=
governance_result_status=0
RESTORED_RUNTIME_FAILURE_CODE=premarket-task-ensure
guarded_command_prefix=(/usr/bin/env -i)
mkdir -p "$ACTIVATION_UNIT_SNAPSHOT_DIR" "$code_root"
controlled_guard_assert_file() {{
  test "$2" = 600 || return 1
  test -f "$1"
}}
controlled_guard_capture_service_gate_with_deadline() {{
  test "$1:$3" = "$service_user:$code_root" || return 1
  printf '%s\n' \
    '{{"trade_date":"2026-08-22","sample_epoch":1782000000,"next_cutoff_epoch":1782020000,"safe_before_epoch":1782009200,"reserve_seconds":10800}}' \
    > "$2"
  return 0
}}
{cutover_parser}
run_final_date_probe() {{
{date_probe}
  return 0
}}
if run_final_date_probe; then exit 30; fi
test "$RESTORED_RUNTIME_FAILURE_CODE" = governance-date-final || exit 31
test -z "$(find "$ACTIVATION_UNIT_SNAPSHOT_DIR" -maxdepth 1 \
  -name '.governance-date-final.*' -print -quit)" || exit 32
"""
    harness_path = tmp_path / "final-authoritative-date-change.sh"
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


def test_governance_cutover_probe_is_embedded_and_compiles() -> None:
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    probe = bodies["controlled_guard_governance_cutover_probe_code"]
    match = re.search(r"<<'PY'\n(?P<code>.*?)\nPY", probe, re.DOTALL)
    assert match is not None
    code = match.group("code")
    compile(code, "<controlled-recovery-cutover-probe>", "exec")
    assert "authoritative_closed_trade_date" in code
    assert "create_tool_engine" in code
    assert "check_governance_cutover_window.py" not in source


@pytest.mark.parametrize(
    "fault",
    ("stale-result", "pre-cleanup", "cleanup-critical", "pre-start", "safe"),
)
def test_forward_cutover_deadline_tail_refences_before_unsafe_start(
    tmp_path: Path,
    fault: str,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable cutover tail test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    recovery_start = source.index(
        "controlled_v2_forward_preserve_no_receipt_recovery() {"
    )
    tail_start = source.index(
        '  governance_trade_date="$RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE"',
        recovery_start,
    )
    tail_end = source.index(
        "  V2_RECOVERY_STEP=forward-commit-phase", tail_start
    )
    cutover_tail = source[tail_start:tail_end]
    root = tmp_path.as_posix()
    harness = f"""
set -u
TEST_ROOT={root!r}
FAULT={fault!r}
GUARDED_SHA={'a' * 40}
guarded_sha="$GUARDED_SHA"
CUTOVER_DEADLINE=9999999999
DATABASE_WRITER_GUARD_FILE="$TEST_ROOT/guard"
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$TEST_ROOT/new.json"
TRACE="$TEST_ROOT/trace"
main_record=loaded,active,enabled
scheduler_record=loaded,active,enabled
ai_service_record=loaded,inactive,static
ai_timer_record=loaded,inactive,disabled
forward_main_record=loaded,active,enabled
forward_scheduler_record=loaded,active,enabled
RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE=2026-08-21
RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH="$CUTOVER_DEADLINE"
V2_RECOVERY_STEP=not-started
DEADLINE_CALLS=0
mkdir -p "$TEST_ROOT"
printf 'guard\n' > "$DATABASE_WRITER_GUARD_FILE"
printf 'new\n' > "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
: > "$TRACE"
if [ "$FAULT" = stale-result ]; then
  RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH=
fi
controlled_guard_assert_activation_deadline() {{
  DEADLINE_CALLS=$((DEADLINE_CALLS + 1))
  test "$1" = "$CUTOVER_DEADLINE" || return 1
  if [ "$FAULT" = pre-cleanup ] && [ "$DEADLINE_CALLS" -ge 2 ]; then
    return 1
  fi
}}
controlled_guard_install_recovery_cutover_dropins() {{
  test "$1" = "$CUTOVER_DEADLINE" || return 1
  printf 'install-cutover\n' >> "$TRACE"
}}
controlled_guard_assert_boundary() {{
  test -f "$DATABASE_WRITER_GUARD_FILE"
}}
controlled_guard_cleanup() {{
  test "$6" = "$CUTOVER_DEADLINE" || return 1
  printf 'cleanup\n' >> "$TRACE"
  if [ "$FAULT" = cleanup-critical ]; then return 1; fi
  rm -f "$DATABASE_WRITER_GUARD_FILE"
}}
controlled_guard_verify_restored_runtime() {{
  test "$6:$7:$8" = "rollback-only:strict:$CUTOVER_DEADLINE" || return 1
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || return 1
  if [ "$FAULT" = pre-start ]; then return 1; fi
  printf 'start\n' >> "$TRACE"
}}
controlled_guard_governance_contract_snapshot() {{
  test "$1:$2:$3" = \
    "verify:$GUARDED_SHA:$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
}}
activation_snapshot_assert_pending_receipt_absent() {{ return 0; }}
controlled_guard_remove_recovery_cutover_dropins() {{
  test "$1" = "$CUTOVER_DEADLINE" || return 1
  printf 'remove-cutover\n' >> "$TRACE"
}}
controlled_guard_refence_after_restore_failure() {{
  printf 'guard\n' > "$DATABASE_WRITER_GUARD_FILE"
  printf 'refence\n' >> "$TRACE"
}}
run_cutover_tail() {{
{cutover_tail}
  return 0
}}
if [ "$FAULT" = safe ]; then
  run_cutover_tail || exit 30
  test ! -e "$DATABASE_WRITER_GUARD_FILE" || exit 31
  test "$(grep -c '^start$' "$TRACE")" -eq 1 || exit 32
  test "$(grep -c '^remove-cutover$' "$TRACE")" -eq 1 || exit 33
  ! grep -q '^refence$' "$TRACE" || exit 34
else
  if run_cutover_tail; then exit 35; fi
  test -f "$DATABASE_WRITER_GUARD_FILE" || exit 36
  test "$(grep -c '^refence$' "$TRACE")" -eq 1 || exit 37
  ! grep -q '^start$' "$TRACE" || exit 38
  if [ "$FAULT" = stale-result ] || [ "$FAULT" = pre-cleanup ]; then
    ! grep -q '^cleanup$' "$TRACE" || exit 39
  fi
fi
"""
    harness_path = tmp_path / f"forward-cutover-tail-{fault}.sh"
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
  GOVERNANCE_CONTRACT_FAILURE_CODE=runner
  return 1
}}
controlled_guard_refence_after_restore_failure() {{
  test "$(<"$PHASE_STATE")" = restoring-new-no-receipt || return 1
  : > "$DATABASE_WRITER_GUARD_FILE"
  printf 'refenced\n' >> "$TRACE"
}}
{recovery}
if controlled_v2_forward_preserve_no_receipt_recovery; then exit 30; fi
test "$V2_RECOVERY_STEP" = forward-governance-restore-runner || exit 31
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
PROBIGA_JOB_LOG_ROOT="$TEST_ROOT/jobs"
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
    database = source.index("GOVERNANCE_TASK_OLD_SOURCE=", gate_start)
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
run_database_boundary_bootstrap() {{ test "$1" = verify; }}
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
run_database_boundary_bootstrap() {{ test "$1" = prepare; }}
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


def test_nginx_static_identity_retries_until_exact_release_bytes(
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
STATIC_RELEASE_LINK="$TEST_ROOT/current"
COUNT_FILE="$TEST_ROOT/app-count"
mkdir -p "$CHECKOUT/server/static/js" "$CHECKOUT/server/static/css"
printf js > "$CHECKOUT/server/static/js/app.js"
printf css > "$CHECKOUT/server/static/css/style.css"
printf 0 > "$COUNT_FILE"
test() {{
  if [ "$#" -eq 2 ] && [ "$1" = -L ]; then return 0; fi
  builtin test "$@"
}}
readlink() {{ printf '%s\n' "$CHECKOUT"; }}
sleep() {{ :; }}
curl() {{
  case "${{@: -1}}" in
    */js/app.js)
      count="$(cat "$COUNT_FILE")"
      count=$((count + 1))
      printf '%s' "$count" > "$COUNT_FILE"
      if [ "$count" -lt 3 ]; then printf stale; else printf js; fi
      ;;
    */css/style.css) printf css ;;
    *) return 90 ;;
  esac
}}
{helper}
assert_nginx_static_matches_checkout "$CHECKOUT"
test "$(cat "$COUNT_FILE")" = 3
"""
    harness_path = tmp_path / "static-identity-retry-harness.sh"
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


def test_detached_failure_handler_preserves_only_sanitized_checkpoint(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable transport regression")
    source = (ROOT / "deploy" / "production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    detach = _function(
        "detach_failure_handler_from_transport",
        bodies["detach_failure_handler_from_transport"],
    )
    emit = _function(
        "emit_deploy_failure_checkpoint",
        bodies["emit_deploy_failure_checkpoint"],
    )
    output = tmp_path / "failure-checkpoint"
    audit_sha = "c" * 64
    expected_sha = "a" * 40
    previous_sha = "b" * 40
    harness = f"""#!/usr/bin/env bash
set -Eeuo pipefail
EXPECTED_SHA={expected_sha}
PREVIOUS_SHA={previous_sha}
exec 6>"$1"
{detach}
{emit}
detach_failure_handler_from_transport
emit_deploy_failure_checkpoint cutover prepare_strategy_governance_database_schema 13342 1 {audit_sha}
"""
    harness_path = tmp_path / "failure-checkpoint-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path), str(output)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout == ""
    assert completed.stderr == ""
    assert output.read_text(encoding="utf-8") == (
        "deploy_failure_checkpoint "
        "schema=probiga.production-deploy-failure-audit.v1 "
        "phase=cutover "
        "cutover_step=prepare_strategy_governance_database_schema "
        "line=13342 status=1 "
        f"expected_sha={expected_sha} previous_sha={previous_sha} "
        f"audit_sha256={audit_sha}\n"
    )

    output.unlink()
    unsafe_harness = harness.replace(
        "emit_deploy_failure_checkpoint cutover prepare_strategy_governance_database_schema 13342 1",
        "emit_deploy_failure_checkpoint $'bad\\nphase' $'bad\\nstep' bad bad",
    )
    harness_path.write_text(unsafe_harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path), str(output)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert output.read_text(encoding="utf-8") == (
        "deploy_failure_checkpoint "
        "schema=probiga.production-deploy-failure-audit.v1 "
        "phase=unknown cutover_step=unknown line=0 status=255 "
        f"expected_sha={expected_sha} previous_sha={previous_sha} "
        f"audit_sha256={audit_sha}\n"
    )


def test_failure_audit_is_canonical_hash_addressed_json(tmp_path: Path) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable audit regression")
    source = (ROOT / "deploy" / "production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    persist = _function(
        "persist_deploy_failure_audit",
        bodies["persist_deploy_failure_audit"],
    )
    expected_sha = "a" * 40
    previous_sha = "b" * 40
    receipt_id = f"{expected_sha}-20260903T151318Z"
    harness = f"""#!/usr/bin/env bash
set -Eeuo pipefail
DEPLOY_FAILURE_AUDIT_DIR="$1/audit"
EXPECTED_SHA={expected_sha}
PREVIOUS_SHA={previous_sha}
DEPLOY_STARTED_AT=2026-09-03T07:13:18Z
RECEIPT_ID={receipt_id}
install() {{ mkdir -p "${{@: -1}}"; }}
chown() {{ return 0; }}
sync() {{ return 0; }}
stat() {{
  case "$*" in
    *%U:%G*) printf 'root:root\n' ;;
    *%a*) if [ -d "${{@: -1}}" ]; then printf '700\n'; else printf '444\n'; fi ;;
    *) command stat "$@" ;;
  esac
}}
{persist}
persist_deploy_failure_audit cutover invalid/step 13342 1
"""
    harness_path = tmp_path / "failure-audit-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path), tmp_path.as_posix()],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    audit_sha = completed.stdout.strip()
    assert re.fullmatch(r"[0-9a-f]{64}", audit_sha)
    audit_files = list((tmp_path / "audit").glob("*.json"))
    assert len(audit_files) == 1
    audit_file = audit_files[0]
    assert audit_file.name == f"{receipt_id}-failure-{audit_sha}.json"
    payload_bytes = audit_file.read_bytes()
    assert hashlib.sha256(payload_bytes).hexdigest() == audit_sha
    payload = json.loads(payload_bytes)
    assert payload == {
        "schema_version": "probiga.production-deploy-failure-audit.v1",
        "phase": "cutover",
        "cutover_step": "unknown",
        "cutover_started": True,
        "line": 13342,
        "status": 1,
        "expected_sha": expected_sha,
        "previous_sha": previous_sha,
        "started_at": "2026-09-03T07:13:18Z",
        "recorded_at": payload["recorded_at"],
    }
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z",
        payload["recorded_at"],
    )


@pytest.mark.parametrize(
    ("cutover_started", "committed_phase", "audit_write_fails", "succeeded"),
    (
        (0, "", False, False),
        (0, "", True, False),
        (1, "new-runtime-verified", False, False),
        (1, "old-runtime-verified", False, False),
        (1, "", False, True),
    ),
)
def test_rollback_seals_original_failure_before_recovery_or_early_exit(
    tmp_path: Path,
    cutover_started: int,
    committed_phase: str,
    audit_write_fails: bool,
    succeeded: bool,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable rollback regression")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    detach = _function(
        "detach_failure_handler_from_transport",
        bodies["detach_failure_handler_from_transport"],
    )
    rollback = _function("rollback", bodies["rollback"])
    expected_sha = "a" * 40
    previous_sha = "b" * 40
    audit_sha = "c" * 64
    harness = f"""#!/usr/bin/env bash
set -u
TEST_ROOT="$1"
TRACE="$TEST_ROOT/trace"
: > "$TRACE"
EXPECTED_SHA={expected_sha}
PREVIOUS_SHA={previous_sha}
PREVIOUS_CODE_ROOT="$TEST_ROOT/previous"
PREPARED_CODE_ROOT="$TEST_ROOT/missing-prepared"
DATABASE_WRITER_GUARD_FILE="$TEST_ROOT/missing-guard"
DATABASE_WRITER_RESTORE_FILE="$TEST_ROOT/missing-restore"
MAIN_SERVICE=probiga
PREVIOUS_INPUT_LOCK_SHA256=input
PREVIOUS_RESOLVED_FREEZE_SHA256=freeze
PREVIOUS_ADATA_SHA=adata
PREVIOUS_ADATA_TREE_SHA256=tree
PRE_CUTOVER_SCHEDULER_STOPPED=0
SCHEDULER_UNIT_PRESENT=1
DATABASE_FORWARD_MIGRATION_STARTED=0
DEFERRED_DB_CUTOVER_STARTED=1
DEPLOY_SUCCEEDED={int(succeeded)}
CUTOVER_STARTED={cutover_started}
CUTOVER_STEP=original_failure_step
COMMITTED_PHASE={committed_phase!r}
AUDIT_WRITE_FAILS={int(audit_write_fails)}
exec 6>"$TEST_ROOT/checkpoint"
persist_deploy_failure_audit() {{
  printf 'persist:%s\n' "$*" >> "$TRACE"
  if [ "$AUDIT_WRITE_FAILS" -eq 1 ]; then return 1; fi
  printf '%s\n' {audit_sha}
}}
emit_deploy_failure_checkpoint() {{ printf 'emit:%s\n' "$*" >> "$TRACE"; }}
rollback_deferred_database_release() {{ printf 'deferred:%s\n' "$1" >> "$TRACE"; }}
activation_snapshot_committed_phase_for_release() {{ printf '%s\n' "$COMMITTED_PHASE"; }}
write_receipt() {{ printf 'receipt:%s\n' "$*" >> "$TRACE"; }}
git() {{ printf '%s\n' "$PREVIOUS_SHA"; }}
systemctl() {{ return 0; }}
curl() {{ return 0; }}
{detach}
{rollback}
set +e
(
  DEPLOY_MAIN_BASHPID="$BASHPID"
  rollback 7 123
)
status=$?
set -e
printf '%s\n' "$status" > "$TEST_ROOT/status"
"""
    harness_path = tmp_path / "rollback-failure-audit-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path), tmp_path.as_posix()],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert (tmp_path / "status").read_text(encoding="utf-8") == "7\n"
    trace = (tmp_path / "trace").read_text(encoding="utf-8").splitlines()
    if succeeded:
        assert trace == []
        return
    phase = "cutover" if cutover_started else "preparation"
    expected_audit = "unavailable" if audit_write_fails else audit_sha
    assert trace[:2] == [
        f"persist:{phase} original_failure_step 123 7",
        f"emit:{phase} original_failure_step 123 7 {expected_audit}",
    ]
    if committed_phase:
        assert trace[2:] == ["deferred:7"]
    else:
        assert trace[2:] == [
            "deferred:7",
            f"receipt:PREPARATION_FAILED {previous_sha}",
        ]


def test_controlled_v2_recovery_does_not_emit_deploy_failure_checkpoint() -> None:
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    for name in (
        "controlled_v2_forward_finalize_recovery",
        "prepared_v2_rollback_release_database_guard",
    ):
        body = bodies[name]
        assert "persist_deploy_failure_audit" not in body
        assert "emit_deploy_failure_checkpoint" not in body


@pytest.mark.parametrize(
    ("signal_name", "expected_status"),
    (("TERM", 143), ("INT", 130), ("HUP", 129)),
)
def test_rollback_signal_traps_preserve_a_nonzero_source_line(
    tmp_path: Path,
    signal_name: str,
    expected_status: int,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable signal regression")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    trap_match = re.search(
        rf"(?m)^trap 'rollback {expected_status} \"\$LINENO\"' {signal_name}$",
        source,
    )
    assert trap_match is not None
    result = tmp_path / f"{signal_name.lower()}-result"
    harness = f"""#!/usr/bin/env bash
set -u
RESULT="$1"
rollback() {{
  printf '%s %s\n' "$1" "$2" > "$RESULT"
  exit "$1"
}}
{trap_match.group(0)}
kill -{signal_name} "$BASHPID"
exit 99
"""
    harness_path = tmp_path / f"{signal_name.lower()}-trap-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path), str(result)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == expected_status
    status_text, line_text = result.read_text(encoding="utf-8").split()
    assert int(status_text) == expected_status
    assert int(line_text) > 0


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
QMT_ANNOUNCEMENT_TASK_OLD_SOURCE=
QMT_ANNOUNCEMENT_TASK_NEW_SOURCE=
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


def test_captured_database_gate_preserves_blocked_exit_status(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable capture gate test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    capture = _function(
        "controlled_guard_capture_service_gate_with_deadline",
        _shell_function_bodies(source)[
            "controlled_guard_capture_service_gate_with_deadline"
        ],
    ).replace(
        'test -x /usr/bin/sudo || return 1',
        "true",
    ).replace(
        '/usr/bin/sudo -u "$service_user" /usr/bin/timeout',
        "/usr/bin/timeout",
    )
    output_file = (tmp_path / "captured-output.json").as_posix()
    working_directory = tmp_path.as_posix()
    harness = f"""
set -Eeuo pipefail
CONTROLLED_DATABASE_GATE_KILL_AFTER=1s
CONTROLLED_DATABASE_GATE_TIMEOUT=5s
OUTPUT_FILE={output_file!r}
WORKING_DIRECTORY={working_directory!r}
: > "$OUTPUT_FILE"
chmod 600 "$OUTPUT_FILE"
controlled_guard_assert_file() {{ test -f "$1" && test ! -L "$1"; }}
{capture}
if controlled_guard_capture_service_gate_with_deadline test-user \
    "$OUTPUT_FILE" "$WORKING_DIRECTORY" /usr/bin/bash -c \
    'printf "%s\\n" blocked-result; exit 2'; then
  status=0
else
  status=$?
fi
test "$status" -eq 2 || exit 20
test "$(<"$OUTPUT_FILE")" = blocked-result || exit 21
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_governance_recovery_parsers_bind_disposition_identity_and_fields(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable governance parser test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    python_executable = Path(shutil.which("python") or sys.executable).as_posix()
    parsers = "".join(
        _function(name, bodies[name])
        for name in (
            "controlled_guard_parse_governance_health_result",
            "controlled_guard_parse_governance_cutover_result",
            "controlled_guard_parse_governance_runner_result",
        )
    ).replace('/usr/bin/python3.14 -I -', '"$TEST_PYTHON" -I -')
    result_file = tmp_path / "governance-result.json"
    harness = f"""
set -u
TEST_PYTHON={python_executable!r}
RESULT_FILE={result_file.as_posix()!r}
CONTROLLED_RECOVERY_CUTOVER_RESERVE_SECONDS=10800
controlled_guard_assert_file() {{ test -f "$1" && test ! -L "$1"; }}
{parsers}
parser="$1"
shift
"$parser" "$RESULT_FILE" "$@"
"""
    harness_path = tmp_path / "governance-parser-harness.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")

    def run_parser(
        parser: str,
        payload: object,
        *arguments: str,
    ) -> subprocess.CompletedProcess[str]:
        content = payload if isinstance(payload, str) else json.dumps(payload)
        result_file.write_text(content, encoding="utf-8")
        return subprocess.run(
            [bash, str(harness_path), parser, *arguments],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
        )

    def clone(payload: dict[str, object]) -> dict[str, object]:
        return json.loads(json.dumps(payload))

    expected_sha = "a" * 40
    trade_date = "2026-08-21"
    adapter = {
        "registry_sealed": True,
        "registry_seal_hash": "b" * 64,
        "registry_integrity_ready": True,
        "adapter_configured": False,
        "candidate_execution_ready": False,
        "funding_pipeline_ready": False,
        "governance_paper_execution_ready": False,
        "production_execution_ready": False,
        "real_order_submission_enabled": False,
        "automatic_real_order_submission": False,
        "adapter_count": 0,
    }
    from tools.check_strategy_governance_health import (
        GOVERNANCE_HEALTH_CONTRACT_VERSION,
        governance_health_required_check_names,
    )
    from tools.prepare_strategy_governance_schema import (
        _final_v3_trigger_contracts,
        _non_v3_trigger_contracts,
        _v2_release_trigger_contract,
    )
    from tools.qmt_operations_task_contract import TASKS as QMT_OPERATIONS_TASKS

    qmt_operation_fields = (
        "task_name",
        "task_type",
        "group_name",
        "script_path",
        "script_args",
        "cron_time",
        "interval_minutes",
        "date_param",
        "enabled",
    )
    qmt_operations_expected = {
        str(task["task_type"]): {
            key: task[key] for key in qmt_operation_fields
        }
        for task in QMT_OPERATIONS_TASKS
    }
    qmt_operations_rows = [
        {"id": 300 + index, **payload}
        for index, payload in enumerate(qmt_operations_expected.values(), 1)
    ]
    full_trigger_names = sorted({
        *_v2_release_trigger_contract()[0],
        *_final_v3_trigger_contracts(),
        *_non_v3_trigger_contracts(),
    })

    def checks_for(disposition: str) -> list[dict[str, object]]:
        waived_names = (
            {
                "authoritative_date_has_one_canonical_revision",
                "expected_build_date_run",
            }
            if disposition == "input_not_ready"
            else set()
        )
        details: dict[str, dict[str, object]] = {
            "qmt_operations_scheduler_tasks_unique": {
                "row_count": len(qmt_operations_rows),
                "expected_row_count": len(qmt_operations_rows),
                "match_counts": {
                    key: 1 for key in qmt_operations_expected
                },
                "rows": qmt_operations_rows,
            },
            "qmt_operations_scheduler_tasks_contract": {
                "actual": deepcopy(qmt_operations_expected),
                "expected": deepcopy(qmt_operations_expected),
            },
            "qmt_announcement_scheduler_task_unique": {
                "row_count": 1,
                "rows": [{"id": 22, "task_type": "qmt_announcement_pit"}],
            },
            "qmt_announcement_scheduler_task_contract": {
                "actual": {
                    "id": 22,
                    "task_name": "国金QMT全市场公告PIT同步",
                    "task_type": "qmt_announcement_pit",
                    "group_name": "strategy_governance",
                    "script_path": "tools/sync_qmt_announcement_pit.py",
                    "script_args": (
                        "--window-days 30 --overlap-days 3 --batch-size 100 "
                        "--fallback-provider cninfo "
                        "--checkpoint-dir /var/lib/probiga/"
                        "qmt-announcement-checkpoints"
                    ),
                    "cron_time": "18:20",
                    "interval_minutes": 0,
                    "date_param": "",
                    "enabled": 1,
                },
                "expected": {
                    "task_name": "国金QMT全市场公告PIT同步",
                    "task_type": "qmt_announcement_pit",
                    "group_name": "strategy_governance",
                    "script_path": "tools/sync_qmt_announcement_pit.py",
                    "script_args": (
                        "--window-days 30 --overlap-days 3 --batch-size 100 "
                        "--fallback-provider cninfo "
                        "--checkpoint-dir /var/lib/probiga/"
                        "qmt-announcement-checkpoints"
                    ),
                    "cron_time": "18:20",
                    "interval_minutes": 0,
                    "date_param": "",
                    "enabled": 1,
                },
                "pipeline_order": {
                    "qmt_announcement_minutes": 1100,
                    "analysis_minutes": 1130,
                    "governance_minutes": 1355,
                },
            },
            "supporting_release_trigger_inventory_exact": {
                "required_count": 81,
                "optional_count": 0,
                "observed_count": 81,
                "expected_trigger_count": 81,
                "owner_counts": {
                    "market_field_capture": 5,
                    "pit_facts": 6,
                    "qmt_attestation": 6,
                    "qmt_history_coverage": 4,
                    "qmt_membership": 6,
                    "qmt_reference": 10,
                    "scheduler_task_history": 2,
                    "schema_recovery_evidence": 2,
                    "strategy_governance": 40,
                },
                "expected_owner_counts": {
                    "market_field_capture": 5,
                    "pit_facts": 6,
                    "qmt_attestation": 6,
                    "qmt_history_coverage": 4,
                    "qmt_membership": 6,
                    "qmt_reference": 10,
                    "scheduler_task_history": 2,
                    "schema_recovery_evidence": 2,
                    "strategy_governance": 40,
                },
                "source_contract_hash": (
                    "076a2b84c15b9dbb54901c63f980c2f85ab17f7652d9334ab661d89ad990d0bc"
                ),
                "database_triggers_required": True,
                "metadata_frozen": True,
                "definer": "probiga_migrator@127.0.0.1",
            },
            "full_database_trigger_inventory_exact": {
                "expected_count": 142,
                "observed_count": 142,
                "v2_count": 41,
                "managed_count": 101,
                "optional_v4_count": 0,
                "expected_names": full_trigger_names,
                "nameset_sha256": (
                    "a1c6aa0e9f241a419bbb87c101fbac7d8dd1404aa9f95493afbd604370644a87"
                ),
                "base_nameset_sha256": (
                    "a1c6aa0e9f241a419bbb87c101fbac7d8dd1404aa9f95493afbd604370644a87"
                ),
                "v2_source_contract_sha256": (
                    "5167f36ee731c2544be73590e4e00716f334c58b5746f776e610254904cf8883"
                ),
                "managed_source_contract_sha256": (
                    "7e42c91e534dd3d61d212f0c16fa7297c29b8f4756812de2e072874179537423"
                ),
                "observed_metadata_sha256": "8" * 64,
                "managed_contract": {
                    "required_count": 101,
                    "optional_count": 0,
                    "observed_count": 101,
                    "definer": "probiga_migrator@127.0.0.1",
                    "metadata_frozen": True,
                    "legacy_rehome_names": [],
                },
                "metadata_frozen": True,
                "read_only": True,
            },
            "qmt_reference_physical_schema_and_seal": {
                "contract_key": "qmt_reference_truth_v2",
                "contract_hash": (
                    "64982c16c517f7e5c0e6ee9b88b1bf33df98f9aebf66440eedc916eae76f3dd5"
                ),
                "table_count": 5,
                "trigger_count": 10,
                "expected_trigger_count": 10,
                "physical_schema_verified": True,
                "physical_seal_verified": True,
            },
            "qmt_history_coverage_physical_schema_and_seal": {
                "database": "probiga",
                "table_count": 2,
                "foreign_key_count": 3,
                "trigger_count": 4,
                "expected_trigger_count": 4,
                "runtime_ddl_required": False,
                "physical_schema_verified": True,
                "physical_seal_verified": True,
            },
            "qmt_history_capability_matrix_fail_closed": {
                "schema": "probiga.qmt-history-capability-matrix.v1",
                "status": "HEALTHY",
                "evidence_healthy": True,
                "dataset_count": 19,
                "strategy_eligible_dataset_count": 0,
                "strategy_ineligible_dataset_count": 19,
                "required_scope_dataset_count": 0,
                "fail_closed_verified": True,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
                "errors": [],
                "datasets": [
                    {"status": "UNAVAILABLE", "strategy_eligible": False}
                    for _index in range(19)
                ],
            },
            "qmt_windows_edge_executor_and_last_success": {
                "status": "AVAILABLE",
                "strategy_eligible": True,
                "executor_role": "qmt_windows_edge",
                "expected_build_sha": expected_sha,
                "expected_poll_seconds": 60,
                "role_row_count": 1,
                "fresh_row_count": 1,
                "future_row_count": 0,
                "current": {
                    "instance_id": "win-edge-9191",
                    "mode": "standalone",
                    "host_name": "win-edge",
                    "pid": 9191,
                    "build_sha": expected_sha,
                    "executor_role": "qmt_windows_edge",
                    "heartbeat_age_seconds": 5,
                    "poll_seconds": 60,
                    "max_concurrent_tasks": 2,
                },
                "required_task_types": [
                    "qmt_local_gap_repair_execute",
                    "qmt_local_history_2024",
                    "qmt_reference_incremental",
                ],
                "task_count": 3,
                "last_success_count": 3,
                "success_max_age_seconds": 345600,
                "tasks": {
                    task_type: {
                        "task_id": next(
                            row["id"] for row in qmt_operations_rows
                            if row["task_type"] == task_type
                        ),
                        "last_run_status": "success",
                        "last_success_age_seconds": 3600,
                        "last_success_host": "win-edge",
                        "last_success_instance_id": "win-edge-8181",
                    }
                    for task_type in (
                        "qmt_local_gap_repair_execute",
                        "qmt_local_history_2024",
                        "qmt_reference_incremental",
                    )
                },
                "errors": [],
            },
            "qmt_windows_edge_release_bootstrap": {
                "status": "AVAILABLE",
                "strategy_eligible": True,
                "expected_build_sha": expected_sha,
                "expected_poll_seconds": 60,
                "receipt_count": 1,
                "immutable_reference_verified": True,
                "identity": {
                    "current": {
                        "instance_id": "win-edge-9191",
                        "host_name": "win-edge",
                        "pid": 9191,
                        "build_sha": expected_sha,
                        "executor_role": "qmt_windows_edge",
                    }
                },
                "receipt": {
                    "build_sha": expected_sha,
                    "request_run_uid": f"qmt-edge-request-{expected_sha}",
                    "host_name": "win-edge",
                    "scheduler_instance_id": "win-edge-9191",
                    "catalog_batch_id": (
                        f"qmt_rel_{expected_sha}_20260825120000"
                    ),
                    "calendar_batch_id": (
                        f"qmt_rel_{expected_sha}_20260825120000"
                    ),
                    "receipt_hash": "9" * 64,
                },
                "errors": [],
            },
            "scheduler_task_history_physical_schema": {
                "table": "st_scheduled_task_history",
                "required_index_count": 3,
                "physical_contract_verified": True,
                "runtime_ddl_required": False,
                "read_only": True,
            },
            "pit_fact_physical_schema_exact": {
                "schema": "probiga.pit-fact-schema-health.v1",
                "status": "HEALTHY",
                "valid": True,
                "table_count": 3,
                "expected_table_count": 3,
                "trigger_count": 6,
                "expected_trigger_count": 6,
                "contract_hash": (
                    "c374e0ba62eb2e5b9bef802ce2bdd89fae0c63391d918e922ff21781707863ae"
                ),
                "physical_schema_verified": True,
            },
            "latest_qmt_announcement_full_market_batch": {
                "status": "COMPLETE",
                "reason_code": (
                    "QMT_ANNOUNCEMENT_EXISTING_FULL_MARKET_COMPLETE"
                ),
                "trade_date": trade_date,
                "source": "qmt.announcement",
                "funding_eligible": True,
                "database_writes": False,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
                "catalog_member_count": 5288,
                "coverage_row_count": 5288,
                "batch_root_hash": "e" * 64,
            },
            "strategy_funding_schema_exact": {
                "table_count": 2,
                "tables": {
                    "st_strategy_funding_daily_fact": {
                        "column_count": 29,
                        "index_count": 9,
                        "foreign_key_count": 3,
                        "check_count": 7,
                    },
                    "st_strategy_funding_checkpoint": {
                        "column_count": 46,
                        "index_count": 12,
                        "foreign_key_count": 7,
                        "check_count": 13,
                    },
                },
                "trigger_count": 4,
                    "contract_hash": (
                        "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde"
                    ),
                "checkpoint_target_average_bytes": 8192,
                "checkpoint_total_target_bytes": 8388608,
                "checkpoint_total_hard_bytes": 16777216,
                "batch_max_rows": 100,
                "batch_max_bytes": 4194304,
                "manifest_max_bytes": 1048576,
                "audit_max_bytes": 131072,
            },
            "strategy_metric_input_application_state_machine": {
                "trigger_count": 2,
                "expected_trigger_count": 2,
                "required_count": 2,
                "observed_count": 2,
                "database_triggers_required": True,
                "metadata_frozen": True,
                "definer": "probiga_migrator@127.0.0.1",
                    "contract_hash": (
                        "c217a42eb6c2a5f7bed592bb7c7e724499546f997061c4daad1db957317bdf28"
                    ),
                    "source_contract_hash": (
                        "5a1a19e0664c715ae0cac7cfa8dd87c47da1b63b1d2df869561cecf3c995f01f"
                    ),
                    "core_append_only_contract_hash": (
                        "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943"
                    ),
                    "core_metric_review_contract_hash": (
                        "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84"
                    ),
            },
            "governance_append_only_application_integrity": {
                "trigger_count": 38,
                "expected_trigger_count": 38,
                "total_governance_trigger_count": 40,
                "required_count": 38,
                "observed_count": 38,
                "database_triggers_required": True,
                "metadata_frozen": True,
                "definer": "probiga_migrator@127.0.0.1",
                    "contract_hash": (
                        "bf537f9ed5fb1d31195092ae6a24262511de6f45bf9addacefebc88e25b6b9d8"
                    ),
                    "source_contract_hash": (
                        "5a1a19e0664c715ae0cac7cfa8dd87c47da1b63b1d2df869561cecf3c995f01f"
                    ),
                    "core_contract_hash": (
                        "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943"
                    ),
                    "core_metric_review_contract_hash": (
                        "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84"
                    ),
                    "funding_contract_hash": (
                        "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde"
                    ),
            },
            "registry_lifecycle_projection_matches_immutable_events": {
                "invalid_count": 0,
                "registry_count": 2,
                "projected_count": 2,
                "projection_hash": "f" * 64,
            },
            "funding_checkpoint_manifest_partition_and_persistence": {
                    "invalid_count": 0,
                    "current_entity_count": 2,
                    "funding_ready_count": 1,
                    "checkpoint_count": 1,
                    "strategy_checkpoint_count": 1,
                    "combination_recipe_count": 0,
                    "ineligible_count": 1,
                    "daily_fact_count": 1,
                "checkpoint_storage_bytes": 1024,
                "fact_storage_bytes": 2048,
                "total_storage_bytes": 3072,
                "target_total_met": True,
                    "manifest_hash": "1" * 64,
                    "checkpoint_root_hash": "2" * 64,
                    "combination_recipe_root_hash": "3" * 64,
                    "ineligible_root_hash": "4" * 64,
            },
        }
        return [
            {
                "name": name,
                "passed": True,
                "waived": name in waived_names,
                "detail": deepcopy(details.get(name, {})),
            }
            for name in sorted(
                governance_health_required_check_names(disposition)
            )
        ]

    completed_health: dict[str, object] = {
        "contract_version": GOVERNANCE_HEALTH_CONTRACT_VERSION,
        "status": "PASS",
        "run_disposition": "completed",
        "automatic_real_order_submission": False,
        "expected": {
            "build_commit_sha": expected_sha,
            "trade_date": trade_date,
            "trade_date_source": "command_line_verified_against_calendar",
        },
        "checks": checks_for("completed"),
        "adapter_registry": adapter,
    }
    input_not_ready_health = clone(completed_health)
    input_not_ready_health["run_disposition"] = "input_not_ready"
    input_not_ready_health["checks"] = checks_for("input_not_ready")

    completed = run_parser(
        "controlled_guard_parse_governance_health_result",
        completed_health,
        expected_sha,
        "completed",
        trade_date,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout.strip() == trade_date
    fallback_health = clone(completed_health)
    fallback_detail = next(
        check
        for check in fallback_health["checks"]
        if check["name"] == "latest_qmt_announcement_full_market_batch"
    )["detail"]
    fallback_detail.update({
        "reason_code": "ANNOUNCEMENT_FALLBACK_EXISTING_FULL_MARKET_COMPLETE",
        "source": "cninfo.announcement",
        "primary_source": "qmt.announcement",
        "fallback_reason": (
            "QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE"
        ),
    })
    fallback = run_parser(
        "controlled_guard_parse_governance_health_result",
        fallback_health,
        expected_sha,
        "completed",
        trade_date,
    )
    assert fallback.returncode == 0, fallback.stdout + fallback.stderr
    assert fallback.stdout.strip() == trade_date
    allowed = run_parser(
        "controlled_guard_parse_governance_health_result",
        input_not_ready_health,
        expected_sha,
        "input_not_ready",
        trade_date,
    )
    assert allowed.returncode == 0, allowed.stdout + allowed.stderr
    assert allowed.stdout.strip() == trade_date
    authoritative_health = clone(completed_health)
    authoritative_health["expected"]["trade_date_source"] = (
        "authoritative_closed_trading_calendar_day"
    )
    authoritative = run_parser(
        "controlled_guard_parse_governance_health_result",
        authoritative_health,
        expected_sha,
        "completed",
    )
    assert (
        authoritative.returncode == 0
    ), authoritative.stdout + authoritative.stderr
    assert authoritative.stdout.strip() == trade_date

    scheduler_pid = 4321
    scheduler_host = socket.gethostname()
    heartbeat_health = clone(authoritative_health)
    heartbeat_health["checks"].append(
        {
            "name": "linux_standalone_scheduler_heartbeat_current",
            "passed": True,
            "waived": False,
            "detail": {
                "executor_role": "linux_standalone",
                "role_row_count": 2,
                "fresh_row_count": 1,
                "future_row_count": 0,
                "expected_host": scheduler_host,
                    "expected_pid": scheduler_pid,
                    "expected_build_sha": expected_sha,
                    "expected_poll_seconds": 60,
                "current": {
                    "instance_id": f"{scheduler_host}-{scheduler_pid}",
                    "mode": "standalone",
                    "host_name": scheduler_host,
                    "pid": scheduler_pid,
                    "build_sha": expected_sha,
                    "executor_role": "linux_standalone",
                    "heartbeat_age_seconds": 5,
                    "poll_seconds": 60,
                    "max_concurrent_tasks": 2,
                },
                "errors": [],
            },
        }
    )
    heartbeat = run_parser(
        "controlled_guard_parse_governance_health_result",
        heartbeat_health,
        expected_sha,
        "completed",
        "",
        str(scheduler_pid),
    )
    assert heartbeat.returncode == 0, heartbeat.stdout + heartbeat.stderr
    heartbeat_pid_drift = clone(heartbeat_health)
    next(
        check for check in heartbeat_pid_drift["checks"]
        if check["name"] == "linux_standalone_scheduler_heartbeat_current"
    )["detail"]["current"]["pid"] = 9999
    rejected_heartbeat = run_parser(
        "controlled_guard_parse_governance_health_result",
        heartbeat_pid_drift,
        expected_sha,
        "completed",
        "",
        str(scheduler_pid),
    )
    assert rejected_heartbeat.returncode != 0

    invalid_health_payloads: list[tuple[dict[str, object], str, str]] = []
    sha_drift = clone(completed_health)
    sha_drift["expected"]["build_commit_sha"] = "c" * 40
    invalid_health_payloads.append((sha_drift, "completed", trade_date))
    waiver_drift = clone(input_not_ready_health)
    next(
        check for check in waiver_drift["checks"]
        if check["name"] == "expected_build_date_run"
    )["waived"] = False
    invalid_health_payloads.append(
        (waiver_drift, "input_not_ready", trade_date)
    )
    adapter_drift = clone(completed_health)
    adapter_drift["adapter_registry"]["registry_sealed"] = False
    invalid_health_payloads.append((adapter_drift, "completed", trade_date))
    failed_check = clone(completed_health)
    failed_check["checks"][0]["passed"] = False
    invalid_health_payloads.append((failed_check, "completed", trade_date))
    omitted_required_check = clone(completed_health)
    omitted_required_check["checks"].pop()
    invalid_health_payloads.append(
        (omitted_required_check, "completed", trade_date)
    )
    contract_version_drift = clone(completed_health)
    contract_version_drift["contract_version"] = (
        "probiga.strategy-governance-health.v0"
    )
    invalid_health_payloads.append(
        (contract_version_drift, "completed", trade_date)
    )
    unsafe_order_flag = clone(completed_health)
    unsafe_order_flag["automatic_real_order_submission"] = True
    invalid_health_payloads.append((unsafe_order_flag, "completed", trade_date))
    extra_top_level = clone(completed_health)
    extra_top_level["unexpected"] = True
    invalid_health_payloads.append((extra_top_level, "completed", trade_date))
    negative_adapter_count = clone(completed_health)
    negative_adapter_count["adapter_registry"]["adapter_count"] = -1
    invalid_health_payloads.append(
        (negative_adapter_count, "completed", trade_date)
    )
    false_candidate_readiness = clone(completed_health)
    false_candidate_readiness["adapter_registry"][
        "candidate_execution_ready"
    ] = True
    invalid_health_payloads.append(
        (false_candidate_readiness, "completed", trade_date)
    )
    unsafe_funding_readiness = clone(completed_health)
    unsafe_funding_readiness["adapter_registry"][
        "funding_pipeline_ready"
    ] = True
    invalid_health_payloads.append(
        (unsafe_funding_readiness, "completed", trade_date)
    )
    source_drift = clone(completed_health)
    source_drift["expected"]["trade_date_source"] = "unexpected"
    invalid_health_payloads.append((source_drift, "completed", trade_date))
    unfrozen_fallback = clone(fallback_health)
    next(
        check
        for check in unfrozen_fallback["checks"]
        if check["name"] == "latest_qmt_announcement_full_market_batch"
    )["detail"]["fallback_reason"] = "LOCAL_DATABASE_ERROR"
    invalid_health_payloads.append((unfrozen_fallback, "completed", trade_date))
    local_module_failure = clone(fallback_health)
    next(
        check
        for check in local_module_failure["checks"]
        if check["name"] == "latest_qmt_announcement_full_market_batch"
    )["detail"]["fallback_reason"] = "ModuleNotFoundError"
    invalid_health_payloads.append(
        (local_module_failure, "completed", trade_date)
    )
    wrong_fallback_primary = clone(fallback_health)
    next(
        check
        for check in wrong_fallback_primary["checks"]
        if check["name"] == "latest_qmt_announcement_full_market_batch"
    )["detail"]["primary_source"] = "cninfo.announcement"
    invalid_health_payloads.append(
        (wrong_fallback_primary, "completed", trade_date)
    )
    for payload, disposition, expected_date in invalid_health_payloads:
        rejected = run_parser(
            "controlled_guard_parse_governance_health_result",
            payload,
            expected_sha,
            disposition,
            expected_date,
        )
        assert rejected.returncode != 0
    date_drift = run_parser(
        "controlled_guard_parse_governance_health_result",
        completed_health,
        expected_sha,
        "completed",
        "2026-08-20",
    )
    assert date_drift.returncode != 0
    duplicate_health_key = json.dumps(completed_health).replace(
        '"status": "PASS"',
        '"status": "PASS", "status": "PASS"',
        1,
    )
    duplicate_health = run_parser(
        "controlled_guard_parse_governance_health_result",
        duplicate_health_key,
        expected_sha,
        "completed",
        trade_date,
    )
    assert duplicate_health.returncode != 0

    cutover_sample = 1_782_000_000
    cutover_cutoff = cutover_sample + 20_000
    cutover_payload: dict[str, object] = {
        "trade_date": trade_date,
        "sample_epoch": cutover_sample,
        "next_cutoff_epoch": cutover_cutoff,
        "safe_before_epoch": cutover_cutoff - 10_800,
        "reserve_seconds": 10_800,
    }
    cutover = run_parser(
        "controlled_guard_parse_governance_cutover_result",
        cutover_payload,
        trade_date,
    )
    assert cutover.returncode == 0, cutover.stdout + cutover.stderr
    assert cutover.stdout.strip() == str(cutover_cutoff - 10_800)
    invalid_cutover_payloads = []
    for field, value in (
        ("trade_date", "2026-08-20"),
        ("sample_epoch", True),
        ("next_cutoff_epoch", cutover_sample),
        ("safe_before_epoch", cutover_sample),
        ("reserve_seconds", 10_799),
    ):
        invalid = clone(cutover_payload)
        invalid[field] = value
        invalid_cutover_payloads.append(invalid)
    extra_cutover = clone(cutover_payload)
    extra_cutover["unexpected"] = True
    invalid_cutover_payloads.append(extra_cutover)
    exact_deadline = clone(cutover_payload)
    exact_deadline["next_cutoff_epoch"] = cutover_sample + 10_800
    exact_deadline["safe_before_epoch"] = cutover_sample
    invalid_cutover_payloads.append(exact_deadline)
    for payload in invalid_cutover_payloads:
        rejected = run_parser(
            "controlled_guard_parse_governance_cutover_result",
            payload,
            trade_date,
        )
        assert rejected.returncode != 0
    duplicate_cutover = json.dumps(cutover_payload).replace(
        '"trade_date": "2026-08-21"',
        '"trade_date": "2026-08-21", "trade_date": "2026-08-21"',
        1,
    )
    rejected_duplicate_cutover = run_parser(
        "controlled_guard_parse_governance_cutover_result",
        duplicate_cutover,
        trade_date,
    )
    assert rejected_duplicate_cutover.returncode != 0

    completed_runner: dict[str, object] = {
        "status": "ok",
        "run_uid": "d" * 32,
        "trade_date": trade_date,
        "summary": {},
        "lifecycle_transitions": [],
        "allocations": [],
        "automatic_real_order_submission": False,
    }
    blocked_runner: dict[str, object] = {
        "status": "blocked",
        "reason": "market inputs are not ready",
        "target_trade_date": trade_date,
        "input_trade_date": trade_date,
        "automatic_real_order_submission": False,
    }
    completed = run_parser(
        "controlled_guard_parse_governance_runner_result",
        completed_runner,
        "0",
        trade_date,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout.strip() == "completed"
    blocked = run_parser(
        "controlled_guard_parse_governance_runner_result",
        blocked_runner,
        "2",
        trade_date,
    )
    assert blocked.returncode == 0, blocked.stdout + blocked.stderr
    assert blocked.stdout.strip() == "input_not_ready"

    runner_field_drift = clone(completed_runner)
    runner_field_drift["unexpected"] = True
    runner_date_drift = clone(completed_runner)
    runner_date_drift["trade_date"] = "2026-08-20"
    runner_unsafe_flag = clone(blocked_runner)
    runner_unsafe_flag["automatic_real_order_submission"] = True
    invalid_runner_cases = (
        (completed_runner, "2"),
        (blocked_runner, "0"),
        (completed_runner, "124"),
        (runner_field_drift, "0"),
        (runner_date_drift, "0"),
        (runner_unsafe_flag, "2"),
        ("{malformed", "0"),
        (
            json.dumps(completed_runner).replace(
                '"status": "ok"',
                '"status": "ok", "status": "ok"',
                1,
            ),
            "0",
        ),
    )
    for payload, status in invalid_runner_cases:
        rejected = run_parser(
            "controlled_guard_parse_governance_runner_result",
            payload,
            status,
            trade_date,
        )
        assert rejected.returncode != 0


def test_recovery_cutover_deadline_and_dropin_contract_are_executable(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable cutover contract test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    helpers = "".join(
        _function(name, bodies[name])
        for name in (
            "controlled_guard_assert_activation_deadline",
            "controlled_guard_cutover_exec_line",
            "controlled_guard_assert_recovery_cutover_dropin",
        )
    ).replace('/usr/bin/date +%s', 'printf "%s\\n" "$NOW_EPOCH"')
    main_dropin = (tmp_path / "main-cutover.conf").as_posix()
    scheduler_dropin = (tmp_path / "scheduler-cutover.conf").as_posix()
    ai_dropin = (tmp_path / "ai-cutover.conf").as_posix()
    harness = f"""
set -u
MAIN_RECOVERY_CUTOVER_DROPIN={main_dropin!r}
SCHEDULER_RECOVERY_CUTOVER_DROPIN={scheduler_dropin!r}
AI_SERVICE_RECOVERY_CUTOVER_DROPIN={ai_dropin!r}
DEADLINE=1782000000
NOW_EPOCH=1781999999
controlled_guard_assert_file() {{
  test -f "$1" && test ! -L "$1" && test "$2" = 644
}}
{helpers}
controlled_guard_assert_activation_deadline "$DEADLINE" || exit 20
NOW_EPOCH="$DEADLINE"
if controlled_guard_assert_activation_deadline "$DEADLINE"; then exit 21; fi
printf '%s\n' '[Service]' > "$MAIN_RECOVERY_CUTOVER_DROPIN"
controlled_guard_cutover_exec_line "$DEADLINE" \
  >> "$MAIN_RECOVERY_CUTOVER_DROPIN" || exit 22
controlled_guard_assert_recovery_cutover_dropin \
  "$MAIN_RECOVERY_CUTOVER_DROPIN" "$DEADLINE" || exit 23
if controlled_guard_assert_recovery_cutover_dropin \
    "$MAIN_RECOVERY_CUTOVER_DROPIN" 1782000001; then exit 24; fi
printf '%s\n' unexpected >> "$MAIN_RECOVERY_CUTOVER_DROPIN"
if controlled_guard_assert_recovery_cutover_dropin \
    "$MAIN_RECOVERY_CUTOVER_DROPIN"; then exit 25; fi
"""
    harness_path = tmp_path / "cutover-contract-harness.sh"
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


def test_cleanup_deadline_expires_before_guard_removal(tmp_path: Path) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable cleanup deadline test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    cleanup = _function(
        "controlled_guard_cleanup",
        _shell_function_bodies(source)["controlled_guard_cleanup"],
    )
    guard = (tmp_path / "guard").as_posix()
    harness = f"""
set -u
DATABASE_WRITER_GUARD_FILE={guard!r}
DATABASE_WRITER_GUARD_DIR={tmp_path.as_posix()!r}
EXPIRED=1
printf 'guard\n' > "$DATABASE_WRITER_GUARD_FILE"
controlled_guard_assert_boundary() {{ test -f "$DATABASE_WRITER_GUARD_FILE"; }}
controlled_guard_assert_activation_deadline() {{ test "$EXPIRED" -eq 0; }}
controlled_guard_assert_dropin_boundary() {{ return 0; }}
controlled_guard_restore_after_cleanup_failure() {{ return 90; }}
sync() {{ return 0; }}
{cleanup}
if controlled_guard_cleanup {'a' * 40} loaded,active,enabled \
    loaded,active,enabled loaded,inactive,static loaded,inactive,disabled \
    1782000000; then exit 20; fi
test -f "$DATABASE_WRITER_GUARD_FILE" || exit 21
EXPIRED=0
controlled_guard_cleanup {'a' * 40} loaded,active,enabled \
  loaded,active,enabled loaded,inactive,static loaded,inactive,disabled \
  1782000000 || exit 22
test ! -e "$DATABASE_WRITER_GUARD_FILE" || exit 23
"""
    harness_path = tmp_path / "cleanup-deadline-harness.sh"
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


def test_unit_start_rechecks_deadline_and_uses_internal_timeout(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable unit deadline test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    apply_state = _function(
        "controlled_guard_apply_unit_state",
        _shell_function_bodies(source)["controlled_guard_apply_unit_state"],
    ).replace("test -x /usr/bin/timeout || return 1", "true").replace(
        "/usr/bin/timeout --signal=TERM --kill-after=10s",
        "timeout --signal=TERM --kill-after=10s",
    )
    trace = (tmp_path / "trace").as_posix()
    harness = f"""
set -u
TRACE={trace!r}
EXPIRED=1
CONTROLLED_RECOVERY_UNIT_START_TIMEOUT=2m
: > "$TRACE"
controlled_guard_assert_activation_deadline() {{ test "$EXPIRED" -eq 0; }}
systemctl() {{
  case "$1" in
    show)
      case "$3" in
        LoadState) printf 'loaded\n' ;;
        UnitFileState) printf 'enabled\n' ;;
        ActiveState) printf 'active\n' ;;
        *) return 1 ;;
      esac
      ;;
    enable) return 0 ;;
    start) printf 'start-%s\n' "$2" >> "$TRACE" ;;
    *) return 1 ;;
  esac
}}
timeout() {{
  test "$1:$2:$3:$4:$5:$6" = \
    "--signal=TERM:--kill-after=10s:2m:systemctl:start:probiga" || return 1
  systemctl "$5" "$6"
}}
{apply_state}
if controlled_guard_apply_unit_state probiga loaded,active,enabled \
    1782000000; then exit 20; fi
test ! -s "$TRACE" || exit 21
EXPIRED=0
controlled_guard_apply_unit_state probiga loaded,active,enabled \
  1782000000 || exit 22
test "$(<"$TRACE")" = start-probiga || exit 23
"""
    harness_path = tmp_path / "unit-start-deadline-harness.sh"
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
      *) return 90 ;;
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


def test_inactive_disabled_timer_accepts_empty_pids_and_rejects_nonzero() -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable timer PID regression")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = _shell_function_bodies(source)["controlled_guard_apply_unit_state"]
    shell_function = _function("controlled_guard_apply_unit_state", body)
    harness = f"""
set -u
TIMER=probiga-ai-recommendation-worker.timer
TEST_MAIN_PID=
TEST_EXEC_MAIN_PID=
systemctl() {{
  local operation="$1"
  shift
  case "$operation" in
    show)
      local property="$2"
      local unit="${{@: -1}}"
      test "$unit" = "$TIMER" || return 80
      case "$property" in
        LoadState) printf 'loaded\n' ;;
        UnitFileState) printf 'disabled\n' ;;
        ActiveState) printf 'inactive\n' ;;
        MainPID) printf '%s\n' "$TEST_MAIN_PID" ;;
        ExecMainPID) printf '%s\n' "$TEST_EXEC_MAIN_PID" ;;
        *) return 81 ;;
      esac
      ;;
    disable|stop)
      test "$1" = "$TIMER" || return 82
      ;;
    *) return 83 ;;
  esac
}}
{shell_function}
controlled_guard_apply_unit_state "$TIMER" loaded,inactive,disabled || exit 11
TEST_MAIN_PID=101
if controlled_guard_apply_unit_state "$TIMER" loaded,inactive,disabled; then
  echo 'nonzero MainPID was accepted' >&2
  exit 12
fi
TEST_MAIN_PID=
TEST_EXEC_MAIN_PID=202
if controlled_guard_apply_unit_state "$TIMER" loaded,inactive,disabled; then
  echo 'nonzero ExecMainPID was accepted' >&2
  exit 13
fi
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
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
