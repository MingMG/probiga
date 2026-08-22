from __future__ import annotations

import re
import shutil
import subprocess
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
        '"$ACTIVATION_UNIT_SNAPSHOT_MANIFEST" "$TEST_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_NEW_MANIFEST" "$TEST_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_PHASE" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_PHASE" "$TEST_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_STATE" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_STATE" "$TEST_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_STATE_SHA" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_STATE_SHA" "$TEST_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" 600',
        '"$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" "$TEST_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_GOVERNANCE_OLD_SHA" 600',
        '"$ACTIVATION_GOVERNANCE_OLD_SHA" "$TEST_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_RELEASE_IDENTITY" 600',
        '"$ACTIVATION_RELEASE_IDENTITY" "$TEST_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_RELEASE_IDENTITY_SHA" 600',
        '"$ACTIVATION_RELEASE_IDENTITY_SHA" "$TEST_FILE_MODE"',
    ).replace(
        '"$DATABASE_WRITER_RESTORE_FILE" 600',
        '"$DATABASE_WRITER_RESTORE_FILE" "$TEST_FILE_MODE"',
    ).replace(
        '"$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload" 600',
        '"$ACTIVATION_UNIT_SNAPSHOT_DIR/$payload" "$TEST_FILE_MODE"',
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
chmod 644 "$PREPARED_MAIN_DROPIN" "$PREPARED_SCHEDULER_DROPIN" \
  "$DATABASE_WRITER_RESTORE_FILE" "$GOVERNANCE_TASK_OLD_SOURCE"
TEST_DIR_MODE="$(stat -c '%a' "$TEST_ROOT")"
TEST_FILE_MODE="$(stat -c '%a' "$UNIT_A")"
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
