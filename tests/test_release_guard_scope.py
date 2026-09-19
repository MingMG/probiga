"""Execute the production finalizer under Bash's dynamic local-variable scope."""

from pathlib import Path
import os
import re
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
SHA = "a" * 40
MAIN = "loaded,active,enabled"
SCHEDULER = "loaded,active,enabled"
AI_SERVICE = "loaded,inactive,static"
AI_TIMER = "not-found,not-found,not-found"


def _bash() -> str | None:
    if os.name == "nt":
        git_bash = Path(r"C:\Program Files\Git\bin\bash.exe")
        if git_bash.is_file():
            return str(git_bash)
    return shutil.which("bash")


def _restore_function(source: str) -> str:
    match = re.search(
        r"^controlled_guard_restore_and_finalize\(\) \{\n.*?^\}",
        source,
        flags=re.MULTILINE | re.DOTALL,
    )
    assert match is not None, "production finalizer must exist"
    return match.group(0)


def _run_finalizer(
    tmp_path: Path, *, source: str, caller_has_record: bool, runtime: str
) -> subprocess.CompletedProcess[str]:
    bash = _bash()
    if bash is None:
        pytest.skip("Bash is required for the executable guard scope regression")
    guard = tmp_path / "guard"
    guard.mkdir()
    (guard / "writer.restore").write_text("test-only restore journal\n", encoding="utf-8")
    # The only permitted external command removes this temporary journal.
    # No production script is sourced; the actual finalizer is extracted intact.
    harness = r"""set -euo pipefail
unset scheduler_record
readonly TEST_ROOT="$PWD"
readonly RM_BINARY="$(command -v rm)"
readonly DATABASE_WRITER_GUARD_DIR="$TEST_ROOT/guard"
readonly DATABASE_WRITER_RESTORE_FILE="$DATABASE_WRITER_GUARD_DIR/writer.restore"
readonly ACTIVATION_UNIT_SNAPSHOT_DIR="$TEST_ROOT/absent-activation-snapshot"
readonly DEPLOY_OPERATION=deploy
readonly DEPLOY_ARTIFACT_MODE=ci-resolved-freeze-v1
readonly EXPECTED_SHA="$1"
PATH="$TEST_ROOT/no-external-commands"
record() { printf '%s' "$1"; shift; printf '\t%s' "$@"; printf '\n'; }
controlled_guard_assert_restore_file() { record assert "$@"; }
controlled_guard_restore_previous_writer_states() { record restore "$@"; }
controlled_guard_verify_restored_runtime() { record verify "$@"; }
controlled_guard_refence_after_restore_failure() { record unexpected-refence "$@"; return 99; }
rm() {
  test "$#" -eq 3 && test "$1" = -f && test "$2" = -- &&
    test "$3" = "$DATABASE_WRITER_RESTORE_FILE" || return 99
  record cleanup "$3"
  "$RM_BINARY" "$@"
}
sync() {
  test "$#" -eq 2 && test "$1" = -f &&
    test "$2" = "$DATABASE_WRITER_GUARD_DIR" || return 99
  record sync "$2"
}
"""
    harness += _restore_function(source) + "\n"
    harness += "invoke() {\n"
    if caller_has_record:
        harness += "  local scheduler_record=loaded,inactive,disabled\n"
    harness += '  controlled_guard_restore_and_finalize "$@"\n'
    if caller_has_record:
        harness += '  record caller-after "$scheduler_record"\n'
    harness += '}\ninvoke "$@"\n'
    script = tmp_path / "finalizer-scope.sh"
    script.write_text(harness, encoding="utf-8", newline="\n")
    return subprocess.run(
        [bash, str(script), SHA, MAIN, SCHEDULER, AI_SERVICE, AI_TIMER, runtime],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
    )


@pytest.mark.parametrize("runtime", ["controlled", "prepared"])
@pytest.mark.parametrize("caller_has_record", [False, True], ids=["no-caller-record", "wrong-caller-record"])
def test_restore_finalizer_uses_its_scheduler_argument(
    tmp_path: Path, caller_has_record: bool, runtime: str
) -> None:
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    result = _run_finalizer(
        tmp_path, source=source, caller_has_record=caller_has_record, runtime=runtime
    )
    assert result.returncode == 0, result.stdout + result.stderr
    events = [line.split("\t") for line in result.stdout.splitlines()]
    assert events[:3] == [
        ["assert", SHA, MAIN, SCHEDULER, AI_SERVICE, AI_TIMER],
        ["restore", MAIN, SCHEDULER, AI_SERVICE, AI_TIMER],
        ["verify", MAIN, SCHEDULER, SHA, AI_SERVICE, AI_TIMER, "full"],
    ]
    assert [event[0] for event in events] == [
        "assert", "restore", "verify", "cleanup", "sync",
        *(["caller-after"] if caller_has_record else []),
    ]
    if caller_has_record:
        assert events[-1] == ["caller-after", "loaded,inactive,disabled"]
    assert not (tmp_path / "guard/writer.restore").exists()
