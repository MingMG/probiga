from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
LINUX = "a" * 40
WINDOWS = "b" * 40


def test_recovery_component_lookup_never_falls_back_for_new_code(tmp_path):
    bash = shutil.which("bash") or str(Path(r"C:\Program Files\Git\bin\bash.exe"))
    if not Path(bash).is_file():
        pytest.skip("Bash is required for the recovery identity test")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = source.split("controlled_guard_component_sha() {\n", 1)[1].split("\n}\n", 1)[0]
    body = body.replace("/var/lib/probiga/release-artifacts", "$TEST_ARTIFACT_ROOT")
    body = body.replace("/usr/bin/python3.14", '"$FAKE_PYTHON"')
    code_root = tmp_path / "code" / LINUX
    (code_root / "server/common").mkdir(parents=True)
    module = code_root / "server/common/component_release.py"
    module.write_text("# sealed component module\n", encoding="utf-8")
    artifacts = tmp_path / "artifacts" / LINUX
    artifacts.mkdir(parents=True)
    manifest = artifacts / "component-release.json"
    manifest.write_text(WINDOWS + "\n", encoding="utf-8")
    fake_python = tmp_path / "sealed-reader"
    # File/seal validation itself is exercised by test_component_release. Here
    # the process boundary proves the resolver keeps actual code identity L.
    fake_python.write_text(f'''#!/usr/bin/env bash
test "$PROBIGA_BUILD_COMMIT_SHA" = "{LINUX}" || exit 40
test "$PROBIGA_EXPECTED_GIT_SHA" = "{LINUX}" || exit 41
test "$PROBIGA_DEPLOYMENT_MODE" = production || exit 42
test -f "$PROBIGA_COMPONENT_RELEASE_PATH" || exit 43
case "${{@: -1}}" in
  windows|contract) cat "$PROBIGA_COMPONENT_RELEASE_PATH" ;;
  linux) printf '%s\\n' "$PROBIGA_BUILD_COMMIT_SHA" ;;
  *) exit 44 ;;
esac
''', encoding="utf-8", newline="\n")
    fake_python.chmod(0o755)
    harness = f'''set -eu
TEST_ROOT={tmp_path.as_posix()!r}
CODE_RELEASE_ROOT="$(cd "$TEST_ROOT/code" && pwd -P)"
TEST_ARTIFACT_ROOT="$(cd "$TEST_ROOT/artifacts" && pwd -P)"
FAKE_PYTHON={fake_python.as_posix()!r}
LINUX_SHA={LINUX}
WINDOWS_SHA={WINDOWS}
TRACKED_MODULE=1
ACTUAL_HEAD="$LINUX_SHA"
stat() {{ printf '%s\\n' root:root; }}
find() {{ return 0; }}
git() {{
  case "$3" in
    rev-parse) printf '%s\\n' "$ACTUAL_HEAD" ;;
    ls-tree) if [ "$TRACKED_MODULE" = 1 ]; then printf '%s\\n' server/common/component_release.py; fi ;;
    *) return 1 ;;
  esac
}}
controlled_guard_assert_recovery_code_tree_clean() {{ test "$2" = "$LINUX_SHA"; }}
controlled_guard_component_sha() {{
{body}
}}
test "$(controlled_guard_component_sha "$LINUX_SHA" windows)" = "$WINDOWS_SHA" || exit 10
test "$(controlled_guard_component_sha "$LINUX_SHA" contract)" = "$WINDOWS_SHA" || exit 11
test "$(controlled_guard_component_sha "$LINUX_SHA" linux)" = "$LINUX_SHA" || exit 12
rm "$TEST_ARTIFACT_ROOT/$LINUX_SHA/component-release.json"
if controlled_guard_component_sha "$LINUX_SHA" contract; then exit 13; fi
TRACKED_MODULE=0
rm "$CODE_RELEASE_ROOT/$LINUX_SHA/server/common/component_release.py"
test "$(controlled_guard_component_sha "$LINUX_SHA" contract)" = "$LINUX_SHA" || exit 14
printf '%s\\n' "$WINDOWS_SHA" > "$TEST_ARTIFACT_ROOT/$LINUX_SHA/component-release.json"
if controlled_guard_component_sha "$LINUX_SHA" windows; then exit 15; fi
rm "$TEST_ARTIFACT_ROOT/$LINUX_SHA/component-release.json"
TRACKED_MODULE=1
if controlled_guard_component_sha "$LINUX_SHA" windows; then exit 16; fi
TRACKED_MODULE=0
ACTUAL_HEAD="$WINDOWS_SHA"
if controlled_guard_component_sha "$LINUX_SHA" windows; then exit 17; fi
'''
    script = tmp_path / "component-lookup.sh"
    script.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run([bash, str(script)], capture_output=True, text=True, timeout=30)
    assert completed.returncode == 0, completed.stdout + completed.stderr
