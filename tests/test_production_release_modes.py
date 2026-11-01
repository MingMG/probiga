from __future__ import annotations

import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _bash() -> str | None:
    discovered = shutil.which("bash")
    if discovered:
        return discovered
    git_bash = Path(r"C:\Program Files\Git\bin\bash.exe")
    return str(git_bash) if git_bash.is_file() else None


def _shell_function_body(source: str, name: str) -> str:
    match = re.search(
        rf"(?ms)^{re.escape(name)}\(\) \{{\s*$\n(.*?)^\}}\s*$",
        source,
    )
    assert match is not None
    return match.group(1)


def _function(name: str, body: str) -> str:
    return f"{name}() {{\n{body}\n}}\n"


@pytest.mark.parametrize(
    ("mode", "acknowledge", "classified", "has_parent", "expected_status", "expected"),
    [
        ("fast", 0, "LINUX", True, 0, ("LINUX", "LINUX", "")),
        ("fast", 0, "COORDINATED", True, 2, None),
        (
            "fast",
            1,
            "COORDINATED",
            True,
            0,
            ("COORDINATED", "LINUX", "fast-forced-over-coordinated"),
        ),
        ("full", 0, "LINUX", True, 0, ("LINUX", "COORDINATED", "")),
        ("fast", 0, "LINUX", False, 2, None),
    ],
)
def test_release_mode_resolution_is_fail_closed(
    tmp_path: Path,
    mode: str,
    acknowledge: int,
    classified: str,
    has_parent: bool,
    expected_status: int,
    expected: tuple[str, str, str] | None,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for release mode regression")
    script = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    resolver = _function(
        "resolve_release_execution_scope",
        _shell_function_body(script, "resolve_release_execution_scope"),
    )
    parent_name = "a" * 40
    target = "b" * 40
    artifacts = tmp_path / "artifacts"
    parent = artifacts / parent_name / "component-release.json"
    if has_parent:
        parent.parent.mkdir(parents=True)
        parent.write_text("{}\n", encoding="utf-8")
    harness = f"""
set -u
RELEASE_MODE={mode}
I_KNOW_DDL_CHANGED={acknowledge}
PREVIOUS_WINDOWS_SHA={"a" * 40}
PREVIOUS_CONTRACT_SHA={"c" * 40}
PREVIOUS_SHA={parent_name}
EXPECTED_SHA={target}
RELEASE_ARTIFACT_ROOT={json.dumps(artifacts.as_posix())}
log_release_error() {{ printf 'ERROR:%s\\n' "$*"; }}
log_release_warning() {{ printf 'WARNING:%s\\n' "$*"; }}
{resolver}
resolve_release_execution_scope {classified}
status=$?
printf 'RESULT:%s:%s:%s:%s\\n' "$status" "$RELEASE_CLASSIFIED_SCOPE" \
  "$RELEASE_EXECUTION_SCOPE" "${{RECEIPT_FLAGS[*]}}"
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )
    assert completed.returncode == 0, completed.stderr
    result = next(
        line for line in completed.stdout.splitlines()
        if line.startswith("RESULT:")
    )
    fields = result.split(":", 4)
    assert int(fields[1]) == expected_status
    if expected is not None:
        assert tuple(fields[2:]) == expected
    else:
        assert "--full" in completed.stdout


@pytest.mark.parametrize(
    ("mode", "scope", "execution_scope", "ledger", "lite_manifest", "flags"),
    [
        ("fast", "LINUX", "LINUX", "async-pending",
         "component-release-lite.json", []),
        (
            "fast",
            "COORDINATED",
            "LINUX",
            "async-pending",
            "component-release-lite.json",
            ["fast-forced-over-coordinated"],
        ),
        ("full", "LINUX", "COORDINATED", "synced", None, []),
    ],
)
def test_receipt_has_uniform_mode_and_ledger_fields(
    tmp_path: Path,
    mode: str,
    scope: str,
    execution_scope: str,
    ledger: str,
    lite_manifest: str | None,
    flags: list[str],
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for receipt regression")
    script = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    renderer = _function(
        "render_receipt_file",
        _shell_function_body(script, "render_receipt_file"),
    )
    output = tmp_path / "receipt.json"
    target = "a" * 40
    previous = "b" * 40
    receipt_flags = " ".join(flags)
    harness = f"""
set -u
RELEASE_MODE={mode}
RELEASE_CLASSIFIED_SCOPE={scope}
RELEASE_EXECUTION_SCOPE={execution_scope}
RECEIPT_FLAGS=({receipt_flags})
COMPONENT_LEDGER_STATUS={ledger}
EXPECTED_SHA={target}
PREVIOUS_SHA={previous}
EXPECTED_INPUT_LOCK_SHA256={"c" * 64}
PREVIOUS_INPUT_LOCK_SHA256=
ACTIVE_INPUT_LOCK_SHA256=
EXPECTED_RESOLVED_FREEZE_SHA256={"d" * 64}
PREVIOUS_RESOLVED_FREEZE_SHA256=
ACTIVE_RESOLVED_FREEZE_SHA256=
EXPECTED_WHEEL_MANIFEST_SHA256={"e" * 64}
EXPECTED_ADATA_SHA={"f" * 40}
EXPECTED_ADATA_TREE_SHA256={"1" * 64}
PREVIOUS_ADATA_SHA=
PREVIOUS_ADATA_TREE_SHA256=
ACTIVE_ADATA_SHA=
ACTIVE_ADATA_TREE_SHA256=
DEPLOY_STARTED_AT=2026-11-01T00:00:00Z
{renderer}
render_receipt_file DEPLOYED {target} {json.dumps(output.as_posix())}
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )
    assert completed.returncode == 0, completed.stderr
    receipt = json.loads(output.read_text(encoding="utf-8"))
    assert receipt["mode"] == mode
    assert receipt["scope"] == scope
    assert receipt["execution_scope"] == execution_scope
    assert receipt["flags"] == flags
    assert receipt["sha"] == target
    assert receipt["signed_manifest"] == "component-release.json"
    assert receipt["lite_manifest"] == lite_manifest
    assert receipt["ledger"] == ledger


def test_engine_log_stream_is_mode_prefixed_and_broker_checks_engine_capability():
    script = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    assert '/usr/bin/sed -u "s/^/$RELEASE_LOG_PREFIX /"' in script
    assert "readonly DEPLOY_ENGINE_RELEASE_MODE_CAPABILITY=probiga-release-mode-v1" in script
    assert "engine does not support --mode; upgrade broker and engine together" in broker


def test_full_release_excludes_only_the_pid_verified_stopped_local_scheduler():
    script = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    capture = script.index(
        "CUTOVER_STEP=capture_linux_scheduler_identity_before_stop"
    )
    stop = script.index(
        "CUTOVER_STEP=stop_linux_scheduler_before_writer_quiescence",
        capture,
    )
    quiescence = script.index(
        "CUTOVER_STEP=verify_cross_host_writer_quiescence_before_api_stop",
        stop,
    )
    assert capture < stop < quiescence
    between = script[capture:quiescence]
    assert '--expected-build-sha "$PREVIOUS_SHA"' in between
    assert '--expected-scheduler-pid "$LOCAL_SCHEDULER_PID"' in between
    assert 'test ! -d "/proc/$LOCAL_SCHEDULER_PID"' in between
    assert '--exclude-instance-id "$LOCAL_SCHEDULER_INSTANCE_ID"' in (
        script[quiescence:quiescence + 2000]
    )
