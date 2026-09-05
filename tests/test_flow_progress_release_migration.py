from __future__ import annotations

import os
from pathlib import Path
import re
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]


def migration_script():
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = deploy.split("migrate_legacy_flow_progress() {", 1)[1]
    return re.search(r"<<'PY' \|\| return 2\n(.*?)\nPY", body, re.S).group(1)


def exercise_migration(script, root):
    """Standard-library-only fixture; also runs on the actual Linux runtime."""
    jobs, state = root / "jobs", root / "state"
    jobs.mkdir(mode=0o700)
    flow = jobs / "flow-2026-09-03"
    flow.mkdir(mode=0o700)
    attempt = flow / "attempt-abc"
    attempt.mkdir(mode=0o700)
    progress = flow / "flow-fetch-progress.json"
    progress.write_bytes(b'{"checkpoint":"preserved"}')
    progress.chmod(0o600)
    evidence = attempt / "manifest.json"
    evidence.write_bytes(b'{"preimage":"preserved"}')
    evidence.chmod(0o644)

    def run(mode):
        return subprocess.run(
            [sys.executable, "-I", "-", str(jobs), str(state), str(os.geteuid()), str(os.getegid()), mode],
            input=script, text=True, capture_output=True, timeout=5,
        )
    assert run("inspect").returncode == 0
    assert progress.exists() and not state.exists()
    external = root / "external"
    external.write_bytes(b"do not alter")
    external.chmod(0o600)
    malicious = flow / "flow-progress-symlink"
    malicious.symlink_to(external)
    assert run("apply").returncode != 0
    assert external.read_bytes() == b"do not alter" and progress.exists()
    malicious.unlink()
    os.link(external, malicious)
    assert run("apply").returncode != 0
    malicious.unlink()
    unknown = flow / "unknown.json"
    unknown.write_bytes(b"unknown")
    unknown.chmod(0o600)
    assert run("apply").returncode != 0
    unknown.unlink()
    result = run("apply")
    assert result.returncode == 0, result.stderr
    assert not flow.exists()
    assert (state / flow.name / progress.name).read_bytes() == b'{"checkpoint":"preserved"}'
    assert (state / flow.name / attempt.name / evidence.name).read_bytes() == b'{"preimage":"preserved"}'
    assert run("apply").returncode == 0


@pytest.mark.skipif(os.name != "posix", reason="production POSIX ownership contract")
def test_flow_progress_migration_preserves_data_and_rejects_links(tmp_path):
    exercise_migration(migration_script(), tmp_path)


def test_flow_evidence_has_separate_root_and_migrates_after_quiescence():
    from tools import repair_linux_recent_data_gaps as repair
    assert repair.DEFAULT_FLOW_EVIDENCE_ROOT.parent == repair.DEFAULT_STATE_FILE.parent.parent
    assert repair.DEFAULT_FLOW_EVIDENCE_ROOT != repair.DEFAULT_STATE_FILE.parent
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    assert deploy.index("\nmigrate_legacy_flow_progress inspect\n") < deploy.index("\nprepare_probiga_job_log_root\n")
    assert deploy.index("CUTOVER_STEP=verify_cross_host_writer_quiescence_before_api_stop") < deploy.index("\nmigrate_legacy_flow_progress apply\n") < deploy.index("\nmigrate_probiga_job_log_legacy_modes\n")
