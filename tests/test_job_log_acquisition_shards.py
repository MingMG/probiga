"""Run the actual deployment checks against a disposable POSIX log tree."""

import os
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def check_scripts():
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    result = []
    for name in ("prepare_probiga_job_log_root", "migrate_probiga_job_log_legacy_modes"):
        body = source.split(name + "() {", 1)[1]
        result.append(re.search(r"<<'PY' \|\| return 2\n(.*?)\nPY", body, re.S).group(1))
    return result


def exercise_shard_checks(scripts, root):
    jobs = root / "jobs"
    jobs.mkdir(mode=0o700)
    shards = jobs / "acquisition-shards"
    shards.mkdir(mode=0o755)
    scope = shards / ("a" * 64)
    scope.mkdir(mode=0o755)
    checkpoint = scope / ("b" * 64 + ".json")
    checkpoint.write_bytes(b'{"checkpoint":"preserved"}')
    checkpoint.chmod(0o600)
    temporary = scope / ".writing-abc12345"
    temporary.write_bytes(b"pending")
    temporary.chmod(0o600)
    external = root / "external"
    external.write_bytes(b"do not touch")
    external.chmod(0o600)

    def run(script):
        return subprocess.run(
            [sys.executable, "-I", "-", str(jobs), str(os.geteuid()), str(os.getegid())],
            input=script, text=True, capture_output=True, timeout=5,
        )

    def assert_rejected():
        for script in scripts:
            result = run(script)
            assert result.returncode != 0, result.stdout
        assert external.read_bytes() == b"do not touch"

    for script in scripts:
        result = run(script)
        assert result.returncode == 0, result.stderr
    assert checkpoint.read_bytes() == b'{"checkpoint":"preserved"}'
    assert temporary.read_bytes() == b"pending"

    checkpoint.chmod(0o644)
    assert_rejected()
    checkpoint.chmod(0o600)
    scope.chmod(0o777)
    assert_rejected()
    scope.chmod(0o755)

    suspicious = scope / ("c" * 64 + ".json")
    suspicious.symlink_to(external)
    assert_rejected()
    suspicious.unlink()
    os.link(external, suspicious)
    assert_rejected()
    suspicious.unlink()
    suspicious.mkdir(mode=0o700)
    assert_rejected()
    suspicious.rmdir()

    link = shards / ("d" * 64)
    link.symlink_to(scope, target_is_directory=True)
    assert_rejected()
    link.unlink()

    unknown = shards / "unknown-scope"
    unknown.mkdir(mode=0o700)
    assert_rejected()
    unknown.rmdir()
    unknown = jobs / "unknown-directory"
    unknown.mkdir(mode=0o700)
    assert_rejected()
    unknown.rmdir()

    for script in scripts:
        result = run(script)
        assert result.returncode == 0, result.stderr


def test_acquisition_shards_are_preserved_and_unsafe_entries_rejected(tmp_path):
    import pytest
    if os.name != "posix":
        pytest.skip("production ownership/openat checks require POSIX")
    exercise_shard_checks(check_scripts(), tmp_path)
