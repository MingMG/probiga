"""Run the actual sealed-manifest preparation program without a database."""
from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_isolated_manifest_preparation_does_not_modify_release_source(tmp_path):
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    function = source.split("prepare_component_release_manifest() {\n", 1)[1]
    invocation, remainder = function.split("<<'PY'\n", 1)
    program = remainder.split("\nPY\n", 1)[0]
    # Use the production flags, including isolated mode which ignores
    # PYTHONDONTWRITEBYTECODE. An explicit -B must survive this boundary.
    flags = re.search(r'"\$BOOTSTRAP_PYTHON" ((?:-[A-Za-z]+ )+)- ', invocation)
    assert flags is not None
    code = tmp_path / "release"
    common = code / "server" / "common"
    common.mkdir(parents=True)
    (code / "server" / "__init__.py").write_text("", encoding="utf-8")
    (common / "__init__.py").write_text("", encoding="utf-8")
    # Filesystem ownership and full manifest validation have independent tests.
    # This fixture makes the real preparation/import boundary portable.
    (common / "component_release.py").write_text(
        "import json\n"
        "def build_component_release(**fields): return fields\n"
        "def _trusted_read(path): return json.loads(path.read_text())\n"
        "def write_component_release(path, fields):\n"
        "    path.parent.mkdir(parents=True, exist_ok=True)\n"
        "    path.write_text(json.dumps(fields))\n", encoding="utf-8",
    )
    target, prior = "a" * 40, "b" * 40
    report = tmp_path / "scope.json"
    report.write_text(json.dumps({
        "base_sha": prior, "target_sha": target, "scope": "COORDINATED",
        "contract_sha256": "c" * 64,
    }), encoding="utf-8")
    artifacts = tmp_path / "artifacts"
    result = subprocess.run(
        [sys.executable, *flags.group(1).split(), "-", str(code), target,
         prior, str(report), str(artifacts)],
        input=program, text=True, capture_output=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    manifest = json.loads((artifacts / target / "component-release.json").read_text())
    assert manifest["linux_build_sha"] == target
    assert manifest["windows_build_sha"] == target
    assert not list(code.rglob("*.pyc"))
    assert not list(code.rglob("__pycache__"))
