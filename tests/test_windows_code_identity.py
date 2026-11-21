"""The Windows release probe must import engine validators in production mode."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def test_actual_windows_source_context_allows_production_engine_import():
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, "PROBIGA_DEPLOYMENT_MODE": "production", "PYTHONDONTWRITEBYTECODE": "1"}
    env.pop("PROBIGA_CODE_ROOT", None)
    env.pop("PROBIGA_RELEASE_TREE_SHA256", None)
    program = (
        "from tools.run_scheduler_daemon import bind_windows_code_identity; "
        "bind_windows_code_identity(); "
        "import server.engine.strategy_governance; "
        "import os,json; print(json.dumps({k:os.environ[k] for k in "
        "('PROBIGA_CODE_ROOT','PROBIGA_RELEASE_TREE_SHA256')}))"
    )
    value = json.loads(subprocess.check_output(
        [sys.executable, "-c", program], cwd=root, env=env, text=True, timeout=30,
    ))
    tree = subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=root, text=True, timeout=10).strip()
    expected = hashlib.sha256(json.dumps(
        {"kind": "git-tree", "tree": tree}, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()
    assert value == {"PROBIGA_CODE_ROOT": str(root.resolve()), "PROBIGA_RELEASE_TREE_SHA256": expected}
