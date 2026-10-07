"""Install the 14 order-free QMT list entries from one exact release checkout.

This installs code, not a second scheduler. Live catalog registration stays with
QMT's own editor; editing its XML while the app is running loses user changes.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.release_identity import git_strategy_artifact, normalize_build_sha
from integrations.bigqmt.spool import resolve_big_qmt_home
from server.engine.qmt_strategy_simulation import strategy_catalog
from tools.run_qmt_strategy_daily import atomic_json

TEMPLATE = "integrations/bigqmt/qmt_strategy/probiga_simulation_entry.py"
MANIFEST_NAME = "probiga_simulation_release.json"
RUNNER = "tools/run_qmt_strategy_daily.py"


def render_entry(source: bytes, *, strategy_key: str, name: str,
                 release_path: Path, build_sha: str) -> bytes:
    text = source.decode("utf-8")
    replacements = {
        "STRATEGY_KEY": strategy_key, "STRATEGY_NAME": name,
        "RELEASE_PATH": str(release_path), "EXPECTED_BUILD": build_sha,
    }
    for field, value in replacements.items():
        token = '"__PROBIGA_' + field + '__"'
        if text.count(token) != 1:
            raise ValueError("QMT simulation template field differs: " + field)
        text = text.replace(token, json.dumps(value, ensure_ascii=True))
    ast.parse(text, feature_version=(3, 6))
    return text.encode("utf-8")


def _release_plan(*, qmt_home: Path, expected_build_sha: str) -> tuple[dict, list[tuple[Path, bytes]]]:
    expected_build_sha = normalize_build_sha(expected_build_sha)
    observed = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"],
                                       text=True, timeout=15).strip()
    if observed != expected_build_sha:
        raise RuntimeError("QMT simulation entries require the exact release HEAD")
    dirty = subprocess.check_output(
        ["git", "-C", str(ROOT), "status", "--porcelain", "--", TEMPLATE, RUNNER,
         "tools/install_qmt_simulation_entries.py", "tools/run_qmt_windows_edge_release_bootstrap.py",
         "server/engine/qmt_strategy_simulation.py", "server/engine/strategy_governance.py",
         "strategies/stock_strategy_v2.json", "strategies/trading_v3.json"],
        text=True, timeout=15,
    ).strip()
    if dirty:
        raise RuntimeError("QMT simulation entry release inputs differ from the exact checkout")
    artifact = git_strategy_artifact(root=ROOT, source_path=ROOT / TEMPLATE,
                                     build_sha=expected_build_sha)
    runner = git_strategy_artifact(root=ROOT, source_path=ROOT / RUNNER,
                                   build_sha=expected_build_sha)
    for source_path, committed in ((ROOT / TEMPLATE, artifact), (ROOT / RUNNER, runner)):
        if source_path.read_bytes().replace(b"\r\n", b"\n") != committed["source_bytes"].replace(b"\r\n", b"\n"):
            raise RuntimeError("QMT simulation installed checkout source differs from Git")
    catalog = strategy_catalog()
    rows = list(catalog["strategies"]) + list(catalog["combinations"])
    if len(rows) != 14 or len({row["strategy_key"] for row in rows}) != 14:
        raise RuntimeError("QMT simulation catalog must contain exactly 10 strategies and 4 combinations")
    python_dir = qmt_home / "python"
    release_path = python_dir / MANIFEST_NAME
    entries, files = [], []
    for row in rows:
        name = "PROBIGA模拟_" + row["name"]
        if any(character in name for character in '\\/:*?"<>|'):
            raise RuntimeError("Unsafe QMT strategy filename")
        target = python_dir / (name + ".py")
        encoded = render_entry(artifact["source_bytes"], strategy_key=row["strategy_key"],
                               name=row["name"], release_path=release_path,
                               build_sha=expected_build_sha)
        digest = hashlib.sha256(encoded).hexdigest()
        files.append((target, encoded))
        entries.append({"strategy_key": row["strategy_key"], "name": row["name"],
                        "qmt_name": name, "path": str(target), "sha256": digest})
    manifest = {
        "schema": "probiga.qmt-simulation-entries.v1", "build_sha": expected_build_sha,
        "production_root": str(ROOT), "simulation_only": True,
        "automatic_real_order_submission": False,
        "state_root": str(qmt_home / "userdata" / "probiga_strategy_simulation"),
        "template_git_blob": artifact["git_blob"], "template_sha256": artifact["source_sha256"],
        "runner_git_blob": runner["git_blob"], "runner_source_sha256": runner["source_sha256"],
        "runner_sha256": hashlib.sha256((ROOT / RUNNER).read_bytes()).hexdigest(),
        "entries": entries,
    }
    return manifest, files


def validate_installed_entries(*, qmt_home: Path, expected_build_sha: str) -> dict:
    """Read only: prove exact rendered files, not QMT's native list registration."""
    manifest, files = _release_plan(qmt_home=qmt_home, expected_build_sha=expected_build_sha)
    release_path = qmt_home / "python" / MANIFEST_NAME
    errors = []
    try:
        actual = json.loads(release_path.read_text(encoding="utf-8"))
        if json.dumps(actual, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) != json.dumps(
            manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ):
            errors.append("SIMULATION_RELEASE_MANIFEST_DIFFERS")
    except (OSError, ValueError):
        errors.append("SIMULATION_RELEASE_MANIFEST_UNAVAILABLE")
    for target, expected in files:
        try:
            if target.read_bytes() != expected:
                errors.append("SIMULATION_ENTRY_CONTENT_DIFFERS:" + target.name)
        except OSError:
            errors.append("SIMULATION_ENTRY_UNAVAILABLE:" + target.name)
    return {
        "schema": "probiga.qmt-simulation-entry-proof.v1",
        "status": "NOT_READY" if errors else "READY", "build_sha": expected_build_sha,
        "manifest_path": str(release_path),
        "manifest_sha256": hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                                                       separators=(",", ":")).encode()).hexdigest(),
        "entry_count": len(files), "errors": errors,
        "simulation_only": True, "automatic_real_order_submission": False,
        "database_writes": False, "qmt_calls": False,
        "catalog_registration": "NOT_ATTESTED",
    }


def install_entries(*, qmt_home: Path, expected_build_sha: str) -> dict:
    """Apply only from an activated release owner; writes no native QMT catalog."""
    manifest, files = _release_plan(qmt_home=qmt_home, expected_build_sha=expected_build_sha)
    release_path = qmt_home / "python" / MANIFEST_NAME
    release_path.parent.mkdir(parents=True, exist_ok=True)
    for target, encoded in files:
        temporary = target.with_name("." + target.name + "." + str(os.getpid()) + ".tmp")
        try:
            with temporary.open("wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    atomic_json(release_path, manifest)
    proof = validate_installed_entries(qmt_home=qmt_home, expected_build_sha=expected_build_sha)
    if proof["status"] != "READY":
        raise RuntimeError("QMT simulation entry readback differs")
    return {"status": "installed", "manifest_path": str(release_path), **manifest,
            "proof": proof, "catalog_registration": "NOT_ATTESTED"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-build-sha", required=True)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--validate-only", action="store_true")
    modes.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    from tools.env_config import create_tool_engine, load_project_env
    load_project_env()
    if args.apply:
        if os.name != "nt" or os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") != "qmt_windows_edge":
            raise RuntimeError("QMT simulation static apply requires the Windows edge release owner")
        from tools.run_qmt_windows_edge_release_bootstrap import read_release_activation
        engine = create_tool_engine()
        try:
            activation = read_release_activation(engine, expected_build_sha=args.expected_build_sha)
        finally:
            engine.dispose()
        if activation.get("status") != "READY":
            raise RuntimeError("QMT simulation static apply requires the exact activation grant")
        result = install_entries(qmt_home=resolve_big_qmt_home(required=True),
                                 expected_build_sha=args.expected_build_sha)
    else:
        result = validate_installed_entries(qmt_home=resolve_big_qmt_home(required=True),
                                           expected_build_sha=args.expected_build_sha)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] in {"READY", "installed"} else 4


if __name__ == "__main__":
    raise SystemExit(main())
