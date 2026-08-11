from __future__ import annotations

import hashlib
from pathlib import Path
import re

import pytest

from tools.run_trading_v4_ci_gate import (
    BASE_MANIFEST_COUNT,
    BASE_MANIFEST_SHA256,
    CORE_MANIFEST_COUNT,
    CORE_MANIFEST_SHA256,
    RESEARCH_MANIFEST_COUNT,
    RESEARCH_MANIFEST_SHA256,
    EXTENDED_MANIFEST_COUNT,
    ALL_MANIFEST_COUNT,
    REQUIRED_SAFETY_TESTS,
    REQUIRED_STAGE3_TESTS,
    TradingV4CIGateError,
    validate_gate_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = PROJECT_ROOT / ".github" / "workflows" / "deploy.yml"
LOCAL_RUNNER = PROJECT_ROOT / "tools" / "run_trading_v4_regression.ps1"


def _workflow_job(source: str, job_name: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(job_name)}:\s*\n"
        rf"(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:\s*\n|\Z)",
        source,
    )
    assert match is not None, f"workflow job is missing: {job_name}"
    return match.group("body")


def _isolated_gate_tree(
    tmp_path: Path,
) -> tuple[Path, list[str], list[str], list[str]]:
    base_source = (
        PROJECT_ROOT / "tests/manifests/trading_v234_regression_62.txt"
    ).read_text(encoding="utf-8").splitlines()
    core_source = (
        PROJECT_ROOT / "tests/manifests/trading_core_compatibility_14.txt"
    ).read_text(encoding="utf-8").splitlines()
    research_source = (
        PROJECT_ROOT / "tests/manifests/trading_v4_stage3_research_20.txt"
    ).read_text(encoding="utf-8").splitlines()
    manifest_root = tmp_path / "tests" / "manifests"
    manifest_root.mkdir(parents=True)
    for relative in (*base_source, *core_source, *research_source):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("# gate fixture\n", encoding="utf-8")
    (manifest_root / "trading_v234_regression_62.txt").write_text(
        "\n".join(base_source) + "\n",
        encoding="utf-8",
    )
    (manifest_root / "trading_core_compatibility_14.txt").write_text(
        "\n".join(core_source) + "\n",
        encoding="utf-8",
    )
    (manifest_root / "trading_v4_stage3_research_20.txt").write_text(
        "\n".join(research_source) + "\n",
        encoding="utf-8",
    )
    return tmp_path, base_source, core_source, research_source


def test_frozen_gate_validates_actual_62_plus_14_plus_20_contract():
    validation = validate_gate_contract(PROJECT_ROOT)

    assert len(validation.base.entries) == BASE_MANIFEST_COUNT == 62
    assert len(validation.core.entries) == CORE_MANIFEST_COUNT == 14
    assert len(validation.research.entries) == RESEARCH_MANIFEST_COUNT == 20
    assert len(validation.extended_entries) == EXTENDED_MANIFEST_COUNT == 76
    assert len(validation.all_entries) == ALL_MANIFEST_COUNT == 96
    assert validation.base.sha256 == BASE_MANIFEST_SHA256
    assert validation.core.sha256 == CORE_MANIFEST_SHA256
    assert validation.research.sha256 == RESEARCH_MANIFEST_SHA256
    assert set(REQUIRED_SAFETY_TESTS).issubset(validation.extended_entries)
    assert set(REQUIRED_STAGE3_TESTS) == set(validation.research.entries)


def test_gate_fails_closed_on_count_duplicate_required_test_and_hash_drift(
    tmp_path: Path,
):
    root, base, _core, _research = _isolated_gate_tree(tmp_path / "count")
    path = root / "tests/manifests/trading_v234_regression_62.txt"
    path.write_text("\n".join(base[:-1]) + "\n", encoding="utf-8")
    with pytest.raises(TradingV4CIGateError, match="count drifted"):
        validate_gate_contract(root)

    root, base, _core, _research = _isolated_gate_tree(tmp_path / "duplicate")
    path = root / "tests/manifests/trading_v234_regression_62.txt"
    base[1] = base[0]
    path.write_text("\n".join(base) + "\n", encoding="utf-8")
    with pytest.raises(TradingV4CIGateError, match="duplicate"):
        validate_gate_contract(root)

    root, base, core, _research = _isolated_gate_tree(
        tmp_path / "cross_duplicate"
    )
    path = root / "tests/manifests/trading_core_compatibility_14.txt"
    core[0] = base[0]
    path.write_text("\n".join(core) + "\n", encoding="utf-8")
    with pytest.raises(TradingV4CIGateError, match="overlapping"):
        validate_gate_contract(root)

    root, base, _core, _research = _isolated_gate_tree(tmp_path / "required")
    path = root / "tests/manifests/trading_v234_regression_62.txt"
    required = "tests/architecture/test_v4_sql_allowlist.py"
    replacement = "tests/test_unique_gate_replacement.py"
    base[base.index(required)] = replacement
    (root / replacement).write_text("# replacement\n", encoding="utf-8")
    path.write_text("\n".join(base) + "\n", encoding="utf-8")
    with pytest.raises(TradingV4CIGateError, match="required safety tests"):
        validate_gate_contract(root)

    root, base, _core, _research = _isolated_gate_tree(tmp_path / "sha")
    path = root / "tests/manifests/trading_v234_regression_62.txt"
    base[-1], base[-2] = base[-2], base[-1]
    path.write_text("\n".join(base) + "\n", encoding="utf-8")
    with pytest.raises(TradingV4CIGateError, match="SHA-256 drifted"):
        validate_gate_contract(root)


def test_manifest_hash_constants_are_raw_byte_hashes():
    base = PROJECT_ROOT / "tests/manifests/trading_v234_regression_62.txt"
    core = PROJECT_ROOT / "tests/manifests/trading_core_compatibility_14.txt"
    research = PROJECT_ROOT / "tests/manifests/trading_v4_stage3_research_20.txt"
    assert hashlib.sha256(base.read_bytes()).hexdigest() == BASE_MANIFEST_SHA256
    assert hashlib.sha256(core.read_bytes()).hexdigest() == CORE_MANIFEST_SHA256
    assert (
        hashlib.sha256(research.read_bytes()).hexdigest()
        == RESEARCH_MANIFEST_SHA256
    )


def test_deploy_is_strictly_downstream_of_successful_ci_gate():
    source = WORKFLOW.read_text(encoding="utf-8")
    gate = _workflow_job(source, "trading-v4-ci-gate")
    deploy = _workflow_job(source, "deploy")

    assert "pull_request:" in source
    assert "permissions:\n  contents: read" in source
    assert "python -m pytest -q tests/test_trading_v4_ci_gate.py" in gate
    assert (
        "python tools/run_trading_v4_ci_gate.py --suite all" in gate
    )
    assert "appleboy/ssh-action" not in gate
    assert re.search(r"(?m)^    needs: trading-v4-ci-gate$", deploy)
    assert "needs.trading-v4-ci-gate.result == 'success'" in deploy
    assert "github.event_name == 'push'" in deploy
    assert "github.ref == 'refs/heads/main'" in deploy
    assert "uses: appleboy/ssh-action@v1.0.3" not in deploy
    assert deploy.count(
        "uses: appleboy/ssh-action@029f5b4aeeeb58fdfe1410a5d17f967dacf36262"
    ) == 1


def test_local_powershell_runner_delegates_to_the_same_gate_contract():
    source = LOCAL_RUNNER.read_text(encoding="utf-8")
    assert "run_trading_v4_ci_gate.py" in source
    assert '--suite $suite --repo-root $repoRoot' in source
    assert "$IncludeTradingCore -and $IncludeStage3" in source
    assert '"research"' in source
    assert '"all"' in source
    assert "Get-FileHash" not in source
