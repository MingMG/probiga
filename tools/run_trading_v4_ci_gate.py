#!/usr/bin/env python3
"""Fail-closed Stage 0-3 regression gate for Trading V4.

The Stage 0-2 manifests remain byte-for-byte frozen.  Stage 3 has a separate
reviewed manifest so research expansion cannot silently rewrite the accepted
V2/V3/V4 compatibility baseline.  CI validates all three and runs their union;
smaller suites remain available for local diagnostics only.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
from pathlib import Path, PurePosixPath
import subprocess
import sys
from typing import Sequence


BASE_MANIFEST = "tests/manifests/trading_v234_regression_62.txt"
CORE_MANIFEST = "tests/manifests/trading_core_compatibility_14.txt"
RESEARCH_MANIFEST = "tests/manifests/trading_v4_stage3_research_20.txt"
BASE_MANIFEST_COUNT = 62
CORE_MANIFEST_COUNT = 14
RESEARCH_MANIFEST_COUNT = 20
EXTENDED_MANIFEST_COUNT = 76
ALL_MANIFEST_COUNT = 96
BASE_MANIFEST_SHA256 = (
    "e2fcabf530efab10dd7724a6d742cd2976735c0e6783650219dd5fa1de85024f"
)
CORE_MANIFEST_SHA256 = (
    "30322345896fccf67c0473cf8d741ba0e41fd3ac79259ea435a88aa18ec73e93"
)
RESEARCH_MANIFEST_SHA256 = (
    "52c58824467f6acb04be75bd43792ed6998d2d880da812910895a732271c597c"
)
GIT_HEAD_TIMEOUT_SECONDS = 10
PYTEST_TIMEOUT_SECONDS = 30 * 60

# These are the minimum named safety contracts that must remain visible in the
# frozen manifests.  The manifest hashes protect the complete set; this list
# also produces an actionable error when a critical test is renamed or omitted.
REQUIRED_SAFETY_TESTS = (
    "tests/architecture/test_v4_artifact_namespace.py",
    "tests/architecture/test_v4_dependency_boundaries.py",
    "tests/architecture/test_v4_sql_allowlist.py",
    "tests/test_audit_trading_v4_runtime_grants.py",
    "tests/test_manage_v2_authority_registry.py",
    "tests/test_manage_v3_baseline_manifest.py",
    "tests/test_render_trading_v4_runtime_grants.py",
    "tests/test_trading_v4_baseline_epoch.py",
    "tests/test_trading_v4_control_plane.py",
    "tests/test_trading_v4_database_roles.py",
    "tests/test_trading_v4_job_lease_migration.py",
    "tests/test_trading_v4_job_store.py",
    "tests/test_trading_v4_mysql_acceptance.py",
    "tests/test_trading_v4_repository.py",
    "tests/test_trading_v4_runtime_control_concurrency.py",
    "tests/test_trading_v4_v3_baseline_manifest.py",
    "tests/test_v2_accounting_evidence_audit.py",
    "tests/test_v2_canonical_commit_coordinator.py",
    "tests/test_v2_canonical_prepared_commit_adapter.py",
    "tests/test_v2_canonical_read_facade.py",
    "tests/test_v2_execution_evidence_audit.py",
    "tests/test_v2_execution_evidence_authority.py",
    "tests/test_v2_execution_evidence_authority_audit.py",
    "tests/test_v2_execution_evidence_authority_registry_writer.py",
    "tests/test_v2_execution_evidence_schema_gate.py",
    "tests/test_v2_execution_evidence_writer.py",
    "tests/test_v3_execution_projection_outbox.py",
    "tests/test_trading_v3_mysql_acceptance.py",
    "tests/test_trading_core_protection.py",
    "tests/test_trading_core_risk_envelope.py",
    "tests/test_trading_core_session_gate.py",
    "tests/test_trading_core_snapshot_batch.py",
    "tests/test_trading_core_v2_accounting_compatibility.py",
    "tests/test_trading_core_v3_projection_subscriber.py",
    "tests/test_trading_core_waiting_reason_receipt.py",
)

REQUIRED_STAGE3_TESTS = (
    "tests/test_engines.py",
    "tests/test_hot_data_detail.py",
    "tests/test_multi_strategy_router.py",
    "tests/test_screener.py",
    "tests/test_sector_preheat.py",
    "tests/test_sim_trade_execution_atomic.py",
    "tests/test_sim_trade_rules.py",
    "tests/test_sim_trade_summary.py",
    "tests/test_stock_analysis_engine.py",
    "tests/test_strategy_center.py",
    "tests/test_sync_analysis_fast.py",
    "tests/test_sync_analysis_result.py",
    "tests/test_trading_dashboard_integration.py",
    "tests/test_trading_v2_execution_buy_gate.py",
    "tests/test_trading_v4_chase_risk.py",
    "tests/test_trading_v4_factor_store.py",
    "tests/test_trading_v4_pit_contracts.py",
    "tests/test_trading_v4_pit_sources.py",
    "tests/test_trading_v4_stage3_migration.py",
    "tests/test_unified_analysis_jobs.py",
)


class TradingV4CIGateError(RuntimeError):
    """Raised when a frozen gate input or invariant drifts."""


@dataclass(frozen=True, slots=True)
class ManifestContract:
    relative_path: str
    expected_count: int
    expected_sha256: str


@dataclass(frozen=True, slots=True)
class ManifestValidation:
    relative_path: str
    entries: tuple[str, ...]
    sha256: str


@dataclass(frozen=True, slots=True)
class GateValidation:
    base: ManifestValidation
    core: ManifestValidation
    research: ManifestValidation
    extended_entries: tuple[str, ...]
    all_entries: tuple[str, ...]


_BASE_CONTRACT = ManifestContract(
    relative_path=BASE_MANIFEST,
    expected_count=BASE_MANIFEST_COUNT,
    expected_sha256=BASE_MANIFEST_SHA256,
)
_CORE_CONTRACT = ManifestContract(
    relative_path=CORE_MANIFEST,
    expected_count=CORE_MANIFEST_COUNT,
    expected_sha256=CORE_MANIFEST_SHA256,
)
_RESEARCH_CONTRACT = ManifestContract(
    relative_path=RESEARCH_MANIFEST,
    expected_count=RESEARCH_MANIFEST_COUNT,
    expected_sha256=RESEARCH_MANIFEST_SHA256,
)


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _manifest_entries(
    repo_root: Path,
    contract: ManifestContract,
) -> ManifestValidation:
    manifest_path = repo_root / contract.relative_path
    if not manifest_path.is_file():
        raise TradingV4CIGateError(
            f"required regression manifest is missing: {contract.relative_path}"
        )
    raw = manifest_path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TradingV4CIGateError(
            f"regression manifest is not UTF-8: {contract.relative_path}"
        ) from exc
    entries = tuple(text.splitlines())
    if len(entries) != contract.expected_count:
        raise TradingV4CIGateError(
            f"regression manifest count drifted for {contract.relative_path}: "
            f"expected={contract.expected_count} actual={len(entries)}"
        )
    if len(set(entries)) != len(entries):
        raise TradingV4CIGateError(
            f"regression manifest contains duplicate paths: {contract.relative_path}"
        )

    root = repo_root.resolve()
    for entry in entries:
        if not entry or entry != entry.strip() or "\\" in entry:
            raise TradingV4CIGateError(
                f"regression manifest path is not canonical: {entry!r}"
            )
        relative = PurePosixPath(entry)
        if (
            relative.is_absolute()
            or not relative.parts
            or relative.parts[0] != "tests"
            or ".." in relative.parts
            or relative.suffix != ".py"
        ):
            raise TradingV4CIGateError(
                f"regression manifest path is outside tests/*.py: {entry!r}"
            )
        candidate = (repo_root / Path(*relative.parts)).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise TradingV4CIGateError(
                f"regression manifest path escapes the repository: {entry!r}"
            ) from exc
        if not candidate.is_file():
            raise TradingV4CIGateError(
                f"regression test file is missing: {entry}"
            )

    digest = hashlib.sha256(raw).hexdigest()
    return ManifestValidation(
        relative_path=contract.relative_path,
        entries=entries,
        sha256=digest,
    )


def validate_gate_contract(repo_root: Path | None = None) -> GateValidation:
    """Validate counts, uniqueness, paths, safety tests and frozen hashes."""

    root = (repo_root or _repository_root()).resolve()
    base = _manifest_entries(root, _BASE_CONTRACT)
    core = _manifest_entries(root, _CORE_CONTRACT)
    research = _manifest_entries(root, _RESEARCH_CONTRACT)
    extended = (*base.entries, *core.entries)
    if len(extended) != EXTENDED_MANIFEST_COUNT:
        raise TradingV4CIGateError(
            "extended regression manifest count drifted: "
            f"expected={EXTENDED_MANIFEST_COUNT} actual={len(extended)}"
        )
    if len(set(extended)) != len(extended):
        raise TradingV4CIGateError(
            "base and trading-core manifests contain overlapping test paths"
        )
    all_entries = (*extended, *research.entries)
    if len(all_entries) != ALL_MANIFEST_COUNT:
        raise TradingV4CIGateError(
            "all regression manifest count drifted: "
            f"expected={ALL_MANIFEST_COUNT} actual={len(all_entries)}"
        )
    if len(set(all_entries)) != len(all_entries):
        raise TradingV4CIGateError(
            "Stage 0-2 and Stage 3 manifests contain overlapping test paths"
        )
    missing = tuple(
        path for path in REQUIRED_SAFETY_TESTS if path not in extended
    )
    if missing:
        raise TradingV4CIGateError(
            "regression manifests omitted required safety tests: "
            + ", ".join(missing)
        )
    missing_stage3 = tuple(
        path for path in REQUIRED_STAGE3_TESTS if path not in research.entries
    )
    if missing_stage3:
        raise TradingV4CIGateError(
            "Stage 3 manifest omitted required research tests: "
            + ", ".join(missing_stage3)
        )
    for validation, contract in (
        (base, _BASE_CONTRACT),
        (core, _CORE_CONTRACT),
        (research, _RESEARCH_CONTRACT),
    ):
        if validation.sha256 != contract.expected_sha256:
            raise TradingV4CIGateError(
                f"regression manifest SHA-256 drifted for "
                f"{contract.relative_path}: expected={contract.expected_sha256} "
                f"actual={validation.sha256}"
            )
    return GateValidation(
        base=base,
        core=core,
        research=research,
        extended_entries=extended,
        all_entries=all_entries,
    )


def _git_head(repo_root: Path) -> str:
    try:
        result = subprocess.run(
            ("git", "-C", str(repo_root), "rev-parse", "HEAD"),
            check=True,
            capture_output=True,
            text=True,
            timeout=GIT_HEAD_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return "UNAVAILABLE"
    return result.stdout.strip() or "UNAVAILABLE"


def _run_pytest(repo_root: Path, entries: Sequence[str]) -> int:
    command = (sys.executable, "-m", "pytest", "-q", *entries)
    try:
        completed = subprocess.run(
            command,
            cwd=repo_root,
            check=False,
            timeout=PYTEST_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        print(
            f"trading_v4_ci_gate=FAILED error=pytest exceeded "
            f"{PYTEST_TIMEOUT_SECONDS}s timeout",
            file=sys.stderr,
        )
        return 124
    return int(completed.returncode)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate and run the frozen Trading V4 Stage 0-3 gate."
    )
    parser.add_argument(
        "--suite",
        choices=("base", "extended", "research", "all"),
        default="all",
        help="CI must use all; smaller suites are local diagnostics only.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the frozen contract without invoking pytest.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repo_root = (args.repo_root or _repository_root()).resolve()
    try:
        validation = validate_gate_contract(repo_root)
    except TradingV4CIGateError as exc:
        print(f"trading_v4_ci_gate=FAILED error={exc}", file=sys.stderr)
        return 2

    print("trading_v4_ci_gate=VALIDATED")
    print(f"git_head={_git_head(repo_root)}")
    print(f"base_manifest_sha256={validation.base.sha256}")
    print(f"core_manifest_sha256={validation.core.sha256}")
    print(f"research_manifest_sha256={validation.research.sha256}")
    print(f"base_test_files={len(validation.base.entries)}")
    print(f"core_test_files={len(validation.core.entries)}")
    print(f"research_test_files={len(validation.research.entries)}")
    print(f"extended_test_files={len(validation.extended_entries)}")
    print(f"all_test_files={len(validation.all_entries)}")
    if args.validate_only:
        return 0

    entries = {
        "base": validation.base.entries,
        "extended": validation.extended_entries,
        "research": validation.research.entries,
        "all": validation.all_entries,
    }[args.suite]
    return _run_pytest(repo_root, entries)


if __name__ == "__main__":
    raise SystemExit(main())
