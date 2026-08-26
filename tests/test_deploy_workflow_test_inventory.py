from __future__ import annotations

from collections import Counter
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"
TEST_PATH_RE = re.compile(r"tests/[A-Za-z0-9_./-]+\.py")
COMPLETION_GATES = (
    "tests/test_account_auth.py",
    "tests/test_admin_auth.py",
    "tests/test_ai_execution_safety.py",
    "tests/test_ai_output_physical_schema.py",
    "tests/test_auth_ai_bridge_runtime_schema.py",
    "tests/test_auxiliary_runtime_schema_boundary.py",
    "tests/test_capital_flow_release_contract.py",
    "tests/test_concept_flow_snapshot_contract.py",
    "tests/test_concept_current_snapshot_safety.py",
    "tests/test_concept_map_runtime_boundary.py",
    "tests/test_daily_derived_date_contract.py",
    "tests/test_daily_stock_universe_coverage.py",
    "tests/test_datasource_scheduler_launch.py",
    "tests/test_deploy_workflow_test_inventory.py",
    "tests/test_dividend_baidu_pipeline.py",
    "tests/test_eastmoney_alist_exact.py",
    "tests/test_etf_dividend_scheduler_contract.py",
    "tests/test_etf_formal_pipeline.py",
    "tests/test_formal_provider_scheduler_receipts.py",
    "tests/test_hot_data_scheduler_launch.py",
    "tests/test_hot_rank_api_safety.py",
    "tests/test_hot_rank_scheduler_chain.py",
    "tests/test_hot_rank_source_contract.py",
    "tests/test_hot_rank_runtime_schema_boundary.py",
    "tests/test_import_gm_minute_runtime_boundary.py",
    "tests/test_legacy_market_sync_atomic.py",
    "tests/test_legacy_table_surface.py",
    "tests/test_linux_recent_data_gap_repair.py",
    "tests/test_main_strategy_pool_historical_fallback_ui.py",
    "tests/test_manual_long_task_enqueue.py",
    "tests/test_market_collector_runtime_boundary.py",
    "tests/test_merge_hot_rank_source_contract.py",
    "tests/test_notice_event_publication_time.py",
    "tests/test_notice_sim_scheduler_receipts.py",
    "tests/test_notice_sync_cli_status.py",
    "tests/test_production_runtime_schema_bundle.py",
    "tests/test_qmt_control_schema.py",
    "tests/test_qmt_canonical_history_gap_repair.py",
    "tests/test_qmt_edge_release_bootstrap.py",
    "tests/test_qmt_historical_contract_discovery.py",
    "tests/test_qmt_history_capabilities.py",
    "tests/test_qmt_history_coverage.py",
    "tests/test_qmt_host_ownership.py",
    "tests/test_qmt_index_edge_task.py",
    "tests/test_qmt_local_history_runtime_schema_boundary.py",
    "tests/test_qmt_minute_flow_exact.py",
    "tests/test_qmt_minute_formal_publisher.py",
    "tests/test_qmt_operations_task_contract.py",
    "tests/test_qmt_reference_index_atomic_publish.py",
    "tests/test_qmt_stock_edge_task.py",
    "tests/test_realtime_quote_schema_boundary.py",
    "tests/test_release_data_readiness.py",
    "tests/test_release_data_readiness_waiter.py",
    "tests/test_required_data_scheduler_contract.py",
    "tests/test_runtime_schema_separation.py",
    "tests/test_scheduler_args_analysis_date.py",
    "tests/test_scheduler_runtime_health.py",
    "tests/test_scheduler_task_history_schema.py",
    "tests/test_scheduler_task_schema_boundary.py",
    "tests/test_schema_recovery_evidence.py",
    "tests/test_sector_heat_east_formal.py",
    "tests/test_sentiment_atomic_refresh.py",
    "tests/test_sim_trade_cli_contract.py",
    "tests/test_sim_trade_runtime_schema.py",
    "tests/test_stock_market_atomic_refresh.py",
    "tests/test_strategy_center_runtime_schema.py",
    "tests/test_sync_eastmoney_concept_market.py",
    "tests/test_sync_finance_cli_status.py",
    "tests/test_sync_news_formal.py",
    "tests/test_sync_stock_info_atomic_refresh.py",
    "tests/test_ths_hot_sync_contract.py",
    "tests/test_trading_v3_empty_forecast_contract.py",
)


def _workflow_source() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def _pytest_batches(source: str) -> list[list[str]]:
    lines = source.splitlines()
    batches: list[list[str]] = []
    for index, line in enumerate(lines):
        if line.strip() != "python -m pytest -q":
            continue
        batch: list[str] = []
        for candidate in lines[index + 1 :]:
            value = candidate.strip()
            if not TEST_PATH_RE.fullmatch(value):
                break
            batch.append(value)
        batches.append(batch)
    return batches


def test_completion_regressions_are_each_exercised_once_by_ci() -> None:
    references = TEST_PATH_RE.findall(_workflow_source())
    counts = Counter(references)

    assert set(COMPLETION_GATES).issubset(counts)
    assert {path: counts[path] for path in COMPLETION_GATES} == {
        path: 1 for path in COMPLETION_GATES
    }
    assert not {path: count for path, count in counts.items() if count != 1}


def test_workflow_test_references_are_files_tracked_by_git() -> None:
    references = set(TEST_PATH_RE.findall(_workflow_source()))
    tracked = subprocess.run(
        ["git", "ls-files", "--", "tests"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.splitlines()

    missing = sorted(path for path in references if not (ROOT / path).is_file())
    untracked = sorted(references.difference(tracked))
    assert not missing
    assert not untracked


def test_each_workflow_pytest_batch_contains_at_most_ten_files() -> None:
    source = _workflow_source()
    batches = _pytest_batches(source)
    batched_references = [path for batch in batches for path in batch]

    assert batches
    assert all(batch for batch in batches)
    assert batched_references == TEST_PATH_RE.findall(source)
    assert max(map(len, batches)) <= 10
