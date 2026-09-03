from __future__ import annotations

from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
ROOT_BROKER = ROOT / "deploy" / "production_deploy_root.sh"
DEPLOY_ENGINE = ROOT / "deploy" / "production_deploy.sh"
RELEASE_BOUNDARY = ROOT / "tools" / "validate_production_release_boundary.py"
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


def test_completion_regression_inventory_is_unique_and_git_tracked() -> None:
    assert len(COMPLETION_GATES) == len(set(COMPLETION_GATES))
    tracked = subprocess.run(
        ["git", "ls-files", "--", "tests"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.splitlines()

    missing = sorted(
        path for path in COMPLETION_GATES if not (ROOT / path).is_file()
    )
    untracked = sorted(set(COMPLETION_GATES).difference(tracked))
    assert not missing
    assert not untracked


def test_root_broker_materializes_only_the_exact_trusted_main_engine() -> None:
    broker = ROOT_BROKER.read_text(encoding="utf-8")

    remote_tip = broker.index('REMOTE_SHA="$(clean_git_ssh ls-remote ')
    exact_tip = broker.index(
        'test "$REMOTE_SHA" = "$EXPECTED_SHA"', remote_tip
    )
    fetched_tip = broker.index(
        'rev-parse refs/remotes/origin/main)" = "$EXPECTED_SHA"',
        exact_tip,
    )
    materialize = broker.index(
        '"${GIT[@]}" show '
        '"${EXPECTED_SHA}:deploy/production_deploy.sh"',
        fetched_tip,
    )
    digest = broker.index("trusted deploy engine digest differs", materialize)
    protocol = broker.index(
        'PROBIGA_DEPLOY_PROTOCOL_VERSION="$DEPLOY_PROTOCOL_VERSION"',
        digest,
    )
    launch = broker.index(
        '/usr/bin/bash --noprofile --norc "$BOOTSTRAP_FILE"', protocol
    )

    assert remote_tip < exact_tip < fetched_tip < materialize < digest
    assert digest < protocol < launch
    assert 'EXPECTED_SHA="$EXPECTED_SHA"' in broker[protocol:launch]


def test_release_engine_runs_git_anchored_validation_before_cutover() -> None:
    engine = DEPLOY_ENGINE.read_text(encoding="utf-8")
    boundary = RELEASE_BOUNDARY.read_text(encoding="utf-8")

    validation = engine.index("tools/validate_production_release_boundary.py")
    git_anchor = engine.index(
        '--require-git-anchor --expected-git-sha "$EXPECTED_SHA"',
        validation,
    )
    sealed_checkout = engine.index(
        'seal_release_checkout "$STAGING_WORKTREE"', git_anchor
    )
    writer_fence = engine.index(
        "tools/add_trading_v3_tasks.py --fence-only", sealed_checkout
    )

    assert validation < git_anchor < sealed_checkout < writer_fence
    assert 'required.update(document["test_files"])' in boundary
    assert 'required.add(document["test_manifest_path"])' in boundary
    assert "protected release files differ from Git HEAD" in boundary
