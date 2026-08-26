from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from server.api import scheduler_runtime
from tools import ensure_quality_gate, setup_guojin_qmt_catalog
from tools.qmt_host_ownership_contract import (
    LINUX_PROVIDER_TASKS,
    LINUX_PROVIDER_TASK_TYPES,
    LINUX_QMT_TASKS,
    LINUX_QMT_TASK_TYPES,
    UNFROZEN_PROVIDER_SCRIPT_PATHS,
    UNFROZEN_PROVIDER_TASK_TYPES,
    WINDOWS_NON_QMT_EGRESS_TASK_TYPES,
    WINDOWS_QMT_EDGE_TASKS,
    WINDOWS_QMT_EDGE_TASK_TYPES,
)


WINDOWS_TASK_ROWS = tuple(dict(task) for task in WINDOWS_QMT_EDGE_TASKS)
LINUX_TASK_ROWS = tuple(
    dict(task) for task in (*LINUX_QMT_TASKS, *LINUX_PROVIDER_TASKS)
) + (
    {
        "task_type": "intraday_realtime",
        "script_path": "tools/crawl_realtime_batch.py",
    },
    {
        "task_type": "intraday_minute_kline",
        "script_path": "tools/crawl_minute_kline.py",
    },
    {
        "task_type": "intraday_minute_flow",
        "script_path": "tools/crawl_minute_kline.py",
    },
    {
        "task_type": "intraday_capital_flow_fast",
        "script_path": "tools/crawl_intraday_capital_flow_fast.py",
    },
)


def test_frozen_qmt_host_sets_are_exact_and_disjoint():
    assert WINDOWS_QMT_EDGE_TASK_TYPES == {
        "qmt_catalog_capability_refresh",
        "qmt_intraday_realtime",
        "qmt_membership_snapshot",
        "qmt_announcement_pit",
        "qmt_local_gap_repair_execute",
        "qmt_local_history_2024",
        "qmt_reference_incremental",
        "qmt_index_current",
        "qmt_index_kline",
        "qmt_index_minute",
        "qmt_stock_daily_canonical",
        "qmt_stock_minute_canonical",
        "qmt_stock_minute_flow_canonical",
        "qmt_canonical_history_gap_repair",
        "etf_forward_daily",
    }
    assert LINUX_QMT_TASK_TYPES == {
        "qmt_nightly_reconciliation",
        "qmt_gap_repair_plan",
    }
    assert not WINDOWS_QMT_EDGE_TASK_TYPES & LINUX_QMT_TASK_TYPES
    assert LINUX_PROVIDER_TASK_TYPES == {
        "linux_recent_data_gap_repair",
        "alist_daily",
        "alist_info",
        "eastmoney_concept_flow_snapshot",
        "eastmoney_concept_current",
        "eastmoney_concept_kline",
        "eastmoney_concept_minute",
        "sector_heat_east",
        "news_sync",
        "stock_dividend_baidu",
    }
    assert "intraday_realtime" not in WINDOWS_QMT_EDGE_TASK_TYPES
    assert "intraday_minute_kline" not in WINDOWS_QMT_EDGE_TASK_TYPES
    assert "intraday_capital_flow_fast" not in WINDOWS_QMT_EDGE_TASK_TYPES


@pytest.mark.parametrize("row", WINDOWS_TASK_ROWS, ids=lambda row: row["task_type"])
@pytest.mark.parametrize(
    ("platform_name", "expected_skip"),
    (("posix", True), ("nt", False)),
)
def test_windows_qmt_edge_tasks_have_bidirectional_exact_ownership(
    row,
    platform_name,
    expected_skip,
):
    assert scheduler_runtime.scheduler_task_host_owner(row) == "qmt_windows_edge"
    assert scheduler_runtime._should_skip_task_for_host(
        row, platform_name=platform_name
    ) is expected_skip


@pytest.mark.parametrize("row", LINUX_TASK_ROWS, ids=lambda row: row["task_type"])
@pytest.mark.parametrize(
    ("platform_name", "expected_skip"),
    (("posix", False), ("nt", True)),
)
def test_linux_and_eastmoney_tasks_have_bidirectional_exact_ownership(
    row,
    platform_name,
    expected_skip,
):
    assert scheduler_runtime.scheduler_task_host_owner(row) == "linux_standalone"
    assert scheduler_runtime._should_skip_task_for_host(
        row, platform_name=platform_name
    ) is expected_skip


@pytest.mark.parametrize("task_type", sorted(UNFROZEN_PROVIDER_TASK_TYPES))
@pytest.mark.parametrize("platform_name", ("posix", "nt"))
def test_provider_generic_legacy_tasks_fail_closed_on_both_hosts(
    task_type,
    platform_name,
):
    row = {"task_type": task_type, "script_path": "tools/run_single_table.py"}
    assert scheduler_runtime.scheduler_task_host_owner(row) == "unavailable"
    assert scheduler_runtime._should_skip_task_for_host(
        row, platform_name=platform_name
    ) is True


@pytest.mark.parametrize("script_path", sorted(UNFROZEN_PROVIDER_SCRIPT_PATHS))
@pytest.mark.parametrize("platform_name", ("posix", "nt"))
def test_provider_generic_script_aliases_fail_closed_on_both_hosts(
    script_path,
    platform_name,
):
    row = {"task_type": "unfrozen_alias", "script_path": script_path}
    assert scheduler_runtime.scheduler_task_host_owner(row) == "unavailable"
    assert scheduler_runtime._should_skip_task_for_host(
        row, platform_name=platform_name
    ) is True


@pytest.mark.parametrize(
    "row",
    (
        {},
        {"task_type": "qmt_unknown_future_task"},
        {
            "task_type": "qmt_catalog_capability_refresh",
            "script_path": "tools/nightly_guojin_qmt_reconciliation.py",
        },
        {
            "task_type": "unknown_alias",
            "script_path": "tools/setup_guojin_qmt_catalog.py",
        },
        {
            "task_type": "unknown_alias",
            "script_path": "tools/sync_qmt_primary.py",
        },
        {"task_type": "unknown_alias", "group_name": "Guojin QMT"},
    ),
)
def test_missing_unknown_or_drifted_qmt_identity_is_unavailable(row):
    assert scheduler_runtime.scheduler_task_host_owner(row) == "unavailable"
    assert scheduler_runtime._should_skip_task_for_host(
        row, platform_name="posix"
    ) is True
    assert scheduler_runtime._should_skip_task_for_host(
        row, platform_name="nt"
    ) is True


def test_xueqiu_windows_egress_is_not_mislabeled_as_qmt():
    row = {
        "task_type": "fetch_hot_rank_xq",
        "script_path": "tools/fetch_hot_rank_xq.py",
    }
    assert WINDOWS_NON_QMT_EGRESS_TASK_TYPES == {"fetch_hot_rank_xq"}
    assert "fetch_hot_rank_xq" not in scheduler_runtime.WINDOWS_QMT_BRIDGE_TASK_TYPES
    assert scheduler_runtime.scheduler_task_host_owner(row) == "windows_non_qmt_egress"
    assert scheduler_runtime._should_skip_task_for_host(row, platform_name="posix")
    assert not scheduler_runtime._should_skip_task_for_host(row, platform_name="nt")


def test_quality_gate_installs_every_frozen_qmt_task_exactly_once():
    installed = {}
    for task in ensure_quality_gate.TASKS:
        task_type = str(task["task_type"])
        if task_type in WINDOWS_QMT_EDGE_TASK_TYPES | LINUX_QMT_TASK_TYPES:
            assert task_type not in installed
            installed[task_type] = task
    expected = {
        str(task["task_type"]): task
        for task in (*WINDOWS_QMT_EDGE_TASKS, *LINUX_QMT_TASKS)
    }
    assert installed == expected


def test_quality_gate_installs_exact_linux_provider_tasks_once():
    installed = [
        task for task in ensure_quality_gate.TASKS
        if task["task_type"] in LINUX_PROVIDER_TASK_TYPES
    ]
    assert installed == [dict(task) for task in LINUX_PROVIDER_TASKS]


def test_provider_contract_rejects_persisted_argument_drift():
    task = next(
        dict(item) for item in WINDOWS_QMT_EDGE_TASKS
        if item["task_type"] == "qmt_index_minute"
    )
    task["script_args"] = "--dataset minute --apply --json"
    assert scheduler_runtime.scheduler_task_host_owner(task) == "unavailable"

    concept = dict(LINUX_PROVIDER_TASKS[0])
    concept["script_args"] = "--json"
    assert scheduler_runtime.scheduler_task_host_owner(concept) == "unavailable"


def test_runtime_catalog_task_contains_no_privileged_ddl_or_seed_path():
    source = inspect.getsource(setup_guojin_qmt_catalog)
    assert "privileged_migrate_catalog_schema" not in source
    assert "privileged_seed_catalog_registry" not in source
    assert "CREATE TABLE" not in source.upper()
    assert "ALTER TABLE" not in source.upper()
    task = next(
        item for item in ensure_quality_gate.TASKS
        if item["task_type"] == "qmt_catalog_capability_refresh"
    )
    assert task["script_path"] == "tools/setup_guojin_qmt_catalog.py"
    assert task["script_args"] == ""


def test_quality_gate_schema_guard_is_select_only_runtime_validation(monkeypatch):
    calls = []
    engine = object()
    monkeypatch.setattr(
        ensure_quality_gate,
        "validate_scheduler_columns",
        lambda value, **kwargs: calls.append((value, kwargs)) or {"id"},
    )

    assert ensure_quality_gate.ensure_scheduler_columns(engine) is None
    assert calls == [(
        engine,
        {
            "table_name": "st_scheduled_tasks",
            "column_definitions": ensure_quality_gate.SCHEDULER_COLUMNS,
        },
    )]
    source = inspect.getsource(ensure_quality_gate.ensure_scheduler_columns)
    assert "ALTER TABLE" not in source.upper()
    assert "CREATE TABLE" not in source.upper()


def test_windows_scheduler_process_freezes_clean_main_build_identity():
    wrapper = (
        Path(__file__).resolve().parents[1]
        / "tools"
        / "run_local_scheduler_task.ps1"
    ).read_text(encoding="utf-8")
    assert 'symbolic-ref", "--short", "HEAD"' in wrapper
    assert 'status", "--porcelain", "--untracked-files=normal"' in wrapper
    assert '$env:PROBIGA_BUILD_COMMIT_SHA = $BuildSha' in wrapper
    assert '$env:PROBIGA_EXPECTED_GIT_SHA = $BuildSha' in wrapper
    assert '$env:PROBIGA_SCHEDULER_EXECUTOR_ROLE = "qmt_windows_edge"' in wrapper
