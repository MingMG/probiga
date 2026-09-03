import pytest

from server.common.scheduler_args import build_scheduler_task_args


def test_analysis_fast_receives_target_date_even_with_other_script_args():
    row = {
        "task_type": "analysis_fast",
        "script_args": "--top-n 80 --min-score 62",
        "date_param": "",
        "_scheduler_execution_time": "2026-08-27T18:50:00",
    }

    assert build_scheduler_task_args(
        row,
        "biz/analysis/sync_analysis_fast.py",
        "2026-08-26",
    ) == [
        "--top-n", "80", "--min-score", "62", "--date", "2026-08-26",
        "--execution-time", "2026-08-27T18:50:00",
    ]


def test_final_pool_delivery_receives_only_scheduler_bound_trade_date():
    row = {
        "task_type": "final_pool_wecom_delivery",
        "script_args": "--json",
        "date_param": "",
        "_scheduler_target_trade_date": "2026-09-02",
    }
    assert build_scheduler_task_args(
        row,
        "tools/send_final_pool_wecom.py",
        "2026-09-02",
    ) == ["--json", "--trade-date", "2026-09-02"]

    with pytest.raises(ValueError, match="differs"):
        build_scheduler_task_args(
            {**row, "script_args": "--json --trade-date 2026-09-01"},
            "tools/send_final_pool_wecom.py",
            "2026-09-02",
        )


def test_analysis_fast_preserves_explicit_split_date():
    row = {
        "task_type": "analysis_fast",
        "script_args": "--date 2026-08-25 --top-n 80",
        "date_param": "",
        "_scheduler_execution_time": "2026-08-27T18:50:00",
    }

    assert build_scheduler_task_args(
        row,
        "biz/analysis/sync_analysis_fast.py",
        "2026-08-26",
    ) == [
        "--date", "2026-08-25", "--top-n", "80",
        "--execution-time", "2026-08-27T18:50:00",
    ]


def test_analysis_fast_preserves_explicit_equals_date():
    row = {
        "task_type": "analysis_fast",
        "script_args": "--date=2026-08-25 --top-n 80",
        "date_param": "",
        "_scheduler_execution_time": "2026-08-27T18:50:00",
    }

    assert build_scheduler_task_args(
        row,
        "biz/analysis/sync_analysis_fast.py",
        "2026-08-26",
    ) == [
        "--date=2026-08-25", "--top-n", "80",
        "--execution-time", "2026-08-27T18:50:00",
    ]


def test_release_catchup_analysis_fast_uses_authoritative_closed_target():
    row = {
        "task_type": "analysis_fast",
        "script_args": "--top-n 80 --min-score 62 --json",
        "date_param": "",
        "_trigger_source": "release_catchup",
        "_scheduler_execution_time": "2026-08-26T22:20:00",
        "_scheduler_pipeline_target_date": "2026-08-26",
        "_scheduler_pipeline_decision_at": "2026-08-26T22:20:00",
    }

    assert build_scheduler_task_args(
        row,
        "tools/run_ai_recommendation_premarket.py",
        "2026-08-26",
    ) == [
        "--top-n",
        "80",
        "--min-score",
        "62",
        "--json",
        "--date",
        "2026-08-26",
        "--execution-time",
        "2026-08-26T22:20:00",
    ]


@pytest.mark.parametrize(
    ("task_type", "script_args", "expected_prefix"),
    (
        (
            "target_turnover_snapshot",
            "--checkpoint-file /var/lib/probiga/jobs/"
            "target-turnover-snapshot-v1.json",
            [
                "--checkpoint-file",
                "/var/lib/probiga/jobs/target-turnover-snapshot-v1.json",
            ],
        ),
        (
            "analysis_upper_evidence_prepare",
            "--prepare-preliminary --min-score 62",
            ["--prepare-preliminary", "--min-score", "62"],
        ),
    ),
)
def test_release_daily_evidence_binds_one_target_and_formal_cutoff(
    task_type,
    script_args,
    expected_prefix,
):
    row = {
        "task_type": task_type,
        "script_args": script_args,
        "date_param": "",
        "_trigger_source": "release_catchup",
        "_scheduler_execution_time": "2026-08-27T22:20:00",
        "_scheduler_pipeline_target_date": "2026-08-27",
        "_scheduler_pipeline_decision_at": "2026-08-27T22:20:00",
    }
    assert build_scheduler_task_args(
        row,
        (
            "tools/sync_upper_limit_snapshot.py"
            if task_type == "analysis_upper_evidence_prepare"
            else "tools/sync_target_turnover_snapshot.py"
        ),
        "2026-08-27",
    ) == [
        *expected_prefix,
        "--target-date",
        "2026-08-27",
        "--decision-at",
        "2026-08-27T22:20:00",
    ]


def test_release_daily_pipeline_rejects_target_or_cutoff_drift():
    evidence = {
        "task_type": "target_turnover_snapshot",
        "script_args": "",
        "date_param": "",
        "_trigger_source": "release_catchup",
        "_scheduler_pipeline_target_date": "2026-08-26",
        "_scheduler_pipeline_decision_at": "2026-08-27T22:20:00",
    }
    with pytest.raises(ValueError, match="target date differs"):
        build_scheduler_task_args(
            evidence,
            "tools/sync_target_turnover_snapshot.py",
            "2026-08-27",
        )

    analysis = {
        "task_type": "analysis_fast",
        "script_args": "--top-n 80 --json",
        "date_param": "",
        "_trigger_source": "release_catchup",
        "_scheduler_execution_time": "2026-08-27T22:19:59",
        "_scheduler_pipeline_target_date": "2026-08-27",
        "_scheduler_pipeline_decision_at": "2026-08-27T22:20:00",
    }
    with pytest.raises(ValueError, match="cutoff differs"):
        build_scheduler_task_args(
            analysis,
            "tools/run_ai_recommendation_premarket.py",
            "2026-08-27",
        )


def test_release_catchup_morning_strict_binds_target_without_changing_normal_run():
    row = {
        "task_type": "analysis_morning_strict",
        "script_args": "--strict-prev-trade-day --top-n 80 --json",
        "date_param": "",
        "_scheduler_execution_time": "2026-08-27T08:30:00",
    }
    ordinary = build_scheduler_task_args(
        row,
        "tools/run_ai_recommendation_premarket.py",
        "2026-08-27",
    )
    assert ordinary == [
        "--strict-prev-trade-day", "--top-n", "80", "--json",
        "--execution-time", "2026-08-27T08:30:00",
    ]

    release = build_scheduler_task_args(
        {
            **row,
            "_trigger_source": "release_catchup",
            "_release_execution_time": "2026-08-27T03:05:00",
            "_scheduler_execution_time": "2026-08-27T03:05:00",
        },
        "tools/run_ai_recommendation_premarket.py",
        "2026-08-26",
    )
    assert release == [
        "--strict-prev-trade-day",
        "--top-n",
        "80",
        "--json",
        "--execution-time",
        "2026-08-27T03:05:00",
    ]


def test_release_catchup_rejects_drifted_explicit_analysis_date():
    row = {
        "task_type": "analysis_fast",
        "script_args": "--date 2026-08-27 --json",
        "date_param": "",
        "_trigger_source": "release_catchup",
        "_scheduler_execution_time": "2026-08-27T18:50:00",
    }
    with pytest.raises(ValueError, match="authoritative target"):
        build_scheduler_task_args(
            row,
            "tools/run_ai_recommendation_premarket.py",
            "2026-08-26",
        )


def test_analysis_execution_time_is_required_and_cannot_drift():
    base = {
        "task_type": "analysis_premarket_external",
        "script_args": "--strict-prev-trade-day --json",
        "date_param": "",
    }
    with pytest.raises(ValueError, match="unavailable"):
        build_scheduler_task_args(
            base,
            "tools/run_ai_recommendation_premarket.py",
            "2026-08-27",
        )
    with pytest.raises(ValueError, match="differs"):
        build_scheduler_task_args(
            {
                **base,
                "script_args": (
                    "--strict-prev-trade-day --json --execution-time "
                    "2026-08-27T01:07:00"
                ),
                "_scheduler_execution_time": "2026-08-27T09:07:00",
            },
            "tools/run_ai_recommendation_premarket.py",
            "2026-08-27",
        )


def test_stock_snapshot_receives_target_date_with_other_args():
    row = {
        "task_type": "stock_snapshot_daily",
        "script_args": "",
        "date_param": "",
    }

    assert build_scheduler_task_args(
        row,
        "biz/stock_market/sync_stock_snapshot.py",
        "2026-08-26",
    ) == ["--date", "2026-08-26"]


def test_stock_snapshot_preserves_explicit_date():
    row = {
        "task_type": "stock_snapshot_daily",
        "script_args": "--date=2026-08-25",
        "date_param": "",
    }

    assert build_scheduler_task_args(
        row,
        "biz/stock_market/sync_stock_snapshot.py",
        "2026-08-26",
    ) == ["--date=2026-08-25"]


def test_release_stock_snapshot_rejects_drifted_explicit_date():
    row = {
        "task_type": "stock_snapshot_daily",
        "script_args": "--date=2026-08-25",
        "date_param": "",
        "_trigger_source": "release_catchup",
    }

    with pytest.raises(ValueError, match="authoritative target"):
        build_scheduler_task_args(
            row,
            "biz/stock_market/sync_stock_snapshot.py",
            "2026-08-26",
        )


def test_market_overview_receives_positional_target_date():
    row = {
        "task_type": "market_overview_daily",
        "script_args": "",
        "date_param": "",
    }

    assert build_scheduler_task_args(
        row,
        "tools/refresh_market_overview_daily.py",
        "2026-08-26",
    ) == ["2026-08-26"]


def test_market_overview_preserves_explicit_history_range():
    row = {
        "task_type": "market_overview_daily",
        "script_args": "--start-date 2026-08-24 --end-date=2026-08-25",
        "date_param": "",
    }

    assert build_scheduler_task_args(
        row,
        "tools/refresh_market_overview_daily.py",
        "2026-08-26",
    ) == ["--start-date", "2026-08-24", "--end-date=2026-08-25"]


def test_market_overview_preserves_explicit_positional_date():
    row = {
        "task_type": "market_overview_daily",
        "script_args": "2026-08-25",
        "date_param": "",
    }

    assert build_scheduler_task_args(
        row,
        "tools/refresh_market_overview_daily.py",
        "2026-08-26",
    ) == ["2026-08-25"]


def test_release_market_overview_rejects_range_or_drifted_date():
    base = {
        "task_type": "market_overview_daily",
        "date_param": "",
        "_trigger_source": "release_catchup",
    }
    with pytest.raises(ValueError, match="authoritative target"):
        build_scheduler_task_args(
            {**base, "script_args": "2026-08-25"},
            "tools/refresh_market_overview_daily.py",
            "2026-08-26",
        )
    with pytest.raises(ValueError, match="one authoritative target"):
        build_scheduler_task_args(
            {
                **base,
                "script_args": "--start-date 2026-08-25 --end-date 2026-08-26",
            },
            "tools/refresh_market_overview_daily.py",
            "2026-08-26",
        )


def test_release_v3_close_is_bound_to_closed_date_and_replay_only_clock():
    row = {
        "task_type": "trading_v3_close_decision",
        "script_args": (
            "--mode close --universe-limit 1200 --per-sleeve-limit 300"
        ),
        "date_param": "",
    }
    ordinary = build_scheduler_task_args(
        row,
        "tools/run_trading_v3_decision.py",
        "2026-08-26",
    )
    args = build_scheduler_task_args(
        {**row, "_trigger_source": "release_catchup"},
        "tools/run_trading_v3_decision.py",
        "2026-08-26",
    )

    assert ordinary == [
        "--mode",
        "close",
        "--universe-limit",
        "1200",
        "--per-sleeve-limit",
        "300",
    ]
    assert args[-5:] == [
        "--as-of",
        "2026-08-26",
        "--decision-at",
        "2026-08-26T16:05:00",
        "--replay-only",
    ]


def test_release_signal_prepare_and_fused_hot_rank_bind_current_date():
    signal_args = build_scheduler_task_args(
        {
            "task_type": "sim_trade_signal_prepare",
            "script_args": "--prepare-signals --reset --json",
            "date_param": "",
            "_trigger_source": "release_catchup",
        },
        "biz/analysis/sync_sim_trade.py",
        "2026-08-27",
    )
    fused_args = build_scheduler_task_args(
        {
            "task_type": "hot_fused",
            "script_args": "--top 100",
            "date_param": "",
            "_trigger_source": "release_catchup",
        },
        "tools/merge_hot_rank.py",
        "2026-08-27",
    )

    assert signal_args[-2:] == ["--trade-date", "2026-08-27"]
    assert fused_args == ["2026-08-27", "--top", "100"]


def test_release_fused_hot_rank_rejects_non_positional_or_wrong_target_date():
    base = {
        "task_type": "hot_fused",
        "date_param": "",
        "_trigger_source": "release_catchup",
    }

    with pytest.raises(ValueError, match="requires a positional date"):
        build_scheduler_task_args(
            {**base, "script_args": "--top 100 --date 2026-08-27"},
            "tools/merge_hot_rank.py",
            "2026-08-27",
        )

    with pytest.raises(ValueError, match="differs from current target"):
        build_scheduler_task_args(
            {**base, "script_args": "2026-08-26 --top 100"},
            "tools/merge_hot_rank.py",
            "2026-08-27",
        )


@pytest.mark.parametrize(
    ("task_type", "dataset"),
    (
        ("qmt_stock_daily_canonical", "daily"),
        ("qmt_stock_minute_canonical", "minute"),
        ("qmt_index_current", "current"),
        ("qmt_index_kline", "kline"),
        ("qmt_index_minute", "minute"),
    ),
)
def test_release_qmt_range_tasks_replace_latest_with_exact_target(
    task_type,
    dataset,
):
    row = {
        "task_type": task_type,
        "script_args": f"--dataset {dataset} --latest-session --apply --json",
        "date_param": "",
    }
    ordinary = build_scheduler_task_args(
        row,
        "tools/sync_qmt_edge.py",
        "2026-08-27",
    )
    release = build_scheduler_task_args(
        {**row, "_trigger_source": "release_catchup"},
        "tools/sync_qmt_edge.py",
        "2026-08-26",
    )

    assert ordinary == [
        "--dataset",
        dataset,
        "--latest-session",
        "--apply",
        "--json",
    ]
    assert release == [
        "--dataset",
        dataset,
        "--apply",
        "--json",
        "--start-date",
        "2026-08-26",
        "--end-date",
        "2026-08-26",
    ]


def test_release_membership_uses_read_only_exact_snapshot_verification():
    row = {
        "task_type": "qmt_membership_snapshot",
        "script_args": "--apply --force-reference-refresh --json",
        "date_param": "",
    }

    assert build_scheduler_task_args(
        row,
        "tools/sync_bigqmt_reference.py",
        "2026-08-27",
    ) == ["--apply", "--force-reference-refresh", "--json"]
    assert build_scheduler_task_args(
        {**row, "_trigger_source": "release_catchup"},
        "tools/sync_bigqmt_reference.py",
        "2026-08-26",
    ) == [
        "--verify-existing-snapshot",
        "--snapshot-date",
        "2026-08-26",
        "--json",
    ]

    assert build_scheduler_task_args(
        {
            **row,
            "_trigger_source": "scheduled",
            "_scheduler_target_trade_date": "2026-08-26",
            "_scheduler_historical_recovery": True,
        },
        "tools/sync_bigqmt_reference.py",
        "2026-08-26",
    ) == [
        "--verify-existing-snapshot",
        "--snapshot-date",
        "2026-08-26",
        "--json",
    ]


def test_historical_membership_requires_exact_scheduler_target_binding():
    with pytest.raises(ValueError, match="target date is unavailable"):
        build_scheduler_task_args(
            {
                "task_type": "qmt_membership_snapshot",
                "script_args": "--apply --force-reference-refresh --json",
                "date_param": "",
                "_trigger_source": "scheduled",
                "_scheduler_target_trade_date": "2026-08-25",
                "_scheduler_historical_recovery": True,
            },
            "tools/sync_bigqmt_reference.py",
            "2026-08-26",
        )


def test_release_membership_rejects_publish_or_argument_drift():
    base = {
        "task_type": "qmt_membership_snapshot",
        "date_param": "",
        "_trigger_source": "release_catchup",
    }

    with pytest.raises(ValueError, match="arguments differ from contract"):
        build_scheduler_task_args(
            {**base, "script_args": "--apply --json"},
            "tools/sync_bigqmt_reference.py",
            "2026-08-26",
        )
    with pytest.raises(ValueError, match="target date is invalid"):
        build_scheduler_task_args(
            {
                **base,
                "script_args": "--apply --force-reference-refresh --json",
            },
            "tools/sync_bigqmt_reference.py",
            "2026-08-26T00:00:00",
        )


def test_release_qmt_minute_flow_replaces_latest_with_exact_target():
    row = {
        "task_type": "qmt_stock_minute_flow_canonical",
        "script_args": "--latest-session --apply --json",
        "date_param": "",
    }

    assert build_scheduler_task_args(
        row,
        "tools/sync_qmt_minute_flow_exact.py",
        "2026-08-27",
    ) == ["--latest-session", "--apply", "--json"]
    assert build_scheduler_task_args(
        {**row, "_trigger_source": "release_catchup"},
        "tools/sync_qmt_minute_flow_exact.py",
        "2026-08-26",
    ) == [
        "--apply",
        "--json",
        "--trade-date",
        "2026-08-26",
    ]


@pytest.mark.parametrize(
    ("task_type", "script_args"),
    (
        ("etf_forward_daily", "--execute"),
        ("eastmoney_concept_current", "--dataset current --json"),
        ("eastmoney_concept_kline", "--dataset kline --json"),
        ("eastmoney_concept_minute", "--dataset minute --json"),
        (
            "eastmoney_concept_flow_snapshot",
            "--strict-authority --json",
        ),
    ),
)
def test_release_close_provider_tasks_receive_exact_trade_date(
    task_type,
    script_args,
):
    ordinary = build_scheduler_task_args(
        {
            "task_type": task_type,
            "script_args": script_args,
            "date_param": "",
        },
        "tools/provider.py",
        "2026-08-27",
    )
    release = build_scheduler_task_args(
        {
            "task_type": task_type,
            "script_args": script_args,
            "date_param": "",
            "_trigger_source": "release_catchup",
        },
        "tools/provider.py",
        "2026-08-26",
    )

    assert ordinary == script_args.split()
    assert release == [*script_args.split(), "--trade-date", "2026-08-26"]


@pytest.mark.parametrize("task_type", ("alist_daily", "alist_info"))
def test_release_alist_replaces_latest_session_with_exact_trade_date(task_type):
    row = {
        "task_type": task_type,
        "script_args": "--dataset daily --latest-session --apply --json",
        "date_param": "",
    }

    assert build_scheduler_task_args(
        {**row, "_trigger_source": "release_catchup"},
        "tools/sync_eastmoney_alist_exact.py",
        "2026-08-26",
    ) == [
        "--dataset",
        "daily",
        "--apply",
        "--json",
        "--trade-date",
        "2026-08-26",
    ]


def test_release_sector_heat_receives_exact_positional_date():
    row = {
        "task_type": "sector_heat_east",
        "script_args": "--formal --json",
        "date_param": "",
        "_trigger_source": "release_catchup",
    }

    assert build_scheduler_task_args(
        row,
        "tools/fetch_sector_heat_east_daily.py",
        "2026-08-26",
    ) == ["--formal", "--json", "2026-08-26"]


def test_stock_finance_daily_and_release_runs_bind_the_target_date():
    row = {
        "task_type": "stock_finance",
        "script_args": (
            "--daily-incremental --workers 4 --sleep 0.3 "
            "--min-code-coverage 1.0 --checkpoint-file /tmp/finance.json"
        ),
        "date_param": "",
    }

    expected = row["script_args"].split() + ["--as-of-date", "2026-08-26"]
    assert build_scheduler_task_args(
        row,
        "biz/stock_finance/sync_finance.py",
        "2026-08-26",
    ) == expected
    assert build_scheduler_task_args(
        {**row, "_trigger_source": "release_catchup"},
        "biz/stock_finance/sync_finance.py",
        "2026-08-26",
    ) == expected
    assert build_scheduler_task_args(
        {**row, "_scheduler_target_trade_date": "2026-08-26"},
        "biz/stock_finance/sync_finance.py",
        "2026-08-26",
    ) == expected


@pytest.mark.parametrize(
    ("task_type", "script_args"),
    (
        (
            "qmt_stock_daily_canonical",
            "--dataset daily --start-date 2026-08-26 --end-date 2026-08-26 --apply --json",
        ),
        (
            "qmt_stock_minute_flow_canonical",
            "--trade-date 2026-08-26 --apply --json",
        ),
    ),
)
def test_release_qmt_target_rewrite_fails_closed_on_scheduler_row_drift(
    task_type,
    script_args,
):
    with pytest.raises(ValueError, match="latest-session selector"):
        build_scheduler_task_args(
            {
                "task_type": task_type,
                "script_args": script_args,
                "date_param": "",
                "_trigger_source": "release_catchup",
            },
            "tools/qmt.py",
            "2026-08-26",
        )
