from __future__ import annotations

import pandas as pd
import pytest

from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS
from tools import merge_hot_rank


def _rank_frame(
    snapshot_date: str,
    *,
    rows: int = 100,
    code_start: int = 600000,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "snapshot_date": snapshot_date,
                "rank": rank,
                "stock_code": f"{code_start + rank - 1:06d}",
                "short_name": f"sample-{rank}",
                "change_pct": 1.0,
            }
            for rank in range(1, rows + 1)
        ]
    )


def _east_frame(snapshot_date: str, *, rows: int = 100) -> pd.DataFrame:
    return _rank_frame(snapshot_date, rows=rows, code_start=600000)


def _ths_frame(snapshot_date: str, *, rows: int = 100) -> pd.DataFrame:
    return _rank_frame(snapshot_date, rows=rows, code_start=600000)


def _legacy_frame(snapshot_date: str, *, code_start: int) -> pd.DataFrame:
    return _rank_frame(snapshot_date, rows=100, code_start=code_start)


def test_read_day_data_never_falls_back_to_an_older_east_snapshot(monkeypatch):
    statements: list[str] = []

    def fake_read_frame(statement, engine, params=None):
        statements.append(str(statement))
        return pd.DataFrame()

    monkeypatch.setattr(merge_hot_rank, "read_frame", fake_read_frame)

    frames = merge_hot_rank._read_day_data(object(), "2026-08-26")

    assert len(frames) == 4
    assert all(frame.empty for frame in frames)
    assert len(statements) == 2
    assert all("snapshot_date = :d" in statement for statement in statements)
    assert all("snapshot_date <= :d" not in statement for statement in statements)
    assert all("st_hot_rank_xq" not in statement for statement in statements)
    assert all("st_hot_rank_sina" not in statement for statement in statements)


def test_fusion_requires_two_exact_date_sources():
    empty = pd.DataFrame()

    with pytest.raises(
        RuntimeError,
        match=(
            "DATA_BLOCKED: hot-rank fusion requires at least two exact-date "
            "complete trusted sources"
        ),
    ):
        merge_hot_rank._require_same_day_sources(
            "2026-08-26",
            _east_frame("2026-08-26"),
            empty,
            empty,
            empty,
        )


def test_fusion_rejects_source_rows_bound_to_another_date():
    with pytest.raises(RuntimeError, match="DATA_BLOCKED: east hot-rank date mismatch"):
        merge_hot_rank._require_same_day_sources(
            "2026-08-26",
            _east_frame("2026-08-25"),
            _ths_frame("2026-08-26"),
            pd.DataFrame(),
            pd.DataFrame(),
        )


def test_fusion_accepts_two_exact_date_sources():
    available = merge_hot_rank._require_same_day_sources(
        "2026-08-26",
        _east_frame("2026-08-26"),
        _ths_frame("2026-08-26"),
        pd.DataFrame(),
        pd.DataFrame(),
    )

    assert available == ("east", "ths")


def test_candidate_dates_intersect_authoritative_trading_calendar(monkeypatch):
    provider_dates = pd.DataFrame(
        {
            "snapshot_date": [
                "2026-08-28",
                "2026-08-29",
                "2026-08-30",
                "2026-08-31",
            ]
        }
    )

    def fake_read_frame(statement, engine, params=None):
        sql = str(statement)
        if "FROM si_trade_calendar" in sql:
            return pd.DataFrame(
                {"trade_date": ["2026-08-28", "2026-08-31"]}
            )
        return provider_dates.copy()

    monkeypatch.setattr(merge_hot_rank, "read_frame", fake_read_frame)

    assert merge_hot_rank._candidate_snapshot_dates(
        object(), "2026-08-31"
    ) == ["2026-08-28", "2026-08-31"]


def test_multi_day_counts_a_stock_only_once_per_snapshot_date(monkeypatch):
    snapshot_date = "2026-08-26"
    east = _east_frame(snapshot_date)
    ths = _ths_frame(snapshot_date)
    captured: list[pd.DataFrame] = []

    monkeypatch.setattr(
        merge_hot_rank,
        "_candidate_snapshot_dates",
        lambda engine, end_date: [snapshot_date],
    )
    monkeypatch.setattr(
        merge_hot_rank,
        "_read_day_data",
        lambda engine, date_value: (
            east.copy(),
            ths.copy(),
            pd.DataFrame(),
            pd.DataFrame(),
        ),
    )
    monkeypatch.setattr(
        merge_hot_rank,
        "validate_hot_rank_fusion_runtime_schema",
        lambda engine, tables: None,
    )
    monkeypatch.setattr(merge_hot_rank, "_load_industry_map", lambda engine: {})
    monkeypatch.setattr(
        merge_hot_rank,
        "replace_table_rows",
        lambda output, table, engine, **kwargs: captured.append(output.copy()),
    )

    result = merge_hot_rank.run_multi_day(
        object(), snapshot_date, num_days=1, top_n=10, save=True
    )

    assert len(captured) == 1
    assert captured[0].iloc[0]["appear_days"] == 1
    assert captured[0].iloc[0]["continuity_rate"] == 100.0
    assert result["stat_date"] == snapshot_date


def _two_source_frames(snapshot_date: str):
    return (
        _east_frame(snapshot_date),
        _ths_frame(snapshot_date),
        pd.DataFrame(),
        pd.DataFrame(),
    )


def _one_source_frames(snapshot_date: str):
    return (
        _east_frame(snapshot_date),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
    )


def test_legacy_sina_never_satisfies_the_formal_two_source_gate():
    with pytest.raises(RuntimeError, match="complete trusted sources"):
        merge_hot_rank._require_same_day_sources(
            "2026-08-26",
            _east_frame("2026-08-26"),
            pd.DataFrame(),
            pd.DataFrame(),
            _legacy_frame("2026-08-26", code_start=300000),
        )


def test_unreceipted_xq_is_excluded_even_when_its_inventory_is_complete():
    trusted, available = merge_hot_rank._trusted_same_day_sources(
        "2026-08-26",
        _east_frame("2026-08-26"),
        _ths_frame("2026-08-26"),
        _legacy_frame("2026-08-26", code_start=300000),
        pd.DataFrame(),
    )

    assert available == ("east", "ths")
    assert trusted[2].empty
    assert trusted[3].empty


@pytest.mark.parametrize(
    ("east_rows", "ths_rows"),
    [(99, 100), (100, 49)],
)
def test_partial_trusted_inventory_cannot_be_fused(east_rows, ths_rows):
    with pytest.raises(RuntimeError, match="complete trusted sources"):
        merge_hot_rank._require_same_day_sources(
            "2026-08-26",
            _east_frame("2026-08-26", rows=east_rows),
            _ths_frame("2026-08-26", rows=ths_rows),
            pd.DataFrame(),
            pd.DataFrame(),
        )


def test_fuse_single_day_ignores_xq_and_sina_rows_and_scores():
    east = _east_frame("2026-08-26")
    ths = _ths_frame("2026-08-26")
    xq = _legacy_frame("2026-08-26", code_start=300000)
    sina = _legacy_frame("2026-08-26", code_start=900000)

    result = merge_hot_rank._fuse_single_day(east, ths, xq, sina)

    assert set(result["stock_code"]) == set(east["stock_code"])
    assert result["xq_rank"].isna().all()
    assert result["sina_rank"].isna().all()
    assert (result["xq_score"] == 0).all()
    assert (result["sina_score"] == 0).all()
    assert set(result["source_flag"]) == {"both"}
    assert (
        result["total_score"]
        == result["east_score"] + result["ths_score"]
    ).all()


def test_weekend_upper_bound_selects_latest_three_qualified_source_dates(
    monkeypatch,
):
    candidates = [
        "2026-08-26",
        "2026-08-27",
        "2026-08-28",
        "2026-08-29",
        "2026-08-30",
    ]
    monkeypatch.setattr(
        merge_hot_rank,
        "_candidate_snapshot_dates",
        lambda engine, end_date: candidates,
    )
    monkeypatch.setattr(
        merge_hot_rank,
        "_read_day_data",
        lambda engine, snapshot_date: (
            _one_source_frames(snapshot_date)
            if snapshot_date in {"2026-08-29", "2026-08-30"}
            else _two_source_frames(snapshot_date)
        ),
    )

    window = merge_hot_rank._select_multi_day_window(
        object(), "2026-08-30", 3
    )

    assert [item[0] for item in window] == [
        "2026-08-26",
        "2026-08-27",
        "2026-08-28",
    ]
    assert all(item[2] == ("east", "ths") for item in window)


def test_monday_window_skips_weekend_without_lowering_two_source_gate(
    monkeypatch,
):
    candidates = [
        "2026-08-26",
        "2026-08-27",
        "2026-08-28",
        "2026-08-29",
        "2026-08-30",
        "2026-08-31",
    ]
    monkeypatch.setattr(
        merge_hot_rank,
        "_candidate_snapshot_dates",
        lambda engine, end_date: candidates,
    )
    monkeypatch.setattr(
        merge_hot_rank,
        "_read_day_data",
        lambda engine, snapshot_date: (
            _one_source_frames(snapshot_date)
            if snapshot_date in {"2026-08-29", "2026-08-30"}
            else _two_source_frames(snapshot_date)
        ),
    )

    window = merge_hot_rank._select_multi_day_window(
        object(), "2026-08-31", 3
    )

    assert [item[0] for item in window] == [
        "2026-08-27",
        "2026-08-28",
        "2026-08-31",
    ]
    assert all(len(item[2]) >= 2 for item in window)


def test_weekend_request_persists_actual_last_source_date(monkeypatch, capsys):
    dates = ["2026-08-26", "2026-08-27", "2026-08-28"]
    window = [
        (snapshot_date, _two_source_frames(snapshot_date), ("east", "ths"))
        for snapshot_date in dates
    ]
    captured: list[tuple[pd.DataFrame, dict]] = []
    monkeypatch.setattr(
        merge_hot_rank,
        "_select_multi_day_window",
        lambda engine, end_date, num_days: window,
    )
    monkeypatch.setattr(
        merge_hot_rank,
        "validate_hot_rank_fusion_runtime_schema",
        lambda engine, tables: None,
    )
    monkeypatch.setattr(merge_hot_rank, "_load_industry_map", lambda engine: {})
    monkeypatch.setattr(
        merge_hot_rank,
        "replace_table_rows",
        lambda output, table, engine, **kwargs: captured.append(
            (output.copy(), kwargs)
        ),
    )

    result = merge_hot_rank.run_multi_day(
        object(), "2026-08-30", num_days=3, top_n=10, save=True
    )
    output = capsys.readouterr().out

    assert result["requested_end_date"] == "2026-08-30"
    assert result["stat_date"] == "2026-08-28"
    assert result["snapshot_dates"] == dates
    assert set(captured[0][0]["stat_date"]) == {"2026-08-28"}
    assert captured[0][1]["params"]["stat_date"] == "2026-08-28"
    assert "DATE=2026-08-28" in output


def test_multi_day_scheduler_validation_uses_actual_output_date():
    assert TASK_OUTPUT_REQUIREMENTS["hot_fused_3"][0].target == "output_date"
    assert TASK_OUTPUT_REQUIREMENTS["hot_fused_5"][0].target == "output_date"
