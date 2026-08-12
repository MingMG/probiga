# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from biz.review.quant_digest import (
    BEIJING_TZ,
    DigestConfig,
    PUBLISH_BLOCKED,
    PUBLISH_READY,
    build_frozen_factor_exposures,
    calc_frozen_factor_validation,
    generate_quant_digest_from_frames,
    load_quant_digest_inputs,
    persist_quant_digest,
)


TARGET = "2026-08-12"
PREVIOUS = "2026-08-11"
NOW = datetime(2026, 8, 12, 16, 10, tzinfo=BEIJING_TZ)


def _codes(count: int = 125) -> list[str]:
    return [f"{600000 + index:06d}" for index in range(count)]


def _bars(
    trade_date: str,
    *,
    count: int = 125,
    adjust_type: int = 0,
    return_start: float = -0.02,
    cutoff: str | None = None,
) -> pd.DataFrame:
    rows = []
    for index, stock_code in enumerate(_codes(count)):
        previous_close = 10.0 + index / 100
        return_value = return_start + index * 0.0004
        open_price = previous_close * (1 + return_value / 4)
        rows.append(
            {
                "stock_code": stock_code,
                "trade_date": trade_date,
                "adjust_type": adjust_type,
                "open": open_price,
                "close": previous_close * (1 + return_value),
                "high": previous_close * (1 + return_value + 0.01),
                "low": previous_close * (1 + return_value - 0.01),
                "pre_close": previous_close,
                "amount": 100_000_000 + index * 1_000_000,
                "volume": 1_000_000 + index,
                "etl_sync_at": cutoff or f"{trade_date} 15:45:00",
            }
        )
    return pd.DataFrame(rows)


def _universe(count: int = 125) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_code": _codes(count),
            "exchange": "SH",
            "list_date": "2020-01-01",
        }
    )


def _industries(count: int = 125, *, snapshot_date: str = TARGET) -> pd.DataFrame:
    names = ("电子", "通信", "房地产", "电力设备", "银行")
    return pd.DataFrame(
        {
            "stock_code": _codes(count),
            "industry_name": [names[index % len(names)] for index in range(count)],
            "snapshot_date": snapshot_date,
            "source": "gj_big_qmt_inner",
            "quality_status": "QMT_VALIDATED",
            "industry_type": "申万一级",
        }
    )


def _industry_run(snapshot_date: str, count: int = 125) -> dict:
    return {
        "snapshot_date": snapshot_date,
        "source": "gj_big_qmt_inner",
        "quality_status": "QMT_VALIDATED",
        "industry_relation_count": count,
        "actual_relation_count": count,
        "captured_at": f"{snapshot_date} 16:00:00",
    }


def _shares(count: int = 125) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_code": _codes(count),
            "total_shares": [1_000_000_000 + index * 1_000_000 for index in range(count)],
            "change_date": PREVIOUS,
        }
    )


def _factors(count: int = 125, *, exposure_date: str = PREVIOUS) -> pd.DataFrame:
    values = list(range(count))
    return pd.DataFrame(
        {
            "stock_code": _codes(count),
            "exposure_date": exposure_date,
            "max_source_date": exposure_date,
            "adjust_type": 0,
            "trend_20d": values,
            "mean_deviation_20d": [item**2 for item in values],
            "volatility_structure_5_20d": [-item for item in values],
            "elasticity_60d": [item / 10 for item in values],
        }
    )


def _ready_result(**overrides):
    arguments = {
        "target_bars": _bars(TARGET, return_start=-0.02),
        "previous_bars": _bars(PREVIOUS, return_start=-0.01),
        "universe": _universe(),
        "industries": _industries(),
        "industry_snapshot_date": TARGET,
        "industry_source": "gj_big_qmt_inner",
        "industry_quality_status": "QMT_VALIDATED",
        "previous_industries": _industries(snapshot_date=PREVIOUS),
        "previous_industry_snapshot_date": PREVIOUS,
        "previous_industry_source": "gj_big_qmt_inner",
        "previous_industry_quality_status": "QMT_VALIDATED",
        "industry_run_metadata": _industry_run(TARGET),
        "previous_industry_run_metadata": _industry_run(PREVIOUS),
        "expected_previous_date": PREVIOUS,
        "frozen_factors": _factors(),
        "shares": _shares(),
        "now": NOW,
        "config": DigestConfig(min_factor_sample=100),
    }
    arguments.update(overrides)
    return generate_quant_digest_from_frames(TARGET, **arguments)


def _gate(result: dict, name: str) -> dict:
    return next(item for item in result["quality_json"]["gates"] if item["name"] == name)


def test_ready_digest_has_structured_three_sections_and_only_calculated_numbers():
    result = _ready_result()

    assert result["publish_status"] == PUBLISH_READY
    assert result["quality_json"]["status"] == "pass"
    assert result["quality_json"]["coverage"]["target"] == 1.0
    assert result["data_cutoff_at"].startswith("2026-08-12T15:45:00")
    assert result["market_structure_json"]["sample_count"] == 125
    assert result["industry_rotation_json"]["industry_count"] == 5
    assert len(result["factor_validation_json"]["factors"]) == 4
    assert result["factor_validation_json"]["frozen_as_of"] == PREVIOUS
    assert "大势分析" in result["compact_review"]
    assert "行业轮动" in result["compact_review"]
    assert "因子特征" in result["compact_review"]
    assert "增量资金" not in result["compact_review"]
    assert "统计显著性" in result["compact_review"]


@pytest.mark.parametrize(
    ("field", "replacement", "gate_name"),
    [
        ("trade_date", "2026-08-11", "target_date"),
        ("adjust_type", 1, "target_adjust_type"),
    ],
)
def test_wrong_target_date_or_adjustment_version_blocks_publication(field, replacement, gate_name):
    bars = _bars(TARGET)
    bars.loc[0, field] = replacement

    result = _ready_result(target_bars=bars)

    assert result["publish_status"] == PUBLISH_BLOCKED
    assert result["compact_review"] == ""
    assert _gate(result, gate_name)["status"] == "blocked"


def test_duplicate_stock_and_low_coverage_fail_closed_without_partial_prose():
    bars = pd.concat([_bars(TARGET, count=100), _bars(TARGET, count=1)], ignore_index=True)

    result = _ready_result(target_bars=bars)

    assert result["publish_status"] == PUBLISH_BLOCKED
    assert result["compact_review"] == ""
    assert _gate(result, "target_unique_stock")["status"] == "blocked"
    assert _gate(result, "target_coverage")["status"] == "blocked"
    assert result["quality_json"]["coverage"]["target"] == 0.8


def test_weekend_and_pre_close_are_hard_gates():
    weekend = generate_quant_digest_from_frames(
        "2026-08-09",
        target_bars=_bars("2026-08-09"),
        previous_bars=_bars("2026-08-07"),
        universe=_universe(),
        industries=_industries(),
        industry_snapshot_date="2026-08-09",
        industry_source="gj_big_qmt_inner",
        industry_quality_status="QMT_VALIDATED",
        previous_industries=_industries(snapshot_date="2026-08-07"),
        previous_industry_snapshot_date="2026-08-07",
        previous_industry_source="gj_big_qmt_inner",
        previous_industry_quality_status="QMT_VALIDATED",
        industry_run_metadata=_industry_run("2026-08-09"),
        previous_industry_run_metadata=_industry_run("2026-08-07"),
        expected_previous_date="2026-08-07",
        frozen_factors=_factors(exposure_date="2026-08-07"),
        now=datetime(2026, 8, 9, 16, 0, tzinfo=BEIJING_TZ),
        config=DigestConfig(min_factor_sample=100),
    )
    pre_close = _ready_result(now=datetime(2026, 8, 12, 15, 29, tzinfo=BEIJING_TZ))

    assert weekend["publish_status"] == PUBLISH_BLOCKED
    assert _gate(weekend, "weekday")["status"] == "blocked"
    assert pre_close["publish_status"] == PUBLISH_BLOCKED
    assert _gate(pre_close, "post_close")["status"] == "blocked"


def test_missing_industry_and_factor_data_are_explicitly_unavailable_not_invented():
    result = _ready_result(
        industries=pd.DataFrame(),
        industry_snapshot_date=None,
        industry_source=None,
        industry_quality_status=None,
        previous_industries=pd.DataFrame(),
        previous_industry_snapshot_date=None,
        previous_industry_source=None,
        previous_industry_quality_status=None,
        industry_run_metadata=None,
        previous_industry_run_metadata=None,
        frozen_factors=pd.DataFrame(),
    )

    assert result["publish_status"] == PUBLISH_READY
    assert result["quality_json"]["status"] == "warn"
    assert result["industry_rotation_json"] == {
        "status": "unavailable",
        "reason": "目标日QMT_VALIDATED申万一级行业快照不可用",
        "industries": [],
    }
    assert result["factor_validation_json"]["status"] == "unavailable"
    assert "行业快照不可用" in result["compact_review"]
    assert "冻结因子数据缺失" in result["compact_review"]


def test_missing_previous_industry_snapshot_keeps_current_ranking_but_not_rotation_claim():
    result = _ready_result(
        previous_industries=pd.DataFrame(),
        previous_industry_snapshot_date=None,
        previous_industry_source=None,
        previous_industry_quality_status=None,
        previous_industry_run_metadata=None,
    )

    industry = result["industry_rotation_json"]
    assert result["publish_status"] == PUBLISH_READY
    assert result["quality_json"]["status"] == "warn"
    assert industry["status"] == "available"
    assert industry["leaders"]
    assert industry["previous_comparison_available"] is False
    assert industry["rotation_state"] == "数据不足"
    assert industry["rank_correlation"] is None
    assert industry["top3_overlap_count"] is None
    assert "延续与轮动结论数据不足" in result["compact_review"]
    assert _gate(result, "previous_industry_snapshot_same_date")["status"] == "warn"
    source = result["quality_json"]["source_dates"]["industry_membership"]
    assert source["current"]["snapshot_date"] == TARGET
    assert source["previous"]["snapshot_date"] is None


def test_factor_snapshot_rejects_t0_exposure_and_source_leakage():
    target = _bars(TARGET)

    with pytest.raises(ValueError, match="not frozen on T-1"):
        calc_frozen_factor_validation(
            target.assign(_valid_bar=True, _return_pct=target["close"] / target["pre_close"] * 100 - 100),
            _factors(exposure_date=TARGET),
            frozen_as_of=PREVIOUS,
            min_sample=100,
        )

    leaked = _factors()
    leaked.loc[0, "max_source_date"] = TARGET
    with pytest.raises(ValueError, match="after T-1"):
        calc_frozen_factor_validation(
            target.assign(_valid_bar=True, _return_pct=target["close"] / target["pre_close"] * 100 - 100),
            leaked,
            frozen_as_of=PREVIOUS,
            min_sample=100,
        )


def test_factor_builder_uses_only_history_through_frozen_date():
    sessions = pd.bdate_range(end=PREVIOUS, periods=61)
    history_rows = []
    for stock_index, stock_code in enumerate(_codes(5)):
        for session_index, session in enumerate(sessions):
            previous_close = 10 + stock_index
            daily_return = 0.001 * (stock_index + 1) + 0.0001 * ((session_index % 7) - 3)
            history_rows.append(
                {
                    "stock_code": stock_code,
                    "trade_date": session.date().isoformat(),
                    "adjust_type": 0,
                    "close": previous_close * (1 + daily_return),
                    "pre_close": previous_close,
                }
            )
    history = pd.DataFrame(history_rows)

    factors = build_frozen_factor_exposures(history, PREVIOUS)

    assert len(factors) == 5
    assert set(factors["exposure_date"]) == {PREVIOUS}
    assert set(factors["max_source_date"]) == {PREVIOUS}
    leaked = pd.concat(
        [
            history,
            pd.DataFrame(
                [
                    {
                        "stock_code": _codes(1)[0],
                        "trade_date": TARGET,
                        "adjust_type": 0,
                        "close": 11,
                        "pre_close": 10,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    with pytest.raises(ValueError, match="after the frozen date"):
        build_frozen_factor_exposures(leaked, PREVIOUS)


def test_db_loader_sql_always_binds_one_adjustment_version_and_target_date():
    calls: list[tuple[str, dict | None]] = []

    def reader(statement, _engine, params=None):
        sql = " ".join(str(statement).split())
        calls.append((sql, params))
        if "FROM si_trade_calendar" in sql:
            return pd.DataFrame({"trade_date": [PREVIOUS]})
        if "FROM sm_stock_kline" in sql and "BETWEEN DATE_SUB" in sql:
            return pd.DataFrame()
        if "FROM sm_stock_kline" in sql:
            return _bars(str(params["d"]), count=1)
        if "FROM si_all_code" in sql:
            return _universe(1)
        if "FROM qmt_membership_snapshot_run" in sql:
            return pd.DataFrame([_industry_run(str(params["d"]), count=1)])
        if "FROM qmt_industry_member_snapshot" in sql:
            return _industries(1, snapshot_date=str(params["d"]))
        if "FROM si_stock_shares" in sql:
            return _shares(1)
        raise AssertionError(sql)

    loaded = load_quant_digest_inputs(object(), TARGET, adjust_type=0, reader=reader)

    kline_calls = [(sql, params) for sql, params in calls if "FROM sm_stock_kline" in sql]
    assert loaded["target_bars"].iloc[0]["trade_date"] == TARGET
    assert kline_calls
    assert all("adjust_type = :adjust_type" in sql for sql, _ in kline_calls)
    assert all(params and params["adjust_type"] == 0 for _, params in kline_calls)
    assert any(params["d"] == TARGET for _, params in kline_calls)
    assert any(params["d"] == PREVIOUS for _, params in kline_calls)
    industry_calls = [
        (sql, params) for sql, params in calls
        if "FROM qmt_industry_member_snapshot" in sql
        and "SELECT snapshot_date, source" in sql
    ]
    assert len(industry_calls) == 2
    assert {params["d"] for _, params in industry_calls} == {TARGET, PREVIOUS}
    assert loaded["industry_snapshot_date"] == TARGET
    assert loaded["previous_industry_snapshot_date"] == PREVIOUS
    assert loaded["expected_previous_date"] == PREVIOUS


def test_calendar_t_minus_one_mismatch_blocks_instead_of_using_t_minus_two():
    result = _ready_result(
        previous_bars=_bars("2026-08-10"),
        expected_previous_date=PREVIOUS,
    )

    assert result["publish_status"] == PUBLISH_BLOCKED
    assert _gate(result, "previous_trade_date")["status"] == "blocked"


def test_industry_snapshot_without_matching_run_receipt_is_unavailable():
    result = _ready_result(industry_run_metadata=None)

    assert result["publish_status"] == PUBLISH_READY
    assert result["quality_json"]["status"] == "warn"
    assert result["industry_rotation_json"]["status"] == "unavailable"
    assert _gate(result, "industry_snapshot_run_complete")["status"] == "warn"


def test_industry_snapshot_relation_count_mismatch_is_unavailable():
    run = _industry_run(TARGET)
    run["actual_relation_count"] = 124
    result = _ready_result(industry_run_metadata=run)

    assert result["industry_rotation_json"]["status"] == "unavailable"
    assert _gate(result, "industry_snapshot_run_complete")["status"] == "warn"


class _Connection:
    def __init__(self, existing_status=None):
        self.calls = []
        self.existing_status = existing_status

    def execute(self, statement, params=None):
        self.calls.append((str(statement), params))
        return _Result(self.existing_status)


class _Result:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _Begin:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class _Engine:
    def __init__(self, existing_status=None):
        self.connection = _Connection(existing_status)

    def begin(self):
        return _Begin(self.connection)


def test_persistence_stores_quality_receipt_for_blocked_generation_too():
    engine = _Engine()
    blocked = _ready_result(target_bars=_bars(TARGET, count=10))

    persist_quant_digest(engine, blocked)

    assert len(engine.connection.calls) == 3
    _, payload = engine.connection.calls[2]
    assert payload["publish_status"] == PUBLISH_BLOCKED
    assert payload["compact_review"] == ""
    assert '"status":"blocked"' in payload["quality_json"]


def test_blocked_rerun_cannot_overwrite_existing_ready_digest():
    engine = _Engine(existing_status=PUBLISH_READY)
    blocked = _ready_result(target_bars=_bars(TARGET, count=10))

    persist_quant_digest(engine, blocked)

    assert len(engine.connection.calls) == 2
    select_sql, keys = engine.connection.calls[1]
    assert "FOR UPDATE" in select_sql
    assert keys == {"review_date": TARGET, "adjust_type": 0}
    assert not any("INSERT INTO st_quant_review_digest" in sql for sql, _ in engine.connection.calls)


def test_ready_upsert_guards_against_older_ready_generation_and_cutoff():
    engine = _Engine(existing_status=PUBLISH_READY)

    persist_quant_digest(engine, _ready_result())

    upsert_sql = engine.connection.calls[-1][0]
    assert "VALUES(generated_at) < generated_at" in upsert_sql
    assert "VALUES(data_cutoff_at) < data_cutoff_at" in upsert_sql
