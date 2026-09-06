from __future__ import annotations

import pandas as pd
import pytest

from server.api.routers import hot_data
from tools import merge_hot_rank


TARGET_DATE = "2026-08-26"


def _source_frame(*, rows: int = 100) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rank": rank,
                "stock_code": f"{600000 + rank - 1:06d}",
                "short_name": f"sample-{rank}",
                "change_pct": 1.0,
                "pop_tag": "hot",
                "concept_tag": "sample",
            }
            for rank in range(1, rows + 1)
        ]
    )


def _persisted_source_rows(*, source: str, rows: int = 100) -> list[dict]:
    batch_time = "2026-08-26 17:14:00" if source == "east" else "2026-08-26 17:12:00"
    result = _source_frame(rows=rows).to_dict(orient="records")
    for row in result:
        row["snapshot_date"] = TARGET_DATE
        row["etl_sync_at"] = batch_time
    return result


def _fused_rows(*, untrusted_source: str | None = None) -> list[dict]:
    row = {
        "snapshot_date": TARGET_DATE,
        "fused_rank": 1,
        "stock_code": "600000",
        "short_name": "sample-1",
        "east_rank": 1,
        "ths_rank": 1,
        "xq_rank": None,
        "sina_rank": None,
        "east_score": 100.0,
        "ths_score": 100.0,
        "xq_score": 0.0,
        "sina_score": 0.0,
        "total_score": 200.0,
        "source_flag": "both",
    }
    if untrusted_source:
        row[f"{untrusted_source}_rank"] = 1
        row[f"{untrusted_source}_score"] = 100.0
        row["total_score"] = 300.0
        row["source_flag"] = f"east_ths_{untrusted_source}"
    return [row]


def _clear_hot_cache() -> None:
    with hot_data._cache_lock:
        hot_data._cache_store.clear()


def test_rank_sina_returns_explicit_semantics_unavailable():
    result = hot_data.rank_sina(top=100)

    assert result["status"] == "DATA_UNAVAILABLE"
    assert result["reason_code"] == "PROVIDER_SEMANTICS_UNVERIFIED"
    assert result["data"] == []
    assert result["total"] == 0


def test_live_fusion_uses_only_validated_east_and_ths(monkeypatch):
    _clear_hot_cache()
    monkeypatch.setattr(hot_data, "_fetch_live_east_rank", lambda top: _source_frame())
    monkeypatch.setattr(hot_data, "_fetch_live_ths_rank", lambda top: _source_frame())
    monkeypatch.setattr(
        hot_data,
        "_fetch_live_xq_rank",
        lambda top: (_ for _ in ()).throw(AssertionError("XQ must not be fetched")),
    )
    monkeypatch.setattr(
        hot_data,
        "_fetch_live_sina_rank",
        lambda top: (_ for _ in ()).throw(AssertionError("Sina must not be fetched")),
    )
    monkeypatch.setattr(hot_data, "get_engine", lambda: object())
    monkeypatch.setattr(merge_hot_rank, "_load_industry_map", lambda engine: {})

    result = hot_data._live_fused_rank(10, force_refresh=True)

    assert result["total"] == 10
    assert result["source_counts"] == {"east": 100, "ths": 100}
    assert all(row["xq_rank"] is None for row in result["data"])
    assert all(row["sina_rank"] is None for row in result["data"])
    assert all(row["xq_score"] == 0 for row in result["data"])
    assert all(row["sina_score"] == 0 for row in result["data"])
    assert all(row["source_flag"] == "both" for row in result["data"])


def test_live_fusion_keeps_ths_when_east_inventory_is_partial(monkeypatch):
    _clear_hot_cache()
    monkeypatch.setattr(
        hot_data,
        "_fetch_live_east_rank",
        lambda top: _source_frame(rows=99),
    )
    monkeypatch.setattr(hot_data, "_fetch_live_ths_rank", lambda top: _source_frame())

    result = hot_data._live_fused_rank(10, force_refresh=True)

    assert result["status"] == "OK"
    assert result["partial"] is True
    assert result["total"] == 10
    assert result["source_counts"] == {"east": 0, "ths": 100}
    assert "east" in result["errors"]
    assert all(row["source_flag"] == "ths_only" for row in result["data"])


def _persisted_sql_stub(
    *,
    ths_rows: list[dict],
    fused_rows: list[dict],
    statements: list[str] | None = None,
):
    def fake_read_sql(statement, params=None):
        sql = str(statement)
        if statements is not None:
            statements.append(sql)
        if "st_hot_rank_fused" in sql:
            return [dict(row) for row in fused_rows]
        if "si_trade_calendar" in sql:
            return [{"n": 1}]
        if "st_hot_pop_rank_east" in sql:
            return _persisted_source_rows(source="east")
        if "st_hot_rank_ths" in sql:
            return [dict(row) for row in ths_rows]
        raise AssertionError(f"unexpected SQL: {sql}")

    return fake_read_sql


def test_persisted_fused_keeps_east_when_ths_is_missing(
    monkeypatch,
):
    statements: list[str] = []
    monkeypatch.setattr(
        hot_data,
        "_read_sql",
        _persisted_sql_stub(
            ths_rows=[],
            fused_rows=_fused_rows(),
            statements=statements,
        ),
    )
    monkeypatch.setattr(
        hot_data,
        "_fallback_date",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("persisted fused must not fall back")
        ),
    )

    result = hot_data.fused(snapshot_date=TARGET_DATE, top=100)

    assert result["status"] == "OK"
    assert result["partial"] is True
    assert result["fallback"] is False
    assert result["total"] == 100
    assert result["source_counts"] == {"east": 100, "ths": 0}
    assert all(row["source_flag"] == "east_only" for row in result["data"])
    assert not any("st_hot_rank_fused" in sql for sql in statements)


def test_persisted_fused_accepts_proven_same_day_east_ths_batches(monkeypatch):
    monkeypatch.setattr(
        hot_data,
        "_read_sql",
        _persisted_sql_stub(
            ths_rows=_persisted_source_rows(source="ths"),
            fused_rows=_fused_rows(),
        ),
    )

    result = hot_data.fused(snapshot_date=TARGET_DATE, top=100)

    assert result["status"] == "OK"
    assert result["date"] == TARGET_DATE
    assert result["fallback"] is False
    assert result["total"] == 100
    assert result["source_evidence"]["east"]["rows"] == 100
    assert result["source_evidence"]["ths"]["rows"] == 100


def test_persisted_fused_rebuilds_without_legacy_xq_or_sina_scores(monkeypatch):
    trusted_ths = _persisted_source_rows(source="ths")
    for source in ("xq", "sina"):
        monkeypatch.setattr(
            hot_data,
            "_read_sql",
            _persisted_sql_stub(
                ths_rows=trusted_ths,
                fused_rows=_fused_rows(untrusted_source=source),
            ),
        )

        result = hot_data.fused(snapshot_date=TARGET_DATE, top=100)

        assert result["status"] == "OK"
        assert result["total"] == 100
        assert all(row[source + "_rank"] is None for row in result["data"])
        assert all(row[source + "_score"] == 0 for row in result["data"])


def test_persisted_fused_rank_must_match_the_proven_source_batch(monkeypatch):
    stale_rows = _fused_rows()
    stale_rows[0]["east_rank"] = 2
    stale_rows[0]["east_score"] = 99.0
    stale_rows[0]["total_score"] = 199.0
    monkeypatch.setattr(
        hot_data,
        "_read_sql",
        _persisted_sql_stub(
            ths_rows=_persisted_source_rows(source="ths"),
            fused_rows=stale_rows,
        ),
    )

    result = hot_data.fused(snapshot_date=TARGET_DATE, top=100)

    assert result["status"] == "OK"
    assert result["data"][0]["east_rank"] == 1
    assert result["data"][0]["total_score"] == 200


@pytest.fixture(autouse=True)
def isolate_hot_rank_runtime(monkeypatch):
    _clear_hot_cache()
    monkeypatch.setattr(hot_data, "get_engine", lambda: object())
    monkeypatch.setattr(merge_hot_rank, "_load_industry_map", lambda engine: {})
    yield
    _clear_hot_cache()


@pytest.mark.parametrize("failed_source", ["east", "ths"])
def test_live_fusion_keeps_healthy_source_on_request_failure(monkeypatch, failed_source):
    def failed(top):
        raise RuntimeError("provider timeout")
    for source in ("east", "ths"):
        monkeypatch.setattr(hot_data, "_fetch_live_" + source + "_rank",
                            failed if source == failed_source else lambda top: _source_frame())
    result = hot_data._live_fused_rank(10, force_refresh=True)
    healthy = "ths" if failed_source == "east" else "east"
    assert result["total"] == 10
    assert result["source_counts"][healthy] == 100
    assert result["source_counts"][failed_source] == 0
    assert all(row["source_flag"] == healthy + "_only" for row in result["data"])
    assert result["errors"][failed_source] == "provider timeout"


def test_live_fusion_all_failed_returns_explicit_error(monkeypatch):
    for source in ("east", "ths"):
        monkeypatch.setattr(hot_data, "_fetch_live_" + source + "_rank", lambda top: pd.DataFrame())
    result = hot_data._live_fused_rank(10, force_refresh=True)
    assert result["status"] == "DATA_UNAVAILABLE"
    assert result["data"] == []
    assert set(result["errors"]) == {"east", "ths"}


def test_force_refresh_bypasses_even_a_just_created_cache(monkeypatch):
    calls = []
    def fetch(top):
        calls.append(top)
        return _source_frame()
    for source in ("east", "ths"):
        monkeypatch.setattr(hot_data, "_fetch_live_" + source + "_rank", fetch)
    first = hot_data._live_fused_rank(10)
    assert hot_data._live_fused_rank(10) is first
    assert len(calls) == 2
    hot_data._live_fused_rank(10, force_refresh=True)
    assert len(calls) == 4


@pytest.mark.parametrize("failed_source", ["east", "ths"])
def test_persisted_source_query_failure_does_not_hide_other_source(monkeypatch, failed_source):
    def read(statement, params=None):
        if "si_trade_calendar" in statement:
            return [{"n": 1}]
        source = "east" if "st_hot_pop_rank_east" in statement else "ths"
        if source == failed_source:
            raise RuntimeError("source table unavailable")
        return _persisted_source_rows(source=source)
    monkeypatch.setattr(hot_data, "_read_sql", read)
    result = hot_data.fused(snapshot_date=TARGET_DATE, top=10)
    assert result["total"] == 10
    assert result["partial"] is True
    assert result["date"] == TARGET_DATE
    assert result["errors"][failed_source] == "source table unavailable"


def test_persisted_all_missing_does_not_invent_history(monkeypatch):
    monkeypatch.setattr(hot_data, "_read_sql", lambda sql, params=None: [{"n": 1}] if "si_trade_calendar" in sql else [])
    result = hot_data.fused(snapshot_date=TARGET_DATE, top=10)
    assert result["status"] == "DATA_UNAVAILABLE"
    assert result["data"] == []
    assert result["date"] == TARGET_DATE
    assert result["fallback"] is False


def test_persisted_excludes_wrong_date_and_preserves_healthy_source(monkeypatch):
    wrong = _persisted_source_rows(source="ths")
    for row in wrong:
        row["snapshot_date"] = "2026-08-25"
    monkeypatch.setattr(hot_data, "_read_sql", _persisted_sql_stub(ths_rows=wrong, fused_rows=[]))
    result = hot_data.fused(snapshot_date=TARGET_DATE, top=10)
    assert result["total"] == 10
    assert all(row["source_flag"] == "east_only" for row in result["data"])


def test_live_fusion_scores_union_of_overlapping_and_distinct_sources(monkeypatch):
    east = _source_frame()
    ths = _source_frame()
    ths["stock_code"] = [f"{600050 + index:06d}" for index in range(100)]
    monkeypatch.setattr(hot_data, "_fetch_live_east_rank", lambda top: east)
    monkeypatch.setattr(hot_data, "_fetch_live_ths_rank", lambda top: ths)
    result = hot_data._live_fused_rank(200, force_refresh=True)
    assert result["total"] == 150
    assert len({row["stock_code"] for row in result["data"]}) == 150
    totals = [row["total_score"] for row in result["data"]]
    assert totals == sorted(totals, reverse=True)
    by_code = {row["stock_code"]: row for row in result["data"]}
    assert by_code["600050"]["total_score"] == 150
    assert by_code["600050"]["source_flag"] == "both"
    assert by_code["600000"]["source_flag"] == "east_only"
    assert by_code["600149"]["source_flag"] == "ths_only"
