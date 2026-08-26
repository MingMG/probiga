from __future__ import annotations

import pandas as pd

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


def test_live_fusion_is_unavailable_when_one_inventory_is_partial(monkeypatch):
    _clear_hot_cache()
    monkeypatch.setattr(
        hot_data,
        "_fetch_live_east_rank",
        lambda top: _source_frame(rows=99),
    )
    monkeypatch.setattr(hot_data, "_fetch_live_ths_rank", lambda top: _source_frame())

    result = hot_data._live_fused_rank(10, force_refresh=True)

    assert result["status"] == "DATA_UNAVAILABLE"
    assert result["reason_code"] == "TRUSTED_DUAL_SOURCE_INCOMPLETE"
    assert result["data"] == []


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


def test_persisted_fused_rejects_east_only_date_before_reading_old_fusion(
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

    assert result["status"] == "DATA_UNAVAILABLE"
    assert result["reason_code"] == "TRUSTED_DUAL_SOURCE_UNPROVEN"
    assert result["fallback"] is False
    assert result["data"] == []
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
    assert result["total"] == 1
    assert result["source_evidence"]["east"]["rows"] == 100
    assert result["source_evidence"]["ths"]["rows"] == 100


def test_persisted_fused_rejects_legacy_xq_or_sina_scores(monkeypatch):
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

        assert result["status"] == "DATA_UNAVAILABLE"
        assert result["reason_code"] == "TRUSTED_DUAL_SOURCE_UNPROVEN"
        assert result["data"] == []
        assert "FUSED_BATCH_HAS_UNTRUSTED_SOURCE" in result["error"]


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

    assert result["status"] == "DATA_UNAVAILABLE"
    assert result["data"] == []
    assert "FUSED_BATCH_HAS_UNTRUSTED_SOURCE" in result["error"]
