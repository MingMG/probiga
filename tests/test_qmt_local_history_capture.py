from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy.engine import make_url

from integrations.bigqmt.spool import PROVIDER_ID as BIGQMT_PROVIDER_ID
from integrations.qmt import local_history


def test_prepare_bigqmt_daily_capture_preserves_raw_unadjusted_provenance(
    monkeypatch,
):
    monkeypatch.setattr(local_history, "_short_name_map", lambda *_args: {})
    frame = pd.DataFrame(
        [
            {
                "stock_code": "000001",
                "trade_time": "2026-08-19 15:00:00",
                "trade_date": "2026-08-19",
                "adjust_type": 0,
                "open": 10,
                "close": 10.1,
                "high": 10.2,
                "low": 9.9,
                "volume": 1000,
                "amount": 10100,
                "data_source": BIGQMT_PROVIDER_ID,
                "quality_status": "VERIFIED",
                "batch_id": "bigqmt-source-batch",
                "pre_close": 9.9,
                "pre_close_origin": "NATIVE_QMT",
            }
        ]
    )

    rows = local_history._prepare_kline_rows(
        frame,
        source_engine=object(),
        period="1d",
        batch_id="fallback-batch",
        provider=BIGQMT_PROVIDER_ID,
    )

    assert rows[0]["provider"] == BIGQMT_PROVIDER_ID
    assert rows[0]["adjust_type"] == 0
    assert rows[0]["quality_status"] == "VERIFIED"
    assert rows[0]["batch_id"] == "bigqmt-source-batch"
    assert rows[0]["pre_close"] == 9.9
    assert rows[0]["pre_close_origin"] == "NATIVE_QMT"


def test_persist_bigqmt_daily_capture_uses_local_evidence_table(monkeypatch):
    local_engine = object()
    events: list[object] = []
    monkeypatch.setattr(
        local_history,
        "validate_local_history_tables",
        lambda engine, **_kwargs: events.append(("validate", engine)),
    )
    monkeypatch.setattr(
        local_history,
        "_prepare_kline_rows",
        lambda *_args, **kwargs: [
                {
                    "provider": kwargs["provider"],
                    "stock_code": "000001",
                    "trade_date": "2026-08-19",
                }
        ],
    )
    monkeypatch.setattr(
        local_history,
        "_load_daily_expected_pairs",
        lambda *_args, **_kwargs: {("000001", "2026-08-19")},
    )
    monkeypatch.setattr(
        local_history,
        "_upsert_rows",
        lambda engine, **kwargs: events.append(
            ("upsert", engine, kwargs)
        )
        or 1,
    )

    written = local_history.persist_daily_kline_capture(
        pd.DataFrame([{"stock_code": "000001"}]),
        source_engine=object(),
        local_engine=local_engine,
        batch_id="batch-1",
    )

    assert written == 1
    assert events[0] == ("validate", local_engine)
    assert events[1][2]["table_name"] == local_history.LOCAL_KLINE_TABLE
    assert events[1][2]["key_columns"] == [
        "provider",
        "stock_code",
        "period",
        "trade_date",
        "adjust_type",
    ]


def test_persist_capture_reuses_authenticated_cross_schema_connection(
    monkeypatch,
):
    class FakeEngine:
        def __init__(self, url: str):
            self.url = make_url(url)

    source_engine = FakeEngine(
        "mysql+pymysql://runtime:secret@127.0.0.1:3306/probiga"
    )
    history_engine = FakeEngine(
        "mysql+pymysql://runtime:secret@localhost:3306/"
        "probiga_qmt_history"
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        local_history,
        "get_local_history_engine",
        lambda: history_engine,
    )
    monkeypatch.setattr(
        local_history,
        "validate_local_history_tables",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("cross-schema route must not reconnect")
        ),
    )
    monkeypatch.setattr(
        local_history,
        "validate_local_history_tables",
        lambda engine, *, database=None: captured.update(
            {
                "validated_engine": engine,
                "validated_database": database,
            }
        )
        or {"ready": True},
    )
    monkeypatch.setattr(
        local_history,
        "_prepare_kline_rows",
        lambda *_args, **_kwargs: [{
            "stock_code": "000001",
            "trade_date": "2026-08-19",
        }],
    )
    monkeypatch.setattr(
        local_history,
        "_load_daily_expected_pairs",
        lambda *_args, **_kwargs: {("000001", "2026-08-19")},
    )
    monkeypatch.setattr(
        local_history,
        "_upsert_rows",
        lambda engine, **kwargs: captured.update(
            {"engine": engine, **kwargs}
        )
        or 1,
    )

    written = local_history.persist_daily_kline_capture(
        pd.DataFrame([{"stock_code": "000001"}]),
        source_engine=source_engine,
    )

    assert written == 1
    assert captured["validated_engine"] is source_engine
    assert captured["validated_database"] == "probiga_qmt_history"
    assert captured["engine"] is source_engine
    assert captured["table_name"] == (
        "probiga_qmt_history.qmt_local_stock_kline"
    )


def test_cross_schema_capture_validates_before_preparing_or_writing(
    monkeypatch,
):
    class FakeEngine:
        def __init__(self, url: str):
            self.url = make_url(url)

    source_engine = FakeEngine(
        "mysql+pymysql://runtime:secret@127.0.0.1:3306/probiga"
    )
    history_engine = FakeEngine(
        "mysql+pymysql://runtime:secret@localhost:3306/"
        "probiga_qmt_history"
    )
    events = []
    monkeypatch.setattr(
        local_history,
        "get_local_history_engine",
        lambda: history_engine,
    )

    def blocked_schema(*_args, **_kwargs):
        events.append("validate")
        raise local_history.LocalHistoryProvenanceSchemaError("missing")

    monkeypatch.setattr(
        local_history,
        "validate_local_history_tables",
        blocked_schema,
    )
    monkeypatch.setattr(
        local_history,
        "_prepare_kline_rows",
        lambda *_args, **_kwargs: events.append("prepare") or [],
    )
    monkeypatch.setattr(
        local_history,
        "_upsert_rows",
        lambda *_args, **_kwargs: events.append("upsert") or 0,
    )

    with pytest.raises(
        local_history.LocalHistoryProvenanceSchemaError,
        match="missing",
    ):
        local_history.persist_daily_kline_capture(
            pd.DataFrame([{"stock_code": "000001"}]),
            source_engine=source_engine,
        )

    assert events == ["validate"]


def test_direct_daily_capture_rejects_pre_listing_native_placeholder(
    monkeypatch,
):
    monkeypatch.setattr(
        local_history, "validate_local_history_tables", lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        local_history,
        "_prepare_kline_rows",
        lambda *_a, **_k: [{
            "stock_code": "920093",
            "trade_date": "2026-08-12",
            "volume": 0,
            "amount": 0,
        }],
    )
    monkeypatch.setattr(
        local_history,
        "_load_daily_expected_pairs",
        lambda *_a, **_k: set(),
    )
    monkeypatch.setattr(
        local_history,
        "_upsert_rows",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("pre-listing placeholder must not be persisted")
        ),
    )

    with pytest.raises(RuntimeError, match="lifecycle extras"):
        local_history.persist_daily_kline_capture(
            pd.DataFrame([{"stock_code": "920093"}]),
            source_engine=object(),
            local_engine=object(),
        )
