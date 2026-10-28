from __future__ import annotations

from sqlalchemy import create_engine, text

from tools import sync_free_index_daily as collector


def test_public_index_scope_excludes_unavailable_fixed_codes():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE sm_index_kline ("
                "index_code TEXT, trade_date TEXT, k_type INTEGER)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO sm_index_kline VALUES "
                "('000001','2026-10-26',1),"
                "('988006','2026-10-26',1),"
                "('988007','2026-10-26',1),"
                "('988106','2026-10-26',1),"
                "('988107','2026-10-26',1),"
                "('988201','2026-10-26',1)"
            )
        )
    try:
        assert collector._expected_codes(engine, "2026-10-27") == [
            "000001"
        ]
    finally:
        engine.dispose()


def test_public_index_publisher_uses_persisted_scope_and_verified_rows(
    monkeypatch,
):
    class Engine:
        def dispose(self):
            pass

    calls = []
    rows = [
        {
            "index_code": "000001",
            "trade_date": "2026-10-26",
            "open": 1,
            "close": 2,
            "high": 3,
            "low": 1,
            "volume": 10,
            "amount": 20,
        }
    ]
    monkeypatch.setattr(collector, "create_batch_engine", Engine)
    monkeypatch.setattr(collector, "get_kline_engine", Engine)
    monkeypatch.setattr(
        collector,
        "_expected_codes",
        lambda *_args, **_kwargs: ["000001"],
    )
    monkeypatch.setattr(
        collector,
        "_read_target",
        lambda *_args, **_kwargs: rows,
    )
    monkeypatch.setattr(
        collector,
        "step_index_kline",
        lambda *_args, **_kwargs: calls.append("fetch"),
    )
    monkeypatch.setattr(
        collector,
        "_write_receipt",
        lambda *_args, **_kwargs: {"receipt_id": "a" * 64},
    )

    result = collector.publish("2026-10-26")

    assert calls == ["fetch"]
    assert result["status"] == "COMPLETE"
    assert result["expected_count"] == result["code_count"] == 1
    assert result["missing_codes"] == []
    assert result["qmt_attestation_claimed"] is False


def test_public_index_publisher_reports_missing_codes_without_claiming_full(
    monkeypatch,
):
    class Engine:
        def dispose(self):
            pass

    monkeypatch.setattr(collector, "create_batch_engine", Engine)
    monkeypatch.setattr(collector, "get_kline_engine", Engine)
    monkeypatch.setattr(
        collector,
        "_expected_codes",
        lambda *_args, **_kwargs: ["000001", "399001"],
    )
    monkeypatch.setattr(
        collector,
        "_read_target",
        lambda *_args, **_kwargs: [
            {
                "index_code": "000001",
                "trade_date": "2026-10-26",
                "open": 1,
                "close": 2,
                "high": 3,
                "low": 1,
                "volume": 10,
                "amount": 20,
            }
        ],
    )
    monkeypatch.setattr(collector, "step_index_kline", lambda *_args: None)
    monkeypatch.setattr(
        collector,
        "_write_receipt",
        lambda *_args, **_kwargs: {"receipt_id": "b" * 64},
    )

    result = collector.publish("2026-10-26")

    assert result["status"] == "PARTIAL"
    assert result["missing_codes"] == ["399001"]


def test_public_index_publisher_accepts_explicit_core_scope(monkeypatch):
    class Engine:
        def dispose(self):
            pass

    calls = []
    monkeypatch.setattr(collector, "create_batch_engine", Engine)
    monkeypatch.setattr(collector, "get_kline_engine", Engine)
    monkeypatch.setattr(
        collector,
        "_expected_codes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("persisted scope must not be enumerated")
        ),
    )
    monkeypatch.setattr(
        collector,
        "_read_target",
        lambda *_args, **_kwargs: [
            {
                "index_code": "000001",
                "trade_date": "2026-10-28",
                "open": 1,
                "close": 2,
                "high": 3,
                "low": 1,
                "volume": 10,
                "amount": 20,
            }
        ],
    )
    monkeypatch.setattr(
        collector,
        "step_index_kline",
        lambda _engine, codes, *_args, **_kwargs: calls.append(codes),
    )
    monkeypatch.setattr(
        collector,
        "_write_receipt",
        lambda *_args, **_kwargs: {"receipt_id": "c" * 64},
    )

    result = collector.publish(
        "2026-10-28",
        codes=["000001"],
    )

    assert calls == [["000001"]]
    assert result["status"] == "COMPLETE"
    assert result["expected_count"] == 1
