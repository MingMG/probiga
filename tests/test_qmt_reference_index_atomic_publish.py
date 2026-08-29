from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from tools import sync_guojin_qmt_reference_data as reference_sync


EXPECTED_INDEXES = ["000300.SH", "000905.SH"]


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_index_weight (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_qmt_code TEXT NOT NULL,
                index_code TEXT NOT NULL,
                qmt_code TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                exchange TEXT,
                weight REAL,
                etl_sync_at DATETIME,
                data_source TEXT,
                received_at DATETIME,
                batch_id TEXT,
                quality_status TEXT,
                permission_status TEXT,
                UNIQUE(index_code, stock_code)
            )
        """))
        connection.execute(text("""
            CREATE TABLE si_index_constituent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_qmt_code TEXT,
                index_code TEXT NOT NULL,
                qmt_code TEXT,
                stock_code TEXT NOT NULL CHECK(stock_code <> '999999'),
                short_name TEXT,
                exchange TEXT,
                weight REAL,
                etl_sync_at DATETIME,
                data_source TEXT,
                received_at DATETIME,
                batch_id TEXT,
                quality_status TEXT,
                permission_status TEXT,
                UNIQUE(index_code, stock_code)
            )
        """))
    return engine


def _weight_frame(
    rows: list[tuple[str, str, str]],
    *,
    batch_id: str = "new_batch",
) -> pd.DataFrame:
    frame = pd.DataFrame([
        {
            "index_qmt_code": index_qmt_code,
            "index_code": index_qmt_code.split(".", 1)[0],
            "qmt_code": f"{stock_code}.{exchange}",
            "stock_code": stock_code,
            "exchange": exchange,
            "weight": 1.0,
        }
        for index_qmt_code, stock_code, exchange in rows
    ])
    return reference_sync._stamp(frame, batch_id)


def _seed_previous(engine) -> None:
    previous = _weight_frame(
        [
            ("000300.SH", "600000", "SH"),
            ("000905.SH", "000001", "SZ"),
        ],
        batch_id="previous_batch",
    )
    business = reference_sync._index_weight_business_frame(previous)
    external = {
        "index_qmt_code": "888888.SH",
        "index_code": "888888",
        "qmt_code": "600888.SH",
        "stock_code": "600888",
        "short_name": "external",
        "exchange": "SH",
        "weight": 1.0,
        "data_source": "external",
        "batch_id": "external_batch",
    }
    with engine.begin() as connection:
        previous.to_sql(
            "qmt_index_weight", connection, if_exists="append", index=False
        )
        business.to_sql(
            "si_index_constituent", connection, if_exists="append", index=False
        )
        connection.execute(text("""
            INSERT INTO si_index_constituent
            (index_qmt_code, index_code, qmt_code, stock_code, short_name,
             exchange, weight, data_source, batch_id)
            VALUES
            (:index_qmt_code, :index_code, :qmt_code, :stock_code, :short_name,
             :exchange, :weight, :data_source, :batch_id)
        """), external)


def _rows(engine, table_name: str) -> list[tuple[str, str, str, str]]:
    with engine.connect() as connection:
        return [
            tuple(row)
            for row in connection.execute(text(
                f"SELECT index_qmt_code, stock_code, data_source, batch_id "
                f"FROM {table_name} ORDER BY index_qmt_code, stock_code"
            )).fetchall()
        ]


def _exchange_rows(engine, table_name: str) -> list[tuple[str, str, str]]:
    with engine.connect() as connection:
        return [
            tuple(row)
            for row in connection.execute(text(
                f"SELECT index_qmt_code, qmt_code, exchange "
                f"FROM {table_name} ORDER BY index_qmt_code, stock_code"
            )).fetchall()
        ]


def test_empty_index_weight_response_preserves_every_previous_partition() -> None:
    engine = _engine()
    _seed_previous(engine)
    before_raw = _rows(engine, "qmt_index_weight")
    before_business = _rows(engine, "si_index_constituent")

    receipt = reference_sync.publish_index_weight_snapshot(
        engine,
        expected_index_symbols=EXPECTED_INDEXES,
        index_weight=pd.DataFrame(),
    )

    assert receipt["coverage_status"] == "EMPTY"
    assert receipt["coverage_complete"] is False
    assert receipt["publication_status"] == "NO_CHANGE_EMPTY_SOURCE"
    assert receipt["preserved_index_qmt_codes"] == EXPECTED_INDEXES
    assert _rows(engine, "qmt_index_weight") == before_raw
    assert _rows(engine, "si_index_constituent") == before_business


def test_partial_index_weight_response_replaces_only_proven_partition() -> None:
    engine = _engine()
    _seed_previous(engine)
    frame = _weight_frame([("000300.SH", "600001", "SH")])

    receipt = reference_sync.publish_index_weight_snapshot(
        engine,
        expected_index_symbols=EXPECTED_INDEXES,
        index_weight=frame,
    )

    assert receipt["coverage_status"] == "PARTIAL"
    assert receipt["coverage_complete"] is False
    assert receipt["publication_status"] == "PARTIAL_ATOMIC_PARTITION_REPLACE"
    assert receipt["successful_index_qmt_codes"] == ["000300.SH"]
    assert receipt["preserved_index_qmt_codes"] == ["000905.SH"]
    for table_name in ("qmt_index_weight", "si_index_constituent"):
        provider_rows = [
            row for row in _rows(engine, table_name) if row[2] == "gj_qmt"
        ]
        assert provider_rows == [
            ("000300.SH", "600001", "gj_qmt", "new_batch"),
            ("000905.SH", "000001", "gj_qmt", "previous_batch"),
        ]


def test_complete_index_weight_response_authorizes_full_atomic_replace() -> None:
    engine = _engine()
    _seed_previous(engine)
    frame = _weight_frame([
        ("000300.SH", "600001", "SH"),
        ("000905.SH", "000002", "SZ"),
    ])

    receipt = reference_sync.publish_index_weight_snapshot(
        engine,
        expected_index_symbols=EXPECTED_INDEXES,
        index_weight=frame,
    )

    assert receipt["coverage_status"] == "COMPLETE"
    assert receipt["coverage_complete"] is True
    assert receipt["publication_status"] == "FULL_ATOMIC_REPLACE"
    for table_name in ("qmt_index_weight", "si_index_constituent"):
        provider_rows = [
            row for row in _rows(engine, table_name) if row[2] == "gj_qmt"
        ]
        assert provider_rows == [
            ("000300.SH", "600001", "gj_qmt", "new_batch"),
            ("000905.SH", "000002", "gj_qmt", "new_batch"),
        ]
    assert (
        "888888.SH", "600888", "external", "external_batch"
    ) in _rows(engine, "si_index_constituent")


def test_membership_only_response_derives_exchange_from_canonical_qmt_code() -> None:
    engine = _engine()
    frame = _weight_frame([
        ("000300.SH", "600001", "SH"),
        ("000905.SH", "000002", "SZ"),
    ]).drop(columns=["exchange"])

    receipt = reference_sync.publish_index_weight_snapshot(
        engine,
        expected_index_symbols=EXPECTED_INDEXES,
        index_weight=frame,
    )

    assert receipt["coverage_status"] == "COMPLETE"
    assert receipt["publication_status"] == "FULL_ATOMIC_REPLACE"
    expected = [
        ("000300.SH", "600001.SH", "SH"),
        ("000905.SH", "000002.SZ", "SZ"),
    ]
    assert _exchange_rows(engine, "qmt_index_weight") == expected
    assert _exchange_rows(engine, "si_index_constituent") == expected


def test_qmt_suffix_is_exchange_authority_for_non_stock_prefix_members() -> None:
    engine = _engine()
    frame = _weight_frame([
        ("000300.SH", "510050", "SZ"),
        ("000905.SH", "200012", "SZ"),
    ]).drop(columns=["exchange"])

    receipt = reference_sync.publish_index_weight_snapshot(
        engine,
        expected_index_symbols=EXPECTED_INDEXES,
        index_weight=frame,
    )

    assert receipt["coverage_status"] == "COMPLETE"
    expected = [
        ("000300.SH", "510050.SZ", "SZ"),
        ("000905.SH", "200012.SZ", "SZ"),
    ]
    assert _exchange_rows(engine, "qmt_index_weight") == expected
    assert _exchange_rows(engine, "si_index_constituent") == expected


def test_stock_code_must_equal_qmt_code_prefix_without_publication() -> None:
    engine = _engine()
    _seed_previous(engine)
    before_raw = _rows(engine, "qmt_index_weight")
    before_business = _rows(engine, "si_index_constituent")
    frame = _weight_frame([
        ("000300.SH", "600001", "SH"),
        ("000905.SH", "000002", "SZ"),
    ]).drop(columns=["exchange"])
    frame.loc[frame["stock_code"] == "600001", "qmt_code"] = "600002.SH"

    with pytest.raises(RuntimeError, match="constituent identities are invalid"):
        reference_sync.publish_index_weight_snapshot(
            engine,
            expected_index_symbols=EXPECTED_INDEXES,
            index_weight=frame,
        )

    assert _rows(engine, "qmt_index_weight") == before_raw
    assert _rows(engine, "si_index_constituent") == before_business


def test_conflicting_exchange_is_rejected_without_publication() -> None:
    engine = _engine()
    _seed_previous(engine)
    before_raw = _rows(engine, "qmt_index_weight")
    before_business = _rows(engine, "si_index_constituent")
    frame = _weight_frame([
        ("000300.SH", "600001", "SH"),
        ("000905.SH", "000002", "SZ"),
    ])
    frame.loc[frame["stock_code"] == "600001", "exchange"] = "SZ"

    with pytest.raises(RuntimeError, match="exchange differs from qmt_code"):
        reference_sync.publish_index_weight_snapshot(
            engine,
            expected_index_symbols=EXPECTED_INDEXES,
            index_weight=frame,
        )

    assert _rows(engine, "qmt_index_weight") == before_raw
    assert _rows(engine, "si_index_constituent") == before_business


def test_index_weight_business_write_failure_rolls_back_both_targets() -> None:
    engine = _engine()
    _seed_previous(engine)
    before_raw = _rows(engine, "qmt_index_weight")
    before_business = _rows(engine, "si_index_constituent")
    frame = _weight_frame([
        ("000300.SH", "600001", "SH"),
        ("000905.SH", "999999", "SH"),
    ])

    with pytest.raises(RuntimeError, match="all index partitions were rolled back"):
        reference_sync.publish_index_weight_snapshot(
            engine,
            expected_index_symbols=EXPECTED_INDEXES,
            index_weight=frame,
        )

    assert _rows(engine, "qmt_index_weight") == before_raw
    assert _rows(engine, "si_index_constituent") == before_business
