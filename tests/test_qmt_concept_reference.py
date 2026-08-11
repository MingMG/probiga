from __future__ import annotations

import pandas as pd
import pytest

from biz.stock_info import sync_stock_info


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement, _params=None):
        self.statements.append(str(statement))


class _Transaction:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class _Engine:
    def __init__(self) -> None:
        self.connection = _Connection()
        self.begins = 0

    def begin(self):
        self.begins += 1
        return _Transaction(self.connection)


def test_qmt_concept_reference_replaces_catalog_and_memberships_in_one_transaction(monkeypatch):
    engine = _Engine()
    writes: list[tuple[str, int, object]] = []
    catalog = pd.DataFrame(
        [{"concept_code": "TGN人工智能", "index_code": "TGN人工智能", "name": "人工智能", "source": "qmt"}]
    )
    members = pd.DataFrame(
        [
            {"concept_code": "TGN人工智能", "stock_code": "1", "short_name": "平安银行"},
            {"concept_code": "TGN人工智能", "stock_code": "000001", "short_name": "平安银行"},
            {"concept_code": "UNKNOWN", "stock_code": "600519", "short_name": "贵州茅台"},
        ]
    )
    monkeypatch.setattr(
        sync_stock_info,
        "_qmt_sector_tables",
        lambda: {"concept_catalog": catalog, "concept_constituents": members},
    )
    monkeypatch.setattr(sync_stock_info, "_stock_pool_name_map", lambda _engine: {"000001": "骞冲畨閾惰"})
    monkeypatch.setattr(
        sync_stock_info,
        "write_frame",
        lambda frame, table, target, **_kwargs: writes.append((table, len(frame), target)) or len(frame),
    )

    result = sync_stock_info.sync_qmt_concept_reference(engine)

    assert result == {"concepts": 1, "memberships": 1}
    assert engine.begins == 1
    assert [item[:2] for item in writes] == [
        ("si_concept_code_east", 1),
        ("si_concept_constituent_east", 1),
    ]
    assert all(item[2] is engine.connection for item in writes)
    assert "si_concept_constituent_east" in engine.connection.statements[0]
    assert "si_concept_code_east" in engine.connection.statements[1]


def test_qmt_concept_reference_preserves_old_snapshot_when_memberships_are_empty(monkeypatch):
    engine = _Engine()
    monkeypatch.setattr(
        sync_stock_info,
        "_qmt_sector_tables",
        lambda: {
            "concept_catalog": pd.DataFrame([{"concept_code": "TGN人工智能"}]),
            "concept_constituents": pd.DataFrame(),
        },
    )
    monkeypatch.setattr(sync_stock_info, "_stock_pool_name_map", lambda _engine: {"000001": "骞冲畨閾惰"})

    with pytest.raises(RuntimeError, match="preserving previous snapshots"):
        sync_stock_info.sync_qmt_concept_reference(engine)

    assert engine.begins == 0


def test_qmt_index_constituents_are_stock_pool_filtered_and_atomically_replaced(monkeypatch):
    engine = _Engine()
    writes: list[tuple[str, list[str]]] = []
    raw = pd.DataFrame(
        [
            {"index_code": "000300", "stock_code": "000001", "short_name": "old"},
            {"index_code": "000300", "stock_code": "110044", "short_name": "bond"},
            {"index_code": "000905", "stock_code": "600519", "short_name": "old"},
        ]
    )
    monkeypatch.setattr(sync_stock_info, "_source_value", lambda *_args, **_kwargs: "qmt")
    monkeypatch.setattr("integrations.qmt.info.fetch_index_constituents", lambda _codes: raw)
    monkeypatch.setattr(
        sync_stock_info,
        "_stock_pool_name_map",
        lambda _engine: {"000001": "平安银行", "600519": "贵州茅台"},
    )
    monkeypatch.setattr(
        sync_stock_info,
        "replace_table_rows",
        lambda frame, table, target, **_kwargs: writes.append(
            (table, frame["stock_code"].tolist())
        ) or len(frame),
    )

    sync_stock_info.sync_index_constituent(
        engine,
        None,
        pd.DataFrame([{"index_code": "000300"}, {"index_code": "000905"}]),
    )

    assert writes == [("si_index_constituent", ["000001", "600519"])]
    assert engine.begins == 0
