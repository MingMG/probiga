from __future__ import annotations

from contextlib import nullcontext

import pytest

from server.common import portfolio_schema as schema


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement, _params=None):
        self.statements.append(str(statement))
        return None


class _Engine:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def begin(self):
        return nullcontext(self.connection)


def test_portfolio_runtime_contract_requires_public_quote_quorum_table(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        schema,
        "_table_metadata",
        lambda _connection: {
            schema.PORTFOLIO_TABLE: {
                "engine": "InnoDB",
                "table_collation": "utf8mb4_unicode_ci",
            },
            schema.PORTFOLIO_TRANSACTION_TABLE: {
                "engine": "InnoDB",
                "table_collation": "utf8mb4_unicode_ci",
            },
        },
    )

    with pytest.raises(RuntimeError, match="st_portfolio_public_quote_v1"):
        schema._validate_on_connection(object())


def test_privileged_portfolio_migration_creates_public_quote_quorum_table(
    monkeypatch,
) -> None:
    connection = _Connection()
    engine = _Engine(connection)
    legacy_columns = {
        column: {}
        for column in (
            "id",
            "stock_code",
            "short_name",
            "cost_price",
            "shares",
            "add_date",
            "sort_order",
            "notes",
            "is_holding",
            "etl_sync_at",
        )
    }
    monkeypatch.setattr(
        schema,
        "_table_metadata",
        lambda _connection: {schema.PORTFOLIO_TABLE: {}},
    )
    monkeypatch.setattr(
        schema,
        "_column_metadata",
        lambda _connection: {
            schema.PORTFOLIO_TABLE: legacy_columns,
            schema.PORTFOLIO_TRANSACTION_TABLE: {},
            schema.PORTFOLIO_PUBLIC_QUOTE_TABLE: {},
        },
    )
    monkeypatch.setattr(schema, "_normalize_column", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        schema,
        "validate_portfolio_runtime_schema",
        lambda *_args, **_kwargs: None,
    )

    schema.privileged_migrate_portfolio_schema(engine)

    sql = "\n".join(connection.statements)
    assert "CREATE TABLE IF NOT EXISTS `st_portfolio_public_quote_v1`" in sql
    assert "PRIMARY KEY (`stock_code`)" in sql
    assert "(`trade_date`, `quote_at`, `quality_status`)" in sql
    assert (
        "ALTER TABLE `st_portfolio_public_quote_v1` ENGINE=InnoDB" in sql
    )


def test_public_quote_runtime_surface_matches_writer_contract() -> None:
    columns = schema.PORTFOLIO_REQUIRED_SURFACE[
        schema.PORTFOLIO_PUBLIC_QUOTE_TABLE
    ]

    assert set(columns) == {
        "stock_code",
        "batch_id",
        "trade_date",
        "quote_at",
        "short_name",
        "price",
        "pre_close",
        "change_pct",
        "volume",
        "amount",
        "source_provider",
        "source_count",
        "provider_mask",
        "price_deviation_pct",
        "received_at",
        "quality_status",
        "created_at",
        "updated_at",
    }
