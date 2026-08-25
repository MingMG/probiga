from __future__ import annotations

from sqlalchemy import create_engine, text
import pytest

from server.common.legacy_table_surface import validate_required_table_surface


def test_required_surface_accepts_extra_legacy_columns_without_writing():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE legacy_quotes (id INTEGER, stock_code TEXT, extra TEXT)")
        )
    result = validate_required_table_surface(
        engine,
        {"legacy_quotes"},
        context="legacy quotes",
        required_columns={"legacy_quotes": {"id", "stock_code"}},
    )
    assert result["required_surface_verified"] is True
    assert result["read_only"] is True


def test_required_surface_fails_closed_for_missing_table():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with pytest.raises(RuntimeError, match="missing_tables"):
        validate_required_table_surface(
            engine,
            {"missing_table"},
            context="missing",
        )


def test_required_surface_fails_closed_for_missing_column():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE prepared_table (id INTEGER)"))
    with pytest.raises(RuntimeError, match="missing_columns"):
        validate_required_table_surface(
            engine,
            {"prepared_table"},
            context="prepared",
            required_columns={"prepared_table": {"id", "required_value"}},
        )


def test_identifiers_are_validated_before_metadata_access():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with pytest.raises(ValueError, match="unsafe table identifier"):
        validate_required_table_surface(
            engine,
            {"bad;drop"},
            context="unsafe",
        )
