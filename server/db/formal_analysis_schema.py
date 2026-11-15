"""Privileged migration and read-only validation for formal analysis storage."""
from __future__ import annotations

from sqlalchemy import inspect, text


ARTIFACT_TABLE = "st_formal_analysis_artifact"
MEMBER_TABLE = "st_formal_analysis_checkpoint"
FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES = 15 * 1024 * 1024

_MYSQL_TABLES = (
    f"""
        CREATE TABLE IF NOT EXISTS {ARTIFACT_TABLE} (
            artifact_sha256 CHAR(64) NOT NULL,
            payload_bytes INT UNSIGNED NOT NULL,
            payload_zlib MEDIUMBLOB NOT NULL,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (artifact_sha256),
            CONSTRAINT ck_formal_analysis_artifact_size CHECK
                (payload_bytes BETWEEN 1 AND {FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES})
        ) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin
    """,
    f"""
        CREATE TABLE IF NOT EXISTS {MEMBER_TABLE} (
            generation_sha256 CHAR(64) NOT NULL,
            stage VARCHAR(48) NOT NULL,
            unit_key VARCHAR(160) NOT NULL,
            artifact_sha256 CHAR(64) NOT NULL,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (generation_sha256,stage,unit_key),
            KEY idx_formal_analysis_checkpoint_artifact (artifact_sha256),
            CONSTRAINT fk_formal_analysis_checkpoint_artifact FOREIGN KEY
                (artifact_sha256) REFERENCES {ARTIFACT_TABLE} (artifact_sha256)
                ON DELETE RESTRICT ON UPDATE RESTRICT
        ) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin
    """,
)

_SQLITE_TABLES = (
    f"""
        CREATE TABLE IF NOT EXISTS {ARTIFACT_TABLE} (
            artifact_sha256 TEXT NOT NULL PRIMARY KEY,
            payload_bytes INTEGER NOT NULL CHECK (payload_bytes BETWEEN 1 AND {FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES}),
            payload_zlib BLOB NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    f"""
        CREATE TABLE IF NOT EXISTS {MEMBER_TABLE} (
            generation_sha256 TEXT NOT NULL,
            stage TEXT NOT NULL,
            unit_key TEXT NOT NULL,
            artifact_sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (generation_sha256,stage,unit_key),
            CONSTRAINT fk_formal_analysis_checkpoint_artifact FOREIGN KEY
                (artifact_sha256) REFERENCES {ARTIFACT_TABLE} (artifact_sha256)
                ON DELETE RESTRICT ON UPDATE RESTRICT
        )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_formal_analysis_checkpoint_artifact ON {MEMBER_TABLE} (artifact_sha256)",
)

_COLUMNS = {
    ARTIFACT_TABLE: ("artifact_sha256", "payload_bytes", "payload_zlib", "created_at"),
    MEMBER_TABLE: ("generation_sha256", "stage", "unit_key", "artifact_sha256", "created_at"),
}
_PRIMARY_KEYS = {
    ARTIFACT_TABLE: ("artifact_sha256",),
    MEMBER_TABLE: ("generation_sha256", "stage", "unit_key"),
}
_MYSQL_TYPES = {
    ARTIFACT_TABLE: ("char(64)", "int unsigned", "mediumblob", "datetime(6)"),
    MEMBER_TABLE: ("char(64)", "varchar(48)", "varchar(160)", "char(64)", "datetime(6)"),
}


def validate_formal_analysis_runtime_schema(engine) -> dict:
    """Inspect physical storage and constraints without modifying the database."""
    with engine.connect() as connection:
        dialect = connection.dialect
        if dialect.name not in {"mysql", "sqlite"}:
            raise RuntimeError("formal analysis storage database dialect is unsupported")
        metadata = inspect(connection)
        for table, names in _COLUMNS.items():
            columns = metadata.get_columns(table)
            if (
                tuple(column["name"] for column in columns) != names
                or any(column["nullable"] for column in columns)
                or tuple(metadata.get_pk_constraint(table)["constrained_columns"]) != _PRIMARY_KEYS[table]
            ):
                raise RuntimeError(f"formal analysis schema differs for {table}")
            if dialect.name == "mysql":
                actual = connection.execute(text("""
                    SELECT COLUMN_TYPE,COLLATION_NAME FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table ORDER BY ORDINAL_POSITION
                """), {"table": table}).all()
                expected = [
                    (column_type, "ascii_bin" if column_type.startswith(("char(", "varchar(")) else None)
                    for column_type in _MYSQL_TYPES[table]
                ]
                if [tuple(column) for column in actual] != expected:
                    raise RuntimeError(f"formal analysis column types differ for {table}")
                actual_options = connection.execute(text("""
                    SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table
                """), {"table": table}).one_or_none()
                if actual_options is None or tuple(actual_options) != ("InnoDB", "ascii_bin"):
                    raise RuntimeError(f"formal analysis table storage differs for {table}")
        links = metadata.get_foreign_keys(MEMBER_TABLE)
        if not any(
            link["constrained_columns"] == ["artifact_sha256"]
            and link["referred_table"] == ARTIFACT_TABLE
            and link["referred_columns"] == ["artifact_sha256"]
            and all(link.get("options", {}).get(option, "RESTRICT") in {"RESTRICT", "NO ACTION"}
                    for option in ("onupdate", "ondelete"))
            for link in links
        ):
            raise RuntimeError("formal analysis checkpoint artifact foreign key differs")
        indexes = metadata.get_indexes(MEMBER_TABLE)
        if not any(index["column_names"] == ["artifact_sha256"] for index in indexes):
            raise RuntimeError("formal analysis checkpoint artifact index differs")
    return {"tables": list(_COLUMNS), "read_only": True, "runtime_ddl_required": False}


def privileged_migrate_formal_analysis_schema(engine) -> dict:
    """Create storage through the existing fenced schema migration."""
    if engine.dialect.name == "mysql":
        statements = _MYSQL_TABLES
    elif engine.dialect.name == "sqlite":
        statements = _SQLITE_TABLES
    else:
        raise RuntimeError("formal analysis storage database dialect is unsupported")
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
    return validate_formal_analysis_runtime_schema(engine)
