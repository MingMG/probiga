"""Release-owned current quote indexes; runtime validation never issues DDL."""
from __future__ import annotations

import hashlib
import json

from sqlalchemy import text

from server.common.mysql_lock import mysql_named_lock


def _indexes(connection):
    rows = connection.execute(text(
        "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME, SUB_PART, INDEX_TYPE "
        "FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='sm_stock_current' "
        "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
    )).mappings().all()
    result = {}
    for row in rows:
        result.setdefault(row['INDEX_NAME'], []).append(dict(row))
    return result


def _stock_index(rows, *, unique):
    return len(rows) == 1 and (
        rows[0]['COLUMN_NAME'] == 'stock_code'
        and int(rows[0]['NON_UNIQUE']) == (0 if unique else 1)
        and rows[0]['SUB_PART'] is None
        and rows[0]['INDEX_TYPE'] == 'BTREE'
    )


def _validate_base(connection, indexes):
    engine = connection.execute(text(
        "SELECT ENGINE FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='sm_stock_current'"
    )).scalar()
    if engine != 'InnoDB' or not _stock_index(
        indexes.get('uk_qmt_sm_stock_current_code', []), unique=True
    ):
        raise RuntimeError('current quote storage requires InnoDB and the full unique stock key')
    primary = indexes.get('PRIMARY', [])
    if len(primary) != 1 or primary[0]['COLUMN_NAME'] != 'id':
        raise RuntimeError('current quote storage requires the id primary key')


def validate_current_quote_storage(engine):
    with engine.connect() as connection:
        indexes = _indexes(connection)
        _validate_base(connection, indexes)
        if 'idx_sc_code' in indexes:
            raise RuntimeError('current quote redundant stock index requires release migration')
    return {
        'schema': 'probiga.current-quote-storage.v1', 'status': 'HEALTHY',
        'physical_schema_verified': True, 'runtime_ddl_required': False,
        'unique_stock_key': 'uk_qmt_sm_stock_current_code', 'read_only': True,
    }


def _rows(connection):
    return [dict(row) for row in connection.execute(text(
        'SELECT * FROM sm_stock_current FORCE INDEX(PRIMARY) ORDER BY id'
    )).mappings()]


def _digest(rows):
    return hashlib.sha256(json.dumps(
        rows, sort_keys=True, ensure_ascii=False, separators=(',', ':'), default=str
    ).encode('utf-8')).hexdigest()


def privileged_migrate_current_quote_storage(engine):
    """Called only by the release migrator while both endpoints are fenced.

    The full unique stock key supersedes the old nonunique stock key. Removing
    that redundant index preserves every business row and leaves no runtime
    repair path or repeated table rebuild. Refuse unexpected index definitions.
    """
    result = {'redundant_index_removed': False}
    with mysql_named_lock(engine, 'probiga:stock_current', timeout_seconds=5) as connection:
        indexes = _indexes(connection)
        _validate_base(connection, indexes)
        if 'idx_sc_code' in indexes:
            if not _stock_index(indexes['idx_sc_code'], unique=False):
                raise RuntimeError('refusing to remove a different idx_sc_code definition')
            before = _rows(connection)
            unique_rows = connection.execute(text(
                'SELECT id,stock_code FROM sm_stock_current '
                'FORCE INDEX(uk_qmt_sm_stock_current_code) ORDER BY id'
            )).all()
            if [tuple(row) for row in unique_rows] != [(r['id'], r['stock_code']) for r in before]:
                raise RuntimeError('current quote unique index does not match primary rows')
            before_hash = _digest(before)
            # Release writer fencing spans MySQL's implicit DDL commit.
            connection.execute(text('ALTER TABLE sm_stock_current DROP INDEX idx_sc_code'))
            after = _rows(connection)
            if before_hash != _digest(after):
                raise RuntimeError('current quote rows changed during index migration')
            result.update(redundant_index_removed=True, preserved_rows=len(after), rows_sha256=before_hash)
        checks = connection.execute(text('CHECK TABLE sm_stock_current')).mappings().all()
        if not checks or any(
            row['Msg_type'] != 'status' or row['Msg_text'] != 'OK' for row in checks
        ):
            raise RuntimeError('current quote table integrity check failed after index migration')
    return {**validate_current_quote_storage(engine), **result, 'integrity_verified': True, 'read_only': False}
