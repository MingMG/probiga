"""Release-owned current quote indexes; runtime validation never issues DDL."""
from __future__ import annotations

import hashlib
import json
import time

from sqlalchemy import text

from server.common.mysql_lock import mysql_named_lock


_LIVE_RECEIPT_TABLE = 'st_qmt_realtime_sync_receipt_v2'
_LIVE_RECEIPT_INDEX = 'idx_qmt_realtime_source_published'
_LIVE_RECEIPT_COLUMNS = ('source_provider', 'published_at', 'created_at')
_LEVEL1_RECEIPT_INDEX = 'idx_qmt_realtime_level1_continuity'
_LEVEL1_RECEIPT_COLUMNS = ('capture_mode', 'quality_status', 'source_generated_at', 'published_at')


def _privileged_migrate_quote_read_index(engine, *, table, index, columns, timeout_seconds):
    """Add the online physical read index without changing collector contracts.

    The release/admin caller owns the bounded connection establishment. A
    dedicated checkout bounds metadata waits and DDL replies; an interrupted
    DDL is never retried here because its server outcome must first be read back.
    No business rows or receipt payloads are scanned for this additive index.
    """
    result = {'table': table, 'index': index,
              'columns': list(columns), 'created': False,
              'verified': False, 'read_only': True}
    if engine.dialect.name != 'mysql':
        return {**result, 'not_applicable': True}
    deadline = time.monotonic() + timeout_seconds
    with engine.connect() as connection:
        # Do not return the migration's socket/session limits to a runtime pool.
        connection.detach()
        driver = connection.connection.dbapi_connection
        original = {name: getattr(driver, name, None) for name in ('_read_timeout', '_write_timeout')}

        def execute(sql, parameters=None, *, stage):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f'quote read index {table} deadline: ' + stage)
            for name, value in original.items():
                setattr(driver, name, min(remaining, value) if value is not None else remaining)
            try:
                rows = connection.execute(text(sql), parameters or {})
            except Exception as exc:
                failure = RuntimeError(f'quote read index {table} failed: ' + stage)
                failure.native_schema_stage = f'{table}:{stage}'
                raise failure from exc
            if time.monotonic() >= deadline:
                raise TimeoutError(f'quote read index {table} deadline: ' + stage)
            return rows

        def inspect():
            rows = execute('''SELECT /*+ MAX_EXECUTION_TIME(4000) */
                NON_UNIQUE,SEQ_IN_INDEX,COLUMN_NAME,SUB_PART,INDEX_TYPE,COLLATION,IS_VISIBLE,EXPRESSION
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name AND INDEX_NAME=:index_name
                ORDER BY SEQ_IN_INDEX''', {'table_name': table, 'index_name': index},
                stage='index metadata').mappings().all()
            actual = [(int(row['NON_UNIQUE']), int(row['SEQ_IN_INDEX']), row['COLUMN_NAME'],
                       row['SUB_PART'], row['INDEX_TYPE'], row['COLLATION'],
                       row['IS_VISIBLE'], row['EXPRESSION']) for row in rows]
            expected = [(1, index, column, None, 'BTREE', 'A', 'YES', None)
                        for index, column in enumerate(columns, 1)]
            if rows and actual != expected:
                raise RuntimeError(f'quote read index {table} definition differs')
            return bool(rows)

        if inspect():
            return {**result, 'verified': True}
        for process in execute('SHOW FULL PROCESSLIST', stage='prior online index owner').mappings():
            statement=' '.join(str(process.get('Info') or '').replace('`','').lower().split())
            if statement.startswith((f'alter table {table} ',f'alter table probiga.{table} ')):
                failure=RuntimeError(f'quote read index {table} prior DDL is not terminal')
                failure.native_schema_stage=f'{table}:prior_ddl'
                failure.blocking_ddl_owner={
                    'connection_id':int(process['Id']),'elapsed_seconds':int(process['Time']),
                    'state':str(process.get('State') or '')[:120],
                }
                raise failure
        execute('SET SESSION lock_wait_timeout=5', stage='metadata lock budget')
        execute(f'ALTER TABLE {table} ADD INDEX {index} '
                f'({",".join(columns)}), ALGORITHM=INPLACE, LOCK=NONE', stage='online index build')
        if not inspect():
            raise RuntimeError(f'quote read index {table} missing after online build')
    return {**result, 'created': True, 'verified': True, 'read_only': False}


def privileged_migrate_live_quote_receipt_read_index(engine):
    return _privileged_migrate_quote_read_index(
        engine, table=_LIVE_RECEIPT_TABLE, index=_LIVE_RECEIPT_INDEX,
        columns=_LIVE_RECEIPT_COLUMNS, timeout_seconds=180,
    )


def privileged_migrate_level1_read_index(engine):
    from server.trading_v2.quotes import LEVEL1_TIME_INDEX, LEVEL1_TIME_INDEX_COLUMNS

    return _privileged_migrate_quote_read_index(
        engine, table='st_quote_event_v2', index=LEVEL1_TIME_INDEX,
        columns=LEVEL1_TIME_INDEX_COLUMNS, timeout_seconds=1800,
    )


def privileged_migrate_level1_receipt_read_index(engine):
    return _privileged_migrate_quote_read_index(
        engine, table=_LIVE_RECEIPT_TABLE, index=_LEVEL1_RECEIPT_INDEX,
        columns=_LEVEL1_RECEIPT_COLUMNS, timeout_seconds=180,
    )


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


def _check_integrity(connection):
    checks = connection.execute(text('CHECK TABLE sm_stock_current')).mappings().all()
    if not checks or any(
        row['Msg_type'] != 'status' or row['Msg_text'] != 'OK' for row in checks
    ):
        raise RuntimeError('current quote table integrity check failed after index migration')


def _privileged_migrate_current_quote_storage(engine):
    """Called only by the release migrator while both endpoints are fenced.

    The full unique stock key supersedes the old nonunique stock key. Removing
    that redundant index preserves every business row and leaves no runtime
    repair path or repeated table rebuild. Refuse unexpected index definitions.
    A canonical table needs only read-only validation and an actual integrity
    check; it must not contend for the business writer lock when no DDL is needed.
    Only an index migration enters that lock, then rechecks the physical state.
    """
    result = {'redundant_index_removed': False}
    with engine.connect() as connection:
        indexes = _indexes(connection)
        _validate_base(connection, indexes)
        requires_migration = 'idx_sc_code' in indexes
        if not requires_migration:
            _check_integrity(connection)
            # Do not endorse a schema that changed while the check ran.
            checked_indexes = _indexes(connection)
            _validate_base(connection, checked_indexes)
            if checked_indexes != indexes:
                raise RuntimeError('current quote schema changed during integrity check')
    if not requires_migration:
        return {
            **validate_current_quote_storage(engine), **result,
            'integrity_verified': True, 'read_only': True,
        }
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
        _check_integrity(connection)
    return {**validate_current_quote_storage(engine), **result, 'integrity_verified': True, 'read_only': False}


def privileged_migrate_current_quote_storage(engine):
    """Own quote read indexes here; unrelated Linux releases need no quote DDL."""
    quotes = _privileged_migrate_current_quote_storage(engine)
    receipt = privileged_migrate_live_quote_receipt_read_index(engine)
    level1 = privileged_migrate_level1_read_index(engine)
    level1_receipt = privileged_migrate_level1_receipt_read_index(engine)
    return {**quotes, 'live_quote_receipt_read_index': receipt,'level1_read_index':level1,
            'level1_receipt_read_index':level1_receipt,
            'read_only': quotes['read_only'] and receipt['read_only'] and level1['read_only'] and level1_receipt['read_only']}
