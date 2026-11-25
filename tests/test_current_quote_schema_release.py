"""Release migration is read-only when the current-quote schema is canonical."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from server.common import current_quote_schema as schema
from server.common.current_quote_schema import privileged_migrate_current_quote_storage


def _index(column, *, unique=False):
    return [{'COLUMN_NAME': column, 'NON_UNIQUE': 0 if unique else 1,
             'SUB_PART': None, 'INDEX_TYPE': 'BTREE'}]


class Result:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value

    def mappings(self):
        return self

    def all(self):
        return deepcopy(self.value)

    def __iter__(self):
        return iter(deepcopy(self.value))


class Connection:
    def __init__(self, engine):
        self.engine = engine
        self.closed = False
        self.connection = SimpleNamespace(dbapi_connection=SimpleNamespace(_read_timeout=None, _write_timeout=None))

    def detach(self):
        self.engine.detached += 1

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def close(self):
        if not self.closed:
            self.closed = True
            self.engine.open_connections -= 1

    def execute(self, statement, parameters=None):
        sql = ' '.join(str(statement).split())
        engine = self.engine
        engine.statements.append((sql, parameters))
        if parameters and parameters.get('table_name') == 'st_qmt_realtime_sync_receipt_v2':
            return Result(engine.receipt_index)
        if parameters and parameters.get('table_name') == 'st_quote_event_v2':
            return Result(engine.level1_index)
        if sql == 'SET SESSION lock_wait_timeout=5':
            engine.metadata_wait_bounded = True
            return Result(None)
        if sql.startswith('ALTER TABLE st_qmt_realtime_sync_receipt_v2 ADD INDEX'):
            assert engine.metadata_wait_bounded
            assert 0 < self.connection.dbapi_connection._read_timeout <= 180
            assert 'ALGORITHM=INPLACE, LOCK=NONE' in sql
            if engine.receipt_build_error:
                raise RuntimeError('online build interrupted')
            engine.receipt_index = deepcopy(engine.canonical_receipt_index)
            return Result(None)
        if sql.startswith('ALTER TABLE st_quote_event_v2 ADD INDEX'):
            assert engine.metadata_wait_bounded
            assert 0 < self.connection.dbapi_connection._read_timeout <= 600
            assert 'ALGORITHM=INPLACE, LOCK=NONE' in sql
            if engine.receipt_build_error:
                raise RuntimeError('online index build interrupted')
            engine.level1_index = deepcopy(engine.canonical_level1_index)
            return Result(None)
        if 'information_schema.STATISTICS' in sql:
            if engine.checked and engine.drift:
                if engine.drift == 'redundant':
                    engine.indexes['idx_sc_code'] = _index('stock_code')
                elif engine.drift == 'unique':
                    engine.indexes.pop('uk_qmt_sm_stock_current_code', None)
                elif engine.drift == 'primary':
                    engine.indexes['PRIMARY'] = _index('stock_code', unique=True)
                elif engine.drift == 'secondary':
                    engine.indexes['idx_sc_snap'] = _index('stock_code')
            return Result([
                {'INDEX_NAME': name, **row}
                for name, rows in engine.indexes.items() for row in rows
            ])
        if 'SELECT ENGINE FROM information_schema.TABLES' in sql:
            return Result(engine.storage_engine)
        if sql.startswith('SELECT GET_LOCK'):
            if engine.lock_drift:
                engine.indexes.pop('uk_qmt_sm_stock_current_code')
            return Result(engine.lock_result)
        if sql.startswith('SELECT RELEASE_LOCK'):
            return Result(1)
        if sql.startswith('SELECT * FROM sm_stock_current'):
            return Result(engine.rows)
        if sql.startswith('SELECT id,stock_code FROM sm_stock_current'):
            values = [(row['id'], row['stock_code']) for row in engine.rows]
            return Result(values[::-1] if engine.unique_mismatch else values)
        if sql == 'ALTER TABLE sm_stock_current DROP INDEX idx_sc_code':
            assert engine.lock_result == 1
            engine.indexes.pop('idx_sc_code')
            if engine.row_drift:
                engine.rows[0]['price'] += 1
            return Result(None)
        if sql == 'CHECK TABLE sm_stock_current':
            engine.checked = True
            return Result(engine.checks)
        raise AssertionError('unexpected SQL in migration model')


class Engine:
    def __init__(self, *, redundant=False, lock_result=1):
        self.dialect = SimpleNamespace(name='mysql')
        self.canonical_receipt_index = [dict(NON_UNIQUE=1, SEQ_IN_INDEX=index, COLUMN_NAME=column,
            SUB_PART=None, INDEX_TYPE='BTREE', COLLATION='A', IS_VISIBLE='YES', EXPRESSION=None) for index, column in enumerate(
                ('source_provider','published_at','created_at'),1)]
        self.receipt_index = deepcopy(self.canonical_receipt_index)
        from server.trading_v2.quotes import LEVEL1_TIME_INDEX_COLUMNS
        self.canonical_level1_index = [dict(row, COLUMN_NAME=column, SEQ_IN_INDEX=index)
            for index, column in enumerate(LEVEL1_TIME_INDEX_COLUMNS, 1)
            for row in [self.canonical_receipt_index[0]]]
        self.level1_index = deepcopy(self.canonical_level1_index)
        self.receipt_build_error = False
        self.metadata_wait_bounded = False
        self.detached = 0
        self.indexes = {
            'PRIMARY': _index('id', unique=True),
            'uk_qmt_sm_stock_current_code': _index('stock_code', unique=True),
            'idx_sc_snap': _index('snapshot_at'),
        }
        if redundant:
            self.indexes['idx_sc_code'] = _index('stock_code')
        self.lock_result = lock_result
        self.storage_engine = 'InnoDB'
        self.rows = [{'id': 1, 'stock_code': '000001', 'price': 10},
                     {'id': 2, 'stock_code': '000002', 'price': 20}]
        self.checks = [{'Msg_type': 'status', 'Msg_text': 'OK'}]
        self.checked = False
        self.drift = None
        self.row_drift = False
        self.unique_mismatch = False
        self.lock_drift = False
        self.statements = []
        self.open_connections = 0

    def connect(self):
        # This also catches nested checkouts that would exhaust a one-slot pool.
        assert self.open_connections == 0
        self.open_connections += 1
        return Connection(self)


@pytest.mark.parametrize('lock_result', [0, None, 1])
def test_canonical_schema_checks_integrity_without_business_lock(lock_result):
    engine = Engine(lock_result=lock_result)
    before = deepcopy(engine.rows)
    result = privileged_migrate_current_quote_storage(engine)
    assert result['status'] == 'HEALTHY'
    assert result['integrity_verified'] is True
    assert result['redundant_index_removed'] is False
    assert result['read_only'] is True
    assert engine.rows == before and engine.open_connections == 0
    sql = [statement for statement, _ in engine.statements]
    assert sql.count('CHECK TABLE sm_stock_current') == 1
    assert all(statement.startswith('SELECT') or statement.startswith('CHECK TABLE')
               for statement in sql)
    assert not any('GET_LOCK' in statement or 'RELEASE_LOCK' in statement
                   or 'SELECT *' in statement for statement in sql)


@pytest.mark.parametrize('checks', [
    [], [{'Msg_type': 'error', 'Msg_text': 'corrupt'}],
    [{'Msg_type': 'status', 'Msg_text': 'NOT OK'}],
    [{'Msg_type': 'warning', 'Msg_text': 'OK'}],
    [{'Msg_type': 'status', 'Msg_text': 'OK'}, {'Msg_type': 'error', 'Msg_text': 'bad'}],
])
def test_canonical_schema_never_fabricates_integrity(checks):
    engine = Engine(lock_result=0)
    engine.checks = checks
    with pytest.raises(RuntimeError, match='integrity check failed'):
        privileged_migrate_current_quote_storage(engine)
    assert engine.open_connections == 0
    assert not any('GET_LOCK' in sql for sql, _ in engine.statements)


@pytest.mark.parametrize('drift', ['redundant', 'unique', 'primary', 'secondary'])
def test_schema_change_during_read_only_check_fails_closed(drift):
    engine = Engine(lock_result=0)
    engine.drift = drift
    with pytest.raises(RuntimeError):
        privileged_migrate_current_quote_storage(engine)
    assert engine.open_connections == 0
    assert not any(sql.startswith('ALTER') or 'GET_LOCK' in sql
                   for sql, _ in engine.statements)


@pytest.mark.parametrize('corruption', ['engine', 'unique', 'prefix', 'primary'])
def test_invalid_canonical_contract_is_rejected_before_integrity_check(corruption):
    engine = Engine(lock_result=0)
    if corruption == 'engine':
        engine.storage_engine = 'MyISAM'
    elif corruption == 'unique':
        engine.indexes.pop('uk_qmt_sm_stock_current_code')
    elif corruption == 'prefix':
        engine.indexes['uk_qmt_sm_stock_current_code'][0]['SUB_PART'] = 4
    else:
        engine.indexes['PRIMARY'] = _index('stock_code', unique=True)
    with pytest.raises(RuntimeError):
        privileged_migrate_current_quote_storage(engine)
    assert engine.open_connections == 0
    assert not any(sql.startswith('CHECK TABLE') or 'GET_LOCK' in sql
                   for sql, _ in engine.statements)


def test_write_migration_rechecks_contract_after_acquiring_lock():
    engine = Engine(redundant=True)
    engine.lock_drift = True
    with pytest.raises(RuntimeError, match='full unique stock key'):
        privileged_migrate_current_quote_storage(engine)
    assert engine.open_connections == 0
    assert any('RELEASE_LOCK' in sql for sql, _ in engine.statements)
    assert not any(sql.startswith('ALTER') for sql, _ in engine.statements)


@pytest.mark.parametrize('lock_result', [0, None])
def test_redundant_index_still_requires_original_lock(lock_result):
    engine = Engine(redundant=True, lock_result=lock_result)
    before = deepcopy(engine.rows)
    with pytest.raises(TimeoutError):
        privileged_migrate_current_quote_storage(engine)
    assert engine.rows == before and 'idx_sc_code' in engine.indexes
    assert engine.open_connections == 0
    lock = [(sql, params) for sql, params in engine.statements if 'GET_LOCK' in sql]
    assert lock == [('SELECT GET_LOCK(:lock_name, :timeout_seconds)',
                     {'lock_name': 'probiga:stock_current', 'timeout_seconds': 5})]
    assert not any(sql.startswith('ALTER') or sql.startswith('CHECK TABLE')
                   for sql, _ in engine.statements)


def test_index_migration_preserves_original_row_and_integrity_proofs():
    engine = Engine(redundant=True)
    before = deepcopy(engine.rows)
    result = privileged_migrate_current_quote_storage(engine)
    assert result['redundant_index_removed'] is True
    assert result['preserved_rows'] == 2 and len(result['rows_sha256']) == 64
    assert result['integrity_verified'] is True and result['read_only'] is False
    assert engine.rows == before and 'idx_sc_code' not in engine.indexes
    assert engine.open_connections == 0
    sql = [statement for statement, _ in engine.statements]
    assert sql.index('SELECT GET_LOCK(:lock_name, :timeout_seconds)') < sql.index(
        'ALTER TABLE sm_stock_current DROP INDEX idx_sc_code')
    assert sql.count('SELECT * FROM sm_stock_current FORCE INDEX(PRIMARY) ORDER BY id') == 2
    assert sql.count('CHECK TABLE sm_stock_current') == 1
    assert sql.count('SELECT RELEASE_LOCK(:lock_name)') == 1


@pytest.mark.parametrize('corruption', ['index', 'unique_mismatch', 'row_drift'])
def test_write_migration_keeps_existing_fail_closed_checks(corruption):
    engine = Engine(redundant=True)
    if corruption == 'index':
        engine.indexes['idx_sc_code'].append(_index('price')[0])
    elif corruption == 'unique_mismatch':
        engine.unique_mismatch = True
    else:
        engine.row_drift = True
    with pytest.raises(RuntimeError):
        privileged_migrate_current_quote_storage(engine)
    assert engine.open_connections == 0
    assert any('RELEASE_LOCK' in sql for sql, _ in engine.statements)
    if corruption != 'row_drift':
        assert not any(sql.startswith('ALTER') for sql, _ in engine.statements)


def test_live_receipt_index_migration_is_online_idempotent_and_does_not_read_business_rows():
    engine = Engine(lock_result=0)
    engine.receipt_index = []
    before = deepcopy(engine.rows)
    result = schema.privileged_migrate_live_quote_receipt_read_index(engine)
    assert result['created'] and result['verified'] and not result['read_only']
    assert schema.privileged_migrate_live_quote_receipt_read_index(engine)['created'] is False
    assert engine.rows == before and engine.open_connections == 0
    assert len([sql for sql, _ in engine.statements if sql.startswith('ALTER')]) == 1
    assert not any('GET_LOCK' in sql or 'SELECT *' in sql or 'CHECK TABLE' in sql for sql, _ in engine.statements)
    assert engine.detached == 2


@pytest.mark.parametrize('field,value',[
    ('NON_UNIQUE',0),('COLUMN_NAME','capture_mode'),('SUB_PART',4),('INDEX_TYPE','HASH'),('COLLATION','D'),
    ('IS_VISIBLE','NO'),('EXPRESSION','lower(source_provider)')])
def test_live_receipt_index_rejects_conflicting_definition_without_ddl(field,value):
    engine = Engine()
    engine.receipt_index[0][field] = value
    with pytest.raises(RuntimeError,match='definition differs'):
        schema.privileged_migrate_live_quote_receipt_read_index(engine)
    assert not any(sql.startswith('ALTER') for sql, _ in engine.statements)


def test_live_receipt_index_build_failure_is_terminal_and_does_not_repeat_ddl():
    engine = Engine()
    engine.receipt_index = []
    engine.receipt_build_error = True
    with pytest.raises(RuntimeError,match='online index build'):
        schema.privileged_migrate_live_quote_receipt_read_index(engine)
    assert not engine.receipt_index and engine.open_connections == 0
    assert len([sql for sql, _ in engine.statements if sql.startswith('ALTER')]) == 1


def test_existing_release_entrypoint_includes_the_receipt_index_migration():
    engine = Engine()
    engine.receipt_index = []
    result = privileged_migrate_current_quote_storage(engine)
    assert result['live_quote_receipt_read_index']['created'] is True
    assert result['read_only'] is False and result['integrity_verified'] is True


def test_level1_index_uses_existing_online_migrator_without_changing_rows():
    engine = Engine()
    engine.level1_index = []
    before = deepcopy(engine.rows)
    result = schema.privileged_migrate_level1_read_index(engine)
    assert result['created'] and result['verified'] and not result['read_only']
    assert not schema.privileged_migrate_level1_read_index(engine)['created']
    assert engine.rows == before and engine.open_connections == 0
    ddl = [sql for sql, _ in engine.statements if sql.startswith('ALTER')]
    assert ddl == ['ALTER TABLE st_quote_event_v2 ADD INDEX idx_quote_event_v2_time_quality '
                   '(quote_at,received_at,source_provider,bid1,ask1,bid1_volume,ask1_volume), '
                   'ALGORITHM=INPLACE, LOCK=NONE']


@pytest.mark.parametrize('fault', ['conflict', 'interrupted'])
def test_level1_index_failure_is_terminal_without_retry(fault):
    engine = Engine()
    if fault == 'conflict': engine.level1_index[0]['SUB_PART'] = 4
    else:
        engine.level1_index = []
        engine.receipt_build_error = True
    with pytest.raises(RuntimeError, match='definition differs|online index build'):
        schema.privileged_migrate_level1_read_index(engine)
    assert len([sql for sql, _ in engine.statements if sql.startswith('ALTER')]) == (fault == 'interrupted')
    assert engine.open_connections == 0
