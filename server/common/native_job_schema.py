"""Linux research queue columns; independent of execution-evidence ledgers."""
from sqlalchemy import inspect, text

LOCK = 'probiga:native-research-job-schema'
INDEX = 'idx_job_native_ready'
COLUMNS = {'next_run_at': ('datetime', None), 'scheduler_run_uid': ('varchar', 64)}
INDEX_COLUMNS = ['job_type', 'status', 'next_run_at', 'requested_at', 'job_id']


def _validate_existing(columns,indexes):
    for name,(kind,length) in COLUMNS.items():
        if name not in columns:continue
        column=columns[name]
        if (str(column['type']).lower().split('(')[0]!=kind or not column['nullable']
                or (length is not None and column['type'].length!=length)):
            raise RuntimeError('NATIVE_JOB_SCHEMA_DRIFT:'+name)
    if INDEX in indexes and (indexes[INDEX]['column_names']!=INDEX_COLUMNS or indexes[INDEX]['unique']):
        raise RuntimeError('NATIVE_JOB_SCHEMA_DRIFT:'+INDEX)


def validate_native_job_schema(engine):
    inspector = inspect(engine)
    columns = {item['name']: item for item in inspector.get_columns('st_job_v2')}
    for name, (kind, length) in COLUMNS.items():
        column = columns.get(name)
        if (column is None or str(column['type']).lower().split('(')[0] != kind
                or not column['nullable'] or (length is not None and column['type'].length != length)):
            raise RuntimeError('NATIVE_JOB_SCHEMA_DIFFERS:' + name)
    indexes = {item['name']: item for item in inspector.get_indexes('st_job_v2')}
    if INDEX not in indexes or indexes[INDEX]['column_names'] != INDEX_COLUMNS or indexes[INDEX]['unique']:
        raise RuntimeError('NATIVE_JOB_SCHEMA_DIFFERS:' + INDEX)
    return {'valid': True, 'table': 'st_job_v2', 'columns': sorted(COLUMNS), 'index': INDEX}


def privileged_migrate_native_job_schema(engine):
    """Only the existing privileged release boundary calls this operation."""
    if engine.dialect.name != 'mysql':
        raise RuntimeError('native research queue migration requires MySQL')
    with engine.connect() as connection:
        if connection.execute(text('SELECT GET_LOCK(:name,10)'), {'name': LOCK}).scalar() != 1:
            raise RuntimeError('NATIVE_JOB_SCHEMA_LOCK_DEADLINE')
        try:
            connection.execute(text('SET SESSION lock_wait_timeout=10'))
            inspector = inspect(connection)
            columns = {item['name']: item for item in inspector.get_columns('st_job_v2')}
            indexes = {item['name']: item for item in inspector.get_indexes('st_job_v2')}
            _validate_existing(columns,indexes)
            for name, declaration in [('next_run_at', 'DATETIME NULL'), ('scheduler_run_uid', 'VARCHAR(64) NULL')]:
                if name not in columns:
                    connection.execute(text(f'ALTER TABLE st_job_v2 ADD COLUMN {name} {declaration}, ALGORITHM=INSTANT'))
                    connection.commit()
            indexes = {item['name']: item for item in inspect(connection).get_indexes('st_job_v2')}
            if INDEX not in indexes:
                connection.execute(text(f'ALTER TABLE st_job_v2 ADD INDEX {INDEX} '
                    '(job_type,status,next_run_at,requested_at,job_id), ALGORITHM=INPLACE, LOCK=NONE'))
                connection.commit()
        finally:
            connection.rollback()
            connection.execute(text('SELECT RELEASE_LOCK(:name)'), {'name': LOCK})
            connection.commit()
    return validate_native_job_schema(engine)
