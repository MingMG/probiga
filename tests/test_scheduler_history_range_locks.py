"""Task-scoped history locks must not block a different scheduler's owner."""
from datetime import datetime, timedelta
import os
import re
import uuid

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from server.api import scheduler_runtime as runtime


HISTORY_FIELDS = """id INTEGER PRIMARY KEY,run_uid VARCHAR(64) UNIQUE,task_id INTEGER,
    task_name VARCHAR(255),task_type VARCHAR(64),run_at DATETIME,finished_at DATETIME,
    status VARCHAR(32),host_name VARCHAR(128),scheduler_instance_id VARCHAR(128),
    build_sha VARCHAR(40),trigger_source VARCHAR(32)"""
TASK_FIELDS = """id INTEGER PRIMARY KEY,task_type VARCHAR(64),last_run_status VARCHAR(32),
    last_run_at DATETIME,last_triggered_at DATETIME"""
NOW = datetime(2026, 11, 24, 5)
TASK_TYPE = 'trading_v2_job_worker'


def history_row(number, task_id, *, at=NOW, host='linux-host'):
    return dict(id=number,run_uid=format(number,'032x'),task_id=task_id,
        task_name='test-worker',task_type=TASK_TYPE,run_at=at,finished_at=None,
        status='running',host_name=host,scheduler_instance_id=host+'-123',
        build_sha='a'*40,trigger_source='scheduled')


def insert_history(connection, row):
    fields=','.join(row)
    connection.execute(text(f"INSERT INTO st_scheduled_task_history ({fields}) VALUES ("+
        ','.join(':'+field for field in row)+')'),row)


def insert_task(connection):
    connection.execute(text("INSERT INTO st_scheduled_tasks VALUES (127,:type,'running',:now,:now)"),
                       {'type':TASK_TYPE,'now':NOW})


def intent(connection, uid):
    row=connection.execute(text('SELECT * FROM st_scheduled_task_history WHERE run_uid=:uid'),
                           {'uid':uid}).mappings().one()
    return {'identity':runtime._terminal_run_identity(row),'outcome':{'task_type':TASK_TYPE}}


@pytest.mark.parametrize('host',['linux-host','WINDOWS-QMT'])
@pytest.mark.parametrize('new_stamp',[NOW,NOW-timedelta(days=1)],ids=['same-second','older-clock'])
def test_sqlite_terminal_fence_keeps_latest_id_and_exact_uid(host,new_stamp):
    engine=create_engine('sqlite://')
    try:
        with engine.begin() as connection:
            connection.execute(text('CREATE TABLE st_scheduled_task_history ('+HISTORY_FIELDS+')'))
            connection.execute(text('CREATE TABLE st_scheduled_tasks ('+TASK_FIELDS+')'))
            insert_task(connection)
            insert_history(connection,history_row(10,127,at=NOW-timedelta(days=2),host=host))
            current=history_row(11,127,host=host)
            insert_history(connection,current)
            insert_history(connection,history_row(12,70))
            original=intent(connection,current['run_uid'])
            for locking in (False,True):
                assert runtime._terminal_check_running(connection,current['run_uid'],original,for_update=locking)
            # A new UID remains a new claim even with the same or an older timestamp.
            insert_history(connection,history_row(13,127,at=new_stamp,host=host))
            for locking in (False,True):
                with pytest.raises(runtime._TerminalPersistencePending,match='current claim differs'):
                    runtime._terminal_check_running(connection,current['run_uid'],original,for_update=locking)
    finally:
        engine.dispose()


@pytest.mark.skipif(not os.environ.get('PROBIGA_MYSQL57_TEST_URL'),
                    reason='PROBIGA_MYSQL57_TEST_URL is not configured')
def test_mysql_terminal_history_lock_does_not_lock_another_tasks_owner():
    """Real InnoDB A/B; unique fixture tables in an explicitly named test DB."""
    engine=create_engine(os.environ['PROBIGA_MYSQL57_TEST_URL'],isolation_level='REPEATABLE READ',
        connect_args={'connect_timeout':5,'read_timeout':10,'write_timeout':5})
    assert 'test' in str(engine.url.database or '').lower()
    suffix=uuid.uuid4().hex[:8]
    history='scheduler_history_lock_test_'+suffix
    tasks='scheduler_task_lock_test_'+suffix
    latest_sql=[]
    unsafe=False

    def fixture_tables(_connection,_cursor,statement,parameters,_context,_many):
        statement=statement.replace('st_scheduled_task_history',history).replace('st_scheduled_tasks',tasks)
        if statement.lstrip().startswith('SELECT run_uid FROM '+history) and 'FOR UPDATE' in statement:
            if unsafe:
                # Reproduce the PRIMARY backward scan observed on production task 127.
                statement=re.sub(r' FORCE INDEX \([^)]+\)','',statement)
                statement=statement.replace('FROM '+history,'FROM '+history+' FORCE INDEX (PRIMARY)')
            latest_sql.append((statement,parameters))
        return statement,parameters

    event.listen(engine,'before_cursor_execute',fixture_tables,retval=True)
    try:
        with engine.begin() as connection:
            connection.execute(text('CREATE TABLE st_scheduled_task_history ('+HISTORY_FIELDS+''',
                KEY idx_scheduled_task_history_task_run (task_id,run_at)) ENGINE=InnoDB'''))
            connection.execute(text('CREATE TABLE st_scheduled_tasks ('+TASK_FIELDS+') ENGINE=InnoDB'))
            insert_task(connection)
            current=history_row(4001,127)
            insert_history(connection,current)
            native=history_row(4002,70)
            insert_history(connection,native)
        with engine.connect() as connection:
            original=intent(connection,current['run_uid'])
        with engine.connect() as owner, engine.connect() as observer:
            observer.execute(text('SET SESSION innodb_lock_wait_timeout=1'));observer.commit()
            for unsafe in (True,False):
                try:
                    assert runtime._terminal_check_running(owner,current['run_uid'],original,for_update=True)
                    statement,parameters=latest_sql[-1]
                    plan=owner.exec_driver_sql('EXPLAIN '+statement,parameters).mappings().one()
                    assert plan['key']==('PRIMARY' if unsafe else 'idx_scheduled_task_history_task_run')
                    exact=text('SELECT run_uid FROM st_scheduled_task_history WHERE run_uid=:uid FOR UPDATE')
                    if unsafe:
                        with pytest.raises(OperationalError) as error:
                            observer.execute(exact,{'uid':native['run_uid']}).scalar_one()
                        assert error.value.orig.args[0]==1205
                    else:
                        assert observer.execute(exact,{'uid':native['run_uid']}).scalar_one()==native['run_uid']
                finally:
                    observer.rollback();owner.rollback()
                assert observer.execute(text('SELECT run_uid FROM st_scheduled_task_history WHERE run_uid=:uid FOR UPDATE'),
                    {'uid':native['run_uid']}).scalar_one()==native['run_uid']
                observer.rollback()
        with engine.connect() as connection:
            assert connection.execute(text('SELECT COUNT(*) FROM st_scheduled_task_history')).scalar_one()==2
    finally:
        with engine.begin() as connection:
            connection.execute(text('DROP TABLE IF EXISTS '+history))
            connection.execute(text('DROP TABLE IF EXISTS '+tasks))
        engine.dispose()
