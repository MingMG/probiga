"""Typed native jobs fenced by the existing scheduler lease and audit head."""
from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from datetime import datetime, timedelta
import json
import os
import socket
import time
import threading
import uuid

from sqlalchemy import text

from server.common.analysis_pool_receipt import canonical_sha256
from server.common.market_dashboard_worker import process_identity
from server.common.scheduler_task_lease import read as read_lease
from server.engine.autonomous_strategy_research import now_shanghai

JOB_TYPE = 'NATIVE_REPLAY'
CLAIM_SECONDS = 20


@contextmanager
def claim_database_deadline(connection, deadline):
    """Cancel this native session's SQL before its claim transaction commits."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('NATIVE_CLAIM_DATABASE_DEADLINE')
    if connection.dialect.name != 'mysql':
        yield
        if time.monotonic() >= deadline:
            raise TimeoutError('NATIVE_CLAIM_DATABASE_DEADLINE')
        return
    from server.common.engine_factory import create_pooled_engine
    from server.engine.strategy_native_backtest import LOCK
    identity = int(connection.connection.driver_connection.thread_id())
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('NATIVE_CLAIM_DATABASE_DEADLINE')
    finished = threading.Event()
    expired = threading.Event()
    gate = threading.Lock()
    errors = []

    def cancel():
        with gate:
            if finished.is_set():
                return
            expired.set()
            control = None
            try:
                control = create_pooled_engine(connection.engine.url, pool_size=1, max_overflow=0,
                    pool_timeout=1, pool_pre_ping=False, isolation_level='AUTOCOMMIT',
                    connect_args={'connect_timeout': 1, 'read_timeout': 1, 'write_timeout': 1})
                with control.connect() as target:
                    if target.execute(text('SELECT IS_USED_LOCK(:name)'), {'name': LOCK}).scalar() != identity:
                        raise RuntimeError('native claim SQL session ownership changed')
                    target.execute(text('KILL QUERY ' + str(identity)))
            except Exception as exc:
                errors.append(type(exc).__name__)
            finally:
                if control is not None:
                    control.dispose()

    timer = threading.Timer(remaining, cancel)
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        with gate:
            finished.set()
        timer.cancel()
        timer.join(timeout=1)
        if expired.is_set() or time.monotonic() >= deadline:
            raise TimeoutError('NATIVE_CLAIM_DATABASE_DEADLINE' + (': cancel failed ' + ','.join(errors) if errors else ''))


def _locked(connection):
    return ' FOR UPDATE' if connection.dialect.name == 'mysql' else ''


def owner(connection, *, locking=False):
    uid = os.environ.get('PROBIGA_SCHEDULER_HISTORY_RUN_UID', '')
    task_id = os.environ.get('PROBIGA_SCHEDULER_TASK_ID', '')
    if not uid or not task_id.isdecimal() or os.environ.get('PROBIGA_SCHEDULER_TASK_TYPE') != 'trading_v2_job_worker':
        raise ValueError('native job requires the exact scheduler execution identity')
    row = connection.execute(text('SELECT * FROM st_scheduled_task_history WHERE run_uid=:uid' +
        (_locked(connection) if locking else '')), {'uid': uid}).mappings().one_or_none()
    if (row is None or row['status'] != 'running' or row['finished_at'] is not None
            or row['host_name'] != socket.gethostname() or int(row['task_id']) != int(task_id)
            or row['task_type'] != 'trading_v2_job_worker'):
        raise ValueError('native scheduler execution is no longer active')
    lease = read_lease(dict(row))
    if lease is None or not lease['renewed_at'] <= now_shanghai() <= lease['lease_until']:
        raise ValueError('native scheduler owner lease expired')
    birth = process_identity(os.getpid())
    if birth is None:
        raise ValueError('native worker process birth identity is unavailable')
    return {'scheduler_run_uid': uid, 'host': socket.gethostname(), 'pid': os.getpid(), 'pid_birth': birth}


def enqueue(connection, state, *, requested_by='native-strategy-research'):
    """The caller transaction creates the immutable experiment and its queue row."""
    from server.engine import strategy_native_backtest as native
    key = canonical_sha256({'job_type': JOB_TYPE, 'experiment_id': state['run_id']})
    row = connection.execute(text('SELECT job_id,status,result_ref FROM st_job_v2 WHERE idempotency_key=:key'),
                             {'key': key}).mappings().first()
    if row:
        if row['result_ref'] != state['run_id']:
            raise ValueError('native queue identity differs')
        return dict(row)
    if native.read_runs(connection, run_id=state['run_id']):
        raise ValueError('native experiment exists without its typed job')
    job_id = uuid.uuid4().hex
    state = deepcopy(state)
    state.update(job_id=job_id, owner=None, checkpoint_sequence=1)
    native._append_state(connection, state)
    connection.execute(text('''INSERT INTO st_job_v2
        (job_id,job_type,idempotency_key,request_json,status,result_ref,requested_by,requested_at,next_run_at)
        VALUES (:id,:kind,:key,:request,'PENDING',:run,:by,:now,:now)'''),
        {'id': job_id, 'kind': JOB_TYPE, 'key': key, 'run': state['run_id'],
         'request': json.dumps({'experiment_id': state['run_id'], 'request_identity': state['request_identity']},
                               ensure_ascii=False, sort_keys=True, separators=(',', ':')),
         'by': requested_by, 'now': now_shanghai()})
    return {'job_id': job_id, 'status': 'PENDING', 'result_ref': state['run_id']}


def _absent(previous):
    if not previous or previous.get('host') != socket.gethostname() or os.name != 'posix':
        return False
    pid = int(previous['pid'])
    if pid <= 0 or previous.get('pid_birth') is None:
        return False
    birth = process_identity(pid)
    if birth is not None and birth != previous['pid_birth']:
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        pass
    return False


def _reclaimable(connection, job, state):
    previous = state.get('owner') or {}
    if previous.get('scheduler_run_uid') != job['scheduler_run_uid']:
        raise ValueError('native queue and checkpoint owners differ')
    history = connection.execute(text('SELECT * FROM st_scheduled_task_history WHERE run_uid=:uid' + _locked(connection)),
                                 {'uid': job['scheduler_run_uid']}).mappings().one_or_none()
    if history is None:
        return False
    lease = read_lease(dict(history)) if history['status'] == 'running' else None
    until = lease['lease_until'] if lease else datetime.fromisoformat(state['owner_lease_until'])
    return until < now_shanghai() and _absent(previous)


def claim(connection, *, deadline=None):
    """One native worker owns the named lock for the whole bounded attempt."""
    from server.engine import strategy_native_backtest as native
    deadline = time.monotonic() + CLAIM_SECONDS if deadline is None else deadline
    connection.rollback()
    candidates=list(connection.execute(text('''SELECT /*+ MAX_EXECUTION_TIME(2000) */ job_id FROM st_job_v2 WHERE job_type=:kind
        AND (status='RUNNING' OR (status='PENDING' AND (next_run_at IS NULL OR next_run_at<=:now)))
        ORDER BY requested_at,job_id LIMIT 64'''),{'kind':JOB_TYPE,'now':now_shanghai()}).scalars())
    connection.rollback()
    for job_id in candidates:
        with connection.begin(), claim_database_deadline(connection, deadline):
            row=connection.execute(text('''SELECT * FROM st_job_v2 WHERE job_id=:id AND job_type=:kind
                AND (status='RUNNING' OR (status='PENDING' AND (next_run_at IS NULL OR next_run_at<=:now)))'''+_locked(connection)),
                {'id':job_id,'kind':JOB_TYPE,'now':now_shanghai()}).mappings().first()
            if row is None:continue
            job=dict(row)
            live=owner(connection,locking=True)
            states=native.read_runs(connection,run_id=job['result_ref'])
            if len(states)!=1:raise ValueError('native job exact experiment is unavailable')
            state=states[0]
            if state['job_id']!=job['job_id'] or state['request_identity']!=json.loads(job['request_json'])['request_identity']:
                raise ValueError('native job immutable request differs')
            if job['status']=='RUNNING' and not _reclaimable(connection,job,state):continue
            changed=connection.execute(text('''UPDATE st_job_v2 SET status='RUNNING',
                scheduler_run_uid=:owner,started_at=COALESCE(started_at,:now),next_run_at=NULL,
                finished_at=NULL,error_code=NULL,error_message=NULL WHERE job_id=:id AND status=:expected
                AND (scheduler_run_uid=:old OR (scheduler_run_uid IS NULL AND :old IS NULL))'''),
                {'owner':live['scheduler_run_uid'],'now':now_shanghai(),'id':job['job_id'],
                 'expected':job['status'],'old':job['scheduler_run_uid']})
            if changed.rowcount!=1:raise RuntimeError('native queue claim CAS lost')
            state.pop('retry_after',None)
            state.update(owner=live,status='RUNNING',step_status='RUNNING',
                executor_build=os.environ.get('PROBIGA_BUILD_COMMIT_SHA',''),attempt=int(state.get('attempt',0))+1)
            claimed=_append_locked(connection,state,live)
        return claimed,live
    return None


def _append_locked(connection, state, live, events=(), *, release=False):
    from server.engine import strategy_native_backtest as native
    from server.common.strategy_audit_heads import latest_head
    expected = int(state['checkpoint_sequence'])
    head = latest_head(connection, native.ENTITY, state['run_id'], locking=True)
    if head is None or head['action'] != f'CHECKPOINT_{expected:08d}':
        raise RuntimeError('native checkpoint sequence CAS lost')
    history = connection.execute(text('SELECT * FROM st_scheduled_task_history WHERE run_uid=:uid'),
                                 {'uid': live['scheduler_run_uid']}).mappings().one()
    lease = read_lease(dict(history))
    if lease is None:
        raise ValueError('native checkpoint lease disappeared')
    candidate = deepcopy(state)
    candidate.update(checkpoint_sequence=expected + 1, updated_at=str(now_shanghai()),
                     owner_lease_until=str(lease['lease_until']))
    if release:
        candidate.update(last_owner=live, owner=None, step_status='COMPLETE')
    native._append_state(connection, candidate, events)
    # Callers replace their state only after the surrounding transaction commits.
    return candidate


def checkpoint(connection, state, live, events=(), *, yield_job=False):
    """Fence queue, lease and exact head before publishing any partition reference."""
    connection.rollback()
    candidate = None
    with connection.begin():
        job = connection.execute(text('SELECT * FROM st_job_v2 WHERE job_id=:id' + _locked(connection)),
                                 {'id': state['job_id']}).mappings().one()
        if (job['status'] != 'RUNNING' or job['job_type'] != JOB_TYPE
                or job['scheduler_run_uid'] != live['scheduler_run_uid'] or job['result_ref'] != state['run_id']):
            raise RuntimeError('native queue publication owner CAS lost')
        if owner(connection, locking=True) != live:
            raise ValueError('native checkpoint scheduler owner differs')
        if connection.dialect.name == 'mysql':
            from server.engine.strategy_native_backtest import LOCK
            if connection.execute(text('SELECT IS_USED_LOCK(:name)=CONNECTION_ID()'), {'name': LOCK}).scalar() != 1:
                raise ValueError('native checkpoint named lock lost')
        terminal = state['status'] in ('COMPLETE', 'FAILED', 'BLOCKED')
        next_status = state['status'] if terminal else 'PENDING' if yield_job else 'RUNNING'
        if next_status == 'COMPLETE':
            next_status = 'COMPLETED'
        changed = connection.execute(text('''UPDATE st_job_v2 SET status=:status,
            scheduler_run_uid=:owner,next_run_at=:next,finished_at=:finished,
            error_code=:error,error_message=:reason WHERE job_id=:id AND status='RUNNING'
            AND scheduler_run_uid=:expected'''),
            {'status': next_status, 'owner': None if yield_job or terminal else live['scheduler_run_uid'],
             'next': (max(now_shanghai()+timedelta(seconds=1),datetime.fromisoformat(state['retry_after']))
                      if state.get('retry_after') else now_shanghai()+timedelta(seconds=1)) if yield_job and not terminal else None,
             'finished': now_shanghai() if terminal else None, 'error': 'NATIVE_STAGE_FAILED' if state['status'] == 'FAILED' else None,
             'reason': state.get('reason', '')[:500] if state['status'] in ('FAILED', 'BLOCKED') else None,
             'id': state['job_id'], 'expected': live['scheduler_run_uid']})
        if changed.rowcount != 1:
            raise RuntimeError('native queue publication CAS lost')
        candidate = _append_locked(connection, state, live, events, release=yield_job or terminal)
    state.clear()
    state.update(candidate)


class PrepareYield(BaseException):
    """A committed source partition reached this attempt's time budget."""


def _source_progress(connection, state, owner, day, stage, *, deadline, checkpoint_interval=5):
    from server.engine.strategy_native_backtest import _save
    from server.common.analysis_pool_receipt import canonical_sha256
    last_saved = time.monotonic()
    starting={key:value['completed'] for key,value in state.get('source_cursors',{}).items()}

    def progress(done, total, scope='', fresh=None):
        nonlocal last_saved
        if not 0<=done<=total:raise ValueError('PREPARE_PROGRESS_SCOPE_DIFFERS:'+stage)
        key=canonical_sha256([day,stage,scope])
        cursors=state.setdefault('source_cursors',{})
        previous=cursors.get(key,{})
        if previous and previous['total']!=total:raise ValueError('PREPARE_PROGRESS_TOTAL_DIFFERS:'+stage)
        advanced = done > previous.get('completed',0)
        if advanced:
            state['source_progress'] = {'trade_date': day, 'dataset': stage, 'scope': scope,
                'completed': done, 'total': total, 'last_progress_at': str(now_shanghai()), 'fresh_source_write': fresh}
            cursors[key]=dict(state['source_progress'])
            state['last_progress_at'] = str(now_shanghai())
        if advanced and (time.monotonic() - last_saved >= checkpoint_interval or done == total):
            _save(connection, state, owner)
            last_saved = time.monotonic()
        if time.monotonic() >= deadline and done < total:
            if done<=starting.get(key,0):raise RuntimeError('PREPARE_NO_PROGRESS:'+stage)
            raise PrepareYield('来源断点已保存：' + stage)
    return progress


def feature_input_progress(connection, state, owner, dataset, day, *, deadline):
    """Observe decoded day trees without changing the frozen feature function."""
    from server.common.analysis_pool_receipt import canonical_sha256
    pending = {id(state['bars'][target]['tree']): target for target in state['feature_windows'][day]}
    total = len(pending)
    scope = canonical_sha256({'run_id': state['run_id'], 'attempt': state['attempt'], 'day': day})
    progress = _source_progress(connection, state, owner, day, 'feature_input_decode',
                                deadline=deadline, checkpoint_interval=30)

    class ObservedInputs:
        def get_tree(self, tree):
            value = dataset.get_tree(tree)
            if id(tree) in pending:
                pending.pop(id(tree))
                try:
                    progress(total - len(pending), total, scope, False)
                except PrepareYield:
                    from server.engine.strategy_native_backtest import NativeStepDeadline
                    # RAM decoding is observation, not a resumable source partition.
                    raise NativeStepDeadline('NATIVE_FEATURE_ATTEMPT_DEADLINE') from None
            if time.monotonic() >= deadline:
                from server.engine.strategy_native_backtest import NativeStepDeadline
                raise NativeStepDeadline('NATIVE_FEATURE_ATTEMPT_DEADLINE')
            return value

    return ObservedInputs()


def source_deadline():
    return time.monotonic()+90
