import faulthandler
import json
import pytest
from sqlalchemy import create_engine, event, text

from server.engine import strategy_native_backtest as native
from tools.run_trading_v2_job_worker import run_with_diagnostics
from tools import run_trading_v2_job_worker as worker


def test_slow_failure_diagnostics_keep_the_business_result_and_stack(tmp_path,monkeypatch):
    from tools import run_ai_recommendation_premarket as diagnostics
    monkeypatch.setenv('PROBIGA_SCHEDULER_HISTORY_RUN_UID','1'*32)
    monkeypatch.setenv('PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT',str(tmp_path))
    def failed(_engine):
        faulthandler.dump_traceback(file=diagnostics._DIAGNOSTIC_STREAM)
        return {'status':'FAILED','run_id':'existing-run','reason':'source deadline'}
    monkeypatch.setattr(worker,'run_one_job',failed)
    assert run_with_diagnostics(None)['run_id']=='existing-run'
    assert 'failed' in (tmp_path/(('1'*32)+'.stack.log')).read_text(encoding='utf-8')
    assert diagnostics._DIAGNOSTIC_STREAM is None


def test_fast_native_reads_do_not_accumulate_empty_diagnostic_files(tmp_path,monkeypatch):
    monkeypatch.setenv('PROBIGA_SCHEDULER_HISTORY_RUN_UID','2'*32)
    monkeypatch.setenv('PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT',str(tmp_path))
    monkeypatch.setattr(worker,'run_one_job',lambda _engine:{'status':'COMPLETE'})
    assert run_with_diagnostics(None)['status']=='COMPLETE'
    assert not list(tmp_path.glob('*.stack.log'))


def test_unconfigured_diagnostics_use_the_existing_job_log_location(tmp_path,monkeypatch):
    monkeypatch.setenv('PROBIGA_SCHEDULER_HISTORY_RUN_UID','3'*32)
    monkeypatch.setenv('PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT','')
    monkeypatch.setenv('PROBIGA_JOB_LOG_ROOT',str(tmp_path))
    monkeypatch.setattr(worker,'run_one_job',lambda _engine:{'status':'COMPLETE'})
    assert run_with_diagnostics(None)['status']=='COMPLETE'
    assert (tmp_path/'analysis-execution-diagnostics').is_dir()


@pytest.mark.parametrize('business_status,expected',[('FAILED',2),('BLOCKED',2),('RUNNING',0),('COMPLETE',0)])
def test_outer_worker_returns_the_business_terminal_status(monkeypatch,capsys,business_status,expected):
    from types import SimpleNamespace
    monkeypatch.setattr(worker,'load_project_env',lambda:None)
    monkeypatch.setattr(worker,'create_batch_engine',lambda **_kwargs:SimpleNamespace(dispose=lambda:None))
    monkeypatch.setattr(worker,'run_with_diagnostics',lambda _engine:{'status':business_status,'run_id':'fixed-experiment'})
    assert worker.main()==expected
    assert json.loads(capsys.readouterr().out)['status']==business_status


def test_idle_planner_enqueues_without_direct_native_execution(monkeypatch,capsys):
    from types import SimpleNamespace
    from server.engine import autonomous_strategy_research as research
    from server.engine import strategy_execution_adapters as adapters
    monkeypatch.setattr(worker,'load_project_env',lambda:None)
    monkeypatch.setattr(worker,'create_batch_engine',lambda **_kwargs:SimpleNamespace(dispose=lambda:None))
    monkeypatch.setattr(worker,'run_with_diagnostics',lambda _engine:{'status':'idle'})
    monkeypatch.setattr(adapters,'bootstrap_strategy_execution_adapter_registry',lambda **_kwargs:None)
    monkeypatch.setattr(research,'research_tick',lambda _engine:{'status':'RUNNING'})
    monkeypatch.setattr(native,'submit_native_experiments',lambda _engine:{'status':'COMPLETE','jobs':[{'job_id':'queued'}],'business_started':False})
    monkeypatch.setattr(native,'native_tick',lambda _engine:pytest.fail('planner directly executed an unclaimed native replay'))
    assert worker.main()==0
    assert json.loads(capsys.readouterr().out)['native_submissions']['business_started'] is False


def test_worker_connection_ends_when_mysql_accepts_without_sending_a_greeting():
    import socket
    import subprocess
    import sys
    import threading

    accepted, release = threading.Event(), threading.Event()
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(1)
    listener.settimeout(15)

    def blackhole():
        with listener.accept()[0]:
            accepted.set()
            release.wait(20)

    thread = threading.Thread(target=blackhole, daemon=True)
    thread.start()
    script = f'''
from sqlalchemy import event
from sqlalchemy.exc import OperationalError
from server.common import engine_factory
from tools import run_trading_v2_job_worker as worker
engine_factory._get_runtime_tls_config = lambda: {{'required': False, 'ssl_ca': None}}
original = worker.create_batch_engine
def create(**kwargs):
    engine = original('mysql+pymysql://probe:probe@127.0.0.1:{listener.getsockname()[1]}/probe', **kwargs)
    @event.listens_for(engine, 'do_connect')
    def shorten_existing_timeout(dialect, record, args, params):
        if params.get('read_timeout') is not None:
            params['read_timeout'] = min(params['read_timeout'], .1)
    return engine
def connect(engine):
    with engine.connect():
        raise AssertionError('A server without a greeting cannot authenticate')
worker.create_batch_engine = create
worker.load_project_env = lambda: None
worker.run_with_diagnostics = connect
try:
    worker.main()
except OperationalError as error:
    assert error.orig.args[0] == 2013
    print('BOUNDED_MYSQL_GREETING_FAILURE')
else:
    raise AssertionError('The greeting failure was hidden')
'''
    process = subprocess.Popen([sys.executable, '-c', script], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True,
                               creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    try:
        assert accepted.wait(15), 'Worker did not reach the real MySQL greeting boundary'
        try:
            stdout, stderr = process.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            pytest.fail('Worker remained blocked after TCP acceptance without a MySQL greeting')
        assert process.returncode == 0, stderr[-1500:]
        assert 'BOUNDED_MYSQL_GREETING_FAILURE' in stdout
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
        release.set()
        listener.close()
        thread.join(timeout=2)
