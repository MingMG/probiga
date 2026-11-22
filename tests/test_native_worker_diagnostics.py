import faulthandler

from server.engine import strategy_native_backtest as native
from tools.run_trading_v2_job_worker import run_native_backtest


def test_slow_failure_diagnostics_keep_the_business_result_and_stack(tmp_path,monkeypatch):
    from tools import run_ai_recommendation_premarket as diagnostics
    monkeypatch.setenv('PROBIGA_SCHEDULER_HISTORY_RUN_UID','1'*32)
    monkeypatch.setenv('PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT',str(tmp_path))
    def failed(_engine):
        faulthandler.dump_traceback(file=diagnostics._DIAGNOSTIC_STREAM)
        return {'status':'FAILED','run_id':'existing-run','reason':'source deadline'}
    monkeypatch.setattr(native,'native_tick',failed)
    assert run_native_backtest(None)['run_id']=='existing-run'
    assert 'failed' in (tmp_path/(('1'*32)+'.stack.log')).read_text(encoding='utf-8')
    assert diagnostics._DIAGNOSTIC_STREAM is None


def test_fast_native_reads_do_not_accumulate_empty_diagnostic_files(tmp_path,monkeypatch):
    monkeypatch.setenv('PROBIGA_SCHEDULER_HISTORY_RUN_UID','2'*32)
    monkeypatch.setenv('PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT',str(tmp_path))
    monkeypatch.setattr(native,'native_tick',lambda _engine:{'status':'COMPLETE'})
    assert run_native_backtest(None)['status']=='COMPLETE'
    assert not list(tmp_path.glob('*.stack.log'))


def test_unconfigured_diagnostics_use_the_existing_job_log_location(tmp_path,monkeypatch):
    monkeypatch.setenv('PROBIGA_SCHEDULER_HISTORY_RUN_UID','3'*32)
    monkeypatch.setenv('PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT','')
    monkeypatch.setenv('PROBIGA_JOB_LOG_ROOT',str(tmp_path))
    monkeypatch.setattr(native,'native_tick',lambda _engine:{'status':'COMPLETE'})
    assert run_native_backtest(None)['status']=='COMPLETE'
    assert (tmp_path/'analysis-execution-diagnostics').is_dir()
