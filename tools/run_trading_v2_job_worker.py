#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import faulthandler
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.job_worker import run_one_job
from tools.env_config import load_project_env


def run_with_diagnostics(engine):
    from tools import run_ai_recommendation_premarket as diagnostics
    uid=os.environ.get('PROBIGA_SCHEDULER_HISTORY_RUN_UID','')
    path=None
    if re.fullmatch(r'[0-9a-f]{32}',uid):
        if not os.environ.get('PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT','').strip():
            os.environ['PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT']=str(
                Path(os.environ.get('PROBIGA_JOB_LOG_ROOT') or '/var/lib/probiga/jobs')/'analysis-execution-diagnostics')
        diagnostics._start_analysis_stack_diagnostics(uid)
        path=diagnostics._analysis_diagnostics_root()/f'{uid}.stack.log'
    try:
        return run_one_job(engine)
    finally:
        if path is not None:
            faulthandler.cancel_dump_traceback_later()
            if diagnostics._DIAGNOSTIC_STREAM is not None:
                diagnostics._DIAGNOSTIC_STREAM.close();diagnostics._DIAGNOSTIC_STREAM=None
            if path.exists() and path.stat().st_size==0:path.unlink()


def main() -> int:
    load_project_env()
    engine = create_batch_engine()
    try:
        result = run_with_diagnostics(engine)
        if result["status"] == "idle" or result.get('job_type')=='NATIVE_REPLAY':
            from server.engine.strategy_execution_adapters import bootstrap_strategy_execution_adapter_registry
            from server.engine.autonomous_strategy_research import research_tick
            bootstrap_strategy_execution_adapter_registry(registry_rows=())
            result["autonomous_research"] = research_tick(engine)
            from server.engine.strategy_native_backtest import submit_native_experiments
            if result['status']=='idle':result["native_submissions"] = submit_native_experiments(engine)
    finally:
        engine.dispose()
    print(json.dumps(result, ensure_ascii=False, default=str))
    healthy=result["status"] in {"idle", "COMPLETED", "COMPLETE", "RUNNING"}
    subfailure=any(isinstance(value,dict) and value.get('status') in ('FAILED','BLOCKED') for value in result.values())
    return 0 if healthy and not subfailure else 2


if __name__ == "__main__":
    raise SystemExit(main())
