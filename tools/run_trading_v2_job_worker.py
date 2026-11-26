#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import faulthandler
import logging
from logging.handlers import QueueHandler
import os
from queue import Empty, Full, Queue
import re
import sys
from contextlib import contextmanager
from pathlib import Path
from threading import Thread

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.job_worker import run_one_job
from tools.env_config import load_project_env


@contextmanager
def worker_timing_logs():
    records = Queue(maxsize=2048)
    stream = logging.StreamHandler()
    stream.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logger = logging.getLogger("server.native_replay.timing")

    class TimingQueue(QueueHandler):
        dropped = 0

        def enqueue(self, record):
            try:
                self.queue.put_nowait(record)
            except Full:
                self.dropped += 1

    handler = TimingQueue(records)
    previous_level, previous_propagate = logger.level, logger.propagate
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)

    def drain():
        while (record := records.get()) is not None:
            stream.handle(record)

    thread = Thread(target=drain, name="native-replay-timing", daemon=True)
    thread.start()
    try:
        yield
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        logger.propagate = previous_propagate
        try:
            records.put_nowait(None)
        except Full:
            try:
                records.get_nowait()
                handler.dropped += 1
            except Empty:
                pass
            records.put_nowait(None)
        thread.join(timeout=1)
        if handler.dropped or thread.is_alive():
            logging.getLogger(__name__).warning(
                "Native worker timing log drain incomplete: dropped=%d pending=%d",
                handler.dropped, records.qsize())


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
    logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
    engine = create_batch_engine(pool_timeout=5,
        connect_args={'connect_timeout': 8, 'read_timeout': 15, 'write_timeout': 10})
    try:
        with worker_timing_logs():
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
