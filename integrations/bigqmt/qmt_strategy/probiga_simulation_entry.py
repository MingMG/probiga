# coding: utf-8
"""QMT embedded-Python-3.6 entry. No account, trading, or order API is used.

The release installer binds all constants to exact merged production bytes.
Every list entry invokes the one shared Windows formula worker; filtering is
only for this entry's console output, not a competing calculation pipeline.
"""
import hashlib
import json
import os
import subprocess
import time

STRATEGY_KEY = "__PROBIGA_STRATEGY_KEY__"
STRATEGY_NAME = "__PROBIGA_STRATEGY_NAME__"
RELEASE_PATH = "__PROBIGA_RELEASE_PATH__"
EXPECTED_BUILD = "__PROBIGA_EXPECTED_BUILD__"
_process = None
_output = None
_reported = False
_started = 0


def _sha(path):
    with open(path, "rb") as handle:
        return hashlib.sha256(handle.read()).hexdigest()


def init(ContextInfo):
    global _process, _output, _started, _reported
    _reported = False
    print("[ProBigA] " + STRATEGY_NAME + " | simulation only; no real orders")
    try:
        with open(RELEASE_PATH, "r", encoding="utf-8") as handle:
            release = json.load(handle)
        if (release.get("schema") != "probiga.qmt-simulation-entries.v1"
                or release.get("build_sha") != EXPECTED_BUILD
                or release.get("simulation_only") is not True):
            raise RuntimeError("SIMULATION_RELEASE_IDENTITY_DIFFERS")
        root = release["production_root"]
        runner = os.path.join(root, "tools", "run_qmt_strategy_daily.py")
        python = os.path.join(root, "runtime", "qmt-py313", "Scripts", "python.exe")
        if _sha(runner) != release["runner_sha256"] or not os.path.isfile(python):
            raise RuntimeError("SIMULATION_WORKER_BYTES_DIFFER")
        state_root = release["state_root"]
        os.makedirs(state_root, exist_ok=True)
        output_path = os.path.join(state_root, "entry_" + STRATEGY_KEY + ".log")
        _output = open(output_path, "w+", encoding="utf-8")
        environment = os.environ.copy()
        environment["PROBIGA_DEPLOYMENT_MODE"] = "production"
        environment["PROBIGA_SCHEDULER_EXECUTOR_ROLE"] = "qmt_windows_edge"
        environment["PROBIGA_BUILD_COMMIT_SHA"] = EXPECTED_BUILD
        environment["PYTHONIOENCODING"] = "utf-8"
        _process = subprocess.Popen(
            [python, "-P", runner, "--expected-build-sha", EXPECTED_BUILD,
             "--strategy-key", STRATEGY_KEY, "--origin", "QMT_ENTRY", "--json"],
            cwd=root, env=environment, stdout=_output, stderr=_output,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        _started = time.time()
        ContextInfo.run_time("probiga_sim_poll", "5nSecond", "2020-01-01 00:00:00")
        print("[ProBigA] shared fact snapshot / original formula worker started")
    except Exception as exc:
        print("[ProBigA] EXECUTION_FAILED " + type(exc).__name__)


def probiga_sim_poll(ContextInfo):
    global _reported, _output
    if _process is None or _reported:
        return
    if _process.poll() is None:
        return
    _reported = True
    _output.flush()
    _output.seek(0)
    lines = _output.read().splitlines()
    _output.close()
    _output = None
    try:
        receipt = json.loads(lines[-1])
        print("[ProBigA] " + json.dumps(receipt, ensure_ascii=False))
    except (ValueError, IndexError):
        print("[ProBigA] WORKER_FAILED: see entry log; no result was invented")


def handlebar(ContextInfo):
    probiga_sim_poll(ContextInfo)


def stop(ContextInfo):
    # A stop of the display entry is not cancellation of an already captured
    # research fact/upload. The worker finishes and preserves its audit receipt.
    print("[ProBigA] display stopped; durable research worker owns its completion")
