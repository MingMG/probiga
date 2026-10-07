"""Authenticated read views of independently verified QMT simulations."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from server.api.qmt_strategy_results import (
    QmtStrategyResultError, read_strategy_performance, read_strategy_results,
    read_strategy_run,
)


router = APIRouter(prefix="/strategy-center/qmt-results", tags=["qmt-strategy-results"])


def _read(operation, *args, **kwargs):
    try:
        return operation(*args, **kwargs)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="QMT simulation run not found") from exc
    except QmtStrategyResultError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("")
def results(trade_date: str = Query(default=""), limit: int = Query(default=30, ge=1, le=100)):
    return _read(read_strategy_results, trade_date, limit)


@router.get("/dates")
def dates():
    value = _read(read_strategy_results, "", 1)
    return {key: value[key] for key in ("status", "dates", "schedule", "catalog", "simulation_only", "real_order_allowed")}


@router.get("/runs/{run_uid}")
def run(run_uid: str):
    return _read(read_strategy_run, run_uid)


@router.get("/performance")
def performance(run_uid: str = Query(default=""), trade_date: str = Query(default="")):
    return _read(read_strategy_performance, run_uid, trade_date)
