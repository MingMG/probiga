# -*- coding: utf-8 -*-
from fastapi import APIRouter

from server.api.scheduler_runtime import scheduler_runtime_info
from server.api.routers._engine import get_engine
from server.common.config import get_minute_mysql_pool_config

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/health/runtime")
def health_runtime():
    return {
        "status": "ok",
        **scheduler_runtime_info(),
        "minute_mysql_pool": get_minute_mysql_pool_config(),
    }


@router.get("/health/intraday-readiness")
def health_intraday_readiness():
    from tools.data_quality_check import intraday_readiness

    return intraday_readiness(get_engine())
