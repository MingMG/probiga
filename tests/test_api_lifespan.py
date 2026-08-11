# -*- coding: utf-8 -*-
import asyncio
from unittest.mock import patch

from fastapi import FastAPI

from server.api import main


def test_lifespan_stops_runtime_and_disposes_engines():
    events: list[str] = []

    async def _run_lifespan():
        with patch("server.api.main.start_embedded_scheduler", side_effect=lambda: events.append("scheduler")), \
             patch("server.api.main.stop_embedded_scheduler", side_effect=lambda: events.append("scheduler_stop")), \
             patch("server.api.main.get_api_lifespan_config", return_value={"qmt_live_runtime_enabled": True}), \
             patch("server.api.main.start_qmt_live_runtime", side_effect=lambda: events.append("qmt_start")), \
             patch("server.api.main.stop_qmt_live_runtime", side_effect=lambda: events.append("qmt_stop")), \
             patch("server.api.main.dispose_shared_engines", side_effect=lambda: events.append("dispose")):
            async with main.lifespan(FastAPI()):
                events.append("inside")

    asyncio.run(_run_lifespan())

    assert events == ["scheduler", "qmt_start", "inside", "scheduler_stop", "qmt_stop", "dispose"]


def test_lifespan_leaves_qmt_live_to_standalone_by_default():
    events: list[str] = []

    async def _run_lifespan():
        with patch("server.api.main.start_embedded_scheduler", side_effect=lambda: events.append("scheduler")), \
             patch("server.api.main.stop_embedded_scheduler", side_effect=lambda: events.append("scheduler_stop")), \
             patch("server.api.main.get_api_lifespan_config", return_value={"qmt_live_runtime_enabled": False}), \
             patch("server.api.main.start_qmt_live_runtime", side_effect=lambda: events.append("qmt_start")), \
             patch("server.api.main.stop_qmt_live_runtime", side_effect=lambda: events.append("qmt_stop")), \
             patch("server.api.main.dispose_shared_engines", side_effect=lambda: events.append("dispose")):
            async with main.lifespan(FastAPI()):
                events.append("inside")

    asyncio.run(_run_lifespan())

    assert events == ["scheduler", "inside", "scheduler_stop", "dispose"]


def test_index_html_is_not_browser_cached():
    response = main._index_html()

    assert response.headers["cache-control"] == "no-store"

