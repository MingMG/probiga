# -*- coding: utf-8 -*-
import asyncio
from unittest.mock import patch

from fastapi import FastAPI
import pytest

from server.api import main


def test_lifespan_stops_runtime_and_disposes_engines():
    events: list[str] = []

    async def _run_lifespan():
        with patch("server.api.main.bootstrap_strategy_execution_adapter_registry", side_effect=lambda: events.append("adapter_bootstrap")), \
             patch("server.api.main.start_embedded_scheduler", side_effect=lambda: events.append("scheduler")), \
             patch("server.api.main.stop_embedded_scheduler", side_effect=lambda: events.append("scheduler_stop")), \
             patch("server.api.main._desktop_runtimes_allowed", return_value=True), \
             patch("server.api.main.get_api_lifespan_config", return_value={"qmt_live_runtime_enabled": True}), \
             patch("server.api.main.start_qmt_live_runtime", side_effect=lambda: events.append("qmt_start")), \
             patch("server.api.main.stop_qmt_live_runtime", side_effect=lambda: events.append("qmt_stop")), \
             patch("server.api.main.start_market_radar_runtime", return_value=None), \
             patch("server.api.main.dispose_shared_engines", side_effect=lambda: events.append("dispose")):
            async with main.lifespan(FastAPI()):
                events.append("inside")

    asyncio.run(_run_lifespan())

    assert events == ["adapter_bootstrap", "scheduler", "qmt_start", "inside", "scheduler_stop", "qmt_stop", "dispose"]


def test_lifespan_skips_desktop_runtimes_on_linux_host():
    async def _run_lifespan():
        with patch("server.api.main.bootstrap_strategy_execution_adapter_registry"), \
             patch("server.api.main.start_embedded_scheduler"), \
             patch("server.api.main.stop_embedded_scheduler"), \
             patch("server.api.main._desktop_runtimes_allowed", return_value=False), \
             patch("server.api.main.get_api_lifespan_config", return_value={"qmt_live_runtime_enabled": True}), \
             patch("server.api.main.start_qmt_live_runtime") as qmt_start, \
             patch("server.api.main.start_market_radar_runtime") as radar_start, \
             patch("server.api.main.dispose_shared_engines"):
            async with main.lifespan(FastAPI()):
                pass
        qmt_start.assert_not_called()
        radar_start.assert_not_called()

    asyncio.run(_run_lifespan())


def test_lifespan_leaves_qmt_live_to_standalone_by_default():
    events: list[str] = []

    async def _run_lifespan():
        with patch("server.api.main.bootstrap_strategy_execution_adapter_registry", side_effect=lambda: events.append("adapter_bootstrap")), \
             patch("server.api.main.start_embedded_scheduler", side_effect=lambda: events.append("scheduler")), \
             patch("server.api.main.stop_embedded_scheduler", side_effect=lambda: events.append("scheduler_stop")), \
             patch("server.api.main.get_api_lifespan_config", return_value={"qmt_live_runtime_enabled": False}), \
             patch("server.api.main.start_qmt_live_runtime", side_effect=lambda: events.append("qmt_start")), \
             patch("server.api.main.stop_qmt_live_runtime", side_effect=lambda: events.append("qmt_stop")), \
             patch("server.api.main.dispose_shared_engines", side_effect=lambda: events.append("dispose")):
            async with main.lifespan(FastAPI()):
                events.append("inside")

    asyncio.run(_run_lifespan())

    assert events == ["adapter_bootstrap", "scheduler", "inside", "scheduler_stop", "dispose"]


def test_adapter_bootstrap_failure_starts_no_background_runtime():
    async def _run_lifespan():
        with patch(
            "server.api.main.bootstrap_strategy_execution_adapter_registry",
            side_effect=RuntimeError("seal mismatch"),
        ), patch("server.api.main.start_embedded_scheduler") as scheduler:
            with pytest.raises(RuntimeError, match="seal mismatch"):
                async with main.lifespan(FastAPI()):
                    pass
            scheduler.assert_not_called()

    asyncio.run(_run_lifespan())


def test_index_html_is_not_browser_cached():
    response = main._index_html()

    assert response.headers["cache-control"] == "no-store"
