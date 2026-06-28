# -*- coding: utf-8 -*-
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from server.api.qmt_live_runtime import start_qmt_live_runtime, stop_qmt_live_runtime
from server.api.routers import commentary, datasource, health, hot_data, jq_minute, notify, scheduler, sim_trade
from server.api.scheduler_runtime import start_embedded_scheduler


@asynccontextmanager
async def lifespan(_: FastAPI):
    start_embedded_scheduler()
    start_qmt_live_runtime()
    try:
        yield
    finally:
        stop_qmt_live_runtime()


app = FastAPI(
    title="ProBigA",
    description="数据分析平台 API",
    lifespan=lifespan,
)

app.include_router(health.router, prefix="/api")
app.include_router(notify.router, prefix="/api")
app.include_router(hot_data.router, prefix="/api")
app.include_router(jq_minute.router, prefix="/api")
app.include_router(scheduler.router, prefix="/api")
app.include_router(sim_trade.router, prefix="/api")
app.include_router(datasource.router, prefix="/api")
app.include_router(commentary.router, prefix="/api")

static_dir = Path(__file__).resolve().parent.parent / "static"


@app.get("/", response_class=HTMLResponse)
def index():
    index_path = static_dir / "index.html"
    if index_path.is_file():
        return HTMLResponse(content=index_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>ProBigA</h1>")


if static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(static_dir), html=False), name="static")
