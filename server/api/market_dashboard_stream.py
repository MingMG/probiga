"""WebSocket delivery of published observations; never calls a collector."""
from __future__ import annotations

import asyncio
import sqlite3
from datetime import date
from urllib.parse import urlsplit

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect

from server.api.admin_auth import validate_admin_request
from server.common import market_dashboard_store as store
from server.common.qmt_attestation_contract import canonical_digest

router = APIRouter()


def bundle(day):
    now = store.clock()
    data = {"snapshot": store.board(day, now=now), "heatmap": store.heatmap(day, now=now),
            "liquidity": store.component(day, "liquidity", now=now),
            "events": store.component(day, "events", now=now)}
    runtime = {key: value for key, value in store.runtime().items() if key != "token"}
    if runtime.get("lease_until") and store.stamp(runtime["lease_until"]) < now:
        runtime["state"] = "FAILED"
        runtime["error"] = "producer_lease_expired"
    return data, runtime


class SnapshotHub:
    def __init__(self, *, interval=1, heartbeat=10):
        self.interval, self.heartbeat = interval, heartbeat
        self.clients, self.latest, self.versions = {}, {}, {}
        self.task = None

    @staticmethod
    def offer(queue, message):
        if queue.full():
            previous = queue.get_nowait()
            message = {**message, "components": {**previous.get("components", {}), **message.get("components", {})}}
        queue.put_nowait(message)

    def subscribe(self, day):
        queue = asyncio.Queue(maxsize=1)
        self.clients.setdefault(day, set()).add(queue)
        if day in self.latest:
            self.offer(queue, {"type": "update", "trade_date": day, "server_time": store.iso(store.clock()),
                               "components": self.latest[day], "initial": True})
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.poll())
        return queue

    async def unsubscribe(self, day, queue):
        clients = self.clients.get(day)
        if clients:
            clients.discard(queue)
            if not clients:
                self.clients.pop(day, None)
                self.latest.pop(day, None)
                self.versions.pop(day, None)
        if not self.clients and self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None

    async def poll(self):
        last_heartbeat = 0
        while self.clients:
            for day in list(self.clients):
                try:
                    data, runtime = await asyncio.wait_for(asyncio.to_thread(bundle, day), timeout=5)
                    before = self.versions.get(day, {})
                    versions = {key: canonical_digest({"version": value.get("version"),
                        "errors": value.get("source_errors"),
                        "observation": {field: value.get("observation", {}).get(field)
                            for field in ("source_at", "received_at", "state", "last_error")}}) if value is not None else None
                        for key, value in data.items()}
                    changed = {key: value for key, value in data.items() if key not in before or versions[key] != before[key]}
                    initial = day not in self.latest
                    self.latest[day], self.versions[day] = data, versions
                    heartbeat = asyncio.get_running_loop().time()-last_heartbeat >= self.heartbeat
                    if changed or heartbeat:
                        message = {"type": "update" if changed else "heartbeat", "trade_date": day,
                                   "server_time": store.iso(store.clock()), "components": changed,
                                   "producer": runtime, "initial": initial}
                        for queue in list(self.clients.get(day, ())):
                            self.offer(queue, message)
                except (OSError, sqlite3.Error, RuntimeError, ValueError, TimeoutError) as exc:
                    for queue in list(self.clients.get(day, ())):
                        self.offer(queue, {"type": "error", "trade_date": day,
                            "server_time": store.iso(store.clock()), "components": {}, "error": type(exc).__name__})
            if asyncio.get_running_loop().time()-last_heartbeat >= self.heartbeat:
                last_heartbeat = asyncio.get_running_loop().time()
            await asyncio.sleep(self.interval)

    async def stop(self):
        for day, clients in self.clients.items():
            for queue in clients:
                self.offer(queue, {"type": "closing", "trade_date": day, "components": {}})
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None


hub = SnapshotHub()


@router.websocket("/monitor/stream")
async def market_stream(websocket: WebSocket):
    # Preserve the HTTP operator boundary; browsers carry the same handshake headers.
    scope = {**websocket.scope, "type": "http", "method": "GET"}
    blocked = await asyncio.to_thread(validate_admin_request, Request(scope))
    origin = websocket.headers.get("origin")
    if blocked is not None or (origin and urlsplit(origin).netloc != websocket.headers.get("host")):
        await websocket.close(code=1008)
        return
    day = websocket.query_params.get("date", "")
    try:
        if date.fromisoformat(day).isoformat() != day:
            raise ValueError("noncanonical session")
    except ValueError:
        await websocket.close(code=1008)
        return
    await websocket.accept()
    queue = hub.subscribe(day)
    try:
        while True:
            message = await asyncio.wait_for(queue.get(), timeout=20)
            await asyncio.wait_for(websocket.send_json(message), timeout=5)
            if message["type"] == "closing":
                await websocket.close(code=1012)
                break
    except (WebSocketDisconnect, OSError, RuntimeError, TimeoutError):
        pass
    finally:
        await hub.unsubscribe(day, queue)
