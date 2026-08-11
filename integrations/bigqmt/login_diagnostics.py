# -*- coding: utf-8 -*-
"""Sanitized diagnostics for the standard Guojin QMT login boundary."""
from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from integrations.bigqmt.spool import bridge_paths, resolve_big_qmt_home


_TIMESTAMP_RE = re.compile(r"^(?P<value>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def _event_time(line: str) -> datetime | None:
    match = _TIMESTAMP_RE.search(str(line or ""))
    if not match:
        return None
    try:
        return datetime.strptime(match.group("value"), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def classify_login_log(
    lines: Iterable[str],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Classify recent login state without returning account, IP or raw log."""
    now = now or datetime.now()
    events = (
        (
            "AUTH_SERVER_EOF",
            lambda line: "End of file" in line
            and (
                "login status: 21" in line
                or "onDefaultServerParam" in line
                or "onSecurityCodeEnabled" in line
            ),
            "国金认证服务在连接建立后主动断开",
        ),
        (
            "BROKER_CHANNEL_DISCONNECTED",
            lambda line: "与Broker之间的连接断开" in line,
            "交易柜台通道已断开",
        ),
        (
            "LOGIN_SUCCEEDED",
            lambda line: "登录成功" in line
            or "login status: 8" in line.lower(),
            "QMT客户端登录成功",
        ),
        (
            "LOGIN_IN_PROGRESS",
            lambda line: "login status: 6" in line or "登录中" in line,
            "QMT客户端正在登录",
        ),
    )
    latest: tuple[datetime, str, str] | None = None
    for line in lines:
        event_at = _event_time(line)
        if event_at is None:
            continue
        for reason_code, predicate, message in events:
            if predicate(line):
                if latest is None or event_at >= latest[0]:
                    latest = (event_at, reason_code, message)
                break
    if latest is None:
        return {
            "status": "unknown",
            "reason_code": "NO_LOGIN_EVENT",
            "message": "未找到可识别的QMT登录事件",
            "observed_at": "",
            "age_seconds": None,
        }
    event_at, reason_code, message = latest
    age_seconds = max(0, int((now - event_at).total_seconds()))
    if reason_code == "LOGIN_SUCCEEDED":
        status = "logged_in"
    elif reason_code == "LOGIN_IN_PROGRESS":
        if age_seconds > 120:
            status = "login_failed"
            reason_code = "LOGIN_STALLED"
            message = "QMT客户端登录超过120秒仍未完成"
        else:
            status = "starting"
    else:
        status = "login_failed"
    return {
        "status": status,
        "reason_code": reason_code,
        "message": message,
        "observed_at": event_at.isoformat(sep=" ", timespec="seconds"),
        "age_seconds": age_seconds,
    }


def _tail_lines(path: Path, max_bytes: int = 2 * 1024 * 1024) -> list[str]:
    size = path.stat().st_size
    with path.open("rb") as handle:
        if size > max_bytes:
            handle.seek(-max_bytes, 2)
        raw = handle.read()
    return raw.decode("utf-8", errors="replace").splitlines()


def diagnose_bigqmt_login(
    *,
    qmt_home: Path | str | None = None,
    heartbeat_max_age_seconds: int = 60,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now()
    home = Path(qmt_home) if qmt_home else resolve_big_qmt_home(required=False)
    if home is None:
        return {
            "status": "not_installed",
            "reason_code": "QMT_HOME_NOT_FOUND",
            "message": "未找到标准QMT安装目录",
            "bridge_status": "missing",
        }
    heartbeat = bridge_paths(home)["heartbeat"]
    heartbeat_age: int | None = None
    if heartbeat.is_file():
        heartbeat_age = max(0, int(time.time() - heartbeat.stat().st_mtime))
        if heartbeat_age <= max(1, int(heartbeat_max_age_seconds)):
            return {
                "status": "ready",
                "reason_code": "BIGQMT_HEARTBEAT_FRESH",
                "message": "BigQMT内置策略心跳正常",
                "bridge_status": "fresh",
                "heartbeat_age_seconds": heartbeat_age,
                "observed_at": datetime.fromtimestamp(
                    heartbeat.stat().st_mtime
                ).isoformat(sep=" ", timespec="seconds"),
            }
    log_dir = home / "userdata" / "log"
    logs = (
        sorted(
            log_dir.glob("XtClient_*.log"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if log_dir.is_dir()
        else []
    )
    if logs:
        result = classify_login_log(_tail_lines(logs[0]), now=now)
    else:
        result = {
            "status": "unknown",
            "reason_code": "LOGIN_LOG_NOT_FOUND",
            "message": "未找到QMT客户端登录日志",
            "observed_at": "",
            "age_seconds": None,
        }
    return {
        **result,
        "bridge_status": "stale" if heartbeat_age is not None else "missing",
        "heartbeat_age_seconds": heartbeat_age,
        "source_log": logs[0].name if logs else "",
    }
