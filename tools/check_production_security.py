#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Probe production-facing API protection without printing credentials."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen


@dataclass(frozen=True)
class HttpResult:
    status_code: int
    data: Any
    error: str = ""


@dataclass(frozen=True)
class CheckResult:
    name: str
    method: str
    path: str
    passed: bool
    skipped: bool
    detail: str


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _url(base_url: str, path: str) -> str:
    normalized = base_url.rstrip("/") + "/"
    return urljoin(normalized, path.lstrip("/"))


def request_json(
    base_url: str,
    method: str,
    path: str,
    *,
    admin_token: str = "",
    timeout_seconds: float = 6.0,
    follow_redirects: bool = True,
) -> HttpResult:
    headers = {
        "Accept": "application/json",
        "User-Agent": "probiga-production-security-check/1.0",
    }
    if admin_token:
        headers["X-ProBigA-Admin-Token"] = admin_token
    req = Request(_url(base_url, path), method=method.upper(), headers=headers)
    try:
        opener = urlopen if follow_redirects else build_opener(_NoRedirectHandler()).open
        with opener(req, timeout=timeout_seconds) as response:
            payload = response.read(256 * 1024)
            return HttpResult(response.status, _parse_payload(payload))
    except HTTPError as exc:
        payload = exc.read(256 * 1024)
        return HttpResult(exc.code, _parse_payload(payload), error=str(exc))
    except URLError as exc:
        return HttpResult(0, None, error=str(exc.reason))
    except TimeoutError as exc:
        return HttpResult(0, None, error=str(exc))


def _parse_payload(payload: bytes) -> Any:
    if not payload:
        return None
    text = payload.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text[:200]


def _expect_status(
    base_url: str,
    name: str,
    method: str,
    path: str,
    expected_status: int,
    *,
    admin_token: str = "",
    timeout_seconds: float = 6.0,
    require_json_ok: bool = False,
    follow_redirects: bool = True,
    retry_attempts: int = 1,
    retry_delay_seconds: float = 1.0,
) -> CheckResult:
    attempts = max(1, int(retry_attempts or 1))
    result = HttpResult(0, None, "not_run")
    passed = False
    detail = ""
    for attempt in range(1, attempts + 1):
        result = request_json(
            base_url,
            method,
            path,
            admin_token=admin_token,
            timeout_seconds=timeout_seconds,
            follow_redirects=follow_redirects,
        )
        passed = result.status_code == expected_status
        detail = f"{result.status_code}"
        if result.error and result.status_code == 0:
            detail = result.error
        if passed and require_json_ok:
            passed = isinstance(result.data, dict) and result.data.get("status") == "ok"
            status_value = result.data.get("status") if isinstance(result.data, dict) else type(result.data).__name__
            detail = f"{detail}, status={status_value}"
        if passed:
            break
        if attempt < attempts and result.status_code in {0, 502, 503, 504}:
            time.sleep(max(0.1, float(retry_delay_seconds or 1.0)))
            continue
        break
    return CheckResult(name, method.upper(), path, passed, False, detail)


def run_checks(
    base_url: str,
    admin_token: str = "",
    timeout_seconds: float = 6.0,
    retry_attempts: int = 1,
    retry_delay_seconds: float = 1.0,
) -> list[CheckResult]:
    common = {
        "timeout_seconds": timeout_seconds,
        "retry_attempts": retry_attempts,
        "retry_delay_seconds": retry_delay_seconds,
    }
    checks = [
        _expect_status(base_url, "public health", "GET", "/api/health", 200, **common),
        _expect_status(base_url, "public login page", "GET", "/login", 200, **common),
        _expect_status(base_url, "static app bundle", "GET", "/static/js/app.js", 200, **common),
        _expect_status(
            base_url,
            "security status",
            "GET",
            "/api/health/security",
            200,
            require_json_ok=True,
            **common,
        ),
        _expect_status(
            base_url,
            "root redirects to login",
            "GET",
            "/",
            303,
            follow_redirects=False,
            **common,
        ),
        _expect_status(
            base_url,
            "legacy static html redirects to login",
            "GET",
            "/static/monitor.html",
            303,
            follow_redirects=False,
            **common,
        ),
        _expect_status(
            base_url,
            "admin read blocked without token",
            "GET",
            "/api/scheduler/tasks",
            401,
            **common,
        ),
        _expect_status(
            base_url,
            "api mutation blocked without token",
            "POST",
            "/api/portfolio/refresh-prices",
            401,
            **common,
        ),
    ]
    if admin_token:
        checks.append(
            _expect_status(
                base_url,
                "admin read allowed with token",
                "GET",
                "/api/scheduler/tasks",
                200,
                admin_token=admin_token,
                **common,
            )
        )
    else:
        checks.append(
            CheckResult(
                "admin read allowed with token",
                "GET",
                "/api/scheduler/tasks",
                True,
                True,
                "set PROBIGA_ADMIN_TOKEN or pass --admin-token to verify",
            )
        )
    return checks


def print_results(results: list[CheckResult]) -> None:
    for item in results:
        label = "SKIP" if item.skipped else "PASS" if item.passed else "FAIL"
        print(f"{label} {item.name}: {item.method} {item.path} -> {item.detail}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify ProBigA production API security controls.")
    parser.add_argument("--base-url", default=os.environ.get("PROBIGA_BASE_URL", "http://127.0.0.1"))
    parser.add_argument("--admin-token", default=os.environ.get("PROBIGA_ADMIN_TOKEN", ""))
    parser.add_argument("--timeout-seconds", type=float, default=6.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-delay-seconds", type=float, default=1.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    results = run_checks(
        args.base_url,
        admin_token=str(args.admin_token or "").strip(),
        timeout_seconds=max(1.0, float(args.timeout_seconds or 6.0)),
        retry_attempts=max(1, int(args.retries or 1)),
        retry_delay_seconds=max(0.1, float(args.retry_delay_seconds or 1.0)),
    )
    print_results(results)
    return 0 if all(item.passed for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
