# -*- coding: utf-8 -*-
"""Poll ProBigA production questions and answer through Codex, then DeepSeek web.

This worker must run on the Windows machine that owns the two Codex desktop
tasks. It deliberately does not call an OpenAI or DeepSeek model API.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import ipaddress
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.remote_support import remote_host

DEFAULT_STOCK_THREAD_ID = "019fbe02-0390-7663-a7ba-bd150e063fe7"
DEFAULT_GENERAL_THREAD_ID = "019fbe02-0a70-7c62-9bf2-9ab439bea770"
SOURCE_CODEX = "codex_gpt"
SOURCE_DEEPSEEK = "deepseek_web"

logger = logging.getLogger("probiga.ai_bridge")
WORKER_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


class ProviderUnavailable(RuntimeError):
    """A provider could not return a usable answer."""


class ResearchTerminationUnconfirmed(ProviderUnavailable):
    """Do not accept another model job until the prior child is proven absent."""


class WorkerLock:
    """Keep scheduled/manual starts from polling the same queue twice."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle: Any = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+", encoding="utf-8")
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0, os.SEEK_END)
                if self.handle.tell() == 0:
                    self.handle.write("0")
                    self.handle.flush()
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as exc:
            self.handle.close()
            self.handle = None
            raise RuntimeError("Another ProBigA AI bridge worker is already running") from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(str(os.getpid()))
        self.handle.flush()

    def release(self) -> None:
        if self.handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def _configure_logging(verbose: bool, log_root: Path | None = None) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    if not root_logger.handlers:
        stream = logging.StreamHandler()
        stream.setFormatter(formatter)
        root_logger.addHandler(stream)
    log_path = (log_root or ROOT / "logs") / "ai_bridge_worker.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not any(isinstance(handler, RotatingFileHandler) for handler in root_logger.handlers):
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=2_000_000,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.environ.get(name, str(default))))
    except (TypeError, ValueError):
        return default


def _settings_token() -> str:
    try:
        from server.common.config import get_settings

        return str(get_settings().probiga_ai_bridge_token or "").strip()
    except Exception:
        return ""


def _find_codex_executable() -> Path:
    configured = os.environ.get("PROBIGA_CODEX_EXE", "").strip()
    if configured:
        path = Path(configured).expanduser().resolve()
        if path.is_file():
            return path
        raise ProviderUnavailable(f"Configured Codex executable does not exist: {path}")

    local_app_data = Path(os.environ.get("LOCALAPPDATA", ""))
    bundled_root = local_app_data / "OpenAI" / "Codex" / "bin"
    candidates = list(bundled_root.glob("*/codex.exe")) if bundled_root.is_dir() else []
    root_executable = bundled_root / "codex.exe"
    if root_executable.is_file():
        candidates.append(root_executable)
    if candidates:
        return max(candidates, key=lambda item: item.stat().st_mtime)

    fallback = shutil.which("codex.exe") or shutil.which("codex")
    if fallback:
        return Path(fallback)
    raise ProviderUnavailable("Codex desktop executable was not found")


class CodexTaskProvider:
    def __init__(self, timeout_seconds: int) -> None:
        self.timeout_seconds = timeout_seconds
        self.executable = _find_codex_executable()
        self.thread_ids = {
            "stock": os.environ.get("PROBIGA_CODEX_STOCK_THREAD_ID", DEFAULT_STOCK_THREAD_ID).strip(),
            "general": os.environ.get("PROBIGA_CODEX_GENERAL_THREAD_ID", DEFAULT_GENERAL_THREAD_ID).strip(),
        }

    def ask(self, channel: str, question: str) -> str:
        thread_id = self.thread_ids.get(channel)
        if not thread_id:
            raise ProviderUnavailable(f"No Codex task is configured for channel {channel}")
        with tempfile.TemporaryDirectory(prefix="probiga-codex-") as temp_dir:
            output_path = Path(temp_dir) / "answer.txt"
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            process: subprocess.Popen[bytes] | None = None
            last_start_error: OSError | None = None
            for attempt in range(2):
                discovered = _find_codex_executable()
                if discovered != self.executable:
                    logger.info("Codex executable changed from %s to %s", self.executable, discovered)
                    self.executable = discovered
                command = [
                    str(self.executable),
                    "exec",
                    "resume",
                    "--ignore-user-config",
                    "--json",
                    "-o",
                    str(output_path),
                    thread_id,
                    question,
                ]
                try:
                    process = subprocess.Popen(
                        command,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        creationflags=creationflags,
                    )
                    break
                except OSError as exc:
                    last_start_error = exc
                    if attempt == 0:
                        time.sleep(0.2)
            if process is None:
                raise ProviderUnavailable(f"GPT（Codex）启动失败：{last_start_error}")
            try:
                stdout, stderr = process.communicate(timeout=self.timeout_seconds)
            except subprocess.TimeoutExpired as exc:
                process.kill()
                process.communicate()
                raise ProviderUnavailable(
                    f"GPT（Codex）在 {self.timeout_seconds} 秒内没有返回答案"
                ) from exc

            messages: list[str] = []
            for raw_line in stdout.decode("utf-8", errors="replace").splitlines():
                try:
                    event = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue
                item = event.get("item") if isinstance(event, dict) else None
                if (
                    event.get("type") == "item.completed"
                    and isinstance(item, dict)
                    and item.get("type") == "agent_message"
                    and isinstance(item.get("text"), str)
                ):
                    messages.append(item["text"])

            answer = messages[-1] if messages else ""
            if not answer and output_path.is_file():
                answer = output_path.read_text(encoding="utf-8")
            if process.returncode != 0 or not answer.strip():
                error_tail = stderr.decode("utf-8", errors="replace").strip().splitlines()[-1:]
                detail = error_tail[0][:240] if error_tail else f"exit code {process.returncode}"
                raise ProviderUnavailable(f"GPT（Codex）不可用：{detail}")
            return answer


_DEEPSEEK_CITATION_ARTIFACT = re.compile(r"-\r?\n[ \t]*(\d+)[ \t]*(?:\r?\n)?")


def _normalize_deepseek_answer(answer: str) -> str:
    """Turn DeepSeek's positioned citation badges into readable inline markers."""

    return _DEEPSEEK_CITATION_ARTIFACT.sub(lambda match: f"[{match.group(1)}]", answer).strip()


class CdpConnection:
    def __init__(self, websocket_url: str) -> None:
        try:
            from websockets.sync.client import connect
        except ImportError as exc:
            raise ProviderUnavailable("DeepSeek 网页兜底需要安装 websockets") from exc
        self._socket = connect(websocket_url, origin=None, open_timeout=10, close_timeout=3)
        self._sequence = 0

    def close(self) -> None:
        try:
            self._socket.close()
        except Exception as exc:
            logger.debug("failed to close DeepSeek websocket: %s", exc)

    def call(self, method: str, params: dict[str, Any] | None = None, timeout: float = 15) -> Any:
        self._sequence += 1
        request_id = self._sequence
        self._socket.send(json.dumps({"id": request_id, "method": method, "params": params or {}}))
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProviderUnavailable(f"DeepSeek browser command timed out: {method}")
            message = json.loads(self._socket.recv(timeout=remaining))
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise ProviderUnavailable(f"DeepSeek browser error: {message['error'].get('message', method)}")
            return message.get("result")

    def evaluate(self, expression: str, timeout: float = 15) -> Any:
        result = self.call(
            "Runtime.evaluate",
            {"expression": expression, "returnByValue": True, "awaitPromise": True},
            timeout=timeout,
        )
        return ((result or {}).get("result") or {}).get("value")


class DeepSeekChromeSession:
    CHAT_URL = "https://chat.deepseek.com/"

    def __init__(self, profile_dir: Path) -> None:
        self.profile_dir = profile_dir.resolve()
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self.port_file = self.profile_dir / "DevToolsActivePort"

    @staticmethod
    def _chrome_path() -> Path:
        configured = os.environ.get("PROBIGA_DEEPSEEK_CHROME_EXE", "").strip()
        candidates = [
            Path(configured) if configured else None,
            Path(os.environ.get("PROGRAMFILES", "")) / "Google/Chrome/Application/chrome.exe",
            Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google/Chrome/Application/chrome.exe",
            Path(os.environ.get("LOCALAPPDATA", "")) / "Google/Chrome/Application/chrome.exe",
        ]
        for candidate in candidates:
            if candidate and candidate.is_file():
                return candidate
        raise ProviderUnavailable("Google Chrome is required for DeepSeek webpage fallback")

    def _port(self) -> int | None:
        if not self.port_file.is_file():
            return None
        try:
            first_line = self.port_file.read_text(encoding="utf-8").splitlines()[0]
            port = int(first_line)
            with urlopen(f"http://127.0.0.1:{port}/json/version", timeout=1):
                return port
        except (OSError, ValueError, IndexError, URLError):
            return None

    def ensure_running(self) -> int:
        existing = self._port()
        if existing:
            return existing
        if self.port_file.exists():
            self.port_file.unlink(missing_ok=True)
        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        subprocess.Popen(
            [
                str(self._chrome_path()),
                "--remote-debugging-port=0",
                "--remote-allow-origins=*",
                f"--user-data-dir={self.profile_dir}",
                "--no-proxy-server",
                "--no-first-run",
                "--no-default-browser-check",
                self.CHAT_URL,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=flags,
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            port = self._port()
            if port:
                return port
            time.sleep(0.25)
        raise ProviderUnavailable("DeepSeek 专用 Chrome 窗口启动失败")

    @staticmethod
    def _json(port: int, path: str, method: str = "GET") -> Any:
        request = Request(f"http://127.0.0.1:{port}{path}", method=method)
        with urlopen(request, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))

    def page(self) -> dict[str, Any]:
        port = self.ensure_running()
        pages = self._json(port, "/json/list")
        for page in pages:
            if page.get("type") == "page" and "chat.deepseek.com" in str(page.get("url", "")):
                return page
        return self._json(port, "/json/new?" + quote(self.CHAT_URL, safe=""), method="PUT")


COMPOSER_STATE_SCRIPT = r"""
(() => {
  const visible = (el) => {
    const style = getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 20 && rect.height > 15;
  };
  document.querySelectorAll('[data-probiga-composer]').forEach((el) => el.removeAttribute('data-probiga-composer'));
  const candidates = Array.from(document.querySelectorAll('textarea, [contenteditable="true"]'))
    .filter((el) => visible(el) && !el.disabled && el.getAttribute('aria-disabled') !== 'true');
  const score = (el) => {
    const hint = [el.getAttribute('placeholder'), el.getAttribute('aria-label'), el.getAttribute('data-placeholder')]
      .filter(Boolean).join(' ').toLowerCase();
    let value = el.tagName === 'TEXTAREA' ? 4 : 2;
    if (/message|ask|chat|deepseek|发送|提问|问点/.test(hint)) value += 10;
    const rect = el.getBoundingClientRect();
    if (rect.top > innerHeight * 0.45) value += 3;
    return value;
  };
  candidates.sort((a, b) => score(b) - score(a));
  const composer = candidates[0] || null;
  if (composer) composer.setAttribute('data-probiga-composer', '1');
  const captcha = Array.from(document.querySelectorAll('iframe'))
    .some((el) => /captcha|verify|challenge/.test((el.src || '').toLowerCase()));
  return {ready: !!composer, candidates: candidates.length, captcha, title: document.title, url: location.href};
})()
"""

PREPARE_COMPOSER_SCRIPT = r"""
(() => {
  const el = document.querySelector('[data-probiga-composer="1"]');
  if (!el) return false;
  el.focus();
  if (el.tagName === 'TEXTAREA' || el.tagName === 'INPUT') {
    const setter = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(el), 'value')?.set;
    if (setter) setter.call(el, ''); else el.value = '';
  } else {
    el.textContent = '';
  }
  el.dispatchEvent(new InputEvent('input', {bubbles: true, inputType: 'deleteContentBackward', data: null}));
  return true;
})()
"""

SEND_BUTTON_SCRIPT = r"""
(() => {
  document.querySelectorAll('[data-probiga-send]').forEach((el) => el.removeAttribute('data-probiga-send'));
  const composer = document.querySelector('[data-probiga-composer="1"]');
  if (!composer) return false;
  const visible = (el) => {
    const style = getComputedStyle(el), rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 10 && rect.height > 10;
  };
  const root = composer.closest('form') || composer.parentElement?.parentElement || document;
  const buttons = Array.from(root.querySelectorAll('button')).filter((el) => visible(el) && !el.disabled);
  const labelled = buttons.filter((el) => /send|发送|提交/.test([
    el.getAttribute('aria-label'), el.getAttribute('title'), el.textContent
  ].filter(Boolean).join(' ').toLowerCase()));
  const submit = buttons.filter((el) => el.type === 'submit');
  const chosen = labelled.length === 1 ? labelled[0] : (submit.length === 1 ? submit[0] : null);
  if (!chosen) return false;
  chosen.setAttribute('data-probiga-send', '1');
  chosen.click();
  return true;
})()
"""

ANSWER_STATE_SCRIPT = r"""
(() => {
  const visible = (el) => {
    const style = getComputedStyle(el), rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 20 && rect.height > 5;
  };
  const selectors = [
    '[data-message-author-role="assistant"]', '[data-role="assistant"]',
    '.ds-markdown', '[class*="assistant-message"]'
  ];
  const unique = Array.from(new Set(selectors.flatMap((selector) => Array.from(document.querySelectorAll(selector)))))
    .filter(visible);
  const leaves = unique.filter((node) => !unique.some((other) => other !== node && node.contains(other)));
  const texts = leaves.map((el) => el.innerText).filter((text) => text && text.trim());
  const generating = Array.from(document.querySelectorAll('button')).filter(visible).some((el) =>
    /stop|停止|终止/.test([el.getAttribute('aria-label'), el.getAttribute('title'), el.textContent]
      .filter(Boolean).join(' ').toLowerCase())
  );
  return {texts, generating};
})()
"""


class DeepSeekWebProvider:
    def __init__(self, timeout_seconds: int, login_wait_seconds: int, profile_dir: Path) -> None:
        self.timeout_seconds = timeout_seconds
        self.login_wait_seconds = login_wait_seconds
        self.chrome = DeepSeekChromeSession(profile_dir)

    def prepare(self) -> bool:
        page = self.chrome.page()
        connection = CdpConnection(page["webSocketDebuggerUrl"])
        try:
            connection.call("Runtime.enable")
            connection.call("Page.bringToFront")
            state = connection.evaluate(COMPOSER_STATE_SCRIPT)
            return bool(state and state.get("ready"))
        finally:
            connection.close()

    def ask(self, question: str) -> str:
        page = self.chrome.page()
        connection = CdpConnection(page["webSocketDebuggerUrl"])
        try:
            connection.call("Runtime.enable")
            connection.call("Page.bringToFront")
            deadline = time.monotonic() + self.login_wait_seconds
            state: dict[str, Any] = {}
            while time.monotonic() < deadline:
                state = connection.evaluate(COMPOSER_STATE_SCRIPT) or {}
                if state.get("captcha"):
                    raise ProviderUnavailable("DeepSeek 页面需要人工完成验证码后重试")
                if state.get("ready"):
                    break
                time.sleep(2)
            if not state.get("ready"):
                raise ProviderUnavailable(
                    "DeepSeek 专用浏览器尚未登录；请在本机弹出的 DeepSeek 窗口完成登录后重试"
                )

            before = connection.evaluate(ANSWER_STATE_SCRIPT) or {"texts": []}
            if not connection.evaluate(PREPARE_COMPOSER_SCRIPT):
                raise ProviderUnavailable("DeepSeek 输入框暂时不可用")
            connection.call("Input.insertText", {"text": question}, timeout=20)
            sent = bool(connection.evaluate(SEND_BUTTON_SCRIPT))
            if not sent:
                connection.call(
                    "Input.dispatchKeyEvent",
                    {"type": "keyDown", "key": "Enter", "code": "Enter", "windowsVirtualKeyCode": 13},
                )
                connection.call(
                    "Input.dispatchKeyEvent",
                    {"type": "keyUp", "key": "Enter", "code": "Enter", "windowsVirtualKeyCode": 13},
                )

            initial_texts = before.get("texts") or []
            initial_last = initial_texts[-1] if initial_texts else ""
            deadline = time.monotonic() + self.timeout_seconds
            stable_text = ""
            stable_count = 0
            while time.monotonic() < deadline:
                time.sleep(2)
                current = connection.evaluate(ANSWER_STATE_SCRIPT) or {"texts": []}
                texts = current.get("texts") or []
                candidate = texts[-1] if texts else ""
                is_new = len(texts) > len(initial_texts) or (candidate and candidate != initial_last)
                if not is_new or not candidate.strip():
                    continue
                if candidate == stable_text:
                    stable_count += 1
                else:
                    stable_text = candidate
                    stable_count = 1
                if stable_count >= 3 and not current.get("generating"):
                    return _normalize_deepseek_answer(stable_text)
            raise ProviderUnavailable(f"DeepSeek 网页在 {self.timeout_seconds} 秒内没有返回完整答案")
        finally:
            connection.close()


class BridgeApi:
    def __init__(self, server_url: str, token: str, timeout_seconds: int = 30) -> None:
        self.client = httpx.Client(
            base_url=server_url.rstrip("/"),
            headers={
                "Accept": "application/json",
                "X-ProBigA-AI-Bridge-Token": token,
            },
            timeout=timeout_seconds,
            # The production queue remains reachable even when the local VPN
            # or HTTP proxy is down; only the GPT route should depend on it.
            trust_env=False,
        )

    def close(self) -> None:
        self.client.close()

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.client.post(path, json=payload)
        response.raise_for_status()
        return response.json()

    def claim(self, worker_id: str) -> dict[str, Any] | None:
        return self._post("/api/ai-bridge/worker/claim", {"worker_id": worker_id}).get("job")

    def progress(self, request_id: str, worker_id: str, source: str) -> None:
        self._post(
            f"/api/ai-bridge/worker/{request_id}/progress",
            {"worker_id": worker_id, "provider_attempt": source},
        )

    def completed(self, request_id: str, worker_id: str, answer: str, source: str, label: str) -> None:
        self._post(
            f"/api/ai-bridge/worker/{request_id}/complete",
            {
                "worker_id": worker_id,
                "status": "completed",
                "answer": answer,
                "source": source,
                "source_label": label,
            },
        )

    def failed(self, request_id: str, worker_id: str, message: str) -> None:
        self._post(
            f"/api/ai-bridge/worker/{request_id}/complete",
            {
                "worker_id": worker_id,
                "status": "failed",
                "error_message": message[:1000],
            },
        )


def process_job(
    api: BridgeApi,
    codex: CodexTaskProvider,
    deepseek: DeepSeekWebProvider,
    *,
    job: dict[str, Any],
    worker_id: str,
    disable_deepseek: bool = False,
) -> None:
    request_id = str(job["request_id"])
    codex_error: Exception | None = None
    try:
        api.progress(request_id, worker_id, SOURCE_CODEX)
        answer = codex.ask(str(job["channel"]), str(job["question"]))
        api.completed(request_id, worker_id, answer, SOURCE_CODEX, "GPT（Codex）")
        logger.info("Completed %s through GPT (Codex)", request_id)
        return
    except Exception as exc:
        codex_error = exc
        logger.warning("GPT (Codex) failed for %s: %s", request_id, exc)

    if disable_deepseek:
        api.failed(request_id, worker_id, f"GPT（Codex）不可用：{codex_error}")
        return

    try:
        api.progress(request_id, worker_id, SOURCE_DEEPSEEK)
        answer = deepseek.ask(str(job["question"]))
        api.completed(request_id, worker_id, answer, SOURCE_DEEPSEEK, "DeepSeek 网页")
        logger.info("Completed %s through DeepSeek webpage", request_id)
    except Exception as deepseek_error:
        logger.error("DeepSeek webpage failed for %s: %s", request_id, deepseek_error)
        api.failed(
            request_id,
            worker_id,
            "；".join(
                [
                    f"GPT（Codex）不可用：{codex_error}",
                    f"DeepSeek 网页不可用：{deepseek_error}",
                ]
            ),
        )


RESEARCH_MAX_INPUT_BYTES = 1_500_000
RESEARCH_MAX_OUTPUT_BYTES = 400_000
RESEARCH_MAX_EVENTS_BYTES = 4_000_000
RESEARCH_RENEW_SECONDS = 10
RESEARCH_CODEX_PROVIDER = "probiga_openai_https"
RESEARCH_CODEX_BASE_URL = "https://chatgpt.com/backend-api/codex"


def research_codex_transport_args() -> list[str]:
    """Official ChatGPT auth over HTTPS, with no failed WebSocket preflight.

    Built-in provider IDs cannot be overridden by this installed CLI. A named
    provider uses the documented fields, never removed feature flags. Model
    selection stays with the CLI; only its transport and retry budget change.
    """
    provider = ('model_providers.' + RESEARCH_CODEX_PROVIDER + '={name="OpenAI",base_url="' +
                RESEARCH_CODEX_BASE_URL + '",wire_api="responses",requires_openai_auth=true,'
                'supports_websockets=false,request_max_retries=0,stream_max_retries=0,stream_idle_timeout_ms=30000}')
    return ["-c", 'model_provider="' + RESEARCH_CODEX_PROVIDER + '"', "-c", provider]


def _research_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _research_deadline(payload: dict[str, Any]) -> float:
    try:
        stamp = datetime.fromisoformat(str(payload["deadline_at"]).replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("timezone required")
        remaining = stamp.timestamp() - time.time()
    except (KeyError, ValueError, TypeError) as exc:
        raise ProviderUnavailable("RESEARCH_FAILED stage=input reason=invalid_deadline") from exc
    if remaining <= 0:
        raise ProviderUnavailable("RESEARCH_FAILED stage=input reason=deadline_expired")
    return time.monotonic() + remaining


def validate_research_job(job: dict[str, Any]) -> dict[str, Any]:
    """Only a verified, typed server claim can enter the research executor."""
    payload = job.get("payload")
    if job.get("channel") != "research" or not isinstance(payload, dict):
        raise ProviderUnavailable("RESEARCH_FAILED stage=input reason=not_research_claim")
    request_id = str(job.get("request_id") or "")
    if (not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", request_id)
            or payload.get("request_id") != request_id
            or not isinstance(job.get("claim_generation"), int) or job["claim_generation"] < 1):
        raise ProviderUnavailable("RESEARCH_FAILED stage=input reason=claim_identity")
    messages, schema = payload.get("messages"), payload.get("response_schema")
    if (not isinstance(messages, list) or not 1 <= len(messages) <= 12
            or any(not isinstance(item, dict) or item.get("role") not in {"system", "user", "assistant"}
                   or not isinstance(item.get("content"), str) for item in messages)
            or not isinstance(schema, dict) or schema.get("type") != "object"):
        raise ProviderUnavailable("RESEARCH_FAILED stage=input reason=invalid_messages_or_schema")
    try:
        encoded = _research_json(payload)
        identity = _sha256(_research_json({key: value for key, value in payload.items()
                                         if key not in {"request_id", "deadline_at", "input_sha256"}}))
    except (TypeError, ValueError) as exc:
        raise ProviderUnavailable("RESEARCH_FAILED stage=input reason=noncanonical_payload") from exc
    if (len(encoded) > RESEARCH_MAX_INPUT_BYTES or identity != payload.get("input_sha256")
            or not re.fullmatch(r"[0-9a-f]{64}", str(payload.get("rules_sha256") or ""))):
        raise ProviderUnavailable("RESEARCH_FAILED stage=input reason=payload_size_or_hash")
    _research_deadline(payload)
    return payload


def _research_checkpoint(directory: Path, value: dict[str, Any]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / ("receipt." + uuid4().hex + ".tmp")
    with temporary.open("wb") as handle:
        handle.write(_research_json(value))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, directory / "receipt.json")


class ResearchBridgeApi(BridgeApi):
    """Typed research transport; never invokes the question queue or fallback."""

    def __init__(self, server_url: str, token: str) -> None:
        self.server_url = server_url.rstrip("/")
        self.headers = {"Accept": "application/json", "X-ProBigA-AI-Bridge-Token": token}

    def close(self) -> None:
        return None

    def _request(self, method: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        async def exchange():
            async with asyncio.timeout(15):
                async with httpx.AsyncClient(headers=self.headers, trust_env=False,
                                             timeout=httpx.Timeout(12, connect=5),
                                             transport=httpx.AsyncHTTPTransport(retries=0)) as client:
                    kwargs = {"params": payload} if method == "GET" else {"json": payload}
                    response = await client.request(method, self.server_url + path, **kwargs)
                    response.raise_for_status()
                    if len(response.content) > RESEARCH_MAX_OUTPUT_BYTES * 3:
                        raise ProviderUnavailable("RESEARCH_FAILED stage=transport reason=response_budget")
                    result = response.json()
                    if not isinstance(result, dict) or result.get("status") != "ok":
                        raise ProviderUnavailable("RESEARCH_FAILED stage=transport reason=invalid_receipt")
                    return result
        return asyncio.run(exchange())

    def claim_research(self, worker_id: str) -> dict[str, Any] | None:
        return self._request("POST", "/api/ai-bridge/worker/research/claim", {"worker_id": worker_id}).get("job")

    def renew_research(self, request_id: str, worker_id: str, generation: int) -> dict[str, Any]:
        return self._request("POST", f"/api/ai-bridge/worker/research/{request_id}/progress",
                             {"worker_id": worker_id, "claim_generation": generation})["job"]

    def complete_research(self, request_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/api/ai-bridge/worker/research/{request_id}/complete", payload)["result"]

    def research_status(self, request_id: str, worker_id: str, generation: int) -> dict[str, Any]:
        return self._request("GET", f"/api/ai-bridge/worker/research/{request_id}",
                             {"worker_id": worker_id, "claim_generation": generation})["result"]


class ResearchLease:
    """Renew during subprocess I/O, and immediately fence a lost owner."""

    def __init__(self, api, request_id: str, worker_id: str, generation: int, *, lease_expires_at=None, deadline_at=None, checkpoint=None) -> None:
        self.api, self.request_id, self.worker_id, self.generation = api, request_id, worker_id, generation
        self.stop = threading.Event()
        self.lost = threading.Event()
        self.thread: threading.Thread | None = None
        self.watchdog: threading.Thread | None = None
        self.valid_until = None
        self.deadline = _research_deadline({"deadline_at":deadline_at}) if deadline_at else float('inf')
        self.checkpoint = checkpoint
        self.renewals = 0
        self.transient_errors = 0
        if lease_expires_at:
            self.valid_until = min(self.deadline,self._expiry(lease_expires_at))

    @staticmethod
    def _expiry(stamp):
        try:
            value=datetime.fromisoformat(str(stamp).replace('Z','+00:00'))
            if value.tzinfo is None: raise ValueError('timezone required')
            return time.monotonic()+value.timestamp()-time.time()
        except (ValueError,TypeError) as exc:
            raise ProviderUnavailable('RESEARCH_FAILED stage=lease reason=invalid_verified_expiry') from exc

    def _record(self,status,**extra):
        if self.checkpoint:
            self.checkpoint({'status':status,'request_id':self.request_id,'worker_id':self.worker_id,
                'claim_generation':self.generation,'renewals':self.renewals,'transient_errors':self.transient_errors,
                'updated_at':_utc_iso(),**extra})

    @staticmethod
    def _transient(exc):
        return (isinstance(exc,(httpx.TransportError,TimeoutError,OSError)) or
                isinstance(exc,httpx.HTTPStatusError) and exc.response.status_code>=500)

    def renew(self) -> None:
        for attempt in range(2):
            if self.lost.is_set() or self.valid_until is not None and time.monotonic()>=self.valid_until-1:
                raise ProviderUnavailable('RESEARCH_FAILED stage=lease reason=verified_lease_expired')
            try:
                self.renewals+=1
                row=self.api.renew_research(self.request_id,self.worker_id,self.generation)
                if (row.get('request_id')!=self.request_id or row.get('worker_id')!=self.worker_id
                        or row.get('claim_generation')!=self.generation or row.get('status')!='processing'):
                    raise ProviderUnavailable('RESEARCH_FAILED stage=lease reason=owner_fenced')
                expiry=min(self.deadline,self._expiry(row.get('lease_expires_at')))
                if self.lost.is_set() or expiry<=time.monotonic()+1:
                    raise ProviderUnavailable('RESEARCH_FAILED stage=lease reason=verified_lease_expired')
                self.valid_until=expiry
                self._record('VERIFIED',lease_expires_at=row['lease_expires_at'])
                return
            except Exception as exc:
                if self._transient(exc):
                    self.transient_errors+=1
                    self._record('TRANSPORT_RETRY',error_type=type(exc).__name__)
                    if attempt==0 and self.valid_until is not None and time.monotonic()<self.valid_until-1:
                        continue
                raise

    def __enter__(self):
        self.renew()
        def expiry_watchdog():
            while not self.stop.wait(min(0.2,max(0,self.valid_until-time.monotonic()-1))):
                if time.monotonic()>=self.valid_until-1:
                    self.lost.set()
                    self._record('FENCED',reason='verified_lease_expired')
                    return
        def heartbeat():
            # Start-to-start scheduling prevents a slow HTTP response from
            # adding its latency to the renewal interval.
            next_renew = time.monotonic() + RESEARCH_RENEW_SECONDS
            while not self.stop.wait(max(0.0, next_renew - time.monotonic())):
                next_renew = time.monotonic() + RESEARCH_RENEW_SECONDS
                try:
                    self.renew()
                except Exception:
                    self.lost.set()
                    self._record('FENCED',reason='renewal_unconfirmed_or_owner_changed')
                    return
        self.thread = threading.Thread(target=heartbeat, name="research-owner-lease", daemon=True)
        self.watchdog = threading.Thread(target=expiry_watchdog,name='research-lease-expiry',daemon=True)
        self.watchdog.start()
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=16)
        if self.watchdog:
            self.watchdog.join(timeout=1)


_WINDOWS_INTERNET_SETTINGS = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
_PROXY_ENV_NAMES = frozenset({"http_proxy", "https_proxy", "all_proxy", "no_proxy"})


def _proxy_error(reason: str):
    # Never include raw registry strings: malformed URLs can contain secrets.
    raise ProviderUnavailable("RESEARCH_FAILED stage=network_route reason=" + reason)


def _current_user_proxy_settings() -> dict[str, Any]:
    """Read HKCU only; no WinHTTP mutation, PAC execution or process-proxy reuse."""
    try:
        import winreg
    except ImportError:
        return {"platform": "non_windows", "ProxyEnable": 0, "ProxyServer": "", "AutoConfigURL": "", "AutoDetect": 0}
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, _WINDOWS_INTERNET_SETTINGS, 0, winreg.KEY_READ) as key:
            def read(name, default, kinds):
                try:
                    value, kind = winreg.QueryValueEx(key, name)
                except FileNotFoundError:
                    return default
                if kind not in kinds:
                    _proxy_error("invalid_system_proxy_value_type")
                return value
            return {"platform": "windows", "ProxyEnable": read("ProxyEnable", 0, {winreg.REG_DWORD}),
                    "ProxyServer": read("ProxyServer", "", {winreg.REG_SZ, winreg.REG_EXPAND_SZ}),
                    "AutoConfigURL": read("AutoConfigURL", "", {winreg.REG_SZ, winreg.REG_EXPAND_SZ}),
                    "AutoDetect": read("AutoDetect", 0, {winreg.REG_DWORD})}
    except OSError as exc:
        raise ProviderUnavailable("RESEARCH_FAILED stage=network_route reason=system_proxy_registry_unavailable") from exc


def _system_https_proxy(settings: dict[str, Any]) -> str | None:
    enabled, detect = settings.get("ProxyEnable"), settings.get("AutoDetect")
    if type(enabled) is not int or enabled not in (0, 1) or type(detect) is not int or detect not in (0, 1):
        _proxy_error("invalid_system_proxy_flags")
    if not isinstance(settings.get("AutoConfigURL"), str) or not isinstance(settings.get("ProxyServer"), str):
        _proxy_error("invalid_system_proxy_value")
    if settings["AutoConfigURL"].strip() or detect:
        _proxy_error("system_proxy_auto_configuration_unsupported")
    if not enabled:
        return None
    configured = settings["ProxyServer"].strip()
    if not configured:
        _proxy_error("enabled_system_proxy_missing_server")
    # WinINET accepts either a single server or a protocol-specific map.
    if "=" in configured or ";" in configured:
        servers = {}
        for entry in configured.split(";"):
            if not entry or entry.count("=") != 1:
                _proxy_error("invalid_system_proxy_protocol_map")
            protocol, value = (part.strip() for part in entry.split("=", 1))
            protocol = protocol.lower()
            if protocol not in {"http", "https"} or protocol in servers or not value:
                _proxy_error("invalid_system_proxy_protocol_map")
            servers[protocol] = value
        if "https" not in servers:
            _proxy_error("system_https_proxy_missing")
        configured = servers["https"]
    if any(char.isspace() or ord(char) < 32 for char in configured):
        _proxy_error("invalid_system_proxy_url")
    try:
        parsed = urlsplit(configured if "://" in configured else "http://" + configured)
        if (parsed.scheme not in {"http", "https"} or parsed.username is not None or parsed.password is not None
                or parsed.path not in {"", "/"} or parsed.query or parsed.fragment or not parsed.hostname):
            _proxy_error("untrusted_system_https_proxy")
        address = ipaddress.ip_address(parsed.hostname)
        port = parsed.port
        if not address.is_loopback or port is None or not 1 <= port <= 65535:
            _proxy_error("system_https_proxy_not_loopback")
    except ValueError as exc:
        raise ProviderUnavailable("RESEARCH_FAILED stage=network_route reason=invalid_or_nonliteral_loopback_proxy") from None
    host = "[" + str(address) + "]" if address.version == 6 else str(address)
    return f"{parsed.scheme}://{host}:{port}"


def _research_child_environment() -> tuple[dict[str, str], dict[str, Any]]:
    environment = {key: value for key, value in os.environ.items()
                   if key.lower() not in _PROXY_ENV_NAMES
                   and key.upper() not in {"OPENAI_API_KEY", "OPENAI_BASE_URL", "CODEX_API_KEY"}}
    settings = _current_user_proxy_settings()
    proxy = _system_https_proxy(settings)
    if proxy:
        environment["HTTPS_PROXY"] = proxy
    route = proxy or "DIRECT"
    receipt = {"source": "windows_hkcu_internet_settings" if settings.get("platform") == "windows" else "non_windows_no_system_proxy",
               "mode": "system_https_loopback_proxy" if proxy else "system_proxy_disabled_direct",
               "route": route, "route_sha256": hashlib.sha256(route.encode("utf-8")).hexdigest(),
               "process_proxy_inherited": False, "pac_used": False}
    return environment, receipt


class CodexResearchProvider:
    """A single tool-disabled ephemeral process, never an existing chat."""

    def __init__(self, timeout_seconds: int = 240, executable: Path | None = None) -> None:
        self.timeout_seconds = min(300, max(1, timeout_seconds))
        self.executable = executable or _find_codex_executable()

    @staticmethod
    def environment() -> dict[str, str]:
        return _research_child_environment()[0]

    @staticmethod
    def environment_with_route() -> tuple[dict[str, str], dict[str, Any]]:
        return _research_child_environment()

    def version(self) -> str:
        try:
            result = subprocess.run([str(self.executable), "--version"], capture_output=True,
                                    timeout=10, check=False, env=self.environment(),
                                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            return result.stdout.decode("utf-8", errors="replace").strip()[:120] if result.returncode == 0 else "UNKNOWN"
        except (OSError, subprocess.TimeoutExpired):
            return "UNKNOWN"

    def ask(self, payload: dict[str, Any], directory: Path, lease_lost: threading.Event) -> dict[str, Any]:
        directory = directory.resolve()
        self.network_route = {"source": "UNAVAILABLE", "mode": "route_not_resolved"}
        child_environment, network_route = self.environment_with_route()
        self.network_route = network_route
        cli_version = self.version()
        deadline = min(_research_deadline(payload), time.monotonic() + self.timeout_seconds)
        schema_path, answer_path = directory / "schema.json", directory / "answer.json"
        events_path, errors_path = directory / "events.jsonl", directory / "stderr.txt"
        schema_path.write_bytes(_research_json(payload["response_schema"]))
        prompt = _research_json({"instruction": "完成以下独立研究任务。messages为服务端冻结任务及证据，按response_schema输出JSON。不得调用工具、读取项目、访问既有聊天或更改事实。",
                                 "messages": payload["messages"], "request_id": payload["request_id"],
                                 "input_sha256": payload["input_sha256"]})
        command = [str(self.executable), "exec", "--ignore-user-config", *research_codex_transport_args(), "--ephemeral",
                   "--skip-git-repo-check", "--sandbox", "read-only", "--disable", "shell_tool",
                   "--disable", "apps", "--disable", "plugins", "--json", "--output-schema", str(schema_path),
                   "-o", str(answer_path), "-C", str(directory / "session"), "-"]
        (directory / "session").mkdir(exist_ok=True)
        started = _utc_iso()
        process = None
        writer_errors = []
        process_state = {"status": "RUNNING", "request_id": payload["request_id"],
                         "input_sha256": payload["input_sha256"], "started_at": started,
                         "deadline_at": payload["deadline_at"], "executable": str(self.executable),
                         "command_sha256": _sha256(_research_json(command)), "parent_pid": os.getpid(),
                         "worker_source_sha256": WORKER_SOURCE_SHA256, "network_route": network_route,
                         "observed_output_bytes": 0, "last_output_at": None, "progress_state": "NO_OUTPUT_YET"}
        with events_path.open("wb") as events, errors_path.open("wb") as errors:
            try:
                process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=events, stderr=errors,
                                           cwd=directory / "session", env=child_environment,
                                           creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
                process_state["pid"] = process.pid
                _research_checkpoint(directory / "process", process_state)
                def write_prompt():
                    try:
                        process.stdin.write(prompt)
                        process.stdin.close()
                    except (OSError, ValueError) as exc:
                        writer_errors.append(type(exc).__name__)
                writer = threading.Thread(target=write_prompt, name="research-prompt", daemon=True)
                writer.start()
                next_checkpoint = time.monotonic()
                last_output = time.monotonic()
                while process.poll() is None:
                    if lease_lost.is_set():
                        raise ProviderUnavailable("RESEARCH_FAILED stage=lease reason=owner_lost")
                    if time.monotonic() >= deadline:
                        raise ProviderUnavailable("RESEARCH_FAILED stage=codex reason=deadline_exceeded")
                    if events_path.stat().st_size > RESEARCH_MAX_EVENTS_BYTES or errors_path.stat().st_size > 100_000:
                        raise ProviderUnavailable("RESEARCH_FAILED stage=codex reason=output_budget")
                    if time.monotonic() >= next_checkpoint:
                        observed = events_path.stat().st_size
                        if observed > process_state["observed_output_bytes"]:
                            process_state.update(observed_output_bytes=observed, last_output_at=_utc_iso())
                            last_output = time.monotonic()
                        process_state.update(last_checked_at=_utc_iso(),
                                             progress_state="NO_NEW_OUTPUT" if time.monotonic() - last_output > 30 else
                                             ("OUTPUT_OBSERVED" if observed else "NO_OUTPUT_YET"))
                        _research_checkpoint(directory / "process", process_state)
                        next_checkpoint = time.monotonic() + 2
                    time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
                writer.join(timeout=2)
                if writer.is_alive() or writer_errors or process.returncode != 0:
                    raise ProviderUnavailable("RESEARCH_FAILED stage=codex reason=process_or_stdin_failed")
            finally:
                if process is not None and process.poll() is None:
                    process.kill()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired as exc:
                        process_state.update(status="BLOCKED", stage="termination_unconfirmed", updated_at=_utc_iso())
                        _research_checkpoint(directory / "process", process_state)
                        raise ResearchTerminationUnconfirmed("RESEARCH_FAILED stage=codex reason=termination_unconfirmed") from exc
                if process is not None:
                    process_state.update(status="TERMINAL", process_exit_code=process.returncode,
                                         observed_output_bytes=events_path.stat().st_size, completed_at=_utc_iso())
                    _research_checkpoint(directory / "process", process_state)
        if lease_lost.is_set() or time.monotonic() >= deadline:
            raise ProviderUnavailable("RESEARCH_FAILED stage=codex reason=deadline_or_lease_lost")
        if not answer_path.is_file() or not 0 < answer_path.stat().st_size <= RESEARCH_MAX_OUTPUT_BYTES:
            raise ProviderUnavailable("RESEARCH_FAILED stage=codex reason=missing_or_large_answer")
        raw = answer_path.read_bytes().decode("utf-8")
        try:
            if not isinstance(json.loads(raw), dict):
                raise ValueError("object required")
        except (ValueError, UnicodeError) as exc:
            raise ProviderUnavailable("RESEARCH_FAILED stage=codex reason=invalid_json_answer") from exc
        if events_path.stat().st_size > RESEARCH_MAX_EVENTS_BYTES:
            raise ProviderUnavailable("RESEARCH_FAILED stage=codex reason=output_budget")
        event_bytes = events_path.read_bytes()
        model, session_id, turn_id = "UNKNOWN", "UNKNOWN", "UNKNOWN"
        for line in event_bytes.splitlines():
            try:
                value = json.loads(line)
            except (ValueError, UnicodeError):
                continue
            if isinstance(value, dict):
                if value.get("type") == "thread.started" and isinstance(value.get("thread_id"), str):
                    session_id = value["thread_id"]
                if isinstance(value.get("model"), str):
                    model = value["model"]
                if isinstance(value.get("turn_id"), str):
                    turn_id = value["turn_id"]
        return {"raw_response": raw, "provider_receipt": {
            "source": SOURCE_CODEX, "request_id": payload["request_id"], "input_sha256": payload["input_sha256"],
            "rules_sha256": payload["rules_sha256"], "schema_sha256": _sha256(_research_json(payload["response_schema"])),
            "raw_sha256": _sha256(raw.encode("utf-8")), "jsonl_sha256": _sha256(event_bytes),
            "model": model, "session_id": session_id, "turn_id": turn_id, "ephemeral": True,
            "provider_id": RESEARCH_CODEX_PROVIDER, "base_url": RESEARCH_CODEX_BASE_URL,
            "network_transport": "https_sse", "websocket_enabled": False,
            "network_route": network_route,
            "request_max_retries": 0, "stream_max_retries": 0,
            "started_at": started, "completed_at": _utc_iso(), "process_id": process.pid,
            "process_exit_code": process.returncode, "cli_version": cli_version,
            "worker_source_sha256": WORKER_SOURCE_SHA256,
        }}


def _confirm_research_completion(api, request_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """An uncertain completion is reconciled, never regenerated."""
    try:
        result = api.complete_research(request_id, payload)
    except Exception:
        result = api.research_status(request_id, payload["worker_id"], payload["claim_generation"])
        if result.get("status") == "processing":
            result = api.complete_research(request_id, payload)  # The only identical POST retry.
    expected_hash = _sha256(payload.get("raw_response", "").encode("utf-8"))
    if (result.get("request_id") != request_id or result.get("status") != payload["status"]
            or (payload["status"] == "completed" and (result.get("raw_sha256") != expected_hash
                or result.get("source") != SOURCE_CODEX))):
        raise ProviderUnavailable("RESEARCH_FAILED stage=completion_readback reason=receipt_mismatch")
    return result


def process_research_job(api, provider, *, job: dict[str, Any], worker_id: str, state_root: Path) -> dict[str, Any]:
    payload = validate_research_job(job)
    request_id, generation = job["request_id"], job["claim_generation"]
    if job.get("worker_id") != worker_id:
        raise ProviderUnavailable("RESEARCH_FAILED stage=lease reason=claim_owner")
    directory = state_root / request_id
    checkpoint = {"request_id": request_id, "worker_id": worker_id, "claim_generation": generation,
                  "input_sha256": payload["input_sha256"], "status": "RUNNING", "stage": "model", "updated_at": _utc_iso()}
    prior_path = directory / "receipt.json"
    if prior_path.is_file():
        prior = json.loads(prior_path.read_text(encoding="utf-8"))
        if (prior.get("input_sha256") != payload["input_sha256"] or prior.get("worker_id") != worker_id
                or prior.get("claim_generation") != generation):
            raise ProviderUnavailable("RESEARCH_FAILED stage=checkpoint reason=identity_mismatch")
        if prior.get("status") == "COMPLETE":
            return prior["result"]
        if prior.get("completion"):
            result = _confirm_research_completion(api, request_id, prior["completion"])
            prior.update(status="COMPLETE", stage="receipt", result=result, updated_at=_utc_iso())
            _research_checkpoint(directory, prior)
            return result
        # An interrupted unknown model outcome must never cause a duplicate call.
        raise ProviderUnavailable("RESEARCH_FAILED stage=checkpoint reason=interrupted_model_outcome_unknown")
    _research_checkpoint(directory, checkpoint)
    with ResearchLease(api, request_id, worker_id, generation,lease_expires_at=job.get('lease_expires_at'),
                       deadline_at=payload['deadline_at'],checkpoint=lambda value:_research_checkpoint(directory/'lease',value)) as lease:
        try:
            answer = provider.ask(payload, directory, lease.lost)
            completion = {"worker_id": worker_id, "claim_generation": generation, "status": "completed", **answer}
        except ResearchTerminationUnconfirmed:
            checkpoint.update(status="BLOCKED", stage="termination_unconfirmed", updated_at=_utc_iso())
            _research_checkpoint(directory, checkpoint)
            raise
        except Exception as exc:
            completion = {"worker_id": worker_id, "claim_generation": generation, "status": "failed",
                          "error_message": str(exc)[:1000], "provider_receipt": {
                              "source": SOURCE_CODEX, "request_id": request_id, "input_sha256": payload["input_sha256"],
                              "rules_sha256": payload["rules_sha256"], "completed_at": _utc_iso(),
                              "network_route": getattr(provider, "network_route", {"source": "UNAVAILABLE"})}}
        checkpoint.update(status="MODEL_COMPLETE", stage="completion", completion=completion, updated_at=_utc_iso())
        _research_checkpoint(directory, checkpoint)
        result = _confirm_research_completion(api, request_id, completion)
        checkpoint.update(status="COMPLETE", stage="receipt", result=result, updated_at=_utc_iso())
        _research_checkpoint(directory, checkpoint)
        return result


def run_research_worker(args, token: str) -> int:
    state_root = Path(args.state_dir or str(Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) /
                                          "ProBigA" / "research-worker")).resolve()
    state_root.mkdir(parents=True, exist_ok=True)
    _configure_logging(args.verbose, state_root / "logs")
    lock = WorkerLock(state_root / "worker.lock")
    lock.acquire()
    api = ResearchBridgeApi(args.server_url, token)
    worker_id = f"research-{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"[:120]
    try:
        # Unconfirmed old child ownership is an operator-visible stop condition,
        # not a reason to launch a second process after a task auto-restart.
        if any(json.loads(path.read_text(encoding="utf-8")).get("stage") == "termination_unconfirmed"
               for path in state_root.glob("*/receipt.json")):
            raise ResearchTerminationUnconfirmed("RESEARCH_FAILED stage=startup reason=prior_termination_unconfirmed")
        provider = CodexResearchProvider(_env_int("PROBIGA_CODEX_RESEARCH_TIMEOUT_SECONDS", 240, 1))
        while True:
            status = {"status": "RUNNING", "purpose": "research", "worker_id": worker_id, "pid": os.getpid(),
                      "worker_source_sha256": WORKER_SOURCE_SHA256, "last_progress_at": _utc_iso(),
                      "stage": "claim"}
            _research_checkpoint(state_root, status)
            try:
                job = api.claim_research(worker_id)
                if job is not None:
                    result = process_research_job(api, provider, job=job, worker_id=worker_id, state_root=state_root)
                    status.update(stage="receipt", request_id=job["request_id"], result_status=result["status"],
                                  last_progress_at=_utc_iso())
                    _research_checkpoint(state_root, status)
            except Exception as exc:
                status.update(status="DEGRADED", stage="failed", error_type=type(exc).__name__, last_progress_at=_utc_iso())
                _research_checkpoint(state_root, status)
                logger.error("Research worker stage failed: %s", type(exc).__name__)
                if args.once or isinstance(exc, ResearchTerminationUnconfirmed):
                    return 1
            if args.once:
                return 0
            time.sleep(max(0.5, min(20.0, args.poll_seconds)))
    finally:
        api.close()
        lock.release()


def build_parser() -> argparse.ArgumentParser:
    # Library consumers may inspect a local browser without configuring a
    # production worker. Keep the required host guard at the worker entry.
    host = os.environ.get("PROBIGA_REMOTE_SSH_HOST", "").strip()
    default_server_url = f"http://{host}" if host else ""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-url", default=os.environ.get("PROBIGA_AI_BRIDGE_SERVER_URL", default_server_url))
    parser.add_argument("--token", default="", help="Worker token; prefer PROBIGA_AI_BRIDGE_TOKEN or .env")
    parser.add_argument("--once", action="store_true", help="Claim at most one job, then exit")
    parser.add_argument("--prepare-deepseek", action="store_true", help="Open the dedicated DeepSeek window and report login readiness")
    parser.add_argument("--poll-seconds", type=float, default=3.0)
    parser.add_argument("--disable-deepseek", action="store_true")
    parser.add_argument("--research-only", action="store_true", help="专用研究队列与独立Codex会话；不领取问答、不启用网页兜底")
    parser.add_argument("--state-dir", default="", help="研究worker持久运行状态与原始回执目录")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.server_url:
        args.server_url = f"http://{remote_host()}"
    if args.research_only:
        if args.prepare_deepseek:
            raise SystemExit("research-only cannot prepare a webpage provider")
        token = (args.token or os.environ.get("PROBIGA_AI_BRIDGE_TOKEN", "") or _settings_token()).strip()
        if not token:
            raise SystemExit("Missing PROBIGA_AI_BRIDGE_TOKEN")
        return run_research_worker(args, token)
    _configure_logging(args.verbose)
    profile_dir = Path(
        os.environ.get(
            "PROBIGA_DEEPSEEK_PROFILE_DIR",
            str(ROOT / "data" / "ai_bridge" / "deepseek_chrome_profile"),
        )
    )
    deepseek = DeepSeekWebProvider(
        timeout_seconds=_env_int("PROBIGA_DEEPSEEK_TIMEOUT_SECONDS", 240, 30),
        login_wait_seconds=_env_int("PROBIGA_DEEPSEEK_LOGIN_WAIT_SECONDS", 90, 5),
        profile_dir=profile_dir,
    )
    if args.prepare_deepseek:
        ready = deepseek.prepare()
        print("DeepSeek webpage ready" if ready else "DeepSeek login required in the opened Chrome window")
        return 0 if ready else 2

    token = (args.token or os.environ.get("PROBIGA_AI_BRIDGE_TOKEN", "") or _settings_token()).strip()
    if not token:
        raise SystemExit("Missing PROBIGA_AI_BRIDGE_TOKEN; run tools/provision_ai_bridge_token.py first")
    worker_lock = WorkerLock(ROOT / "data" / "ai_bridge" / "worker.lock")
    worker_lock.acquire()
    worker_id = os.environ.get(
        "PROBIGA_AI_BRIDGE_WORKER_ID",
        f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:6]}",
    )[:120]
    api = BridgeApi(args.server_url, token)
    codex = CodexTaskProvider(_env_int("PROBIGA_CODEX_TIMEOUT_SECONDS", 240, 30))
    try:
        while True:
            try:
                job = api.claim(worker_id)
            except Exception as exc:
                logger.warning("Production queue is unavailable: %s", exc)
                if args.once:
                    return 1
                time.sleep(max(1.0, args.poll_seconds))
                continue
            if job is None:
                if args.once:
                    return 0
                time.sleep(max(0.5, args.poll_seconds))
                continue

            request_id = str(job["request_id"])
            logger.info("Claimed %s question %s", job.get("channel"), request_id)
            process_job(
                api,
                codex,
                deepseek,
                job=job,
                worker_id=worker_id,
                disable_deepseek=args.disable_deepseek,
            )
            if args.once:
                return 0
    finally:
        api.close()
        worker_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
