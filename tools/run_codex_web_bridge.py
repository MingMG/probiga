# -*- coding: utf-8 -*-
"""Poll ProBigA production questions and answer through Codex, then DeepSeek web.

This worker must run on the Windows machine that owns the two Codex desktop
tasks. It deliberately does not call an OpenAI or DeepSeek model API.
"""
from __future__ import annotations

import argparse
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
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from uuid import uuid4

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.remote_support import remote_host

DEFAULT_SERVER_URL = f"http://{remote_host()}"
DEFAULT_STOCK_THREAD_ID = "019fbe02-0390-7663-a7ba-bd150e063fe7"
DEFAULT_GENERAL_THREAD_ID = "019fbe02-0a70-7c62-9bf2-9ab439bea770"
SOURCE_CODEX = "codex_gpt"
SOURCE_DEEPSEEK = "deepseek_web"

logger = logging.getLogger("probiga.ai_bridge")


class ProviderUnavailable(RuntimeError):
    """A provider could not return a usable answer."""


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


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    if not root_logger.handlers:
        stream = logging.StreamHandler()
        stream.setFormatter(formatter)
        root_logger.addHandler(stream)
    log_path = ROOT / "logs" / "ai_bridge_worker.log"
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-url", default=os.environ.get("PROBIGA_AI_BRIDGE_SERVER_URL", DEFAULT_SERVER_URL))
    parser.add_argument("--token", default="", help="Worker token; prefer PROBIGA_AI_BRIDGE_TOKEN or .env")
    parser.add_argument("--once", action="store_true", help="Claim at most one job, then exit")
    parser.add_argument("--prepare-deepseek", action="store_true", help="Open the dedicated DeepSeek window and report login readiness")
    parser.add_argument("--poll-seconds", type=float, default=3.0)
    parser.add_argument("--disable-deepseek", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
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
