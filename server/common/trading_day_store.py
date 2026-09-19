"""Permanent personal trading journals in the server's external runtime root.

Only user-authored observations live here. This store never changes positions,
strategy evidence or order state. Each account/day has one atomically replaced
document and an OS lock, so multiple API workers share the same revision gate.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import time
from typing import Any


SCHEMA = "probiga.trading-day-journal.v1"
CHINA = timezone(timedelta(hours=8))
MAX_BYTES = 4 * 1024 * 1024
PLAN_LIMITS = {
    "stock_code": 6, "stock_name": 80, "theme": 160,
    "reason": 1200, "trigger": 1200, "invalidation": 1200,
    "source_as_of": 40, "source_run_uid": 160, "note": 2000,
}
PLAN_STATUSES = frozenset({"WATCHING", "WAITING", "PAUSED", "REVIEWED"})
ORIGINAL_FIELDS = tuple(key for key in PLAN_LIMITS if key not in {"stock_code", "note"})


class JournalStoreError(RuntimeError):
    """Unreadable or unsafe storage must not become an empty journal."""


class JournalConflict(RuntimeError):
    def __init__(self, revision: int):
        super().__init__("journal revision changed")
        self.revision = revision


def journal_runtime_root() -> Path:
    """Use the already provisioned, release-independent flat-file directory."""
    configured = os.environ.get("PROBIGA_JOB_LOG_ROOT", "").strip()
    if configured:
        return Path(configured)
    if os.name == "nt":
        program_data = os.environ.get("PROGRAMDATA", "").strip()
        if not program_data:
            raise JournalStoreError("external journal runtime root is not configured")
        return Path(program_data) / "ProBigA" / "scheduler"
    return Path("/var/lib/probiga/jobs")


def _now() -> datetime:
    return datetime.now(CHINA)


def _identity(user_id: int, trade_date: str) -> tuple[int, str]:
    if type(user_id) is not int or user_id <= 0 or user_id > 2**63 - 1:
        raise ValueError("invalid account identity")
    if not isinstance(trade_date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", trade_date):
        raise ValueError("trade_date must be YYYY-MM-DD")
    target = date.fromisoformat(trade_date)
    if not 2000 <= target.year <= 2100:
        raise ValueError("trade_date must be between 2000 and 2100")
    return user_id, target.isoformat()


def _text(value: Any, limit: int, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be text")
    value = value.strip()
    if len(value) > limit or any(ord(char) < 32 and char not in "\n\r\t" for char in value):
        raise ValueError(f"{field} contains unsupported text or exceeds its limit")
    return value


def _source_time(value: str, trade_date: str, now: datetime) -> None:
    if not value:
        return
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            observed = datetime.combine(date.fromisoformat(value), datetime.min.time(), CHINA)
        else:
            observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=CHINA)
            observed = observed.astimezone(CHINA)
    except ValueError as exc:
        raise ValueError("source_as_of must be an ISO date or timestamp") from exc
    if observed.date() > date.fromisoformat(trade_date) or observed > now:
        raise ValueError("source_as_of cannot contain future evidence")


def validate_journal_input(payload: dict[str, Any], trade_date: str, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or _now()
    if not isinstance(payload, dict) or set(payload) != {"revision", "plans", "review"}:
        raise ValueError("journal requires revision, plans and review")
    revision = payload["revision"]
    if type(revision) is not int or revision < 0 or revision > 2**53 - 1:
        raise ValueError("invalid revision")
    rows = payload["plans"]
    if not isinstance(rows, list) or len(rows) > 100:
        raise ValueError("a journal can contain at most 100 plans")
    plans, codes = [], set()
    for raw in rows:
        if not isinstance(raw, dict) or set(raw) - (set(PLAN_LIMITS) | {"status"}):
            raise ValueError("unknown plan field")
        item = {key: _text(raw.get(key, ""), limit, key) for key, limit in PLAN_LIMITS.items()}
        code = item["stock_code"]
        if not re.fullmatch(r"[0-9]{6}", code) or code == "000000":
            raise ValueError("stock_code must be a six-digit security code")
        if code in codes:
            raise ValueError("a security can appear only once per journal")
        codes.add(code)
        item["status"] = raw.get("status", "WATCHING")
        if not isinstance(item["status"], str) or item["status"] not in PLAN_STATUSES:
            raise ValueError("unsupported observation status")
        _source_time(item["source_as_of"], trade_date, now)
        plans.append(item)
    review = payload["review"]
    if not isinstance(review, dict) or set(review) != {"text"}:
        raise ValueError("review requires only text")
    return {"revision": revision, "plans": plans,
            "review": {"text": _text(review["text"], 12000, "review.text")}}


def _safe_components(path: Path) -> None:
    for candidate in (path, *path.parents):
        if os.path.lexists(candidate):
            info = candidate.lstat()
            if stat.S_ISLNK(info.st_mode) or bool(getattr(info, "st_file_attributes", 0) & 0x400):
                raise JournalStoreError("journal runtime path contains a link")


def _safe_regular(info: os.stat_result) -> None:
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise JournalStoreError("journal state must be a single-link regular file")
    if os.name != "nt" and (info.st_uid != os.geteuid() or info.st_gid != os.getegid() or stat.S_IMODE(info.st_mode) != 0o600):
        raise JournalStoreError("journal state ownership or mode is unsafe")


def _stored_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("journal timestamp must be text")
    observed = datetime.fromisoformat(value)
    if observed.tzinfo is None:
        raise ValueError("journal timestamp must have a timezone")
    return observed


def _validate_stored_document(raw: Any, user_id: int, trade_date: str) -> None:
    """A damaged document must never be accepted as valid observation evidence."""
    if (not isinstance(raw, dict)
            or set(raw) != {"schema", "user_id", "trade_date", "revision", "plans", "review", "updated_at"}
            or raw["schema"] != SCHEMA or type(raw["user_id"]) is not int
            or raw["user_id"] != user_id or raw["trade_date"] != trade_date
            or type(raw["revision"]) is not int or not 1 <= raw["revision"] <= 2**53 - 1
            or not isinstance(raw["plans"], list) or not isinstance(raw["review"], dict)
            or set(raw["review"]) != {"text", "updated_at"}):
        raise ValueError("journal state identity or structure differs")
    updated = _stored_timestamp(raw["updated_at"])
    writable_plans = []
    for plan in raw["plans"]:
        if (not isinstance(plan, dict)
                or set(plan) != set(PLAN_LIMITS) | {"status", "created_at", "updated_at", "original"}
                or not isinstance(plan["original"], dict)
                or set(plan["original"]) != set(ORIGINAL_FIELDS)):
            raise ValueError("journal plan structure differs")
        if not _stored_timestamp(plan["created_at"]) <= _stored_timestamp(plan["updated_at"]) <= updated:
            raise ValueError("journal plan timestamps differ")
        item = {key: plan[key] for key in (*PLAN_LIMITS, "status")}
        writable_plans.append(item)
        original = {key: _text(plan["original"][key], PLAN_LIMITS[key], key) for key in ORIGINAL_FIELDS}
        _source_time(original["source_as_of"], trade_date, _now())
    validate_journal_input({"revision": raw["revision"], "plans": writable_plans,
                            "review": {"text": raw["review"]["text"]}}, trade_date)
    review_updated = raw["review"]["updated_at"]
    if review_updated is None:
        if raw["review"]["text"]:
            raise ValueError("journal review has no timestamp")
    elif _stored_timestamp(review_updated) > updated:
        raise ValueError("journal review timestamp differs")


class TradingDayStore:
    def __init__(self, root: Path, *, lock_timeout: float = 5.0):
        self.root = Path(root)
        if not self.root.is_absolute():
            raise JournalStoreError("journal runtime root must be absolute")
        _safe_components(self.root)
        code_root = Path(__file__).resolve().parents[2]
        if self.root.resolve().is_relative_to(code_root):
            raise JournalStoreError("journal runtime root must be outside the code tree")
        # Provisioning belongs to the runtime owner, not a write to the release.
        # Test/development callers can create a fresh leaf under an existing root.
        self.root.mkdir(mode=0o700, exist_ok=True)
        _safe_components(self.root)
        info = self.root.stat()
        if not stat.S_ISDIR(info.st_mode):
            raise JournalStoreError("journal runtime root is not a directory")
        if os.name != "nt" and (info.st_uid != os.geteuid() or info.st_gid != os.getegid() or stat.S_IMODE(info.st_mode) != 0o700):
            raise JournalStoreError("journal runtime directory ownership or mode is unsafe")
        self.lock_timeout = lock_timeout

    def _path(self, user_id: int, trade_date: str, suffix: str = ".json") -> Path:
        user_id, trade_date = _identity(user_id, trade_date)
        return self.root / f"trading-day-user-{user_id}-{trade_date}{suffix}"

    @contextmanager
    def _locked(self, user_id: int, trade_date: str):
        _safe_components(self.root)
        lock_path = self._path(user_id, trade_date, ".lock")
        _safe_components(lock_path)
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        acquired = False
        try:
            _safe_regular(os.fstat(descriptor))
            if os.fstat(descriptor).st_size == 0:
                os.write(descriptor, b"0")
            until = time.monotonic() + self.lock_timeout
            while True:
                try:
                    if os.name == "nt":
                        import msvcrt
                        os.lseek(descriptor, 0, os.SEEK_SET)
                        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except OSError as exc:
                    if time.monotonic() >= until:
                        raise JournalStoreError("journal is busy; retry shortly") from exc
                    time.sleep(0.025)
            yield
        finally:
            if acquired:
                if os.name == "nt":
                    import msvcrt
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @staticmethod
    def _empty(trade_date: str) -> dict[str, Any]:
        return {"trade_date": trade_date, "revision": 0, "plans": [],
                "review": {"text": "", "updated_at": None}, "updated_at": None}

    def _read(self, user_id: int, trade_date: str) -> dict[str, Any]:
        path = self._path(user_id, trade_date)
        _safe_components(path)
        try:
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except FileNotFoundError:
            return self._empty(trade_date)
        try:
            info = os.fstat(descriptor)
            _safe_regular(info)
            if info.st_size > MAX_BYTES:
                raise JournalStoreError("journal state exceeds the storage limit")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                raw = json.load(stream)
            _validate_stored_document(raw, user_id, trade_date)
            return {key: raw[key] for key in self._empty(trade_date)}
        except (ValueError, KeyError, UnicodeError, TypeError) as exc:
            raise JournalStoreError("journal state is unreadable") from exc
        finally:
            os.close(descriptor)

    def read(self, user_id: int, trade_date: str) -> dict[str, Any]:
        _identity(user_id, trade_date)
        with self._locked(user_id, trade_date):
            return self._read(user_id, trade_date)

    def save(self, user_id: int, trade_date: str, payload: dict[str, Any]) -> dict[str, Any]:
        _identity(user_id, trade_date)
        validated = validate_journal_input(payload, trade_date)
        with self._locked(user_id, trade_date):
            current = self._read(user_id, trade_date)
            if validated["revision"] != current["revision"]:
                raise JournalConflict(current["revision"])
            now = _now()
            target_day = date.fromisoformat(trade_date)
            if target_day > now.date():
                raise ValueError("future journals cannot be edited")
            old_plans = {item["stock_code"]: item for item in current["plans"]}
            if target_day < now.date():
                if {item["stock_code"] for item in validated["plans"]} != set(old_plans):
                    raise ValueError("historical observation plans cannot be added or removed")
                for item in validated["plans"]:
                    old = old_plans[item["stock_code"]]
                    if any(item[key] != old[key] for key in PLAN_LIMITS if key != "note"):
                        raise ValueError("historical observation conditions cannot be changed; update status or notes instead")
            timestamp = now.isoformat(timespec="microseconds")
            plans = []
            for item in validated["plans"]:
                old = old_plans.get(item["stock_code"])
                unchanged = old is not None and all(old.get(key) == value for key, value in item.items())
                plans.append({**item,
                    "created_at": old["created_at"] if old else timestamp,
                    "updated_at": old["updated_at"] if unchanged else timestamp,
                    "original": deepcopy(old["original"]) if old else {key: item[key] for key in ORIGINAL_FIELDS}})
            review_text = validated["review"]["text"]
            document = {"trade_date": trade_date, "revision": current["revision"] + 1,
                "plans": plans, "review": {"text": review_text,
                    "updated_at": current["review"]["updated_at"] if review_text == current["review"]["text"] else timestamp},
                "updated_at": timestamp}
            stored = {"schema": SCHEMA, "user_id": user_id, **document}
            encoded = json.dumps(stored, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
            if len(encoded) > MAX_BYTES:
                raise ValueError("journal exceeds the storage limit")
            path = self._path(user_id, trade_date)
            _safe_components(path)
            if path.exists():
                _safe_regular(path.stat())
            descriptor, temporary = tempfile.mkstemp(prefix=path.stem + "-", suffix=".tmp", dir=self.root)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                _safe_regular(Path(temporary).stat())
                os.replace(temporary, path)
                if os.name != "nt":
                    directory = os.open(self.root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
            return document
