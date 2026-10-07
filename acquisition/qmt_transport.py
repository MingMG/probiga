"""Local file handoff for the new standalone full-QMT model, not an RPC queue."""
import os
import time

from .qmt_model import (
    MAX_REQUEST_BYTES, MAX_RESULT_BYTES, _ordinary, parse_instant,
    publish_json, read_json, trusted_root, validate_id, validate_request,
)


_WINDOWS_FILESYSTEM = os.name == "nt"
_WINDOWS_RETRY_DELAYS = (0.05, 0.1, 0.2, 0.4, 0.8, 1.6)


def _file_operation(operation):
    """Bound Windows sharing/access races; permanent denial still fails closed.

    32/33 identify sharing/byte-range locks. 5 can also mean a real ACL or
    read-only denial, so it is never treated as success or repaired by changing
    permissions. Only the same file operation is retried, for at most 3.15s of
    waiting, then the original OS error propagates with recoverable files intact.
    """
    for attempt in range(len(_WINDOWS_RETRY_DELAYS) + 1):
        try:
            return operation()
        except OSError as exc:
            if (
                not _WINDOWS_FILESYSTEM
                or getattr(exc, "winerror", None) not in (5, 32, 33)
                or attempt == len(_WINDOWS_RETRY_DELAYS)
            ):
                raise
            time.sleep(_WINDOWS_RETRY_DELAYS[attempt])


def _read_json(path, limit):
    return _file_operation(lambda: read_json(path, limit))


def _unlink_ordinary(path):
    def remove():
        # Validate again after each wait; a changed path is not trusted merely
        # because it was ordinary before a sharing conflict.
        _ordinary(path)
        os.unlink(path)
    return _file_operation(remove)


def validate_result(result, request=None):
    """Validate one complete native result without requiring shared storage."""
    if not isinstance(result, dict):
        raise ValueError("result must be an object")
    embedded = result.get("request")
    validate_request(embedded)
    if request is not None and embedded != request:
        raise ValueError("result does not match the immutable prepared request")
    parse_instant(result.get("received_at"))
    if not isinstance(result.get("source_method"), str) or not result["source_method"]:
        raise ValueError("result must identify its native method")
    outcomes = result.get("outcomes")
    if not isinstance(outcomes, dict) or set(outcomes) != set(embedded["codes"]):
        raise ValueError("result must contain exactly one outcome per requested security")
    for outcome in outcomes.values():
        if not isinstance(outcome, dict) or outcome.get("status") not in ("data", "no_data", "error"):
            raise ValueError("invalid native outcome")
        rows = outcome.get("rows")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError("raw rows must be a list of objects")
        if outcome["status"] == "data" and not rows:
            raise ValueError("data outcome cannot be empty")
        if outcome["status"] != "data" and (rows or not outcome.get("reason")):
            raise ValueError("empty/error outcomes require a reason and no rows")
    return result


class QmtTransport:
    def __init__(self, root):
        self.root = trusted_root(root, create=True)
        self.processed = os.path.join(self.root, "processed")
        os.makedirs(self.processed, exist_ok=True)
        trusted_root(self.processed)

    def _path(self, request_id, suffix):
        return os.path.join(self.root, validate_id(request_id) + suffix)

    def prepare(self, request):
        validate_request(request)
        path = self._path(request["request_id"], ".prepared.json")
        if os.path.lexists(os.path.join(self.processed, request["request_id"])):
            raise ValueError("request_id was already archived")
        previous = _read_json(path, MAX_REQUEST_BYTES)
        if previous is not None:
            if previous != request:
                raise ValueError("request_id already belongs to another immutable request")
            return
        try:
            _file_operation(lambda: publish_json(path, request, MAX_REQUEST_BYTES, immutable=True))
        except FileExistsError:
            if _read_json(path, MAX_REQUEST_BYTES) != request:
                raise ValueError("concurrent immutable request differs")

    def activate(self, request_id):
        prepared = self._path(request_id, ".prepared.json")
        request = _read_json(prepared, MAX_REQUEST_BYTES)
        if request is None:
            raise FileNotFoundError("prepare request and persist running units before activation")
        validate_request(request)
        if request["request_id"] != request_id:
            raise ValueError("prepared request_id differs")
        if os.path.lexists(os.path.join(self.processed, request_id)):
            raise ValueError("archived request cannot be activated")
        active = os.path.join(self.root, "active.json")
        try:
            _file_operation(lambda: os.link(prepared, active))
        except FileExistsError:
            if _read_json(active, MAX_REQUEST_BYTES) != request:
                raise RuntimeError("another QMT request is active; timeout is not cancellation")

    def read_request(self, request_id):
        """Read the immutable prepared identity without dispatching native work."""
        request = self._read_retained(request_id, ".prepared.json", MAX_REQUEST_BYTES)
        if request is not None:
            validate_request(request)
            if request["request_id"] != request_id:
                raise ValueError("prepared request_id differs")
        return request

    def read_result(self, request_id):
        result = self._read_retained(request_id, ".ready.json", MAX_RESULT_BYTES)
        if result is None:
            return None
        request = self.read_request(request_id)
        if request is None:
            raise ValueError("result does not match the immutable prepared request")
        return validate_result(result, request)

    def _read_retained(self, request_id, suffix, limit):
        raw = _read_json(self._path(request_id, suffix), limit)
        if raw is not None:
            return raw
        directory = os.path.join(self.processed, request_id)
        if not os.path.exists(directory):
            return None
        trusted_root(directory)
        return _read_json(os.path.join(directory, request_id + suffix), limit)

    def archive(self, request_id):
        """Caller must first commit ALL data/no_data/error unit states to DB."""
        validate_id(request_id)
        active_path = os.path.join(self.root, "active.json")
        active = _read_json(active_path, MAX_REQUEST_BYTES)
        if active is not None and active.get("request_id") != request_id:
            raise RuntimeError("cannot archive another active request")
        if self.read_result(request_id) is None:
            raise FileNotFoundError("cannot archive before a complete result exists")
        destination = os.path.join(self.processed, request_id)
        os.makedirs(destination, exist_ok=True)
        trusted_root(destination)
        # The ready result stays recoverable throughout a partially completed
        # archive; active is released last, after both retained files exist.
        for suffix, limit in ((".prepared.json", MAX_REQUEST_BYTES), (".ready.json", MAX_RESULT_BYTES)):
            source = self._path(request_id, suffix)
            target = os.path.join(destination, request_id + suffix)
            raw = _read_json(source, limit)
            retained = _read_json(target, limit)
            if raw is None and retained is None:
                raise FileNotFoundError("cannot archive an incomplete request")
            if raw is not None:
                if retained is not None and retained != raw:
                    raise ValueError("archive content differs")
                if retained is None:
                    _file_operation(lambda: os.link(source, target))
        for suffix in (".prepared.json", ".ready.json"):
            source = self._path(request_id, suffix)
            if os.path.lexists(source):
                _unlink_ordinary(source)
        if active is not None:
            # Only this request may own active; no other caller can activate
            # while this file exists. Do not remove another request's plan.
            def release_active():
                # Recheck ownership on every retry, never release a replacement
                # request after an access/sharing failure.
                if read_json(active_path, MAX_REQUEST_BYTES) != active:
                    raise RuntimeError("active request changed during archive")
                _ordinary(active_path)
                os.unlink(active_path)
            _file_operation(release_active)

    def recover(self):
        """Inventory only. No expiry cleanup, replay, model control or DB claims."""
        active = _read_json(os.path.join(self.root, "active.json"), MAX_REQUEST_BYTES)
        prepared, ready, temporary = [], [], []
        for name in sorted(os.listdir(self.root)):
            if name.endswith(".prepared.json"):
                prepared.append(validate_id(name[:-14]))
            elif name.endswith(".ready.json"):
                ready.append(validate_id(name[:-11]))
            elif name.endswith(".tmp"):
                temporary.append(name)
        return {"active": active, "prepared": prepared, "ready": ready,
                "temporary": temporary, "processed": sorted(os.listdir(self.processed)),
                "heartbeat": self.heartbeat()}

    def heartbeat(self):
        return _read_json(os.path.join(self.root, "heartbeat.json"), MAX_REQUEST_BYTES) or {}

    def wait_result(self, request_id, timeout=180, poll_seconds=0.1):
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            result = self.read_result(request_id)
            if result is not None:
                return result
            if time.monotonic() >= deadline:
                raise TimeoutError("QMT wait ended; active request is retained and native execution is not cancelled")
            time.sleep(max(0.01, min(float(poll_seconds), deadline - time.monotonic())))
