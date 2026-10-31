"""Reproduce the QMT embedded-interpreter file sandbox and prove the bridge
still reads through it.

Since 2026-10-23 QMT's embedded Python refuses every python-level ``open()``
that names a path it did not create itself, for reads, appends and overwrites
alike, with ``PermissionError("Foribdden FileIO")``.  ``os.listdir``,
``os.stat``, creation of new files and ``os.replace`` keep working.  Measured
inside the terminal on 2026-10-31: kernel32 ``CreateFileW`` in the same
process is *not* intercepted, which is what ``_open_bridge_file`` uses.

The sandbox is simulated here by refusing path-based ``open`` for existing
paths, so these tests fail on the pre-2026-10-31 bridge exactly the way
production failed: identity ``UNAVAILABLE`` and no request ever claimed.
"""
import builtins
import hashlib
import importlib.util
import io
import json
import os
import tempfile
from pathlib import Path

import pytest

BRIDGE_PATH = (Path(__file__).resolve().parents[1]
               / 'integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py')

pytestmark = pytest.mark.skipif(
    os.name != 'nt',
    reason='the QMT sandbox bypass is kernel32 CreateFileW on Windows',
)


def load_bridge():
    spec = importlib.util.spec_from_file_location('sandbox_bridge', BRIDGE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class sandbox(object):
    """Refuse python-level opens of paths that already exist."""

    def __enter__(self):
        self._open = builtins.open
        self._io_open = io.open
        self._os_open = os.open
        blocked = self._open

        def refuse(path, *args, **kwargs):
            if isinstance(path, (str, bytes)) and os.path.exists(path):
                raise PermissionError('Foribdden FileIO')
            return blocked(path, *args, **kwargs)

        def refuse_io(path, *args, **kwargs):
            if isinstance(path, (str, bytes)) and os.path.exists(path):
                raise PermissionError('Foribdden FileIO')
            return self._io_open(path, *args, **kwargs)

        def refuse_os(path, flags, *args, **kwargs):
            if isinstance(path, (str, bytes)) and os.path.exists(path):
                raise PermissionError("Forbidden to open '%s'" % path)
            return self._os_open(path, flags, *args, **kwargs)

        builtins.open = refuse
        io.open = refuse_io
        os.open = refuse_os
        return self

    def __exit__(self, *exc_info):
        builtins.open = self._open
        io.open = self._io_open
        os.open = self._os_open
        return False

    @staticmethod
    def write(path, payload):
        """Create a file the way the host does, outside the sandbox."""
        with builtins.open(path, 'w', encoding='utf-8') as handle:
            handle.write(payload)


def test_sandbox_blocks_python_level_open(tmp_path):
    target = tmp_path / 'existing.json'
    target.write_text('{}', encoding='utf-8')
    with sandbox():
        with pytest.raises(PermissionError, match='Foribdden FileIO'):
            open(str(target), 'rb')


def test_reads_survive_the_sandbox(tmp_path):
    bridge = load_bridge()
    payload = '{"hello": 1}'
    target = tmp_path / 'existing.json'
    sandbox.write(str(target), payload)
    with sandbox():
        assert bridge._read_json_file(str(target)) == {'hello': 1}
        assert bridge._file_sha256(str(target)) == hashlib.sha256(
            payload.encode('utf-8')).hexdigest()
        with bridge._open_bridge_file(str(target)) as handle:
            assert handle.read().decode('utf-8') == payload


def test_fault_log_appends_to_an_existing_file(tmp_path):
    bridge = load_bridge()
    target = tmp_path / 'native_fault.log'
    target.write_bytes(b'previous\n')
    with sandbox():
        with bridge._open_bridge_file(str(target), append=True) as handle:
            handle.write(b'appended\n')
        with bridge._open_bridge_file(str(target)) as handle:
            assert handle.read() == b'previous\nappended\n'


def test_missing_file_still_raises_filenotfound(tmp_path):
    bridge = load_bridge()
    with sandbox():
        with pytest.raises(FileNotFoundError):
            bridge._open_bridge_file(str(tmp_path / 'absent.json'))


def test_claimed_request_is_readable_under_the_sandbox(tmp_path):
    bridge = load_bridge()
    requests_root = tmp_path / 'requests'
    inflight_root = tmp_path / 'inflight'
    requests_root.mkdir()
    inflight_root.mkdir()
    sandbox.write(str(requests_root / 'req-1.json'),
                  json.dumps({'request_id': 'req-1', 'action': 'capabilities'}))
    pending = str(requests_root / 'req-1.json')
    claimed = str(inflight_root / 'req-1.json')
    with sandbox():
        os.replace(pending, claimed)
        assert bridge._read_json_file(claimed)['action'] == 'capabilities'


def test_identity_binds_under_the_sandbox(tmp_path, monkeypatch):
    bridge = load_bridge()
    qmt_home = tmp_path / 'qmt'
    python_dir = qmt_home / 'python'
    bridge_root = qmt_home / 'userdata' / 'probiga_bridge'
    python_dir.mkdir(parents=True)
    bridge_root.mkdir(parents=True)
    build_sha, git_blob, source_sha = 'a' * 40, 'b' * 40, 'c' * 64
    identity_sha = hashlib.sha256(
        (bridge.STRATEGY_IDENTITY_PROTOCOL + '\n'
         + build_sha + '\n' + git_blob + '\n' + source_sha).encode('ascii')
    ).hexdigest()
    sandbox.write(
        str(python_dir / bridge.STRATEGY_RELEASE_MANIFEST_NAME),
        json.dumps({
            'schema': bridge.STRATEGY_RELEASE_MANIFEST_SCHEMA,
            'strategy_release_protocol': bridge.STRATEGY_RELEASE_PROTOCOL,
            'strategy_identity_protocol': bridge.STRATEGY_IDENTITY_PROTOCOL,
            'strategy_build_sha': build_sha,
            'strategy_git_blob': git_blob,
            'strategy_source_sha256': source_sha,
            'strategy_loaded_identity_sha256': identity_sha,
            'strategy_artifact_sha256': 'd' * 64,
        }),
    )
    monkeypatch.setattr(bridge, 'EMBEDDED_STRATEGY_BUILD_SHA', build_sha)
    monkeypatch.setattr(bridge, 'EMBEDDED_STRATEGY_GIT_BLOB', git_blob)
    monkeypatch.setattr(bridge, 'EMBEDDED_STRATEGY_SOURCE_SHA256', source_sha)
    monkeypatch.setattr(bridge, 'EMBEDDED_STRATEGY_IDENTITY_SHA256',
                        identity_sha)
    monkeypatch.setattr(bridge, '_find_bridge_root', lambda: str(bridge_root))
    # QMT's model namespace has no __file__.
    monkeypatch.delattr(bridge, '__file__', raising=False)
    with sandbox():
        frozen = bridge._freeze_loaded_strategy_identity()
    assert frozen['strategy_identity_status'] == 'BOUND', frozen
    assert frozen['strategy_build_sha'] == build_sha
    assert frozen['strategy_loaded_identity_sha256'] == identity_sha
