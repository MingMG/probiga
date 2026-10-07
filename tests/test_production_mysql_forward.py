from __future__ import annotations

from types import SimpleNamespace

from tools import run_production_mysql_forward as forward


class _Socket:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _Channel:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_bridge_opens_only_the_fixed_remote_database_endpoint(monkeypatch):
    local = _Socket()
    channel = _Channel()
    calls = []

    class _Transport:
        def open_channel(self, kind, destination, source):
            calls.append((kind, destination, source))
            return channel

    monkeypatch.setattr(
        forward,
        "_relay",
        lambda observed_local, observed_channel: calls.append(
            ("relay", observed_local, observed_channel)
        ),
    )

    forward._bridge_connection(
        _Transport(), local, ("127.0.0.1", 41000), "127.0.0.1", 13306
    )

    assert calls == [
        ("direct-tcpip", ("127.0.0.1", 13306), ("127.0.0.1", 41000)),
        ("relay", local, channel),
    ]


def test_bridge_closes_local_socket_when_remote_channel_fails():
    local = _Socket()

    class _Transport:
        def open_channel(self, *_args):
            raise OSError("unavailable")

    forward._bridge_connection(
        _Transport(), local, ("127.0.0.1", 41000), "127.0.0.1", 13306
    )

    assert local.closed is True


def test_forward_uses_pinned_production_ssh_policy(monkeypatch):
    events = []

    class _Listener:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def accept(self):
            raise AssertionError("inactive transport must reconnect before accept")

    class _Client:
        def connect(self, **kwargs):
            events.append(("connect", kwargs))

        def get_transport(self):
            return SimpleNamespace(
                set_keepalive=lambda seconds: events.append(("keepalive", seconds)),
                is_active=lambda: False,
            )

        def close(self):
            events.append(("close", None))

    monkeypatch.setattr(forward, "_listener", lambda *_args: _Listener())
    monkeypatch.setattr(forward, "production_ssh_client", lambda *_args: _Client())
    monkeypatch.setattr(
        forward,
        "production_ssh_connect_kwargs",
        lambda **kwargs: {"pinned": True, **kwargs},
    )
    monkeypatch.setattr(forward.time, "sleep", lambda _seconds: (_ for _ in ()).throw(StopIteration))

    try:
        forward._run_forward(
            ssh_host="prod.internal",
            ssh_port=22,
            ssh_user="deploy",
            remote_host_name="127.0.0.1",
            remote_port=13306,
            local_host="127.0.0.1",
            local_port=3306,
            connect_timeout=20,
            keepalive_seconds=10,
            retry_min_seconds=1,
            retry_max_seconds=2,
        )
    except StopIteration:
        pass

    assert events[0][0] == "connect"
    assert events[0][1]["pinned"] is True
    assert "password" not in events[0][1]
    assert ("keepalive", 10) in events
    assert events[-1] == ("close", None)
