"""Scheduled history entrypoints must enforce the production TLS policy."""
from types import SimpleNamespace
from unittest.mock import Mock, call
import sys

import pytest

from server.common import engine_factory
from tools import backfill_guojin_qmt_local_history as backfill
from tools import nightly_guojin_qmt_reconciliation as nightly
from tools import repair_guojin_qmt_gaps as gap_plan
from tools import run_guojin_qmt_full_market_history_2024 as bulk


@pytest.fixture(params=[bulk, backfill, nightly, gap_plan], ids=['bulk', 'backfill', 'nightly', 'gap-plan'])
def entrypoint(request, monkeypatch):
    module = request.param
    monkeypatch.setattr(module, 'get_mysql_url', lambda **_: 'mysql+pymysql://test:test@127.0.0.1/probiga')
    if module in (bulk, backfill):
        return module._source_engine
    monkeypatch.setattr(sys, 'argv', [module.__file__, '--json'])
    if module is nightly:
        monkeypatch.setattr(module, 'run_nightly_reconciliation', lambda *_args, **_kwargs: SimpleNamespace(status='SUCCESS'))
    else:
        monkeypatch.setattr(module, 'plan_gap_repairs', lambda *_args, **_kwargs: SimpleNamespace())
    monkeypatch.setattr(module, 'result_dict', lambda _: {})
    return module.main


def test_scheduled_source_connection_uses_verified_ca(entrypoint, monkeypatch, tmp_path):
    ca = tmp_path / 'runtime-ca.pem'
    ca.write_text('test CA metadata; driver is mocked', encoding='utf8')
    monkeypatch.setattr(engine_factory, '_get_runtime_tls_config', lambda: {'required': True, 'ssl_ca': str(ca)})
    create = Mock(return_value=object())
    listen = Mock()
    monkeypatch.setattr(engine_factory, 'create_engine', create)
    monkeypatch.setattr(engine_factory.event, 'listen', listen)

    entrypoint()

    assert create.call_count == 1
    assert create.call_args.kwargs['connect_args'] == {'ssl_ca': str(ca.resolve()), 'ssl_verify_cert': True}
    assert listen.call_args_list == [
        call(create.return_value, 'connect', engine_factory._verify_runtime_mysql_tls),
        call(create.return_value, 'connect', engine_factory._configure_runtime_mysql_session),
    ]


def test_scheduled_source_connection_cannot_skip_missing_required_ca(entrypoint, monkeypatch):
    monkeypatch.setattr(engine_factory, '_get_runtime_tls_config', lambda: {'required': True, 'ssl_ca': None})
    create = Mock()
    monkeypatch.setattr(engine_factory, 'create_engine', create)

    with pytest.raises(RuntimeError, match='requires MYSQL_SSL_CA'):
        entrypoint()

    create.assert_not_called()


def test_mysql_runtime_session_uses_canonical_collation(monkeypatch):
    monkeypatch.setattr(
        engine_factory,
        '_get_runtime_tls_config',
        lambda: {'required': False, 'ssl_ca': None},
    )
    create = Mock(return_value=object())
    listen = Mock()
    monkeypatch.setattr(engine_factory, 'create_engine', create)
    monkeypatch.setattr(engine_factory.event, 'listen', listen)

    engine = engine_factory.create_pooled_engine(
        'mysql+pymysql://test:test@127.0.0.1/probiga'
    )

    assert engine is create.return_value
    listen.assert_called_once_with(
        create.return_value,
        'connect',
        engine_factory._configure_runtime_mysql_session,
    )

    connection = Mock()
    cursor = connection.cursor.return_value
    engine_factory._configure_runtime_mysql_session(connection, object())
    cursor.execute.assert_called_once_with(
        'SET NAMES utf8mb4 COLLATE utf8mb4_general_ci'
    )
    cursor.close.assert_called_once_with()
