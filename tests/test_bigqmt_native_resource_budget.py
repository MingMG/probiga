import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def producer():
    path = Path(__file__).resolve().parents[1] / 'integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py'
    spec = importlib.util.spec_from_file_location('budget_producer', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def healthy():
    gib = 1024 ** 3
    return dict(total_physical=32*gib, available_physical=8*gib,
                available_commit=12*gib, private_bytes=2*gib,
                working_set_bytes=gib, handles=4000)


@pytest.mark.parametrize('field,value', [
    ('available_physical', 512*1024**2),
    ('available_commit', 512*1024**2),
    ('private_bytes', 4*1024**3),
])
def test_pressure_prevents_native_call_but_keeps_quotes_available(field, value):
    p = producer()
    snapshot = healthy()
    snapshot[field] = value
    p._native_resource_snapshot = lambda: snapshot
    calls = []
    native = SimpleNamespace(get_market_data_ex_ori=lambda *a, **k: calls.append(1))
    context = p._QuoteCacheContext(native)
    p._quote_cache = {'000001.SZ': {'lastPrice': 10}}
    p._managed_codes = frozenset(p._quote_cache)
    with pytest.raises(RuntimeError, match='QMT_HISTORY_RESOURCE_PRESSURE'):
        context.get_market_data_ex_ori([], ['000001.SZ'])
    assert calls == []
    assert context.get_full_tick(['000001.SZ'])['000001.SZ']['lastPrice'] == 10
    assert p._native_resource_state['status'] == 'BLOCKED'
    # Recovery needs actual resource recovery, not a timer or process restart.
    p._native_resource_snapshot = healthy
    context.get_market_data_ex_ori([], ['000001.SZ'])
    assert calls == [1]


def test_sampling_failure_does_not_admit_native_work():
    p = producer()
    p._native_resource_snapshot = lambda: (_ for _ in ()).throw(OSError('probe failed'))
    calls = []
    with pytest.raises(RuntimeError, match='QMT_RESOURCE_SAMPLE_UNAVAILABLE'):
        p._guard_native_history(lambda: calls.append(1), 'download_history_data')()
    assert not calls


def test_download_loop_rechecks_budget_between_symbols():
    p = producer()
    samples = [healthy(), dict(healthy(), available_physical=0)]
    p._native_resource_snapshot = lambda: samples.pop(0)
    calls = []
    p.download_history_data = lambda *a, **k: calls.append(a[0])
    with pytest.raises(RuntimeError, match='QMT_HISTORY_RESOURCE_PRESSURE'):
        p._download_history(['000001.SZ', '600000.SH'], '1m', '20260911', '20260911')
    assert calls == ['000001.SZ']


def test_batch_downloader_is_chunked_and_checked_after_every_native_call():
    p = producer()
    calls = []
    samples = []
    for _ in range(6):
        samples.append(healthy())
    p._native_resource_snapshot = lambda: samples.pop(0)
    p.download_history_data2 = lambda *a, **k: calls.append(
        list(k.get('stock_list') or a[0])
    )
    symbols = ['%06d.SZ' % value for value in range(12)]

    p._download_history(symbols, '1m', '20260911', '20260911')

    assert calls == [symbols[:5], symbols[5:10], symbols[10:]]
    assert samples == []
    assert p._native_resource_state['phase'] == 'after'


def test_post_call_growth_is_blocked_before_another_native_call():
    p = producer()
    phases = []
    def check(method, phase='before'):
        phases.append((method, phase))
        if phase == 'after':
            raise p._NativeHistoryResourceBlocked('QMT_HISTORY_RESOURCE_PRESSURE')
    p._check_native_history_budget = check
    calls = []
    guarded = p._guard_native_history(lambda: calls.append(1), 'download_history_data2')

    with pytest.raises(RuntimeError, match='QMT_HISTORY_RESOURCE_PRESSURE'):
        guarded()

    assert calls == [1]
    assert phases == [
        ('download_history_data2', 'before'),
        ('download_history_data2', 'after'),
    ]


def test_handle_growth_is_a_native_history_budget_boundary():
    p = producer()
    snapshot = dict(healthy(), handles=20000)
    p._native_resource_snapshot = lambda: snapshot

    with pytest.raises(RuntimeError, match='QMT_HISTORY_RESOURCE_PRESSURE'):
        p._check_native_history_budget('get_market_data_ex_ori')

    assert p._native_resource_state['blocked_reasons'] == ['HANDLE_COUNT']


def test_private_memory_uses_32gib_rotation_and_35gib_hard_boundaries():
    p = producer()
    gib = 1024 ** 3
    snapshot = healthy()
    snapshot['private_bytes'] = (32 * gib) // 10
    p._native_resource_snapshot = lambda: snapshot
    with pytest.raises(RuntimeError, match='QMT_HISTORY_RESOURCE_PRESSURE'):
        p._check_native_history_budget('download_history_data2')
    assert p._native_resource_state['blocked_reasons'] == ['PRIVATE_BYTES_ROTATE']
    assert p._native_resource_state['private_limit_bytes'] == (32 * gib) // 10
    assert p._native_resource_state['private_hard_limit_bytes'] == (35 * gib) // 10
    assert p._native_resource_state['rotation_required'] is True

    snapshot['private_bytes'] = (35 * gib) // 10
    with pytest.raises(RuntimeError, match='QMT_HISTORY_RESOURCE_PRESSURE'):
        p._check_native_history_budget('download_history_data2', 'after')
    assert p._native_resource_state['blocked_reasons'] == ['PRIVATE_BYTES_HARD']
    assert p._native_resource_state['phase'] == 'after'


def test_capacity_response_is_typed_and_cli_does_not_request_login(monkeypatch, capsys):
    from integrations.bigqmt.spool import BigQmtResourceBlocked, _raise_response_error
    from biz.stock_market import sync_stock_market

    def blocked_main():
        _raise_response_error('minute', {'error_code': 'QMT_HISTORY_RESOURCE_PRESSURE',
                                       'error': 'native capacity exhausted'})

    with pytest.raises(BigQmtResourceBlocked):
        blocked_main()
    monkeypatch.setattr(sync_stock_market, 'main', blocked_main)
    assert sync_stock_market._cli() == 75
    output = capsys.readouterr().out
    assert 'DATA_BLOCKED' in output
    assert 'QMT_HISTORY_RESOURCE_PRESSURE' in output
