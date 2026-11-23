"""Deterministic local prepared-feature computation and complete input proofs."""
from datetime import date, datetime


def _assert_read_succeeded(reason):
    if any(token in reason for token in ('OperationalError','Timeout','SQLAlchemyError',
            'REPLY_FAILED','REPLY_DEADLINE','VALIDATION_DEADLINE','SCHEMA_UNAVAILABLE','FormalAnalysis')):
        raise RuntimeError('历史事实读取失败：'+reason)


def build_window(days, coverage, *, cutoff):
    from server.common.analysis_pool_receipt import canonical_sha256
    proofs = {day: coverage[day]['proof'] for day in days}
    scopes = {day: coverage[day]['scope'] for day in days}
    return {'schema': 'probiga.strategy-daily-input-window.v1', 'sessions': days, 'session_count': len(days),
        'session_set_sha256': canonical_sha256(days), 'daily_truth_sha256': canonical_sha256([proofs[day] for day in days]),
        'daily_attested_row_count': sum(scopes[day]['stock_count'] for day in days),
        'catalog_batches_by_session': {day: scopes[day]['catalog'] for day in days},
        'daily_providers_by_session': {day: proofs[day].get('provider', 'qmt') for day in days},
        'daily_partition_roots': {day: canonical_sha256(proofs[day]) for day in days},
        'latest_daily_truth': proofs[days[-1]], 'decision_known_at': cutoff}


def feature_frame(dataset, plans, days, coverage, *, day, cutoff):
    import pandas as pd
    from server.trading_v3.daily_feature_math import normalize_daily_bars
    identity = plans['identity_rows']
    rows = []
    for target in days:
        rows.extend(dataset.get_tree(coverage[target]['tree']))
    frame = pd.DataFrame(rows)
    del rows
    frame['trade_date'] = pd.to_datetime(frame.trade_date).dt.date
    frame['short_name'] = [identity.get(code, {}).get('short_name', '') if str(target) == day else ''
        for code, target in zip(frame.stock_code, frame.trade_date)]
    frame.attrs['qmt_daily_input_window'] = build_window(days, coverage, cutoff=cutoff)
    return normalize_daily_bars(frame)


def _frame(dataset,state,plan,day):
    return feature_frame(dataset,plan,state['feature_windows'][day],state['bars'],day=day,cutoff=state['price_cutoff'])


def _features(state,dataset,day):
    from server.trading_v3.daily_feature_math import compute_price_features,compute_finance_features,finish_feature_universe
    inputs=state['day_inputs'][day]
    plan=dataset.get_tree(state['plans'][day])
    context=dataset.get_tree(inputs['ready']['context'])
    expected={'day':day,'decision_at':state['requested_at'],'price_cutoff':state['price_cutoff'],
        'identity_sha256':plan['identity']['sha256'],'finance_seal':plan['finance_seal']}
    if context['binding']!=expected:raise ValueError('READY feature context binding differs')
    frame=_frame(dataset,state,plan,day)
    work=compute_price_features(as_of=date.fromisoformat(day),
        dates=[date.fromisoformat(value) for value in state['feature_windows'][day]],frame=frame,
        context=context,context_cutoff_at=datetime.fromisoformat(state['requested_at']),limit=len(plan['eligible']),
        entry_scope_codes=plan['eligible'])
    selection=dataset.get_tree(inputs['ready']['selection'])
    work=compute_finance_features(work,selection['cutoff'],dataset.get_tree(inputs['ready']['finance']))
    events=dataset.get_tree(inputs['ready']['events'])
    codes=work['ranked_codes']
    evidence=dict(events['evidence'])
    for field in ('status_by_code','reason_by_code','coverage_by_code'):
        evidence[field]={code:evidence.get(field,{}).get(code) for code in codes}
    all_available=all(value=='AVAILABLE' for value in evidence['status_by_code'].values())
    evidence.update(status='AVAILABLE' if all_available else 'DATA_BLOCKED',
                    reason='' if all_available else 'PIT_EVENT_PARTIAL_OR_UNVERIFIED_COVERAGE')
    value=finish_feature_universe(work,{code:events['notices'][code] for code in codes if code in events['notices']},evidence)
    _assert_read_succeeded(str(value['market_features'].get('pit_common_cutoff_reason') or ''))
    for stock in value['stocks']:
        _assert_read_succeeded(str(stock.get('finance_pit_reason') or '')+';'+str(stock.get('event_pit_reason') or ''))
    return value
