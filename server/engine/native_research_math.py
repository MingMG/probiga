"""Deterministic native research over prepared inputs, without acquisition."""
from datetime import timedelta
import json
import logging
from time import perf_counter

from server.engine.native_signal_math import _v3_signal
from server.trading_v3.engine import TradingV3Engine
from server.trading_v3.regime import classify_regime_probabilities

timing = logging.getLogger("server.native_replay.timing")


def evaluate_native_signals(dataset, strategies, *, config, day, signal_clock, allocation_policy,identities):
    started = perf_counter()
    market = classify_regime_probabilities(dataset['market_features'], config=config)
    if market.quality_status != 'PASS':
        return {'status': 'BLOCKED', 'reason': '市场状态输入不足：' + ';'.join(market.evidence),
                'signals': {}, 'data_snapshot_hash': dataset['data_snapshot_hash']}
    if any(item['evaluator_config']['model_version'] != config['strategy_version'] for item in strategies):
        raise ValueError('native frozen V3 model identity differs')
    native = TradingV3Engine(calibrations={}, config=config)
    selected = {item['strategy_key']: [] for item in strategies}
    scored = {key: 0 for key in selected}
    missing = {key: 0 for key in selected}
    missing_details = {key: {} for key in selected}
    for stock in dataset['stocks']:
        member=identities.get(stock['stock_code'])
        if member is None or member.get('short_name')!=stock['stock_name']:
            raise ValueError('native signal historical identity differs')
        forecasts, _ = native.evaluate_stock_with_theme_signals(
            stock['stock_code'], stock['stock_name'], stock,
            dataset['feature_time'], dataset['feature_time'] + timedelta(days=30))
        by_key = {item.strategy_key: item for item in forecasts}
        for strategy in strategies:
            key = strategy['strategy_key']
            forecast = by_key.get(key)
            if forecast is None or forecast.status == 'INSUFFICIENT_DATA':
                if not (key == 'event_drift' and stock.get('event_authoritative_empty')):
                    missing[key] += 1
                    missing_details[key][stock['stock_code']] = list(forecast.reasons) if forecast else ['BUILDER_NOT_FOUND']
                continue
            scored[key] += 1
            candidate = _v3_signal(strategy, {
                **forecast.as_dict(), 'forecast_status': forecast.status,
                'features_json': json.dumps(forecast.features, default=str), 'short_name': forecast.stock_name,
            }, day)
            weights = strategy['evaluator_config']['shadow_trial_routing']['regime_weights']
            multiplier = sum(market.probabilities[key] * weights[key] for key in weights)
            if candidate is None or multiplier <= 0 or market.risk_asset_cap <= 0:
                continue
            selected[key].append({
                **candidate, 'industry': member.get('industry_name'), 'signal_known_at': str(signal_clock),
                'source_snapshot_hash': dataset['data_snapshot_hash'], 'historical_production_decision': False,
                'allocation_multiplier': multiplier if allocation_policy == 'ADAPTIVE_REGIME' else 1.0,
                'risk_asset_cap': market.risk_asset_cap if allocation_policy == 'ADAPTIVE_REGIME' else 1.0,
            })
    timing.info("signal-score day=%s family=v3 stocks=%d elapsed=%.3fs",
                day, len(dataset['stocks']), perf_counter() - started)
    started = perf_counter()
    for rows in selected.values():
        rows.sort(key=lambda row: (-row['effective_score'], row['stock_code']))
        for rank, row in enumerate(rows, 1):
            row['rank'] = rank
    timing.info("candidate-sort day=%s family=v3 candidates=%d elapsed=%.3fs",
                day, sum(map(len, selected.values())), perf_counter() - started)
    return {
        'status': 'COMPLETE', 'signals': selected, 'scored_stocks': scored,
        'missing_feature_stocks': missing, 'missing_features_by_stock': missing_details,
        'data_snapshot_hash': dataset['data_snapshot_hash'],
        'evaluation_security_sets': {
            key: sorted(stock['stock_code'] for stock in dataset['stocks']
                        if stock['stock_code'] not in missing_details[key]) for key in selected
        },
        'interpretation': 'CURRENT_FROZEN_RULES_WITHOUT_FUTURE_CALIBRATIONS',
    }


def evaluate_manifest_signals(prepared, strategies, *, day):
    from server.engine.native_signal_math import _manifest_signal
    identities,market,uid=prepared['identities'],prepared['market'],prepared['source_run_uid']
    selected={};evaluated={};missing={}
    for strategy in (s for s in strategies if s['source_kind']=='immutable_manifest'):
        started = perf_counter()
        key=strategy['strategy_key'];picked=[]
        evaluated[key]=[];missing[key]=0
        for row in prepared['scored_rows']:
            if row.get(strategy['evaluator_config']['score_field']) is not None and row.get('model_version')!=strategy['evaluator_config']['model_version']:
                raise ValueError('冻结评分模型版本与原生策略不一致')
            code=str(row['stock_code']).zfill(6)
            member=identities.get(code,{})
            if row.get(strategy['evaluator_config']['score_field']) is None or not member.get('short_name') or not member.get('industry_name'):
                missing[key]+=1
                continue
            evaluated[key].append(code)
            candidate=_manifest_signal(strategy,row,target=day,market=market)
            if candidate['signal_status']=='READY':
                member=identities.get(candidate['stock_code'],{})
                picked.append({**candidate,'stock_name':member.get('short_name',''),'industry':member.get('industry_name'),
                    'signal_known_at':prepared['signal_clock'], 'source_run_uid':uid})
        timing.info("signal-score day=%s strategy=%s stocks=%d elapsed=%.3fs",
                    day, key, len(prepared['scored_rows']), perf_counter() - started)
        started = perf_counter()
        picked.sort(key=lambda r:(-r['effective_score'],r['stock_code']))
        for rank,row in enumerate(picked):row['rank']=rank+1
        timing.info("candidate-sort day=%s strategy=%s candidates=%d elapsed=%.3fs",
                    day, key, len(picked), perf_counter() - started)
        selected[key]=picked
    return {'status':'COMPLETE','signals':selected,'source_run_uid':uid,
        'source_sha256':prepared['source_sha256'],'identity':prepared['identity'],
        'evaluation_security_sets':{key:sorted(set(codes)) for key,codes in evaluated.items()},
        'missing_feature_stocks':missing,'scored_stocks':{key:len(codes) for key,codes in evaluated.items()}}
