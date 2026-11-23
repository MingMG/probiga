"""Account replay of frozen native SHADOW execution contracts; never orders."""
from datetime import date, datetime

from server.common.pit_execution_guard import daily_bar_execution_disposition
from server.engine.shadow_trial_policy import frozen_shadow_execution_contract, shadow_initial_stop, shadow_exit_reason
from server.common.research_execution_math import _stock_rule, _execution_fee_with_impact


def replay_native(strategy, signals, bars, sessions, *, evaluation_start, source_gaps,
                  account=None, allocation_policy='FIXED_SHADOW'):
    if account is None:
        from server.engine.strategy_rule_replay import ACCOUNT
        account = dict(ACCOUNT)
    if allocation_policy not in ('FIXED_SHADOW', 'ADAPTIVE_REGIME'):
        raise ValueError('native allocation policy differs')
    if sessions != sorted(set(sessions)) or evaluation_start not in sessions:
        raise ValueError("native replay sessions differ")
    prices = {(str(r['trade_date'])[:10], r['stock_code']): r for r in bars}
    if len(prices) != len(bars):
        raise ValueError("native replay duplicate price key")
    policy = strategy['parameters']['shadow_execution_policy']
    cash, initial = 250000., 250000.
    entries, held, trades, nav, dispositions = {}, {}, [], [], []
    invalid = False
    for signal in signals:
        known = datetime.fromisoformat(signal['signal_known_at'])
        following = next((d for d in sessions if datetime.fromisoformat(d+' 09:30:00') > known), None)
        if following:
            entries.setdefault(following, []).append(signal)
        else:
            dispositions.append({'stock_code': signal['stock_code'], 'status': 'RIGHT_CENSORED', 'reason': '窗口内没有决策后的交易日'})
    for index, day in enumerate(sessions):
        opening_cash = cash
        for code in list(held):
            p = held[code]
            row = prices.get((day, code))
            prior = prices.get((sessions[index-1], code)) if index else None
            if row and prior and abs(float(row['pre_close'])-float(prior['close'])) > max(.011, float(prior['close'])*1e-4):
                invalid = True
                p['unknown'] = True
                dispositions.append({'stock_code': code, 'trade_date': day, 'status': 'DATA_BLOCKED', 'reason': '公司行动或价格版本变化缺少现金流证据'})
            if not p['exit_pending'] or index <= p['entry_index']:
                continue
            fill = daily_bar_execution_disposition(row, side='SELL')
            if not fill['executable']:
                dispositions.append({'stock_code': code, 'trade_date': day, 'side': 'SELL', **fill})
                continue
            price = float(fill['open_price'])
            value = price*p['quantity']
            if value/float(row['amount']) > .01:
                dispositions.append({'stock_code': code, 'trade_date': day, 'status': 'KNOWN_UNFILLED', 'reason': '退出成交容量不足，顺延'})
                continue
            fee, impact = _execution_fee_with_impact(value, account=account, sell=True,
                participation_rate=value/float(row['amount']), maximum_participation_rate=.01)
            cash += value-fee
            pnl = None if p.get('unknown') else value-fee-p['entry_value']-p['entry_fee']
            trades.append({'stock_code': code, 'stock_name': p['stock_name'],
                'entry_at': p['entry_day']+' 09:30:00', 'exit_at': day+' 09:30:00',
                'entry_price': p['entry_price'], 'exit_price': price, 'quantity': p['quantity'],
                'signal_known_at': p['signal_known_at'], 'estimated_fee_cny': p['entry_fee']+fee,
                'estimated_impact_cny': p['entry_impact']+impact, 'net_pnl_cny': pnl,
                'net_return_pct': pnl/(p['entry_value']+p['entry_fee'])*100 if pnl is not None else None,
                'exit_reason': p['exit_pending'], 'execution_contract': p['contract'],
                'instrument_rule_version': p['instrument_rule_version'], 'execution_mode': 'MODELED_DAILY_BAR', 'funding_eligible': False})
            del held[code]
        for s in sorted(entries.get(day, []), key=lambda r: (r['rank'], r['stock_code'])):
            code = s['stock_code']
            row = prices.get((day, code))
            fill = daily_bar_execution_disposition(row, side='BUY')
            def exclude(reason, status='DATA_BLOCKED'):
                dispositions.append({'stock_code': code, 'trade_date': day, 'side': 'BUY', 'status': status, 'reason': reason})
            if not fill['executable']:
                dispositions.append({'stock_code': code, 'trade_date': day, 'side': 'BUY', **fill})
                continue
            if not s.get('stock_name') or not s.get('industry'):
                exclude('缺少决策时点证券身份或行业')
                continue
            if code in held or len(held) >= int(policy['capacity_policy']['maximum_positions']):
                exclude('重复持仓或持仓容量不足', 'KNOWN_UNFILLED')
                continue
            instrument = _stock_rule({'stock_code': code, 'short_name': s['stock_name']}, date.fromisoformat(day))
            if not instrument['can_buy']:
                exclude('冻结证券状态不允许买入', 'KNOWN_UNFILLED')
                continue
            amounts = [float(prices[(d,code)]['amount']) for d in sessions[max(0,index-20):index] if (d,code) in prices]
            if len(amounts) != 20:
                exclude('缺少入场前20日流动性证据')
                continue
            price = float(fill['open_price'])
            contract = frozen_shadow_execution_contract(strategy_key=strategy['strategy_key'],
                strategy_version=strategy['current_version'], version_hash=strategy['version_hash'],
                source_kind=strategy['source_kind'], parameters=strategy['parameters'], candidate=s)
            try:
                stop = shadow_initial_stop(contract, price)
            except ValueError:
                exclude('开盘价与冻结保护止损冲突', 'KNOWN_UNFILLED')
                continue
            industry_value = sum(p['entry_value'] for p in held.values() if p['industry']==s['industry'])
            multiplier = float(s['allocation_multiplier']) if allocation_policy == 'ADAPTIVE_REGIME' else 1.0
            cap = float(s['risk_asset_cap']) if allocation_policy == 'ADAPTIVE_REGIME' else 1.0
            if not 0 <= multiplier <= 100 or not 0 <= cap <= 1:
                raise ValueError('native allocation multiplier or cap invalid')
            held_value = sum(p['entry_value'] for p in held.values())
            budget = min(initial*policy['maximum_target_bp']/10000*min(multiplier,1.0), initial*.3-industry_value,
                         initial*cap-held_value if allocation_policy == 'ADAPTIVE_REGIME' else cash,
                         sum(amounts)/20*.01, opening_cash, cash)
            quantity = max(0,int(budget/(price*1.01)/instrument['buy_lot_size'])*instrument['buy_lot_size'])
            if quantity < instrument['first_buy_minimum']:
                exclude('现金或流动性容量不足最小交易单位', 'KNOWN_UNFILLED')
                continue
            value = price*quantity
            if value/float(row['amount']) > .01:
                exclude('当日成交容量不足，原订单不按未来成交额缩量', 'KNOWN_UNFILLED')
                continue
            fee,impact = _execution_fee_with_impact(value, account=account, sell=False,
                participation_rate=value/(sum(amounts)/20), maximum_participation_rate=.01)
            if value+fee > min(opening_cash,cash):
                exclude('含费用现金不足', 'KNOWN_UNFILLED')
                continue
            cash -= value+fee
            opening_cash -= value+fee
            held[code] = {**s, 'quantity': quantity, 'entry_day': day, 'entry_index': index,
                'entry_price': price, 'entry_value': value, 'entry_fee': fee, 'entry_impact': impact,
                'contract': contract, 'stop': stop, 'exit_pending': '', 'instrument_rule_version': instrument['rule_version']}
        value = 0.
        for code,p in held.items():
            row = prices.get((day,code))
            if row is None or float(row['close']) <= 0:
                invalid = True
                p['unknown'] = True
                dispositions.append({'stock_code': code, 'trade_date': day, 'status': 'DATA_BLOCKED', 'reason': '持仓估值缺少行情'})
                continue
            value += p['quantity']*float(row['close'])
            reason = shadow_exit_reason(p['contract'], holding_sessions=index-p['entry_index']+1,
                session_low=float(row['low']), protective_stop=p['stop'])
            if reason:
                p['exit_pending'] = reason
        if cash < -.01:
            raise ValueError('native replay cash overdraft')
        if day >= evaluation_start:
            previous = nav[-1]['nav_cny'] if nav else initial
            nav.append({'trade_date': day, 'nav_cny': None if invalid else cash+value,
                'return_pct': ((cash+value)/previous-1)*100 if not invalid and previous else None})
    excluded = sum(d['status']=='DATA_BLOCKED' for d in dispositions)
    partial = bool(source_gaps or excluded)
    result = (cash+value)/initial*100-100 if not invalid else None
    return {'status': 'BLOCKED' if invalid else 'PARTIAL' if partial else 'COMPLETE',
        'initial_cash_cny': initial, 'execution_mode': 'MODELED_DAILY_BAR',
        'execution_policy': policy, 'trades': trades, 'nav': nav, 'dispositions': dispositions,
        'completed_trade_count': len(trades), 'open_position_count': len(held),
        'input_exclusion_count': excluded, 'net_return_pct': result if not partial and not invalid else None,
        'verified_input_subset_return_pct': result if not invalid and trades else None,
        'return_scope': 'UNKNOWN' if invalid else 'VERIFIED_INPUT_SUBSET' if partial else 'FULL_REQUESTED_INPUTS',
        'fees': {'kind':'ESTIMATE', **account}, 'out_of_sample_status': 'RETROSPECTIVE_NOT_PRISTINE',
        'allocation_policy': allocation_policy,
        'funding_eligible': False, 'real_order_authority': False}
