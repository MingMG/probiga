"""Prepare verified original historical decisions, without strategy evaluation."""
from datetime import date, datetime
from server.engine.strategy_price_backtest import _identity_rows


def prepare_original_day(c,state,day):
    from server.common.daily_delivery_control import load_published_analysis_receipt
    from server.common.analysis_pool_receipt import read_score_snapshot_header,decode_score_snapshot
    from server.engine.strategy_center import _fixed_market_snapshot
    uid=state['published'].get(day)
    if not uid:return {'status':'BLOCKED','reason':'缺少完整冻结评分及市场上下文批次','signals':{}}
    receipt=load_published_analysis_receipt(c,day,run_uid=uid,decision_at=datetime.fromisoformat(state['requested_at']))
    header=read_score_snapshot_header(receipt['score_snapshot'],trade_date=day,engine=c)
    decision=datetime.fromisoformat(header['decision_at'])
    if decision.date()!=date.fromisoformat(day):
        return {'status':'BLOCKED','reason':'冻结评分实际可用时间晚于目标历史交易日','source_run_uid':uid,'actual_decision_at':str(decision),'signals':{}}
    snapshot=decode_score_snapshot(receipt['score_snapshot'],trade_date=day,engine=c)
    if any(s['evaluator_config']['market_state_config_hash']!=snapshot['fixed_market_context']['market_state_config_hash']
           for s in state['strategies'] if s['source_kind']=='immutable_manifest'):
        raise ValueError('冻结市场状态配置与原生策略版本不一致')
    market=_fixed_market_snapshot(day,snapshot['fixed_market_context'],decision)
    identities,proof=_identity_rows(c,day)
    if proof['status']!='VERIFIED':
        return {'status':'BLOCKED','reason':'缺少精确日期证券身份和行业冻结快照','identity':proof,
                'signals':{},'source_run_uid':uid,'source_sha256':receipt['score_snapshot_sha256']}
    return {'status':'COMPLETE','scored_rows':snapshot['scored_rows'],'market':market,
        'identities':identities,'identity':proof,'source_run_uid':uid,
        'source_sha256':receipt['score_snapshot_sha256'],
        'signal_clock':str(max(decision,datetime.fromisoformat(str(receipt['published_at'])))),
        'fixed_market_context':snapshot['fixed_market_context']}
