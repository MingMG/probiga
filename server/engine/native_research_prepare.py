"""Bounded data preparation for frozen native experiments."""
from datetime import date, datetime, timedelta

from server.common.formal_analysis_store import FormalAnalysisStore
from server.engine.native_research_jobs import PrepareYield, _source_progress, source_deadline
from server.engine.autonomous_strategy_research import now_shanghai
from server.engine.strategy_native_fact_snapshot import snapshot_identity, load_snapshot, save_snapshot
from server.trading_v3.daily_features import DailyFactInputs, _load_finance, _load_recent_notices


def plan_v3_day(connection, state, day):
    from server.engine.strategy_price_backtest import _identity_rows
    from server.common.pit_facts import load_latest_finance_atomic_batch_baseline
    from server.common.historical_announcement_read import has_dated_announcement_candidate
    identity, proof = _identity_rows(connection, day)
    if proof['status'] != 'VERIFIED':
        return {'status': 'BLOCKED', 'reason': '缺少精确日期证券身份和行业快照', 'identity': proof}
    known, target = datetime.fromisoformat(state['requested_at']), date.fromisoformat(day)
    observation = min(known, datetime.combine(target, datetime.max.time()))
    baseline = load_latest_finance_atomic_batch_baseline(connection.engine, decision_at=observation)
    if baseline.get('as_of_date') != day:
        return {'status': 'BLOCKED', 'reason': 'PIT_HISTORICAL_FINANCE_BATCH_NOT_FOUND',
                'identity': proof, 'finance_observation_at': str(observation), 'latest_finance_date': baseline.get('as_of_date')}
    eligible = sorted((set(identity) & set(baseline.get('members', {}))) - set(baseline.get('data_excluded_codes', [])))
    if not eligible:
        return {'status': 'BLOCKED', 'reason': '历史证券集合没有可验证的财务封存覆盖', 'identity': proof}
    if not has_dated_announcement_candidate(connection.engine, code=min(eligible), decision_at=known,
            target=target, window_start=target - timedelta(days=20)):
        return {'status': 'BLOCKED', 'reason': 'QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND', 'identity': proof}
    generation = snapshot_identity(day=day, decision=known, identity_hash=proof['sha256'],
                                   finance_seal=baseline['seal_coverage_id'], codes=eligible)
    return {'status': 'COMPLETE', 'identity_rows': identity, 'identity': proof,
            'eligible': eligible, 'excluded': sorted(set(identity) - set(eligible)),
            'finance_seal': baseline['seal_coverage_id'], 'finance_observation_at': str(observation),
            'snapshot_generation': generation, 'decision_at': str(known), 'day': day}


def prepare_selection(connection, state, owner, plan):
    from server.common.pit_facts import resolve_strategy_fact_cutoffs, PIT_AVAILABLE
    from server.engine.strategy_native_backtest import _assert_read_succeeded
    generation, day = plan['snapshot_generation'], plan['day']
    facts = load_snapshot(connection.engine, generation)
    known = datetime.fromisoformat(plan['decision_at'])
    target = date.fromisoformat(day)
    if facts is None:
        finances, events = [], []
        store = FormalAnalysisStore(connection.engine)
        deadline = source_deadline()
        progress = _source_progress(connection, state, owner, day, 'event_selection', deadline=deadline)
        cutoff = resolve_strategy_fact_cutoffs(connection.engine, codes=plan['eligible'], decision_at=known,
            finance_start_date='1900-01-01', finance_end_date=day,
            event_start_date=target - timedelta(days=20), event_end_date=day,
            require_qmt_event_batch=True, event_trade_date=day,
            capture_finance_selection=finances.append, capture_historical_selection=events.append,
            finance_observation_at=datetime.fromisoformat(plan['finance_observation_at']), event_progress=progress,
            finance_coverage_store=store,
            finance_coverage_progress=_source_progress(connection, state, owner, day, 'finance_coverage', deadline=deadline),
            event_coverage_store=store,
            event_coverage_progress=_source_progress(connection, state, owner, day, 'event_coverage', deadline=deadline),
            event_metadata_progress=_source_progress(connection, state, owner, day, 'event_metadata', deadline=deadline))
        if cutoff['status'] != PIT_AVAILABLE:
            _assert_read_succeeded(cutoff['reason'])
            return {'status': 'BLOCKED', 'reason': cutoff['reason'], 'identity': plan['identity']}
        if len(finances) != 1 or len(events) != 1:
            raise ValueError('历史事实快照未完整移交')
        facts = DailyFactInputs(cutoff, finances[0], events[0])
        save_snapshot(connection.engine, generation, facts)
    clocks = {kind: datetime.fromisoformat(value) for kind, value in facts.cutoff['source_cutoff_at'].items()}
    if any(clock.date() != target for clock in clocks.values()) or clocks['event'] < datetime.fromisoformat(day + ' 15:35:00'):
        return {'status': 'BLOCKED', 'reason': '历史公告截止时间不能覆盖当日收盘或财务批次日期错误',
                'source_cutoff_at': facts.cutoff['source_cutoff_at']}
    if (facts.finance.decision_at != known or facts.finance.as_of_date != target
            or facts.finance.fact_cutoff_at != clocks['finance']
            or set(facts.events.selection.members) != set(plan['eligible'])):
        raise ValueError('native cached historical fact scope differs')
    signal_clock = max(*clocks.values(), datetime.fromisoformat(str(plan['identity']['captured_at'])),
        datetime.fromisoformat(str(facts.events.selection.batch.get('received_at') or max(clocks.values()))))
    return {'status': 'COMPLETE', 'cutoff': facts.cutoff, 'signal_clock': signal_clock,
            'fact_clock': max(clocks.values())}


def prepare_finance(connection, state, owner, plan, selection):
    from server.common.pit_sealed_finance_read import load_sealed_finance_facts
    inputs = load_snapshot(connection.engine, plan['snapshot_generation'])
    if inputs is None:
        raise RuntimeError('PREPARE_FINANCE_EXACT_SELECTION_MISSING')
    known, target = datetime.fromisoformat(plan['decision_at']), date.fromisoformat(plan['day'])
    clock = selection['cutoff']['source_cutoff_at']['finance']
    batch = load_sealed_finance_facts(connection.engine, selection=inputs.finance,
        codes=plan['eligible'], decision_at=known, fact_cutoff_at=clock, as_of_date=target,
        formal_store=FormalAnalysisStore(connection.engine), formal_generation_sha256=plan['snapshot_generation'],
        read_progress=_source_progress(connection, state, owner, plan['day'], 'finance_facts', deadline=source_deadline()))
    return _load_finance(connection.engine, as_of=target, codes=plan['eligible'], decision_at=known,
        fact_cutoff_at=clock, sealed_finance_inputs=inputs.finance, prepared_batch=batch)


def prepare_events(connection, state, owner, plan, selection):
    from server.common.pit_sealed_event_read import load_sealed_event_facts
    inputs = load_snapshot(connection.engine, plan['snapshot_generation'])
    if inputs is None:
        raise RuntimeError('PREPARE_EVENTS_EXACT_SELECTION_MISSING')
    batch = load_sealed_event_facts(connection.engine, selection=inputs.events, codes=plan['eligible'],
        formal_store=FormalAnalysisStore(connection.engine),
        read_progress=_source_progress(connection, state, owner, plan['day'], 'event_facts', deadline=source_deadline()))
    notices, evidence = _load_recent_notices(connection.engine, as_of=date.fromisoformat(plan['day']),
        codes=plan['eligible'], decision_at=datetime.fromisoformat(plan['decision_at']),
        fact_cutoff_at=selection['cutoff']['source_cutoff_at']['event'], include_evidence=True,
        historical_selection=inputs.events, prepared_batch=batch)
    return {'notices': notices, 'evidence': evidence}


def validate_input_index(inputs):
    seen={}
    for part in inputs.values():
        for day in part['sessions']:
            identity={'proof':part['proofs'][day],'rows':part['row_counts'][day],'catalog':part['catalogs'][day]}
            if day in seen and seen[day]!=identity:
                raise ValueError('冻结历史输入在恢复引用中发生冲突：'+day)
            seen[day]=identity
    return inputs


def validate_ready_inputs(plan,selection,finance,events,context,*,requested_at,price_cutoff,config):
    from server.common.pit_facts import normalize_decision_at
    known=normalize_decision_at(requested_at)
    cutoff=selection['cutoff']
    expected={'day':plan['day'],'decision_at':requested_at,'price_cutoff':price_cutoff,
              'identity_sha256':plan['identity']['sha256'],'finance_seal':plan['finance_seal']}
    if context.get('binding')!=expected or cutoff.get('status')!='AVAILABLE':
        raise ValueError('READY_CONTEXT_OR_CUTOFF_DIFFERS')
    from server.trading_v3.theme_features import research_theme_keywords
    if context.get('research_keywords')!=research_theme_keywords(config):
        raise ValueError('READY_FROZEN_TAXONOMY_DIFFERS')
    if normalize_decision_at(cutoff.get('decision_at'))!=known:
        raise ValueError('READY_DECISION_CLOCK_DIFFERS')
    clocks=cutoff.get('source_cutoff_at') or {}
    if set(clocks)!={'finance','event'}:raise ValueError('READY_SOURCE_CLOCKS_MISSING')
    clocks={kind:normalize_decision_at(value) for kind,value in clocks.items()}
    target=date.fromisoformat(plan['day'])
    if any(value.date()!=target or value>known for value in clocks.values()):
        raise ValueError('READY_SOURCE_CLOCK_DIFFERS')
    eligible=set(plan['eligible'])
    if set(finance)!=eligible or set(events['evidence']['status_by_code'])!=eligible:
        raise ValueError('READY_FACT_SECURITY_COVERAGE_DIFFERS')
    if not set(context['codes'])<=eligible or not set(events['notices'])<=eligible:
        raise ValueError('READY_FEATURE_SECURITY_COVERAGE_DIFFERS')
    for value in finance.values():
        if (normalize_decision_at(value.get('finance_decision_at'))!=known
                or normalize_decision_at(value.get('finance_fact_cutoff_at'))!=clocks['finance']):
            raise ValueError('READY_FINANCE_CLOCK_DIFFERS')
    evidence=events['evidence']
    if (normalize_decision_at(evidence.get('decision_at'))!=known
            or normalize_decision_at(evidence.get('fact_cutoff_at'))!=clocks['event']):
        raise ValueError('READY_EVENT_CLOCK_DIFFERS')


def base_data_identity(plan,selection,context,*,finance_reference,event_reference):
    from server.common.analysis_pool_receipt import canonical_sha256
    observation=context['market_context']
    if observation.get('context_evidence_status')!='DATA_BLOCKED' or observation.get('context_strategy_eligible') is not False:
        raise ValueError('COMPARISON_CONTEXT_RAW_SOURCE_IDENTITY_REQUIRED')
    unavailable={field:observation.get(field) for field in (
        'context_evidence_status','context_reason','context_cutoff_at','context_knowledge_time_column')}
    return canonical_sha256({'identity_rows':plan['identity_rows'],'identity_receipt':plan['identity'],
        'finance':finance_reference,'events':event_reference,'cutoff':selection['cutoff'],
        'industry_members':context['industries'],'industry_receipt':context['industry_evidence'],
        'theme_members':context['theme_memberships'],'concept_snapshot_date':str(context['concept_snapshot_date']),
        'news_context_source_status':unavailable})
