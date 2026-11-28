"""Research-only fixed-strategy models evaluated at the next actual market open."""
from datetime import datetime
from sqlalchemy import text

from server.common.analysis_pool_receipt import PREMARKET_POOL_TASK, canonical_sha256
from server.engine.strategy_governance import _strategy_version_digest

OPENING_POLICY = 'LATEST_PUBLICATION_BEFORE_NEXT_OPEN_V1'
MODEL_VERSION = 'ai-rec-v3-tactical-v4'
OPENING_STRATEGIES = (
    ('short_term', 'short_term_score', '短线多因子（开盘模拟）'),
    ('ultra_short', 'ultra_short_score', '超短确认（开盘模拟）'),
    ('swing', 'swing_score', '波段价值质量（开盘模拟）'),
    ('main_wave', 'main_wave_score', '主升浪趋势（开盘模拟）'),
)
_OPENING_SPECS = {key: (score_field, name) for key, score_field, name in OPENING_STRATEGIES}


def opening_strategy(strategies, strategy_key='short_term'):
    spec = _OPENING_SPECS.get(strategy_key)
    if spec is None:
        raise ValueError(f'不支持开盘模拟的策略键：{strategy_key}')
    score_field, display_name = spec
    base = next(item for item in strategies if item['strategy_key'] == strategy_key)
    if base['source_kind'] != 'immutable_manifest' or base['evaluator_config']['score_field'] != score_field:
        raise ValueError(f'{display_name}需要冻结的评分合同')
    config = dict(base['evaluator_config'], model_version=MODEL_VERSION, publication_policy=OPENING_POLICY)
    version = base['current_version'] + ':tactical-v4-open-v1'
    digest = _strategy_version_digest(
        strategy_key=base['strategy_key'], version=version, evaluator_type=base['evaluator_type'],
        evaluator_config=config, parameters=base['parameters'], source_kind=base['source_kind'],
    )
    return dict(base, current_version=version, evaluator_config=config, version_hash=digest,
                strategy_name=display_name)


def is_opening(strategies):
    return len(strategies) == 1 and strategies[0].get('evaluator_config', {}).get('publication_policy') == OPENING_POLICY


def select_before_open(days, rows):
    openings = {day: datetime.fromisoformat(following + ' 09:30:00') for day, following in zip(days, days[1:])}
    selected = {}
    for row in rows:
        day = str(row['trade_date'])[:10]
        opening = openings.get(day)
        if not opening:
            continue
        decision, published, finished = (datetime.fromisoformat(str(row[k])) for k in ('decision_at','published_at','finished_at'))
        # Only a completed close publication can precede the next session's open.
        if decision < datetime.fromisoformat(day + ' 15:00:00') or max(decision, published, finished) >= opening:
            continue
        priority = (published, finished, row['id'])
        if day not in selected or priority > selected[day][0]:
            selected[day] = (priority, row['output_dataset_id'])
    return {day: item[1] for day, item in selected.items()}


def enqueue_opening(connection, strategies, days, *, strategy_keys=None):
    from server.engine.native_research_jobs import enqueue
    from server.engine.strategy_native_backtest import _start
    available = {item['strategy_key'] for item in strategies if item.get('source_kind') == 'immutable_manifest'}
    targets = [key for key in (strategy_keys or [k for k, _, _ in OPENING_STRATEGIES]) if key in available]
    if not targets:
        return []
    rows = connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(7000) */ s.trade_date,a.id,a.finished_at,
        a.output_dataset_id,JSON_UNQUOTE(JSON_EXTRACT(a.checkpoint_json,'$.publication_receipt.published_at')) published_at,
        JSON_UNQUOTE(JSON_EXTRACT(a.checkpoint_json,'$.publication_receipt.score_snapshot.decision_at')) decision_at
        FROM st_daily_delivery_session s JOIN st_daily_stage_attempt a ON s.session_uid=a.session_uid
        WHERE s.trade_date BETWEEN :start AND :end AND a.stage_name IN ('analysis_fast',:morning)
        AND a.status='SUCCESS' ORDER BY s.trade_date,a.finished_at,a.id LIMIT 5001"""),
        {'start': days[0], 'end': days[-1], 'morning': PREMARKET_POOL_TASK}).mappings().all()
    if len(rows) > 5000:
        raise ValueError('开盘模拟发布清单超过5000条')
    published = select_before_open(days, rows)
    members = connection.execute(text("""SELECT snapshot_date,industry_hash,captured_at,quality_status
        FROM qmt_membership_snapshot_run WHERE source='gj_big_qmt_inner' AND snapshot_date BETWEEN :start AND :end
        ORDER BY snapshot_date"""), {'start': days[0], 'end': days[-1]}).mappings()
    source_hash = canonical_sha256({'opening_publications': published, 'membership': [
        {k: str(v) for k, v in row.items()} for row in members if str(row['snapshot_date'])[:10] in published
    ]})
    queued = []
    for key in targets:
        state = _start(connection, [opening_strategy(strategies, key)], days, published, source_hash)
        queued.append(enqueue(connection, state, requested_by=f"{key.replace('_', '-')}-opening-research"))
    return queued
