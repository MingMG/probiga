"""Research identities exclude executor builds and bind full math dependencies."""
import hashlib
import json
from importlib.metadata import version
from pathlib import Path
import platform

from server.common.analysis_pool_receipt import canonical_sha256
from server.common.formal_source_identity import frozen_source_sha256

ROOT = Path(__file__).resolve().parents[2]

# Full modules, rather than selected function text, include helper/global changes.
ALGORITHM_FILES = (
    'server/engine/native_feature_math.py', 'server/engine/manifest_signal_math.py', 'server/engine/native_research_math.py', 'server/engine/native_signal_math.py',
    'server/engine/strategy_native_replay.py', 'server/engine/shadow_trial_policy.py',
    'server/common/research_execution_math.py', 'server/common/pit_execution_guard.py',
    'server/trading_v2/config.py',
    *('server/trading_v3/' + name + '.py' for name in (
        'daily_feature_math', 'context_scoring', 'theme_features', 'engine', 'domain',
        'sleeves', 'regime', 'hypotheses', 'calibration', 'portfolio', 'portfolio_constraints',
        'right_side_policy', 'structural_mainline', 'exit_policy', 'metrics', 'config')),
    'deploy/production_requirements.lock', 'deploy/production_wheel_manifest.lock',
)

# Exact source revisions that add output evidence and timing while preserving
# the frozen model decisions and execution economics of a running replay.
_OBSERVABILITY_EQUIVALENT = {
    'server/engine/strategy_native_replay.py': (
        '747451925d1539a63c60dd48c463e6b22fee943b6f0fb326eaf0a3e762ef5fb5',
        'd7779f4bcf11f55dd88679a4a039229b61f2adffce75921762b1a04d1b2a54d7',
    ),
    'server/engine/native_research_math.py': (
        '72c9890966f89e31f1813a79aa9fbf9a75f2e9e916329b5e13ecf8ac9836e528',
        '5944b80992411f49aed92af4beea1e28807cab2c737f94491cde9faa1a1bb8cf',
    ),
}


def algorithm_identity(config):
    files = {relative: hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() for relative in ALGORITHM_FILES}
    for relative, (observed, frozen) in _OBSERVABILITY_EQUIVALENT.items():
        if files[relative] == observed:
            files[relative] = frozen
    payload = {'schema': 'probiga.native-research-algorithm.v2', 'full_modules': files,
               'config': config, 'calibration': {}, 'calibration_policy': 'NO_FUTURE_CALIBRATIONS',
               'runtime':{'python':platform.python_version(),'pandas':version('pandas'),'numpy':version('numpy')}}
    return {'sha256': canonical_sha256(payload), **payload}


def producer_identity(family="immutable_v3_sleeve"):
    files = (
        'server/trading_v3/daily_features.py', 'server/trading_v3/context.py',
        'server/trading_v3/theme_history.py', 'server/engine/strategy_native_fact_snapshot.py',
        'server/engine/native_research_prepare.py', 'server/common/pit_facts.py',
        'server/common/historical_announcement_read.py', 'server/common/pit_sealed_finance_read.py',
        'server/common/pit_sealed_event_read.py', 'server/engine/strategy_price_backtest.py',
    )
    if family=='immutable_manifest':
        files=('server/engine/native_original_prepare.py','server/engine/strategy_center.py',
            'server/common/daily_delivery_control.py','server/common/analysis_pool_receipt.py',
            'server/common/versioned_strategy_config.py','server/engine/market_state_v2.py',
            'server/engine/strategy_price_backtest.py')
    return {relative: frozen_source_sha256(ROOT / relative) for relative in files}


def feature_algorithm_identity(algorithm):
    modules=algorithm['full_modules']
    paths=['server/engine/native_feature_math.py']+['server/trading_v3/'+name+'.py' for name in ('daily_feature_math','context_scoring','theme_features')]
    return canonical_sha256({'full_modules':{path:modules[path] for path in paths},
        'runtime':algorithm['runtime'],'environment_lock':{path:modules[path] for path in
        ('deploy/production_requirements.lock','deploy/production_wheel_manifest.lock')}})


def request_identity(*, strategies, sessions, published, source_hash, requested_at,
                     price_cutoff, algorithm, account, allocation_policy, producer,execution_end_date):
    payload = {'schema': 'probiga.native-experiment-request.v2',
        'experiment_kind': 'CURRENT_FROZEN_RULES_RETROSPECTIVE' if strategies[0]['source_kind'] == 'immutable_v3_sleeve'
            else 'ORIGINAL_HISTORICAL_DECISIONS',
        'strategies': strategies, 'evaluation_sessions': sessions, 'published': published,
        'source_hash': source_hash, 'requested_at': requested_at, 'price_cutoff': price_cutoff,
        'algorithm_sha256': algorithm['sha256'], 'account': account, 'initial_cash_cny': 250000.0,
        'data_transform_sha256':canonical_sha256(producer),
        'execution_end_date':execution_end_date,
        'allocation_policy': allocation_policy, 'execution_mode': 'MODELED_DAILY_BAR'}
    # Canonical JSON types are frozen once, not silently refreshed by readers.
    payload = json.loads(json.dumps(payload, ensure_ascii=False, default=str))
    return {'sha256': canonical_sha256(payload), **payload}
