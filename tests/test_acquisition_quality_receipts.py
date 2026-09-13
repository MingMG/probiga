import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from tools import data_quality_check as quality
from tools import ensure_quality_gate as gate
from server.common import scheduler_validation as validation


NOW = datetime(2026, 9, 13, 15)
TARGET = '2026-09-11'


def seal_evidence(evidence):
    evidence = dict(evidence)
    evidence.pop('evidence_sha256', None)
    evidence['evidence_sha256'] = gate._canonical_sha256(evidence)
    return json.dumps(evidence)


@pytest.fixture
def published(monkeypatch):
    history = dict(history_id=42, run_uid='a' * 32, task_id=118,
                   task_name='Eastmoney current', task_type='eastmoney_concept_current',
                   run_at=datetime(2026, 9, 12, 19), finished_at=datetime(2026, 9, 12, 19, 1),
                   status='success', exit_code=0, build_sha='b' * 40,
                   trigger_source='release_catchup', script_path='tools/sync_eastmoney_concept_market.py',
                   script_args='--dataset current', date_param='')
    evidence = {key: history[key] for key in (
        'run_uid', 'task_id', 'task_name', 'task_type', 'status', 'exit_code', 'build_sha')}
    evidence.update(schema=gate.RELEASE_VALIDATION_EVIDENCE_SCHEMA, started_at='2026-09-12 19:00:00',
                    validation_checked=True, validation_ok=True, target_trade_date=TARGET,
                    release_target_date=TARGET,
                    replay_output='source payload', replay_output_sha256=gate._text_sha256('source payload'),
                    machine_output_sha256='c' * 64)
    history['output'] = seal_evidence(evidence)
    monkeypatch.setattr(quality, '_row', lambda *_args, **_kwargs: history)
    monkeypatch.setattr(validation, 'scheduler_output_status', lambda *_args, **_kwargs: 'success')
    monkeypatch.setattr(validation, '_eastmoney_concept_market_payload', lambda _output: {'target_trade_date': TARGET})
    monkeypatch.setattr(validation, 'validate_scheduler_task_result',
                        lambda *_args, **_kwargs: SimpleNamespace(checked=True, ok=True, message='exact values verified'))
    return history, evidence


def test_scheduled_receipt_is_checked_without_inventing_release_authority(monkeypatch, published):
    history, _evidence = published
    history['trigger_source'] = 'scheduled'
    def replay(task, **_kwargs):
        assert '_release_target_date' not in task
        assert task['_trigger_source'] == 'scheduled'
        return SimpleNamespace(checked=True, ok=True, message='exact values verified')
    monkeypatch.setattr(validation, 'validate_scheduler_task_result', replay)
    assert quality._check_published_concept_dataset(object(), 'eastmoney_concept_current', TARGET, NOW)['status'] == 'PASS'


def test_receipt_envelope_cannot_hide_a_different_native_source_date(monkeypatch, published):
    monkeypatch.setattr(validation, '_eastmoney_concept_market_payload', lambda _output: {'target_trade_date': '2026-09-10'})
    with pytest.raises(ValueError, match='native source receipt date'):
        quality._check_published_concept_dataset(object(), 'eastmoney_concept_current', TARGET, NOW)


def test_published_receipt_is_replayed_against_current_database(monkeypatch, published):
    assert quality._check_published_concept_dataset(object(), 'eastmoney_concept_current', TARGET, NOW)['status'] == 'PASS'
    monkeypatch.setattr(validation, 'validate_scheduler_task_result',
                        lambda *_args, **_kwargs: SimpleNamespace(checked=True, ok=False, message='one member is missing'))
    result = quality._check_published_concept_dataset(object(), 'eastmoney_concept_current', TARGET, NOW)
    assert result['status'] == 'FAIL'
    assert 'missing' in result['validation']


@pytest.mark.parametrize('field,value', [
    ('target_trade_date', '2026-09-10'), ('run_uid', 'd' * 32),
    ('task_type', 'qmt_index_current'), ('validation_ok', False),
    ('started_at', '2026-09-14 19:00:00'), ('replay_output', 'changed payload'),
])
def test_resealed_wrong_date_identity_and_invalid_execution_stay_blocked(published, field, value):
    history, evidence = published
    evidence[field] = value
    history['output'] = seal_evidence(evidence)
    with pytest.raises((ValueError, RuntimeError)):
        quality._check_published_concept_dataset(object(), 'eastmoney_concept_current', TARGET, NOW)


def test_corrupt_envelope_and_future_history_stay_blocked(published):
    history, evidence = published
    history['output'] = history['output'].replace('"validation_ok": true', '"validation_ok": false')
    with pytest.raises(RuntimeError, match='hash differs'):
        quality._check_published_concept_dataset(object(), 'eastmoney_concept_current', TARGET, NOW)
    history['output'] = seal_evidence(evidence)
    history['finished_at'] = datetime(2026, 9, 14)
    with pytest.raises(ValueError, match='execution time'):
        quality._check_published_concept_dataset(object(), 'eastmoney_concept_current', TARGET, NOW)


def test_missing_receipt_is_not_replaced_by_legacy_qmt_directory(monkeypatch):
    monkeypatch.setattr(quality, '_row', lambda *_args, **_kwargs: {})
    monkeypatch.setattr(quality, 'expected_scheduled_trade_date', lambda *_args, **_kwargs: TARGET)
    result = quality.check_concept_data_freshness(object(), TARGET, now=NOW)
    assert result.status == 'FAIL'
    assert result.details['failures'] == ['concept_current', 'concept_kline', 'concept_flow']


@pytest.mark.parametrize('latest,target_count,notices,status', [
    (datetime(2026, 9, 11, 23), 1169, 9475, 'PASS'),
    (datetime(2026, 9, 13, 14), 1169, 9475, 'PASS'),
    (datetime(2026, 9, 13, 6, tzinfo=timezone.utc), 1169, 9475, 'PASS'),
    (datetime(2026, 9, 13, 15, 1), 1169, 9475, 'FAIL'),
    (datetime(2026, 9, 14), 1169, 9475, 'FAIL'),
    (datetime(2026, 9, 10, 23), 1169, 9475, 'FAIL'),
    (datetime(2026, 9, 13, 14), 0, 9475, 'FAIL'),
    (datetime(2026, 9, 13, 14), 1169, 0, 'FAIL'),
    (None, 0, 0, 'FAIL'),
])
def test_weekend_news_does_not_fail_but_missing_target_or_future_news_does(monkeypatch, latest, target_count, notices, status):
    monkeypatch.setattr(quality, 'expected_intraday_date', lambda *_args, **_kwargs: TARGET)
    monkeypatch.setattr(quality, '_row', lambda *_args, **_kwargs: {'latest_publish_time': latest, 'news_count': target_count})
    monkeypatch.setattr(quality, '_scalar', lambda *_args, **_kwargs: notices)
    assert quality.check_news_and_notices(object(), TARGET, now=NOW).status == status


def test_source_exception_is_redacted_and_other_datasets_are_still_checked(monkeypatch):
    calls = []
    def check(_engine, task_type, _target, _now):
        calls.append(task_type)
        if task_type == 'eastmoney_concept_current':
            raise RuntimeError('mysql://private-user:password@db')
        return {'status': 'PASS'}
    monkeypatch.setattr(quality, '_check_published_concept_dataset', check)
    monkeypatch.setattr(quality, 'expected_scheduled_trade_date', lambda *_args, **_kwargs: TARGET)
    result = quality.check_concept_data_freshness(object(), TARGET, now=NOW)
    assert len(calls) == 3
    assert result.details['failures'] == ['concept_current']
    assert 'password' not in str(result)
