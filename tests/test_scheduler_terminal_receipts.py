import json

import pytest

from server.api import scheduler_runtime as runtime
from server.common.scheduler_validation import _single_nested_machine_payload


def test_incomplete_source_receipt_survives_verbose_multibyte_stderr():
    receipt = {
        'schema': 'probiga.notice-history-repair-result.v1', 'status': 'PROGRESS',
        'completed_code_count': 2900, 'remaining_code_count': 2985, 'retryable': True,
    }
    stdout = json.dumps(receipt)
    output = runtime._terminal_history_output(stdout, '请求成功日志\n' * 20000, stdout)
    assert len(output.encode('utf8')) <= runtime._HISTORY_OUTPUT_LIMIT
    assert _single_nested_machine_payload(output, schema=receipt['schema']) == receipt
    assert 'scheduler-validation-evidence' not in output


def test_failure_reason_and_credentials_are_handled_without_success_receipt():
    receipt = {'schema': 'probiga.collector-result.v1', 'status': 'DATA_BLOCKED', 'reason': 'HTTP_403'}
    stdout = json.dumps(receipt)
    output = runtime._terminal_history_output(stdout, 'Bearer private-access-token\n' * 10000, stdout)
    assert 'private-access-token' not in output
    assert _single_nested_machine_payload(output, schema=receipt['schema']) == receipt
    assert 'DATA_VALIDATION_OK' not in output


def test_oversized_machine_receipt_is_rejected_instead_of_truncated():
    receipt = json.dumps({'schema': 'probiga.collector-result.v1', 'payload': 'x' * 25000})
    with pytest.raises(RuntimeError, match='bounded history evidence'):
        runtime._terminal_history_output(receipt, '', receipt)


def test_no_machine_receipt_keeps_diagnostic_tail():
    assert runtime._terminal_history_output('plain text', 'failure detail', 'plain text') == (
        'plain text\n---STDERR---\nfailure detail')
