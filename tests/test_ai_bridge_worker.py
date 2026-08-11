# -*- coding: utf-8 -*-
from __future__ import annotations

from tools.run_codex_web_bridge import (
    ProviderUnavailable,
    SOURCE_CODEX,
    SOURCE_DEEPSEEK,
    _normalize_deepseek_answer,
    process_job,
)


class FakeApi:
    def __init__(self) -> None:
        self.progress_calls = []
        self.completed_calls = []
        self.failed_calls = []

    def progress(self, request_id, worker_id, source):
        self.progress_calls.append((request_id, worker_id, source))

    def completed(self, request_id, worker_id, answer, source, label):
        self.completed_calls.append((request_id, worker_id, answer, source, label))

    def failed(self, request_id, worker_id, message):
        self.failed_calls.append((request_id, worker_id, message))


class FakeCodex:
    def __init__(self, answer=None, error=None) -> None:
        self.answer = answer
        self.error = error

    def ask(self, channel, question):
        if self.error:
            raise self.error
        return self.answer


class FakeDeepSeek:
    def __init__(self, answer=None, error=None) -> None:
        self.answer = answer
        self.error = error
        self.calls = []

    def ask(self, question):
        self.calls.append(question)
        if self.error:
            raise self.error
        return self.answer


JOB = {"request_id": "job-1", "channel": "general", "question": "原始问题"}


def test_codex_success_is_labelled_as_actual_gpt_source():
    api = FakeApi()
    deepseek = FakeDeepSeek(answer="不应调用")
    process_job(api, FakeCodex(answer="GPT 原文"), deepseek, job=JOB, worker_id="worker")

    assert api.progress_calls == [("job-1", "worker", SOURCE_CODEX)]
    assert api.completed_calls == [("job-1", "worker", "GPT 原文", SOURCE_CODEX, "GPT（Codex）")]
    assert api.failed_calls == []
    assert deepseek.calls == []


def test_codex_failure_switches_to_deepseek_and_labels_web_source():
    api = FakeApi()
    deepseek = FakeDeepSeek(answer="DeepSeek 原文")
    process_job(
        api,
        FakeCodex(error=ProviderUnavailable("proxy down")),
        deepseek,
        job=JOB,
        worker_id="worker",
    )

    assert api.progress_calls == [
        ("job-1", "worker", SOURCE_CODEX),
        ("job-1", "worker", SOURCE_DEEPSEEK),
    ]
    assert api.completed_calls == [
        ("job-1", "worker", "DeepSeek 原文", SOURCE_DEEPSEEK, "DeepSeek 网页")
    ]
    assert api.failed_calls == []
    assert deepseek.calls == ["原始问题"]


def test_both_provider_failures_do_not_claim_a_false_source():
    api = FakeApi()
    process_job(
        api,
        FakeCodex(error=ProviderUnavailable("GPT down")),
        FakeDeepSeek(error=ProviderUnavailable("login required")),
        job=JOB,
        worker_id="worker",
    )

    assert api.completed_calls == []
    assert len(api.failed_calls) == 1
    assert "GPT down" in api.failed_calls[0][2]
    assert "login required" in api.failed_calls[0][2]


def test_deepseek_citation_badges_are_rendered_inline():
    answer = "收盘价上涨-\n3\n。成交活跃-\n1\n-\n3\n。\n\n下一段。"

    assert _normalize_deepseek_answer(answer) == "收盘价上涨[3]。成交活跃[1][3]。\n\n下一段。"
