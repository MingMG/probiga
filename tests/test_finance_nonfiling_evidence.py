from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from types import SimpleNamespace

import pytest

from server.common import finance_nonfiling_evidence as evidence


CODE = "002731"
PERIOD = "2026-06-30"
BODY = (
    "证券代码：002731 证券简称：测试公司\n"
    "因公司未在法定期限内（2026年8月31日）披露2026年半年度报告，"
    "公司股票已于2026年9月1日起停牌。"
)
STATEMENT = "因公司未在法定期限内（2026年8月31日）披露2026年半年度报告"


def _mock_reader(monkeypatch, pages, *, encrypted=False):
    import pypdf
    monkeypatch.setattr(
        pypdf, "PdfReader",
        lambda *_args, **_kwargs: SimpleNamespace(
            is_encrypted=encrypted,
            pages=[SimpleNamespace(extract_text=lambda value=value: value) for value in pages],
        ),
    )


def _sealed_body(text=BODY):
    return {
        "evidence_schema": evidence.EVIDENCE_SCHEMA,
        "announcement_document_sha256": "a" * 64,
        "announcement_document_text": text,
        "announcement_document_text_sha256": sha256(text.encode()).hexdigest(),
        "nonfiling_statement": evidence.nonfiling_statement(text, CODE, PERIOD),
    }


@pytest.mark.parametrize("text", [
    BODY,
    BODY.replace("2026年", "2026 年 ").replace("披露", "披\n露"),
    "股票代码:002731。\n公司尚未披露2026年半年度报告。",
    "证券代码：002731。截至本公告披露日，本公司仍未披露2026年半年度报告。",
    "证券代码：002731。公司未能按期披露2026年半年度报告。",
])
def test_requires_actual_company_nonfiling_with_precise_code_and_period(text):
    assert evidence.nonfiling_statement(text, CODE, PERIOD) is not None


@pytest.mark.parametrize(("period", "title"), [
    ("2026-03-31", "2026年第一季度报告"),
    ("2026-06-30", "2026年半年度报告"),
    ("2026-09-30", "2026年第三季度报告"),
    ("2025-12-31", "2025年年度报告"),
    ("2025-12-31", "2025年度报告"),
])
def test_periods_are_exact(period, title):
    text = "证券代码：002731。公司尚未披露" + title + "。"
    assert evidence.nonfiling_statement(text, CODE, period) == "公司尚未披露" + title
    assert evidence.nonfiling_statement(text, CODE, "2024-06-30") is None


@pytest.mark.parametrize("sentence", [
    "上市公司未在法定期限内披露2026年半年度报告的，应当停牌。",
    "如果公司未在法定期限内披露2026年半年度报告，将被停牌。",
    "若本公司未按期披露2026年半年度报告，将被停牌。",
    "预计公司未能按期披露2026年半年度报告。",
    "公司可能无法在法定期限内披露2026年半年度报告。",
    "公司拟于下周披露2026年半年度报告。",
    "公司将无法按期披露2026年半年度报告。",
    "公司曾未按期披露2026年半年度报告。",
    "公司已披露2026年半年度报告。",
    "公司已补充披露2026年半年度报告。",
    "公司已于2026年9月1日披露2026年半年度报告。",
    "公司尚未披露2025年年度报告。",
    "公司尚未披露2026年半年度报告摘要。",
    "公司尚未披露2026年半年度报告更正公告。",
    "其他公司未在法定期限内披露2026年半年度报告。",
    "子公司未在法定期限内披露2026年半年度报告。",
    "公司引用其他公司公告称：因公司未在法定期限内披露2026年半年度报告。",
    "据甲公司公告，因公司未在法定期限内披露2026年半年度报告。",
    "甲公司表示：公司未在法定期限内披露2026年半年度报告。",
    "本公司并非因公司未在法定期限内披露2026年半年度报告而停牌。",
    "公司澄清：因公司未在法定期限内披露2026年半年度报告的传闻不实。",
    "根据规则规定：公司未在法定期限内披露2026年半年度报告的，应当停牌。",
    "公司披露了《关于公司未在法定期限内披露2026年半年度报告的公告》。",
    "公司转述：“因公司未在法定期限内披露2026年半年度报告”。",
    "公司否认未披露2026年半年度报告。",
    "公司未否认已披露2026年半年度报告。",
])
def test_hypothetical_third_party_completed_or_wrong_period_does_not_prove_nonfiling(sentence):
    assert evidence.nonfiling_statement("证券代码：002731。" + sentence, CODE, PERIOD) is None


@pytest.mark.parametrize("extra", [
    "现已披露2026年半年度报告。",
    "公司已于2026年9月2日补充披露2026年半年度报告。",
    "本公司已完成披露2026年半年度报告。",
])
def test_current_completion_supersedes_historical_failure(extra):
    assert evidence.nonfiling_statement(BODY + extra, CODE, PERIOD) is None


def test_other_period_completion_does_not_erase_precise_missing_period():
    assert evidence.nonfiling_statement(BODY + "公司已披露2025年年度报告。", CODE, PERIOD) == STATEMENT


@pytest.mark.parametrize("text", [
    BODY.replace("002731", "002732"),
    BODY.replace("证券代码：002731", "002731"),
    BODY.replace("002731", "1002731"),
    BODY.replace("002731", "0027310"),
    BODY + "其他股票代码：000001。",
])
def test_code_must_be_exact_and_unambiguous(text):
    assert evidence.nonfiling_statement(text, CODE, PERIOD) is None


@pytest.mark.parametrize(("code", "period"), [
    ("2731", PERIOD), ("002731.SZ", PERIOD), ("００２７３１", PERIOD),
    (CODE, "2026-06-29"), (CODE, "20260630"), (CODE, "2026-06-30T00:00:00"),
])
def test_bad_caller_identity_is_an_error(code, period):
    with pytest.raises(ValueError):
        evidence.nonfiling_statement(BODY, code, period)


def test_pdf_extraction_builds_replayable_exact_body_seal(monkeypatch):
    _mock_reader(monkeypatch, [BODY[:20], BODY[20:]])
    raw = b"%PDF-1.7\nsynthetic bounded reader fixture"
    result = evidence.build_document_evidence(raw, CODE, PERIOD)
    assert result["announcement_document_sha256"] == sha256(raw).hexdigest()
    assert result["announcement_document_text"] == BODY[:20] + "\n" + BODY[20:]
    assert result["nonfiling_statement"] == STATEMENT
    evidence.validate_document_body_evidence(result | {"announcement_id": "caller-owned"}, CODE, PERIOD)


def test_readable_document_without_current_fact_returns_none(monkeypatch):
    _mock_reader(monkeypatch, ["证券代码：002731。公司已披露2026年半年度报告。"])
    assert evidence.build_document_evidence(b"%PDF-1.7\nfixture", CODE, PERIOD) is None


@pytest.mark.parametrize("raw", [b"", b"not pdf", b"%PDF-" + b"x" * evidence.MAX_PDF_BYTES], ids=["empty", "not_pdf", "too_large"])
def test_invalid_pdf_input_is_rejected_before_parser(raw):
    with pytest.raises(ValueError):
        evidence.extract_document_text(raw)


@pytest.mark.parametrize(("pages", "encrypted", "reason"), [
    (["body"], True, "ENCRYPTED"),
    ([], False, "PAGE_LIMIT"),
    (["body"] * 31, False, "PAGE_LIMIT"),
    (["", "  "], False, "TEXT_INVALID"),
    (["body\ufffd"], False, "TEXT_INVALID"),
    (["中" * (evidence.MAX_TEXT_BYTES // 3 + 1)], False, "TEXT_TOO_LARGE"),
    (["x" * evidence.MAX_TEXT_BYTES, "x"], False, "TEXT_TOO_LARGE"),
])
def test_pdf_output_budgets_and_decoding_are_strict(monkeypatch, pages, encrypted, reason):
    _mock_reader(monkeypatch, pages, encrypted=encrypted)
    with pytest.raises(ValueError, match=reason):
        evidence.extract_document_text(b"%PDF-1.7\nfixture")


def test_real_pdf_parser_rejects_empty_and_encrypted_pdf():
    from pypdf import PdfWriter
    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    raw = BytesIO()
    writer.write(raw)
    with pytest.raises(ValueError, match="TEXT_INVALID"):
        evidence.extract_document_text(raw.getvalue())
    writer.encrypt("fixture-password")
    raw = BytesIO()
    writer.write(raw)
    with pytest.raises(ValueError, match="ENCRYPTED"):
        evidence.extract_document_text(raw.getvalue())


@pytest.mark.parametrize(("name", "value"), [
    ("evidence_schema", "probiga.cninfo-finance-nonfiling-evidence.v1"),
    ("announcement_document_sha256", "A" * 64),
    ("announcement_document_text_sha256", "0" * 64),
    ("announcement_document_text", BODY + "tampered"),
    ("announcement_document_text", ""),
    ("nonfiling_statement", "公司未披露报告"),
    ("nonfiling_statement", None),
])
def test_replay_rejects_schema_hash_or_statement_tampering(name, value):
    sealed = _sealed_body()
    sealed[name] = value
    with pytest.raises(ValueError):
        evidence.validate_document_body_evidence(sealed, CODE, PERIOD)


def test_rehashed_hypothetical_text_is_still_not_evidence():
    sealed = _sealed_body("证券代码：002731。若公司未在法定期限内披露2026年半年度报告。")
    sealed["nonfiling_statement"] = STATEMENT
    with pytest.raises(ValueError, match="STATEMENT_MISMATCH"):
        evidence.validate_document_body_evidence(sealed, CODE, PERIOD)
