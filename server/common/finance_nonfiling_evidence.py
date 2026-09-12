"""Bounded PDF-body evidence for a company's current non-filing fact.

Announcement identity, provenance, URL and point-in-time eligibility belong to
the caller. This module deliberately accepts only conservative factual wording.
"""
from __future__ import annotations

from datetime import date
from hashlib import sha256
from io import BytesIO
import re
from typing import Any, Mapping


EVIDENCE_SCHEMA = "probiga.cninfo-finance-nonfiling-evidence.v2"
MAX_PDF_BYTES = 8 * 1024 * 1024
MAX_PDF_PAGES = 30
MAX_TEXT_BYTES = 256 * 1024
_HASH = re.compile(r"[0-9a-f]{64}")
_CODE = re.compile(r"(?:证券|股票)代码[:：]([0-9]{6})(?![0-9])")
_QUOTED = re.compile(r"《[^》]*》|“[^”]*”|「[^」]*」|『[^』]*』|\"[^\"]*\"")
_SUBJECT = r"(?:(?:因|由于|鉴于)(?:本公司|公司)|(?<![\u3400-\u9fffA-Za-z0-9])(?:本公司|公司))"
_NEGATIVE = (
    r"(?:目前|至今)?(?:尚未|仍未|未能|未)"
    r"(?:按期|按时|按规定|如期|及时|"
    r"在(?:法定|规定|指定)?(?:期限|时间)内"
    r"(?:[（(][^（）()。；;!?！？]{1,40}[）)])?)?"
    r"披露"
)
_CONDITIONAL = re.compile(
    r"如果|假如|假设|倘若|若|可能|预计|拟|例如|引用|转述|引述|援引|"
    r"转载|转发|摘录|案例|公告称|报道称|表示|声称|指出|"
    r"并非|并不|不是|不代表|不意味着|否认|澄清|不实|"
    r"其他公司|另一家公司|子公司|关联公司|规定|法规|规则|"
    r"据[^。；;!?！？]{0,60}公告"
)
_COMPLETED = (
    r"(?:已(?:经|于[^，,。；;!?！？]{1,25})?(?:完成|补充|补发)?披露|"
    r"完成披露|补充披露|补发披露)"
)


def _checked_text(text: str) -> str:
    if not isinstance(text, str) or not text.strip() or "\ufffd" in text:
        raise ValueError("FINANCE_NONFILING_TEXT_INVALID")
    try:
        size = len(text.encode("utf-8"))
    except UnicodeError:
        raise ValueError("FINANCE_NONFILING_TEXT_INVALID") from None
    if size > MAX_TEXT_BYTES:
        raise ValueError("FINANCE_NONFILING_TEXT_TOO_LARGE")
    return text


def extract_document_text(raw: bytes) -> str:
    """Extract text without OCR; unreadable/scanned documents prove nothing."""
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_PDF_BYTES:
        raise ValueError("FINANCE_NONFILING_PDF_SIZE_INVALID")
    if not raw.startswith(b"%PDF-"):
        raise ValueError("FINANCE_NONFILING_PDF_INVALID")
    # Import lazily: ordinary finance readers need not initialize a PDF parser.
    from pypdf import PdfReader

    try:
        reader = PdfReader(BytesIO(raw), strict=True)
        if reader.is_encrypted:
            raise ValueError("FINANCE_NONFILING_PDF_ENCRYPTED")
        if not 1 <= len(reader.pages) <= MAX_PDF_PAGES:
            raise ValueError("FINANCE_NONFILING_PDF_PAGE_LIMIT")
        pages: list[str] = []
        byte_count = 0
        for page in reader.pages:
            text = page.extract_text() or ""
            if not isinstance(text, str) or "\ufffd" in text:
                raise ValueError("FINANCE_NONFILING_TEXT_INVALID")
            byte_count += len(text.encode("utf-8")) + (1 if pages else 0)
            if byte_count > MAX_TEXT_BYTES:
                raise ValueError("FINANCE_NONFILING_TEXT_TOO_LARGE")
            pages.append(text)
        return _checked_text("\n".join(pages).strip())
    except ValueError as exc:
        if str(exc).startswith("FINANCE_NONFILING_"):
            raise
        raise ValueError("FINANCE_NONFILING_PDF_INVALID") from None
    except Exception:
        raise ValueError("FINANCE_NONFILING_PDF_INVALID") from None


def _period_pattern(stock_code: str, report_date: str) -> str:
    if not isinstance(stock_code, str) or re.fullmatch(r"[0-9]{6}", stock_code) is None:
        raise ValueError("FINANCE_NONFILING_STOCK_CODE_INVALID")
    try:
        period = date.fromisoformat(report_date)
        if period.isoformat() != report_date:
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError("FINANCE_NONFILING_REPORT_DATE_INVALID") from None
    names = {
        (3, 31): r"年(?:第一|一)季度报告",
        (6, 30): r"年半年度报告",
        (9, 30): r"年(?:第三|三)季度报告",
        (12, 31): r"(?:年年度|年度)报告",
    }
    name = names.get((period.month, period.day))
    if name is None:
        raise ValueError("FINANCE_NONFILING_REPORT_DATE_INVALID")
    return rf"(?<![0-9]){period.year}{name}(?!摘要|更正|补充|英文)"


def nonfiling_statement(text: str, stock_code: str, report_date: str) -> str | None:
    """Return a whitespace-normalized fact, never a predicted filing failure."""
    period = _period_pattern(stock_code, report_date)
    compact = re.sub(r"\s+", "", _checked_text(text))
    if set(_CODE.findall(compact)) != {stock_code}:
        return None
    # A quoted announcement title or a cited rule is not the company's current
    # factual statement. Do not concatenate across the removed quotation.
    body = _QUOTED.sub(" ", compact)
    statement = re.compile(_SUBJECT + _NEGATIVE + period)
    completed = re.compile(_COMPLETED + r"[^，,。；;!?！？]{0,30}?" + period)
    # Later completion of this same report supersedes a historical failure.
    # Conservative rejection also covers an ambiguous third-party completion.
    if completed.search(body):
        return None
    for sentence in re.split(r"[。；;!?！？]", body):
        for match in statement.finditer(sentence):
            prefix = sentence[:match.start()]
            if _CONDITIONAL.search(prefix):
                continue
            suffix = sentence[match.end():]
            if re.match(r"(?:的|时|则|，则|,则)", suffix):
                continue
            return match.group(0)
    return None


def build_document_evidence(
    raw: bytes, stock_code: str, report_date: str,
) -> dict[str, str] | None:
    text = extract_document_text(raw)
    statement = nonfiling_statement(text, stock_code, report_date)
    if statement is None:
        return None
    return {
        "evidence_schema": EVIDENCE_SCHEMA,
        "announcement_document_sha256": sha256(raw).hexdigest(),
        "announcement_document_text": text,
        "announcement_document_text_sha256": sha256(text.encode("utf-8")).hexdigest(),
        "nonfiling_statement": statement,
    }


def validate_document_body_evidence(
    evidence: Mapping[str, Any], stock_code: str, report_date: str,
) -> None:
    """Validate the body seal; the raw-PDF digest is a format-only check here."""
    if not isinstance(evidence, Mapping) or evidence.get("evidence_schema") != EVIDENCE_SCHEMA:
        raise ValueError("FINANCE_NONFILING_EVIDENCE_SCHEMA_INVALID")
    for name in ("announcement_document_sha256", "announcement_document_text_sha256"):
        value = evidence.get(name)
        if not isinstance(value, str) or _HASH.fullmatch(value) is None:
            raise ValueError("FINANCE_NONFILING_EVIDENCE_HASH_INVALID")
    text = _checked_text(evidence.get("announcement_document_text"))
    if sha256(text.encode("utf-8")).hexdigest() != evidence["announcement_document_text_sha256"]:
        raise ValueError("FINANCE_NONFILING_EVIDENCE_TEXT_HASH_MISMATCH")
    statement = nonfiling_statement(text, stock_code, report_date)
    if statement is None or evidence.get("nonfiling_statement") != statement:
        raise ValueError("FINANCE_NONFILING_EVIDENCE_STATEMENT_MISMATCH")
