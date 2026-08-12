from __future__ import annotations

import json
import logging

import pytest
from sqlalchemy import create_engine, text

from biz.early_briefing import generate as early_generate
from biz.evening_review import generate as evening_generate
from integrations.wecom import delivery


class _Response:
    def __init__(self, status_code: int = 200, payload: object = None) -> None:
        self.status_code = status_code
        self._payload = {"errcode": 0, "errmsg": "ok"} if payload is None else payload

    def json(self):
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload


class _Client:
    def __init__(self, responses: list[_Response], captured: list[tuple[str, dict]]) -> None:
        self._responses = iter(responses)
        self._captured = captured

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def post(self, url: str, *, json: dict, timeout: float):
        self._captured.append((url, json))
        return next(self._responses)


def _install_client(monkeypatch, responses: list[_Response]):
    captured: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        delivery.httpx,
        "Client",
        lambda *, timeout: _Client(responses, captured),
    )
    return captured


def _receipt(engine):
    with engine.connect() as connection:
        return connection.execute(
            text(f"SELECT * FROM {delivery.DELIVERY_RECEIPT_TABLE}")
        ).mappings().one()


def _receipt_indexes(engine):
    with engine.connect() as connection:
        rows = connection.execute(
            text(f"PRAGMA index_list('{delivery.DELIVERY_RECEIPT_TABLE}')")
        ).mappings().all()
        return {
            str(row["name"]): tuple(
                item["name"]
                for item in connection.execute(
                    text(f"PRAGMA index_info('{row['name']}')")
                ).mappings().all()
            )
            for row in rows
            if not str(row["name"]).startswith("sqlite_autoindex_")
        }


def _create_legacy_receipt_table(engine):
    with engine.begin() as connection:
        connection.execute(text(delivery._RECEIPT_DDL))


def test_http_client_request_logs_are_suppressed_to_protect_webhook_urls():
    assert logging.getLogger("httpx").getEffectiveLevel() >= logging.WARNING
    assert logging.getLogger("httpcore").getEffectiveLevel() >= logging.WARNING


def test_new_receipt_table_has_started_at_retention_index():
    engine = create_engine("sqlite+pysqlite:///:memory:")

    delivery.ensure_delivery_receipt_table(engine)

    assert _receipt_indexes(engine)[delivery.DELIVERY_RECEIPT_STARTED_AT_INDEX] == (
        "started_at",
    )


def test_legacy_receipt_table_gets_started_at_index_idempotently():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_legacy_receipt_table(engine)
    assert _receipt_indexes(engine) == {}

    delivery.ensure_delivery_receipt_table(engine)
    delivery.ensure_delivery_receipt_table(engine)

    indexes = _receipt_indexes(engine)
    assert indexes == {
        delivery.DELIVERY_RECEIPT_STARTED_AT_INDEX: ("started_at",),
    }


def test_equivalent_legacy_started_at_index_is_not_duplicated():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_legacy_receipt_table(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                f"CREATE INDEX custom_receipt_retention "
                f"ON {delivery.DELIVERY_RECEIPT_TABLE} (started_at)"
            )
        )

    delivery.ensure_delivery_receipt_table(engine)

    assert _receipt_indexes(engine) == {
        "custom_receipt_retention": ("started_at",),
    }


def test_utf8_segmentation_is_lossless_and_includes_headers_in_limit():
    # No paragraph or line boundary: this was the old implementation's data-loss case.
    content = "甲🙂乙" * 1500 + "尾部不可丢"

    segments = delivery.build_markdown_segments(
        content,
        title="## 早报",
        max_bytes=4000,
    )

    assert len(segments) > 1
    assert "".join(segment.body for segment in segments) == content
    assert segments[-1].body.endswith("尾部不可丢")
    assert all(len(segment.message.encode("utf-8")) <= 4000 for segment in segments)
    assert all(
        segment.message.startswith(f"## 早报 ({index}/{len(segments)})\n\n")
        for index, segment in enumerate(segments, start=1)
    )


def test_short_message_is_sent_unchanged():
    content = "## 已有标题\n\n正文"
    segments = delivery.build_markdown_segments(content, title="## 早报")
    assert len(segments) == 1
    assert segments[0].body == content
    assert segments[0].message == content


def test_successful_delivery_records_a_sanitized_auditable_receipt(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=top-secret"
    captured = _install_client(monkeypatch, [_Response()])

    result = delivery.deliver_markdown(
        webhook,
        "盘前正文",
        engine=engine,
        delivery_kind="early_briefing",
        title="## 早报",
        pause_seconds=0,
    )

    assert result.success is True
    assert result.delivered_count == result.segment_count == 1
    assert captured[0][0] == webhook
    receipt = _receipt(engine)
    assert receipt["status"] == "SUCCEEDED"
    assert receipt["delivered_count"] == 1
    assert receipt["content_sha256"] == result.content_sha256
    serialized_receipt = json.dumps(dict(receipt), ensure_ascii=False, default=str)
    assert webhook not in serialized_receipt
    assert "top-secret" not in serialized_receipt


def test_missing_webhook_is_failure_and_is_recorded(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    captured = _install_client(monkeypatch, [])

    with pytest.raises(delivery.WeComDeliveryError, match="not configured"):
        delivery.deliver_markdown(
            None,
            "正文",
            engine=engine,
            delivery_kind="evening_review",
            title="## 复盘",
            pause_seconds=0,
        )

    assert captured == []
    receipt = _receipt(engine)
    assert receipt["status"] == "FAILED"
    assert receipt["error_code"] == "MISSING_WEBHOOK"
    assert receipt["finished_at"] is not None


def test_nonzero_errcode_makes_partial_delivery_fail(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    captured = _install_client(
        monkeypatch,
        [
            _Response(),
            _Response(payload={"errcode": 93000, "errmsg": "invalid webhook key=leak-me"}),
            _Response(),
        ],
    )
    content = "段落🙂" * 80

    with pytest.raises(delivery.WeComDeliveryError, match="incomplete") as exc_info:
        delivery.deliver_markdown(
            "https://example.invalid/send?key=secret-key",
            content,
            engine=engine,
            delivery_kind="evening_review",
            title="## 复盘",
            max_bytes=180,
            pause_seconds=0,
        )

    assert exc_info.value.result is not None
    assert 0 < exc_info.value.result.delivered_count < exc_info.value.result.segment_count
    receipt = _receipt(engine)
    assert receipt["status"] == "PARTIAL"
    assert receipt["error_code"] == "WECOM_ERRCODE"
    assert "leak-me" not in (receipt["error_message"] or "")
    segment_receipts = json.loads(receipt["segments_json"])
    assert len(segment_receipts) == len(captured)
    assert any(not item["success"] for item in segment_receipts)
    assert all(len(item["message_sha256"]) == 64 for item in segment_receipts)


@pytest.mark.parametrize(
    ("response", "expected_code"),
    [
        (_Response(status_code=503), "HTTP_STATUS"),
        (_Response(payload=ValueError("not json")), "INVALID_JSON"),
        (_Response(payload={"errmsg": "missing errcode"}), "WECOM_ERRCODE"),
    ],
)
def test_http_and_malformed_responses_fail_closed(monkeypatch, response, expected_code):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _install_client(monkeypatch, [response])

    with pytest.raises(delivery.WeComDeliveryError):
        delivery.deliver_markdown(
            "https://example.invalid/send?key=secret-key",
            "正文",
            engine=engine,
            delivery_kind="early_briefing",
            title="## 早报",
            pause_seconds=0,
        )

    receipt = _receipt(engine)
    assert receipt["status"] == "FAILED"
    assert receipt["error_code"] == expected_code


def test_transport_exception_never_exposes_webhook_or_key(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    webhook = "https://example.invalid/send?key=opaque-value-123"

    class _FailingClient(_Client):
        def post(self, url: str, *, json: dict, timeout: float):
            raise delivery.httpx.ConnectError(f"failed for {url}")

    monkeypatch.setattr(
        delivery.httpx,
        "Client",
        lambda *, timeout: _FailingClient([], []),
    )

    with pytest.raises(delivery.WeComDeliveryError) as exc_info:
        delivery.deliver_markdown(
            webhook,
            "正文",
            engine=engine,
            delivery_kind="early_briefing",
            title="## 早报",
            pause_seconds=0,
        )

    receipt = _receipt(engine)
    serialized = json.dumps(dict(receipt), ensure_ascii=False, default=str)
    assert webhook not in str(exc_info.value)
    assert "opaque-value-123" not in str(exc_info.value)
    assert webhook not in serialized
    assert "opaque-value-123" not in serialized


@pytest.mark.parametrize(
    "module",
    [early_generate, evening_generate],
)
def test_report_push_wrapper_propagates_delivery_failure(monkeypatch, module):
    sentinel_engine = object()
    monkeypatch.setattr(module, "get_wecom_webhook", lambda *_args, **_kwargs: None)

    def fail(*_args, **kwargs):
        assert kwargs["engine"] is sentinel_engine
        raise delivery.WeComDeliveryError(
            "briefing webhook is not configured",
            delivery_id="delivery-test",
        )

    monkeypatch.setattr(module, "deliver_markdown", fail)

    with pytest.raises(delivery.WeComDeliveryError):
        module.push_to_wecom("正文", engine=sentinel_engine)
