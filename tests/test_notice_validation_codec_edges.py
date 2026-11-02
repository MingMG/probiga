"""Reject incomplete or ambiguous MySQL notice readback packages."""
import gzip
import json
import zlib

import pytest

from server.common import scheduler_validation as validation


def _row(**changes):
    row = {field: None for field in validation._NOTICE_READBACK_FIELDS}
    row.update(stock_code="000001", art_code="AN-1", notice_date="2026-11-02",
               title="公告：完整业务值", display_time="2026-11-02T20:00:00.123456",
               association_validated=1, received_at="2026-11-02 20:00:00.123456")
    return {**row, **changes}


def _frame(raw, *, declared=None):
    compressed = zlib.compress(raw)
    return ((len(raw) if declared is None else declared).to_bytes(4, "little")
            + compressed + (b"." if compressed[-1:] == b" " else b""))


def _json_frame(rows):
    return _frame(json.dumps(rows, ensure_ascii=False).encode("utf-8"))


def _decode(packed, **changes):
    group = {"stock_code": "000001", "row_count": 1, "packed_rows": packed}
    return validation._decode_notice_readback_group(
        {**group, **changes}, expected_codes={"000001"})


def test_complete_unicode_package_preserves_every_sql_value_and_microsecond():
    original = _row()
    for packed in (_json_frame([original]), bytearray(_json_frame([original])),
                   memoryview(_json_frame([original]))):
        assert _decode(packed) == [original]


@pytest.mark.parametrize("changes", [
    {"stock_code": "000002"}, {"stock_code": "1"}, {"stock_code": None},
    {"row_count": True}, {"row_count": "1"}, {"row_count": 1.0},
    {"row_count": 0}, {"row_count": -1}, {"row_count": 10001},
    {"packed_rows": "bytes"}, {"packed_rows": None},
])
def test_group_requires_exact_sql_types_security_and_bounded_count(changes):
    with pytest.raises(RuntimeError, match="identity/type"):
        _decode(_json_frame([_row()]), **changes)


@pytest.mark.parametrize("packed", [b"", b"\x01", b"\x01\x00\x00",
                                     b"\x01\x00\x00\x00"])
def test_short_mysql_length_headers_never_form_a_package(packed):
    with pytest.raises(RuntimeError, match="truncated"):
        _decode(packed)


@pytest.mark.parametrize("declared", [0, validation._NOTICE_READBACK_MAX_JSON_BYTES + 1])
def test_declared_length_is_bounded_before_decompression(declared):
    with pytest.raises(RuntimeError, match="declared decompressed size"):
        _decode(_frame(b"[]", declared=declared))


@pytest.mark.parametrize("delta", [-1, 1])
def test_declared_length_must_equal_complete_decompressed_json(delta):
    raw = json.dumps([_row()]).encode()
    with pytest.raises(RuntimeError, match="compressed length"):
        _decode(_frame(raw, declared=len(raw) + delta))


def test_little_endian_header_is_not_silently_treated_as_big_endian():
    raw = json.dumps([_row()]).encode()
    packed = len(raw).to_bytes(4, "big") + zlib.compress(raw)
    with pytest.raises(RuntimeError):
        _decode(packed)


def test_compressed_package_itself_has_a_wire_size_budget():
    packed = b"x" * (validation._NOTICE_READBACK_MAX_PACKAGE_BYTES + 1)
    with pytest.raises(RuntimeError, match="package size"):
        _decode(packed)


def test_actual_inflation_cannot_exceed_budget_even_with_small_declared_size():
    raw = b" " * (validation._NOTICE_READBACK_MAX_JSON_BYTES + 1)
    with pytest.raises(RuntimeError, match="inflation limit"):
        _decode(_frame(raw, declared=1))


@pytest.mark.parametrize("variant", ["truncated", "gzip", "raw_deflate", "not_zlib",
                                      "extra_bytes", "second_stream", "false_mysql_dot"])
def test_only_one_complete_mysql_zlib_stream_and_its_exact_tail_are_allowed(variant):
    raw = json.dumps([_row()]).encode()
    compressed = zlib.compress(raw)
    if variant == "truncated":
        compressed = compressed[:-1]
    elif variant == "gzip":
        compressed = gzip.compress(raw)
    elif variant == "raw_deflate":
        compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        compressed = compressor.compress(raw) + compressor.flush()
    elif variant == "not_zlib":
        compressed = b"not a zlib stream"
    elif variant == "extra_bytes":
        compressed += b"junk"
    elif variant == "second_stream":
        compressed += zlib.compress(b"[]")
    else:
        # The sentinel is legal only after a compressed last byte of 0x20.
        assert compressed[-1:] != b" "
        compressed += b"."
    with pytest.raises(RuntimeError):
        _decode(len(raw).to_bytes(4, "little") + compressed)


@pytest.mark.parametrize("raw", [
    b"\xff", b"[", b"{}", b"null", b"true", b"[]", b"[1]", b"[null]",
    b'[{"stock_code":"000001","stock_code":"000001"}]',
    b'[{"title":NaN}]', b'[{"title":Infinity}]', b'[{"title":-Infinity}]',
])
def test_utf8_json_shape_duplicate_keys_and_nonfinite_constants_fail_closed(raw):
    with pytest.raises(RuntimeError):
        _decode(_frame(raw))


@pytest.mark.parametrize("changes", [
    {"stock_code": "000002"}, {"stock_code": 1}, {"art_code": " "},
    {"art_code": None}, {"art_code": 7}, {"title": ["公告"]},
    {"notice_date": 20261102}, {"received_at": {"datetime": "2026-11-02"}},
    {"association_validated": True}, {"association_validated": "1"},
    {"association_validated": 1.0},
])
def test_each_full_row_requires_string_sql_values_and_exact_security_identity(changes):
    with pytest.raises(RuntimeError, match="fields/security identity"):
        _decode(_json_frame([_row(**changes)]))


def test_all_seventeen_fields_are_required_without_extra_or_duplicate_rows():
    missing = _row()
    missing.pop("permission_status")
    for rows in ([missing], [{**_row(), "unexpected": "value"}], [_row(), _row()]):
        with pytest.raises(RuntimeError):
            _decode(_json_frame(rows), row_count=len(rows))


def test_valid_json_row_count_must_match_independent_sql_count():
    with pytest.raises(RuntimeError, match="JSON row count"):
        _decode(_json_frame([_row()]), row_count=2)
