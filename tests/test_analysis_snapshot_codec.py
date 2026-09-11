"""Storage round trips preserve complete JSON; malformed references fail closed."""
from copy import deepcopy
import hashlib
import json

import pytest

from server.common import analysis_snapshot_codec as codec


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _payload():
    return {
        "schema": "example.snapshot.v1", "metadata_fact": {"中文": [True, 1, 1.0, None]},
        "analysis_rows": [{"a": {"proof": ["公告", {"value": 1}]}}, {"a": None}, {}],
        "candidate_rows": [{"a": {"proof": ["公告", {"value": 1}]}, "text": ' {"raw": 1.0} '}],
        "scored_rows": [{"a": {"proof": ["公告", {"value": 1}]}}, {"a": True},
                        {"a": 1}, {"a": 1.0}, {"a": -0.0}, {"a": False}],
    }


def _wire():
    return json.loads(codec.encode_snapshot_payload(_payload()))


def test_round_trip_preserves_canonical_hash_complete_cells_and_scalar_types():
    payload = _payload()
    original = deepcopy(payload)
    wire = codec.encode_snapshot_payload(payload)
    restored = codec.decode_snapshot_payload(wire)
    assert payload == original
    assert _canonical(restored) == _canonical(payload)
    assert hashlib.sha256(_canonical(restored)).digest() == hashlib.sha256(_canonical(payload)).digest()
    assert codec.encode_snapshot_payload(restored) == wire
    assert restored["analysis_rows"][1] == {"a": None}
    assert restored["analysis_rows"][2] == {}
    assert [type(row["a"]) for row in restored["scored_rows"]] == [dict, bool, int, float, float, bool]
    assert restored["candidate_rows"][0]["text"] == ' {"raw": 1.0} '
    pool = json.loads(wire)["values"]
    assert len({_canonical(value) for value in pool}) == len(pool)


def test_dictionary_is_global_and_restored_mutable_cells_have_no_aliases():
    payload = _payload()
    payload["analysis_rows"][0]["second"] = payload["analysis_rows"][0]["a"]
    raw = codec.encode_snapshot_payload(payload)
    wire = json.loads(raw)
    nested = _canonical(payload["analysis_rows"][0]["a"])
    assert sum(_canonical(value) == nested for value in wire["values"]) == 1
    restored = codec.decode_snapshot_payload(raw)
    restored["analysis_rows"][0]["a"]["proof"][1]["value"] = 999
    assert restored["analysis_rows"][0]["second"]["proof"][1]["value"] == 1
    assert restored["candidate_rows"][0]["a"]["proof"][1]["value"] == 1
    assert restored["scored_rows"][0]["a"]["proof"][1]["value"] == 1
    assert codec.decode_snapshot_payload(raw) == payload


def test_representation_is_deterministic_but_row_order_is_preserved():
    payload = _payload()
    reordered = {key: payload[key] for key in reversed(list(payload))}
    for name in ("analysis_rows", "candidate_rows", "scored_rows"):
        reordered[name] = [dict(reversed(list(row.items()))) for row in payload[name]]
    assert codec.encode_snapshot_payload(payload) == codec.encode_snapshot_payload(reordered)
    reordered["scored_rows"].reverse()
    assert codec.encode_snapshot_payload(payload) != codec.encode_snapshot_payload(reordered)
    assert _canonical(codec.decode_snapshot_payload(codec.encode_snapshot_payload(reordered))) == _canonical(reordered)


@pytest.mark.parametrize("rows", [[], [{}], [{}, {}], [{"": None}], [{"a": None}, {}, {"b": None}]])
def test_empty_and_sparse_rows_and_optional_candidate_table(rows):
    payload = {"analysis_rows": rows, "scored_rows": deepcopy(rows)}
    assert codec.decode_snapshot_payload(codec.encode_snapshot_payload(payload)) == payload


@pytest.mark.parametrize("mutation", [
    lambda w: w.update(extra=None),
    lambda w: w.pop("metadata"),
    lambda w: w.update(metadata=[]),
    lambda w: w["metadata"].update(scored_rows=[]),
    lambda w: w["metadata"].update(candidate_rows=[]),
    lambda w: w.update(values={}),
    lambda w: w.update(tables=[]),
    lambda w: w["tables"].pop("analysis_rows"),
    lambda w: w["tables"].update(analysis=w["tables"]["analysis_rows"]),
    lambda w: w["tables"]["analysis_rows"].update(extra=0),
    lambda w: w["tables"]["analysis_rows"].pop("indices"),
    lambda w: w["tables"].update(analysis_rows=[]),
    lambda w: w["tables"]["analysis_rows"].update(row_count=True),
    lambda w: w["tables"]["analysis_rows"].update(row_count=-1),
    lambda w: w["tables"]["analysis_rows"].update(row_count=3.0),
    lambda w: w["tables"]["analysis_rows"].update(row_count=10_001),
    lambda w: w["tables"]["analysis_rows"].update(columns="a"),
    lambda w: w["tables"]["analysis_rows"].update(columns=[False]),
    lambda w: w["tables"]["analysis_rows"].update(columns=["a", "a"]),
    lambda w: w["tables"]["analysis_rows"].update(columns=["b", "a"]),
    lambda w: w["tables"]["analysis_rows"].update(columns=[f"k{i:04}" for i in range(513)]),
    lambda w: w["tables"]["analysis_rows"].update(indices={}),
    lambda w: w["tables"]["analysis_rows"].update(indices=[]),
    lambda w: w["tables"]["analysis_rows"].update(indices=[{}]),
    lambda w: w["tables"]["analysis_rows"]["indices"][0].pop(),
    lambda w: w["tables"]["analysis_rows"]["indices"][0].append(0),
    lambda w: w["tables"]["analysis_rows"]["indices"][0].__setitem__(0, True),
    lambda w: w["tables"]["analysis_rows"]["indices"][0].__setitem__(0, -1),
    lambda w: w["tables"]["analysis_rows"]["indices"][0].__setitem__(0, 1.0),
    lambda w: w["tables"]["analysis_rows"]["indices"][0].__setitem__(0, 999),
    lambda w: w["tables"]["analysis_rows"]["indices"].__setitem__(0, [0, 0, 0]),
    lambda w: w["values"].append({"unused": "value"}),
    lambda w: w["values"].append(deepcopy(w["values"][0])),
])
def test_malformed_shape_or_references_are_rejected_before_copy(monkeypatch, mutation):
    wire = _wire()
    mutation(wire)
    monkeypatch.setattr(codec, "deepcopy", lambda _v: pytest.fail("invalid storage reached expansion"))
    with pytest.raises(ValueError):
        codec.decode_snapshot_payload(_canonical(wire))


def test_reordered_pool_with_adjusted_indices_is_noncanonical():
    wire = _wire()
    wire["values"][0], wire["values"][1] = wire["values"][1], wire["values"][0]
    for table in wire["tables"].values():
        table["indices"] = [[2 if index == 1 else 1 if index == 2 else index
                             for index in column] for column in table["indices"]]
    with pytest.raises(ValueError, match="pool order"):
        codec.decode_snapshot_payload(_canonical(wire))


@pytest.mark.parametrize("raw", [
    b'{}', b'[]', b'null', b'{"metadata":{},"metadata":{},"tables":{},"values":[]}',
    b'{"metadata":{"x":1,"x":2},"tables":{},"values":[]}',
    b'{"metadata":NaN,"tables":{},"values":[]}',
    b'{"metadata":Infinity,"tables":{},"values":[]}',
    b'{"metadata":1e9999,"tables":{},"values":[]}',
    b'\xff', b'{', b'{}{}', "{}", bytearray(b'{}'),
])
def test_invalid_json_and_non_byte_input_raise_value_error(raw):
    with pytest.raises(ValueError):
        codec.decode_snapshot_payload(raw)


def test_noncanonical_wire_whitespace_and_unicode_escaping_are_rejected():
    wire = _wire()
    for raw in (_canonical(wire) + b"\n", json.dumps(wire, ensure_ascii=True, sort_keys=True,
                                                   separators=(",", ":")).encode()):
        with pytest.raises(ValueError, match="not canonical"):
            codec.decode_snapshot_payload(raw)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), (1, 2), {1: "not a JSON key"}, object(), "\ud800"])
def test_encoder_rejects_non_json_cell_values(value):
    with pytest.raises(ValueError):
        codec.encode_snapshot_payload({"analysis_rows": [{"a": value}], "scored_rows": []})


@pytest.mark.parametrize("payload", [None, [], {1: []}, {"analysis_rows": []},
    {"analysis_rows": (), "scored_rows": []},
    {"analysis_rows": [None], "scored_rows": []},
    {"analysis_rows": [{1: "value"}], "scored_rows": []},
])
def test_encoder_rejects_invalid_logical_shapes(payload):
    with pytest.raises(ValueError):
        codec.encode_snapshot_payload(payload)


def test_expanded_size_accounts_for_escaped_keys_utf8_metadata_and_absence(monkeypatch):
    payload = _payload()
    payload["analysis_rows"] = [{"\"\n中文": [None, "\\\t"]}, {}, {"": None}]
    raw = codec.encode_snapshot_payload(payload)
    exact_size = len(_canonical(payload))
    monkeypatch.setattr(codec, "SNAPSHOT_EXPANDED_MAX_BYTES", exact_size)
    assert _canonical(codec.decode_snapshot_payload(raw)) == _canonical(payload)
    assert codec.encode_snapshot_payload(payload) == raw
    monkeypatch.setattr(codec, "SNAPSHOT_EXPANDED_MAX_BYTES", exact_size - 1)
    monkeypatch.setattr(codec, "deepcopy", lambda _v: pytest.fail("over-budget storage reached expansion"))
    with pytest.raises(ValueError, match="expanded size"):
        codec.decode_snapshot_payload(raw)
    with pytest.raises(ValueError, match="expanded size"):
        codec.encode_snapshot_payload(payload)


def test_dictionary_amplification_is_rejected_before_allocating_rows(monkeypatch):
    wire = {"metadata": {}, "values": ["x" * (256 * 1024)], "tables": {
        "analysis_rows": {"columns": ["a"], "row_count": 1024, "indices": [[1] * 1024]},
        "scored_rows": {"columns": [], "row_count": 0, "indices": []},
    }}
    monkeypatch.setattr(codec, "deepcopy", lambda _v: pytest.fail("dictionary bomb was expanded"))
    with pytest.raises(ValueError, match="expanded size"):
        codec.decode_snapshot_payload(_canonical(wire))


def test_cell_budget_counts_absent_slots_across_all_tables(monkeypatch):
    wire = _wire()
    count = sum(table["row_count"] * len(table["columns"]) for table in wire["tables"].values())
    monkeypatch.setattr(codec, "SNAPSHOT_MAX_CELLS", count)
    assert codec.decode_snapshot_payload(_canonical(wire)) == _payload()
    monkeypatch.setattr(codec, "SNAPSHOT_MAX_CELLS", count - 1)
    with pytest.raises(ValueError, match="cell count"):
        codec.decode_snapshot_payload(_canonical(wire))
    with pytest.raises(ValueError, match="dimensions"):
        codec.encode_snapshot_payload(_payload())


def test_excessive_json_nesting_is_a_controlled_value_error():
    value = []
    for _ in range(2000):
        value = [value]
    with pytest.raises(ValueError):
        codec.encode_snapshot_payload({"analysis_rows": [{"a": value}], "scored_rows": []})
    with pytest.raises(ValueError):
        codec.decode_snapshot_payload(b"[" * 2000 + b"]" * 2000)
