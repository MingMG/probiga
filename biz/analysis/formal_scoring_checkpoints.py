"""Registered formal-score factor checkpoints with exact Pandas values."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
import math
from typing import TYPE_CHECKING, Any, Callable, Mapping

import numpy as np
import pandas as pd

from server.common.analysis_pool_receipt import canonical_sha256

if TYPE_CHECKING:
    from server.common.formal_analysis_store import FormalAnalysisStore


_SCHEMA = "probiga.formal-scoring-frame.v1"
_STOCK_FACTORS = "probiga.formal-scoring-stock-factors.v1"
_DEFAULT_ROWS_PER_PART = 1024
_MAX_ROWS_PER_PART = 4096
_MAX_FRAME_ROWS = 200_000
_BATCH_PARTS = 4


def formal_factor_generation_sha256(
    *, trade_date: str, decision_at: datetime, stage: str, source_identity: Mapping[str, Any],
) -> str:
    return canonical_sha256({
        "schema": _SCHEMA,
        "trade_date": trade_date,
        "decision_at": decision_at.isoformat(timespec="microseconds"),
        "stage": stage,
        "source": dict(source_identity),
    })


def stock_factor_input_hashes(
    frame: pd.DataFrame, *, codes: list[str], columns: tuple[str, ...],
) -> dict[str, str]:
    """Hash each security's ordered dated input values without per-row SQL."""
    groups = frame.groupby("stock_code", sort=False).indices
    dates = pd.to_datetime(frame["trade_date"]).to_numpy(dtype="datetime64[D]").astype("<i8")
    values = [frame[column].to_numpy(copy=False) for column in columns]
    fingerprints = {}
    for code in codes:
        positions = groups[code]
        digest = hashlib.sha256()
        digest.update(len(positions).to_bytes(8, "little"))
        digest.update(dates.take(positions).tobytes())
        for series in values:
            stock = series.take(positions)
            digest.update(
                canonical_sha256([_pack(value) for value in stock]).encode("ascii")
                if stock.dtype.hasobject else stock.tobytes()
            )
        fingerprints[code] = digest.hexdigest()
    return fingerprints


def _pack(value: Any) -> Any:
    if value is pd.NA:
        return {"missing": "NA"}
    if value is pd.NaT:
        return {"missing": "NaT"}
    if isinstance(value, np.generic):
        value = value.item()
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {"float": value.hex()}
    if isinstance(value, pd.Timestamp):
        return {"timestamp": value.isoformat()}
    if isinstance(value, datetime):
        return {"datetime": value.isoformat()}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {"mapping": [[key, _pack(item)] for key, item in value.items()]}
    if isinstance(value, list):
        return [_pack(item) for item in value]
    if isinstance(value, tuple):
        return {"tuple": [_pack(item) for item in value]}
    raise TypeError(f"unsupported formal factor value: {type(value).__name__}")


def _unpack(value: Any) -> Any:
    if isinstance(value, list):
        return [_unpack(item) for item in value]
    if not isinstance(value, dict):
        return value
    if len(value) != 1:
        raise ValueError("formal factor checkpoint cell has invalid type")
    kind, raw = next(iter(value.items()))
    if kind == "mapping":
        return {key: _unpack(item) for key, item in raw}
    if kind == "tuple":
        return tuple(_unpack(item) for item in raw)
    if kind == "missing":
        return {"NA": pd.NA, "NaT": pd.NaT}[raw]
    return {
        "float": float.fromhex,
        "timestamp": pd.Timestamp,
        "datetime": datetime.fromisoformat,
        "date": date.fromisoformat,
        "decimal": Decimal,
    }[kind](raw)


def _read_frame(store: FormalAnalysisStore, header: dict[str, Any]) -> pd.DataFrame:
    if header.get("schema") != _SCHEMA:
        raise ValueError("formal factor checkpoint schema differs")
    columns = header["columns"]
    dtypes = header["dtypes"]
    parts = header["parts"]
    row_count = header["row_count"]
    rows_per_part = header["rows_per_part"]
    if len(columns) != len(dtypes) or len(set(columns)) != len(columns):
        raise ValueError("formal factor checkpoint columns differ")
    if (
        not 0 <= row_count <= _MAX_FRAME_ROWS
        or not 1 <= rows_per_part <= _MAX_ROWS_PER_PART
        or len(parts) != (row_count + rows_per_part - 1) // rows_per_part
    ):
        raise ValueError("formal factor checkpoint part count differs")
    data: list[list[Any]] = []
    for start in range(0, len(parts), _BATCH_PARTS):
        hashes = parts[start:start + _BATCH_PARTS]
        artifacts = store.load_artifacts(hashes)
        for index, artifact_sha256 in enumerate(hashes, start=start):
            rows = artifacts[artifact_sha256]["rows"]
            if len(rows) != min(rows_per_part, row_count - index * rows_per_part):
                raise ValueError("formal factor checkpoint row count differs")
            if any(len(row) != len(columns) + 1 for row in rows):
                raise ValueError("formal factor checkpoint row width differs")
            data.extend(rows)
    values = {
        name: pd.Series((_unpack(row[offset]) for row in data), dtype=dtype)
        for offset, (name, dtype) in enumerate(zip(columns, dtypes), start=1)
    }
    frame = pd.DataFrame(values, index=range(row_count))
    frame.columns.name = _unpack(header["columns_name"])
    name = _unpack(header["index_name"])
    bounds = header["range_index"]
    if bounds is not None:
        frame.index = pd.RangeIndex(*bounds, name=name)
    else:
        frame.index = pd.Index(
            (_unpack(row[0]) for row in data), dtype=header["index_dtype"], name=name,
        )
    frame.attrs = _unpack(header["attrs"])
    return frame


def checkpoint_factor_frame(
    store: FormalAnalysisStore,
    generation_sha256: str,
    stage: str,
    source_sha256: str,
    produce: Callable[[], pd.DataFrame],
    *,
    rows_per_part: int = _DEFAULT_ROWS_PER_PART,
) -> tuple[pd.DataFrame, bool]:
    """Load one source-bound frame or publish its row parts then its final header."""
    header = store.get(generation_sha256, stage, source_sha256)
    if header is not None:
        return _read_frame(store, header), True

    frame = produce()
    if len(frame) > _MAX_FRAME_ROWS or not 1 <= rows_per_part <= _MAX_ROWS_PER_PART:
        raise ValueError("formal factor checkpoint frame exceeds the row limit")
    columns = frame.columns.tolist()
    if not all(isinstance(name, str) for name in columns) or len(set(columns)) != len(columns):
        raise ValueError("formal factor checkpoint requires unique named columns")
    if isinstance(frame.index, pd.MultiIndex):
        raise ValueError("formal factor checkpoint requires a flat index")
    headers = {
        "schema": _SCHEMA,
        "columns": columns,
        "columns_name": _pack(frame.columns.name),
        "rows_per_part": rows_per_part,
        "dtypes": [str(dtype) for dtype in frame.dtypes],
        "index_name": _pack(frame.index.name),
        "index_dtype": str(frame.index.dtype),
        "range_index": (
            [frame.index.start, frame.index.stop, frame.index.step]
            if isinstance(frame.index, pd.RangeIndex) else None
        ),
        "attrs": _pack(frame.attrs),
        "row_count": len(frame),
    }
    artifacts: list[str] = []
    batch: dict[str, dict[str, Any]] = {}
    for ordinal, offset in enumerate(range(0, len(frame), rows_per_part)):
        rows = [
            [_pack(value) for value in row]
            for row in frame.iloc[offset:offset + rows_per_part].itertuples(name=None)
        ]
        payload = {"rows": rows}
        member = f"{source_sha256}:{ordinal}:{canonical_sha256(payload)[:16]}"
        batch[member] = payload
        if len(batch) == _BATCH_PARTS:
            hashes = store.put_many(generation_sha256, stage, batch)
            artifacts.extend(hashes[key] for key in batch)
            batch.clear()
    if batch:
        hashes = store.put_many(generation_sha256, stage, batch)
        artifacts.extend(hashes[key] for key in batch)
    store.put(generation_sha256, stage, source_sha256, {**headers, "parts": artifacts})
    return frame, False


def checkpoint_stock_factor_frame(
    store: FormalAnalysisStore,
    generation_sha256: str,
    stage: str,
    bucket: int,
    fingerprints: dict[str, str],
    produce: Callable[[list[str]], pd.DataFrame],
) -> tuple[pd.DataFrame, int]:
    """Fill one immutable stock factor partition using its retained baseline."""
    def source_key(inputs: dict[str, str]) -> str:
        return canonical_sha256({"schema": _STOCK_FACTORS, "bucket": bucket, "stocks": inputs})

    source = source_key(fingerprints)
    baseline_key = f"baseline:{bucket}"
    changed_count = 0
    establish_baseline = False

    def assemble() -> pd.DataFrame:
        nonlocal changed_count, establish_baseline
        baseline = store.get(generation_sha256, stage, baseline_key)
        retained = pd.DataFrame()
        unchanged: list[str] = []
        if baseline is None:
            establish_baseline = True
        else:
            prior_source = baseline["source_sha256"]
            header = store.get(generation_sha256, stage, prior_source)
            if header is None:
                raise ValueError("stock factor baseline checkpoint is unavailable")
            prior = _read_frame(store, header)
            prior_inputs = prior.attrs.get("stock_fingerprints")
            if (
                not isinstance(prior_inputs, dict) or source_key(prior_inputs) != prior_source
                or prior["stock_code"].tolist() != sorted(prior_inputs)
            ):
                raise ValueError("stock factor baseline identity differs")
            unchanged = [code for code, digest in fingerprints.items() if prior_inputs.get(code) == digest]
            retained = prior.loc[prior["stock_code"].isin(unchanged)]
        changed = sorted(fingerprints.keys() - set(unchanged))
        changed_count = len(changed)
        updated = produce(changed) if changed else pd.DataFrame()
        frame = pd.concat([part for part in (retained, updated) if not part.empty], ignore_index=True)
        frame = frame.sort_values("stock_code").reset_index(drop=True)
        if frame["stock_code"].tolist() != sorted(fingerprints):
            raise ValueError("stock factor checkpoint security coverage differs")
        frame.attrs = {"stock_fingerprints": fingerprints}
        return frame

    frame, reused = checkpoint_factor_frame(store, generation_sha256, stage, source, assemble)
    if frame.attrs.get("stock_fingerprints") != fingerprints or frame["stock_code"].tolist() != sorted(fingerprints):
        raise ValueError("stock factor checkpoint content identity differs")
    if establish_baseline:
        store.get_or_put(generation_sha256, stage, baseline_key, {"source_sha256": source})
    return frame, 0 if reused else changed_count
