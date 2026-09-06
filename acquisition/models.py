"""Plain data contracts shared by the new acquisition components."""
from dataclasses import dataclass, field
from datetime import datetime, time
import zlib


@dataclass(frozen=True)
class WorkUnit:
    dataset: str
    source: str
    target_date: str
    code: str
    period: str = "1d"
    adjustment: str = "none"

    @property
    def partition_key(self) -> str:
        return f"{self.code}:{self.period}:{self.adjustment}"


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    source: str
    table: str
    database: str
    code_column: str
    key_columns: tuple[str, ...]
    period: str
    adjustments: tuple[str, ...]
    asset_class: str
    ready_time: time
    event_data: bool = False
    persisted_source: str = ""
    replace_date_column: str = ""


@dataclass
class NormalizedUnit:
    unit: WorkUnit
    status: str
    rows: list[dict]
    error_code: str = ""
    error: str = ""
    detail: dict = field(default_factory=dict)


@dataclass
class NormalizedBatch:
    request_id: str
    units: list[NormalizedUnit]
    received_at: datetime


def key_fingerprint(keys) -> tuple[int, int, int, int, int]:
    """Compact, order-independent set evidence used by bounded due planning."""
    values = [str(key) for key in keys]
    sums = [0, 0]
    xors = [0, 0]
    for key in values:
        for index, salt in enumerate(("0:", "1:")):
            value = zlib.crc32((salt + key).encode("utf-8")) & 0xFFFFFFFF
            sums[index] += value
            xors[index] ^= value
    return len(values), sums[0], xors[0], sums[1], xors[1]
