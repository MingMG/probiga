"""Read-only point-in-time adapters for fixed legacy fact sources."""

from .adapters import (
    AnnouncementPitAdapter,
    ConceptSnapshotPitAdapter,
    DailyKlinePitAdapter,
    FinancialPitAdapter,
    MinuteKlinePitAdapter,
    NewsFlashPitAdapter,
)
from .base import (
    CHINA_TIMEZONE,
    PitSourceDataError,
    PitSourceError,
    PitSourceReadError,
    PitSourceRowLimitExceeded,
)
from .contracts import (
    ANNOUNCEMENT_SOURCE,
    CONCEPT_SNAPSHOT_SOURCE,
    DAILY_KLINE_SOURCE,
    FINANCIAL_SOURCE,
    MINUTE_KLINE_SOURCE,
    NEWS_FLASH_SOURCE,
    PitSourceContract,
    source_contract,
    source_contracts,
)

__all__ = [
    "ANNOUNCEMENT_SOURCE",
    "CHINA_TIMEZONE",
    "CONCEPT_SNAPSHOT_SOURCE",
    "DAILY_KLINE_SOURCE",
    "FINANCIAL_SOURCE",
    "MINUTE_KLINE_SOURCE",
    "NEWS_FLASH_SOURCE",
    "AnnouncementPitAdapter",
    "ConceptSnapshotPitAdapter",
    "DailyKlinePitAdapter",
    "FinancialPitAdapter",
    "MinuteKlinePitAdapter",
    "NewsFlashPitAdapter",
    "PitSourceContract",
    "PitSourceDataError",
    "PitSourceError",
    "PitSourceReadError",
    "PitSourceRowLimitExceeded",
    "source_contract",
    "source_contracts",
]
