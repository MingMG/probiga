"""Literal-SQL, read-only adapters for approved V4 PIT fact sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping

from sqlalchemy import bindparam, text

from server.trading_v4.domain import AsOfRecord, DatasetResult, ScopeRef, ScopeType
from server.trading_v4.domain.enums import QualityStatus

from .base import (
    StaticPitAdapter,
    aware_datetime,
    canonical_entities,
    canonical_entity,
    chronology_is_visible,
    dataset_result,
    decoded_json,
    default_start,
    linked_stock_codes,
    local_naive,
    maximum_time,
    normalized_payload,
    requested_fields,
    require_aware,
    session_close,
)
from .contracts import (
    ANNOUNCEMENT_SOURCE,
    CONCEPT_SNAPSHOT_SOURCE,
    DAILY_KLINE_SOURCE,
    FINANCIAL_SOURCE,
    MINUTE_KLINE_SOURCE,
    NEWS_FLASH_SOURCE,
)


# SQL identifiers are intentionally literal.  Do not replace these statements
# with a generic table/column builder: an adapter must not become an arbitrary
# legacy-table reader.
_DAILY_KLINE_SQL = text(
    """
    SELECT stock_code, short_name, trade_date,
           open, high, low, close, pre_close, volume, amount,
           change_pct, turnover_ratio, k_type, adjust_type,
           data_source, source_time, received_at, etl_sync_at,
           batch_id, data_version, quality_status, permission_status
    FROM sm_stock_kline
    WHERE stock_code IN :entities
      AND k_type = 1
      AND adjust_type = 0
      AND trade_date BETWEEN :start_date AND :cutoff_date
    ORDER BY trade_date DESC, stock_code ASC, etl_sync_at DESC
    LIMIT :fetch_limit
    """
).bindparams(bindparam("entities", expanding=True))

_MINUTE_KLINE_SQL = text(
    """
    SELECT stock_code, trade_time, price, open, high, low, close,
           volume, amount, change_pct, avg_price, data_source,
           source_time, received_at, etl_sync_at, batch_id,
           data_version, quality_status, permission_status
    FROM sm_stock_minute
    WHERE stock_code IN :entities
      AND trade_time BETWEEN :start_at AND :cutoff_at
    ORDER BY trade_time ASC, stock_code ASC, data_source ASC,
             etl_sync_at ASC
    LIMIT :fetch_limit
    """
).bindparams(bindparam("entities", expanding=True))

_NEWS_FLASH_SQL = text(
    """
    SELECT id, source, source_id, title, content, publish_time,
           first_seen_at, level, stocks, subjects, reading_num,
           is_top, jpush, extra, etl_sync_at
    FROM st_news_flash
    WHERE publish_time BETWEEN :start_at AND :cutoff_at
    ORDER BY publish_time DESC, first_seen_at DESC, id DESC
    LIMIT :fetch_limit
    """
)

_ANNOUNCEMENT_SQL = text(
    """
    SELECT id, stock_code, notice_date, title, column_name,
           display_time, detail_url, art_code,
           association_validated, etl_sync_at
    FROM si_notice_eastmoney
    WHERE stock_code IN :entities
      AND association_validated = 1
      AND notice_date BETWEEN :start_date AND :cutoff_date
    ORDER BY notice_date DESC, etl_sync_at DESC, id DESC
    LIMIT :fetch_limit
    """
).bindparams(bindparam("entities", expanding=True))

_FINANCIAL_SQL = text(
    """
    SELECT id, stock_code, report_date, notice_date,
           net_asset_ps, oper_cf_ps, total_rev_yoy_gr,
           net_profit_yoy_gr, roe_wtd, gross_margin, net_margin,
           cash_flow_ratio, asset_liab_ratio, etl_sync_at
    FROM si_stock_finance
    WHERE stock_code IN :entities
      AND report_date <= :cutoff_date
      AND notice_date <= :cutoff_date
    ORDER BY stock_code ASC, report_date DESC, notice_date DESC,
             etl_sync_at DESC, id DESC
    LIMIT :fetch_limit
    """
).bindparams(bindparam("entities", expanding=True))

_CONCEPT_SNAPSHOT_SQL = text(
    """
    SELECT id, snapshot_date, source, concept_code, concept_name,
           stock_code, short_name, quality_status, captured_at
    FROM qmt_concept_member_snapshot
    WHERE stock_code IN :entities
      AND snapshot_date BETWEEN :start_date AND :cutoff_date
    ORDER BY snapshot_date DESC, stock_code ASC, concept_code ASC, id ASC
    LIMIT :fetch_limit
    """
).bindparams(bindparam("entities", expanding=True))


def _row_value(row: Mapping[str, Any], name: str) -> Any:
    if name not in row:
        # This should only occur with a non-conforming fake executor; real SQL
        # fails before this point when a contracted column is absent.
        raise KeyError(f"contracted source row is missing {name}")
    return row[name]


def _revision_id(row: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return str(value).strip()
    return ""


@dataclass(frozen=True)
class DailyKlinePitAdapter(StaticPitAdapter):
    """Observable unadjusted daily-bar window for requested instruments.

    One newest visible revision is retained per instrument and trade date;
    history is not collapsed to the latest date.  Payload aliases follow the
    V4 daily-bar factor contract so this dataset can be consumed directly.
    """

    lookback_days: int = 62

    def __post_init__(self) -> None:
        super().__post_init__()
        if not isinstance(self.lookback_days, int) or self.lookback_days < 1:
            raise ValueError("lookback_days must be a positive integer")

    def load_market_data(
        self,
        instruments: tuple[str, ...],
        *,
        knowledge_cutoff: datetime,
        fields: tuple[str, ...] = (),
    ) -> DatasetResult:
        cutoff = require_aware(knowledge_cutoff, "knowledge_cutoff")
        entities = canonical_entities(instruments)
        selected_fields = requested_fields(fields, contract=DAILY_KLINE_SOURCE)
        local_cutoff = local_naive(cutoff)
        rows = self._read_bounded(
            contract=DAILY_KLINE_SOURCE,
            statement=_DAILY_KLINE_SQL,
            params={
                "entities": entities,
                "start_date": local_naive(
                    default_start(cutoff, self.lookback_days)
                ).date().isoformat(),
                "cutoff_date": local_cutoff.date().isoformat(),
            },
        )
        latest: dict[tuple[str, str], tuple[tuple[Any, ...], AsOfRecord]] = {}
        returned: set[str] = set()
        for row in rows:
            entity = canonical_entity(_row_value(row, "stock_code"))
            trade_close = session_close(_row_value(row, "trade_date"), "trade_date")
            source_time = aware_datetime(row.get("source_time"), "source_time")
            received_at = aware_datetime(row.get("received_at"), "received_at")
            etl_sync_at = aware_datetime(row.get("etl_sync_at"), "etl_sync_at")
            # A trade/session timestamp or provider timestamp does not prove
            # when this overwrite-only row became observable.  Historical
            # rows without either acquisition timestamp must fail closed.
            ingested_at = maximum_time(received_at, etl_sync_at)
            knowledge_time = maximum_time(
                trade_close,
                source_time,
                received_at,
                etl_sync_at,
            )
            if not chronology_is_visible(knowledge_time, cutoff):
                continue
            trade_date = trade_close.date().isoformat()
            payload = {
                "instrument": entity,
                "stock_code": entity,
                "trade_date": trade_date,
                "previous_close": normalized_payload(
                    _row_value(row, "pre_close")
                ),
                "turnover_pct": normalized_payload(
                    _row_value(row, "turnover_ratio")
                ),
                "k_type": 1,
                "adjust_type": 0,
                "data_source": normalized_payload(row.get("data_source")),
                "batch_id": normalized_payload(row.get("batch_id")),
                "data_version": normalized_payload(row.get("data_version")),
                "source_quality_status": normalized_payload(
                    row.get("quality_status")
                ),
                "permission_status": normalized_payload(
                    row.get("permission_status")
                ),
                **{
                    field: normalized_payload(_row_value(row, field))
                    for field in selected_fields
                },
            }
            record = AsOfRecord(
                record_id=f"{entity}:{trade_date}:k1:a0",
                source=DAILY_KLINE_SOURCE.source_name,
                knowledge_time=knowledge_time,
                ingested_at=ingested_at,
                event_time=trade_close,
                source_published_at=source_time,
                received_at=received_at,
                revision_id=_revision_id(row, "data_version", "batch_id"),
                quality_status=QualityStatus.PASS,
                payload=payload,
            )
            key = (knowledge_time, record.revision_id, record.record_hash)
            identity = (entity, trade_date)
            previous = latest.get(identity)
            if previous is None or key > previous[0]:
                latest[identity] = (key, record)
            returned.add(entity)

        records = tuple(item[1] for item in latest.values())
        return dataset_result(
            contract=DAILY_KLINE_SOURCE,
            cutoff=cutoff,
            requested=entities,
            returned=returned,
            fields=selected_fields,
            records=records,
        )


@dataclass(frozen=True)
class MinuteKlinePitAdapter(StaticPitAdapter):
    """Observable minute rows, retaining receipt time in PIT chronology."""

    lookback_minutes: int = 600

    def __post_init__(self) -> None:
        super().__post_init__()
        if not isinstance(self.lookback_minutes, int) or self.lookback_minutes < 1:
            raise ValueError("lookback_minutes must be a positive integer")

    def load_market_data(
        self,
        instruments: tuple[str, ...],
        *,
        knowledge_cutoff: datetime,
        fields: tuple[str, ...] = (),
    ) -> DatasetResult:
        cutoff = require_aware(knowledge_cutoff, "knowledge_cutoff")
        entities = canonical_entities(instruments)
        selected_fields = requested_fields(fields, contract=MINUTE_KLINE_SOURCE)
        start = cutoff - timedelta(minutes=self.lookback_minutes)
        rows = self._read_bounded(
            contract=MINUTE_KLINE_SOURCE,
            statement=_MINUTE_KLINE_SQL,
            params={
                "entities": entities,
                "start_at": local_naive(start),
                "cutoff_at": local_naive(cutoff),
            },
        )
        records: list[AsOfRecord] = []
        returned: set[str] = set()
        for row in rows:
            entity = canonical_entity(_row_value(row, "stock_code"))
            trade_time = aware_datetime(
                _row_value(row, "trade_time"),
                "trade_time",
                required=True,
            )
            source_time = aware_datetime(row.get("source_time"), "source_time")
            received_at = aware_datetime(row.get("received_at"), "received_at")
            etl_sync_at = aware_datetime(row.get("etl_sync_at"), "etl_sync_at")
            assert trade_time is not None
            # At least one receipt timestamp is mandatory.  A physical row's
            # trade_time alone is never proof that the row was then knowable.
            ingested_at = maximum_time(received_at, etl_sync_at)
            knowledge_time = maximum_time(
                trade_time,
                source_time,
                received_at,
                etl_sync_at,
            )
            if not chronology_is_visible(knowledge_time, cutoff):
                continue
            provider = str(_row_value(row, "data_source")).strip()
            if not provider:
                raise ValueError("minute data_source must not be empty")
            returned.add(entity)
            records.append(
                AsOfRecord(
                    record_id=f"{entity}:{trade_time.isoformat()}:{provider}",
                    source=MINUTE_KLINE_SOURCE.source_name,
                    knowledge_time=knowledge_time,
                    ingested_at=ingested_at,
                    event_time=trade_time,
                    source_published_at=source_time,
                    received_at=received_at,
                    revision_id=_revision_id(row, "data_version", "batch_id"),
                    quality_status=QualityStatus.PASS,
                    payload={
                        "stock_code": entity,
                        "trade_time": trade_time.isoformat(),
                        "data_source": provider,
                        "batch_id": normalized_payload(row.get("batch_id")),
                        "data_version": normalized_payload(
                            row.get("data_version")
                        ),
                        "source_quality_status": normalized_payload(
                            row.get("quality_status")
                        ),
                        "permission_status": normalized_payload(
                            row.get("permission_status")
                        ),
                        **{
                            field: normalized_payload(_row_value(row, field))
                            for field in selected_fields
                        },
                    },
                )
            )
        return dataset_result(
            contract=MINUTE_KLINE_SOURCE,
            cutoff=cutoff,
            requested=entities,
            returned=returned,
            fields=selected_fields,
            records=records,
        )


def _scope_key(scope: ScopeRef) -> str:
    if type(scope) is not ScopeRef:
        raise TypeError("scopes must contain exact ScopeRef values")
    if scope.scope_type == ScopeType.INSTRUMENT:
        return canonical_entity(scope.scope_id)
    return f"{scope.scope_type.value}:{scope.scope_id}"


def _scope_matches_news(
    scope: ScopeRef,
    *,
    stocks: frozenset[str],
    subjects: Any,
) -> bool:
    if scope.scope_type == ScopeType.MARKET:
        return True
    if scope.scope_type == ScopeType.INSTRUMENT:
        return canonical_entity(scope.scope_id) in stocks
    if scope.scope_type == ScopeType.SECTOR:
        subject_tokens: set[str] = set()
        values = subjects if isinstance(subjects, list) else [subjects]
        for item in values:
            if isinstance(item, Mapping):
                subject_tokens.update(
                    str(item.get(name) or "").strip()
                    for name in ("name", "code", "subject_name", "subject_code")
                )
            elif item not in (None, ""):
                subject_tokens.add(str(item).strip())
        return scope.scope_id in subject_tokens
    return False


@dataclass(frozen=True)
class NewsFlashPitAdapter(StaticPitAdapter):
    """Forward-only news-flash reader using first-seen chronology."""

    default_window_days: int = 3

    def load_events(
        self,
        scopes: tuple[ScopeRef, ...],
        *,
        knowledge_cutoff: datetime,
        since: datetime | None = None,
    ) -> DatasetResult:
        cutoff = require_aware(knowledge_cutoff, "knowledge_cutoff")
        if not scopes:
            raise ValueError("at least one scope is required")
        scope_by_key = {_scope_key(scope): scope for scope in scopes}
        requested = tuple(sorted(scope_by_key))
        start = (
            require_aware(since, "since")
            if since is not None
            else default_start(cutoff, self.default_window_days)
        )
        if start > cutoff:
            raise ValueError("since must not be after knowledge_cutoff")
        rows = self._read_bounded(
            contract=NEWS_FLASH_SOURCE,
            statement=_NEWS_FLASH_SQL,
            params={
                "start_at": local_naive(start),
                "cutoff_at": local_naive(cutoff),
            },
        )
        records: list[AsOfRecord] = []
        returned: set[str] = set()
        for row in rows:
            published_at = aware_datetime(
                _row_value(row, "publish_time"),
                "publish_time",
                required=True,
            )
            first_seen_at = aware_datetime(
                _row_value(row, "first_seen_at"),
                "first_seen_at",
                required=True,
            )
            etl_sync_at = aware_datetime(
                _row_value(row, "etl_sync_at"),
                "etl_sync_at",
                required=True,
            )
            assert published_at is not None
            assert first_seen_at is not None
            assert etl_sync_at is not None
            knowledge_time = maximum_time(published_at, first_seen_at, etl_sync_at)
            if not chronology_is_visible(knowledge_time, cutoff):
                continue
            stocks = linked_stock_codes(row.get("stocks"))
            subjects = decoded_json(row.get("subjects"), "subjects")
            matched = {
                key
                for key, scope in scope_by_key.items()
                if _scope_matches_news(scope, stocks=stocks, subjects=subjects)
            }
            if not matched:
                continue
            returned.update(matched)
            provider = str(_row_value(row, "source")).strip()
            source_id = str(_row_value(row, "source_id")).strip()
            if not provider or not source_id:
                raise ValueError("news source and source_id must not be empty")
            records.append(
                AsOfRecord(
                    record_id=f"{provider}:{source_id}",
                    source=NEWS_FLASH_SOURCE.source_name,
                    knowledge_time=knowledge_time,
                    ingested_at=maximum_time(first_seen_at, etl_sync_at),
                    event_time=published_at,
                    source_published_at=published_at,
                    first_seen_at=first_seen_at,
                    quality_status=QualityStatus.PASS,
                    payload={
                        "provider": provider,
                        "source_id": source_id,
                        "title": normalized_payload(row.get("title")),
                        "content": normalized_payload(row.get("content")),
                        "level": normalized_payload(row.get("level")),
                        "stocks": normalized_payload(
                            decoded_json(row.get("stocks"), "stocks")
                        ),
                        "subjects": normalized_payload(subjects),
                        "reading_num": normalized_payload(row.get("reading_num")),
                        "is_top": normalized_payload(row.get("is_top")),
                        "jpush": normalized_payload(row.get("jpush")),
                        "extra": normalized_payload(decoded_json(row.get("extra"), "extra")),
                    },
                )
            )
        return dataset_result(
            contract=NEWS_FLASH_SOURCE,
            cutoff=cutoff,
            requested=requested,
            returned=returned,
            fields=tuple(sorted(NEWS_FLASH_SOURCE.default_fields)),
            records=records,
        )


def _instrument_scopes(scopes: Iterable[ScopeRef]) -> tuple[dict[str, ScopeRef], tuple[str, ...]]:
    by_key: dict[str, ScopeRef] = {}
    for scope in scopes:
        if type(scope) is not ScopeRef:
            raise TypeError("scopes must contain exact ScopeRef values")
        if scope.scope_type != ScopeType.INSTRUMENT:
            raise ValueError("announcement source supports INSTRUMENT scopes only")
        by_key[canonical_entity(scope.scope_id)] = scope
    if not by_key:
        raise ValueError("at least one instrument scope is required")
    return by_key, tuple(sorted(by_key))


@dataclass(frozen=True)
class AnnouncementPitAdapter(StaticPitAdapter):
    """Display-only announcement reader with no current-table fallback."""

    default_window_days: int = 20

    def load_events(
        self,
        scopes: tuple[ScopeRef, ...],
        *,
        knowledge_cutoff: datetime,
        since: datetime | None = None,
    ) -> DatasetResult:
        cutoff = require_aware(knowledge_cutoff, "knowledge_cutoff")
        _, entities = _instrument_scopes(scopes)
        start = (
            require_aware(since, "since")
            if since is not None
            else default_start(cutoff, self.default_window_days)
        )
        if start > cutoff:
            raise ValueError("since must not be after knowledge_cutoff")
        rows = self._read_bounded(
            contract=ANNOUNCEMENT_SOURCE,
            statement=_ANNOUNCEMENT_SQL,
            params={
                "entities": entities,
                "start_date": local_naive(start).date().isoformat(),
                "cutoff_date": local_naive(cutoff).date().isoformat(),
            },
        )
        records: list[AsOfRecord] = []
        returned: set[str] = set()
        for row in rows:
            entity = canonical_entity(_row_value(row, "stock_code"))
            notice_close = session_close(_row_value(row, "notice_date"), "notice_date")
            display_at = aware_datetime(row.get("display_time"), "display_time")
            etl_sync_at = aware_datetime(
                _row_value(row, "etl_sync_at"),
                "etl_sync_at",
                required=True,
            )
            assert etl_sync_at is not None
            published_at = maximum_time(notice_close, display_at)
            knowledge_time = maximum_time(published_at, etl_sync_at)
            if not chronology_is_visible(knowledge_time, cutoff):
                continue
            returned.add(entity)
            source_id = _revision_id(row, "art_code", "id")
            records.append(
                AsOfRecord(
                    record_id=f"{entity}:{source_id}",
                    source=ANNOUNCEMENT_SOURCE.source_name,
                    knowledge_time=knowledge_time,
                    ingested_at=etl_sync_at,
                    event_time=notice_close,
                    source_published_at=published_at,
                    quality_status=QualityStatus.PASS,
                    payload={
                        "stock_code": entity,
                        "notice_date": notice_close.date().isoformat(),
                        "title": normalized_payload(row.get("title")),
                        "column_name": normalized_payload(row.get("column_name")),
                        "display_time": normalized_payload(row.get("display_time")),
                        "detail_url": normalized_payload(row.get("detail_url")),
                        "art_code": normalized_payload(row.get("art_code")),
                        "association_validated": True,
                    },
                )
            )
        return dataset_result(
            contract=ANNOUNCEMENT_SOURCE,
            cutoff=cutoff,
            requested=entities,
            returned=returned,
            fields=tuple(sorted(ANNOUNCEMENT_SOURCE.default_fields)),
            records=records,
        )


@dataclass(frozen=True)
class FinancialPitAdapter(StaticPitAdapter):
    """Display-only latest financial disclosure known at the cutoff."""

    def load_fundamentals(
        self,
        instruments: tuple[str, ...],
        *,
        knowledge_cutoff: datetime,
        fields: tuple[str, ...] = (),
    ) -> DatasetResult:
        cutoff = require_aware(knowledge_cutoff, "knowledge_cutoff")
        entities = canonical_entities(instruments)
        selected_fields = requested_fields(fields, contract=FINANCIAL_SOURCE)
        rows = self._read_bounded(
            contract=FINANCIAL_SOURCE,
            statement=_FINANCIAL_SQL,
            params={
                "entities": entities,
                "cutoff_date": local_naive(cutoff).date().isoformat(),
            },
        )
        latest: dict[str, tuple[tuple[Any, ...], AsOfRecord]] = {}
        for row in rows:
            entity = canonical_entity(_row_value(row, "stock_code"))
            report_close = session_close(_row_value(row, "report_date"), "report_date")
            notice_close = session_close(_row_value(row, "notice_date"), "notice_date")
            etl_sync_at = aware_datetime(
                _row_value(row, "etl_sync_at"),
                "etl_sync_at",
                required=True,
            )
            assert etl_sync_at is not None
            knowledge_time = maximum_time(notice_close, etl_sync_at)
            if not chronology_is_visible(knowledge_time, cutoff):
                continue
            record = AsOfRecord(
                record_id=f"{entity}:{report_close.date().isoformat()}",
                source=FINANCIAL_SOURCE.source_name,
                knowledge_time=knowledge_time,
                ingested_at=etl_sync_at,
                event_time=report_close,
                source_published_at=notice_close,
                revision_id=_revision_id(row, "id"),
                quality_status=QualityStatus.PASS,
                payload={
                    "stock_code": entity,
                    "report_date": report_close.date().isoformat(),
                    "notice_date": notice_close.date().isoformat(),
                    **{
                        field: normalized_payload(_row_value(row, field))
                        for field in selected_fields
                    },
                },
            )
            key = (report_close.date(), notice_close, knowledge_time, record.revision_id)
            previous = latest.get(entity)
            if previous is None or key > previous[0]:
                latest[entity] = (key, record)
        return dataset_result(
            contract=FINANCIAL_SOURCE,
            cutoff=cutoff,
            requested=entities,
            returned=latest,
            fields=selected_fields,
            records=(item[1] for item in latest.values()),
        )


@dataclass(frozen=True)
class ConceptSnapshotPitAdapter(StaticPitAdapter):
    """Latest complete-by-source concept membership rows visible at cutoff."""

    lookback_days: int = 62

    def load_market_data(
        self,
        instruments: tuple[str, ...],
        *,
        knowledge_cutoff: datetime,
        fields: tuple[str, ...] = (),
    ) -> DatasetResult:
        cutoff = require_aware(knowledge_cutoff, "knowledge_cutoff")
        entities = canonical_entities(instruments)
        selected_fields = requested_fields(fields, contract=CONCEPT_SNAPSHOT_SOURCE)
        rows = self._read_bounded(
            contract=CONCEPT_SNAPSHOT_SOURCE,
            statement=_CONCEPT_SNAPSHOT_SQL,
            params={
                "entities": entities,
                "start_date": local_naive(default_start(cutoff, self.lookback_days)).date().isoformat(),
                "cutoff_date": local_naive(cutoff).date().isoformat(),
            },
        )
        eligible: list[tuple[str, Any, AsOfRecord]] = []
        latest_date: dict[str, Any] = {}
        for row in rows:
            entity = canonical_entity(_row_value(row, "stock_code"))
            snapshot_close = session_close(
                _row_value(row, "snapshot_date"), "snapshot_date"
            )
            captured_at = aware_datetime(
                _row_value(row, "captured_at"),
                "captured_at",
                required=True,
            )
            assert captured_at is not None
            knowledge_time = maximum_time(snapshot_close, captured_at)
            if not chronology_is_visible(knowledge_time, cutoff):
                continue
            snapshot_date = snapshot_close.date()
            provider = str(_row_value(row, "source")).strip()
            concept_code = str(_row_value(row, "concept_code")).strip()
            if not provider or not concept_code:
                raise ValueError("concept provider and concept_code must not be empty")
            record = AsOfRecord(
                record_id=(
                    f"{entity}:{snapshot_date.isoformat()}:{provider}:{concept_code}"
                ),
                source=CONCEPT_SNAPSHOT_SOURCE.source_name,
                knowledge_time=knowledge_time,
                ingested_at=captured_at,
                event_time=snapshot_close,
                revision_id=captured_at.isoformat(),
                quality_status=QualityStatus.PASS,
                payload={
                    "stock_code": entity,
                    "snapshot_date": snapshot_date.isoformat(),
                    "provider": provider,
                    **{
                        field: normalized_payload(_row_value(row, field))
                        for field in selected_fields
                    },
                },
            )
            eligible.append((entity, snapshot_date, record))
            if entity not in latest_date or snapshot_date > latest_date[entity]:
                latest_date[entity] = snapshot_date
        records = tuple(
            record
            for entity, snapshot_date, record in eligible
            if latest_date.get(entity) == snapshot_date
        )
        return dataset_result(
            contract=CONCEPT_SNAPSHOT_SOURCE,
            cutoff=cutoff,
            requested=entities,
            returned=latest_date,
            fields=selected_fields,
            records=records,
        )
