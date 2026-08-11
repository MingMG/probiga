"""Static source contracts for the V4 point-in-time read boundary.

The contracts describe legacy *fact* tables only.  They are not a source of
SQL identifiers: every adapter owns a literal statement for exactly one
table.  Constructing another :class:`PitSourceContract` therefore cannot make
an adapter read an arbitrary table.
"""

from __future__ import annotations

from dataclasses import dataclass

from server.trading_v4.domain.enums import ReplayEligibility, ResearchStatus


@dataclass(frozen=True)
class PitSourceContract:
    """Reviewable metadata for one fixed, read-only legacy source."""

    dataset_name: str
    source_name: str
    table_name: str
    entity_column: str
    event_time_column: str
    knowledge_time_columns: tuple[str, ...]
    required_columns: tuple[str, ...]
    default_fields: tuple[str, ...]
    default_replay_eligibility: ReplayEligibility
    default_research_status: ResearchStatus

    def __post_init__(self) -> None:
        for field_name in (
            "dataset_name",
            "source_name",
            "table_name",
            "entity_column",
            "event_time_column",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        for field_name in (
            "knowledge_time_columns",
            "required_columns",
            "default_fields",
        ):
            values = getattr(self, field_name)
            if not values or any(
                not isinstance(value, str) or not value.strip() for value in values
            ):
                raise ValueError(f"{field_name} must contain non-empty names")
            if len(values) != len(set(values)):
                raise ValueError(f"{field_name} contains duplicates")
        object.__setattr__(
            self,
            "default_replay_eligibility",
            ReplayEligibility(self.default_replay_eligibility),
        )
        object.__setattr__(
            self,
            "default_research_status",
            ResearchStatus(self.default_research_status),
        )
        if not set(self.default_fields).issubset(self.required_columns):
            raise ValueError("default_fields must be contracted required columns")


DAILY_KLINE_SOURCE = PitSourceContract(
    dataset_name="v4.daily_kline.pit",
    source_name="sm_stock_kline.daily.unadjusted",
    table_name="sm_stock_kline",
    entity_column="stock_code",
    event_time_column="trade_date",
    knowledge_time_columns=(
        "trade_date@15:00:00 Asia/Shanghai",
        "source_time",
        "received_at",
        "etl_sync_at",
    ),
    required_columns=(
        "stock_code",
        "short_name",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
        "change_pct",
        "turnover_ratio",
        "k_type",
        "adjust_type",
        "data_source",
        "source_time",
        "received_at",
        "etl_sync_at",
        "batch_id",
        "data_version",
        "quality_status",
        "permission_status",
    ),
    default_fields=(
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
        "change_pct",
        "turnover_ratio",
    ),
    # Rows are uniquely overwritten by instrument/date/type/adjustment and
    # there is no revision-history table.  Receipt metadata supports safe
    # forward reads, but cannot prove which revision existed at an arbitrary
    # historical cutoff.
    default_replay_eligibility=ReplayEligibility.FORWARD_ONLY,
    default_research_status=ResearchStatus.FORWARD_ONLY,
)

MINUTE_KLINE_SOURCE = PitSourceContract(
    dataset_name="v4.minute_kline.pit",
    source_name="sm_stock_minute.one_minute",
    table_name="sm_stock_minute",
    entity_column="stock_code",
    event_time_column="trade_time",
    knowledge_time_columns=(
        "trade_time",
        "source_time",
        "received_at",
        "etl_sync_at",
    ),
    required_columns=(
        "stock_code",
        "trade_time",
        "price",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "change_pct",
        "avg_price",
        "data_source",
        "source_time",
        "received_at",
        "etl_sync_at",
        "batch_id",
        "data_version",
        "quality_status",
        "permission_status",
    ),
    default_fields=(
        "price",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "change_pct",
        "avg_price",
    ),
    # The table contains several providers and its history starts recently.
    # Certification must be granted per provider outside this adapter.
    default_replay_eligibility=ReplayEligibility.FORWARD_ONLY,
    default_research_status=ResearchStatus.FORWARD_ONLY,
)

NEWS_FLASH_SOURCE = PitSourceContract(
    dataset_name="v4.news_flash.pit",
    source_name="st_news_flash",
    table_name="st_news_flash",
    entity_column="stocks",
    event_time_column="publish_time",
    knowledge_time_columns=("publish_time", "first_seen_at", "etl_sync_at"),
    required_columns=(
        "id",
        "source",
        "source_id",
        "title",
        "content",
        "publish_time",
        "first_seen_at",
        "level",
        "stocks",
        "subjects",
        "reading_num",
        "is_top",
        "jpush",
        "extra",
        "etl_sync_at",
    ),
    default_fields=(
        "title",
        "content",
        "level",
        "stocks",
        "subjects",
    ),
    default_replay_eligibility=ReplayEligibility.FORWARD_ONLY,
    default_research_status=ResearchStatus.FORWARD_ONLY,
)

ANNOUNCEMENT_SOURCE = PitSourceContract(
    dataset_name="v4.announcement.pit",
    source_name="si_notice_eastmoney",
    table_name="si_notice_eastmoney",
    entity_column="stock_code",
    event_time_column="notice_date",
    knowledge_time_columns=("display_time", "etl_sync_at"),
    required_columns=(
        "id",
        "stock_code",
        "notice_date",
        "title",
        "column_name",
        "display_time",
        "detail_url",
        "art_code",
        "association_validated",
        "etl_sync_at",
    ),
    default_fields=("title", "column_name", "display_time", "detail_url"),
    default_replay_eligibility=ReplayEligibility.DISPLAY_ONLY,
    default_research_status=ResearchStatus.DISPLAY_ONLY,
)

FINANCIAL_SOURCE = PitSourceContract(
    dataset_name="v4.financial_statement.pit",
    source_name="si_stock_finance",
    table_name="si_stock_finance",
    entity_column="stock_code",
    event_time_column="report_date",
    knowledge_time_columns=("notice_date@15:00:00 Asia/Shanghai", "etl_sync_at"),
    required_columns=(
        "id",
        "stock_code",
        "report_date",
        "notice_date",
        "net_asset_ps",
        "oper_cf_ps",
        "total_rev_yoy_gr",
        "net_profit_yoy_gr",
        "roe_wtd",
        "gross_margin",
        "net_margin",
        "cash_flow_ratio",
        "asset_liab_ratio",
        "etl_sync_at",
    ),
    default_fields=(
        "net_asset_ps",
        "oper_cf_ps",
        "total_rev_yoy_gr",
        "net_profit_yoy_gr",
        "roe_wtd",
        "gross_margin",
        "net_margin",
        "cash_flow_ratio",
        "asset_liab_ratio",
    ),
    default_replay_eligibility=ReplayEligibility.DISPLAY_ONLY,
    default_research_status=ResearchStatus.DISPLAY_ONLY,
)

CONCEPT_SNAPSHOT_SOURCE = PitSourceContract(
    dataset_name="v4.concept_membership_snapshot.pit",
    source_name="qmt_concept_member_snapshot",
    table_name="qmt_concept_member_snapshot",
    entity_column="stock_code",
    event_time_column="snapshot_date",
    knowledge_time_columns=(
        "snapshot_date@15:00:00 Asia/Shanghai",
        "captured_at",
    ),
    required_columns=(
        "id",
        "snapshot_date",
        "source",
        "concept_code",
        "concept_name",
        "stock_code",
        "short_name",
        "quality_status",
        "captured_at",
    ),
    default_fields=("concept_code", "concept_name", "short_name", "quality_status"),
    default_replay_eligibility=ReplayEligibility.FORWARD_ONLY,
    default_research_status=ResearchStatus.FORWARD_ONLY,
)


_CONTRACTS = {
    contract.source_name: contract
    for contract in (
        DAILY_KLINE_SOURCE,
        MINUTE_KLINE_SOURCE,
        NEWS_FLASH_SOURCE,
        ANNOUNCEMENT_SOURCE,
        FINANCIAL_SOURCE,
        CONCEPT_SNAPSHOT_SOURCE,
    )
}


def source_contract(source_name: str) -> PitSourceContract:
    """Return one registered contract; never synthesize an unknown source."""

    if not isinstance(source_name, str) or not source_name.strip():
        raise ValueError("source_name must not be empty")
    try:
        return _CONTRACTS[source_name.strip()]
    except KeyError as exc:
        raise ValueError(f"unregistered PIT source: {source_name!r}") from exc


def source_contracts() -> tuple[PitSourceContract, ...]:
    return tuple(_CONTRACTS[name] for name in sorted(_CONTRACTS))
