from __future__ import annotations

import json
from dataclasses import asdict, dataclass
import hashlib
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from integrations.qmt._control_schema import (
    FrozenColumn,
    FrozenIndex,
    FrozenTable,
    character_column,
    privileged_migrate_frozen_tables,
    validate_frozen_tables,
)


PROVIDER_ID = "gj_qmt"
STOCK_DOC = "https://dict.thinktrader.net/dictionary/stock.html"
INDUSTRY_DOC = "https://dict.thinktrader.net/dictionary/industry.html"
INDEX_DOC = "https://dict.thinktrader.net/dictionary/indexes.html"


@dataclass(frozen=True)
class ApiDefinition:
    category: str
    api_name: str
    period: str = ""
    execution_mode: str = "native"
    required_permission: str = "basic"
    document_url: str = STOCK_DOC
    target_table: str = ""
    consumer_module: str = ""

    @property
    def capability_key(self) -> str:
        return f"{self.execution_mode}:{self.api_name}:{self.period or '-'}"


def _market_periods() -> list[ApiDefinition]:
    definitions: list[ApiDefinition] = []
    period_config = {
        "tick": ("stock_market", "basic", "sm_stock_five_level", "realtime"),
        "1m": ("stock_market", "basic", "sm_stock_minute", "realtime,backtest"),
        "5m": ("stock_market", "basic", "sm_stock_minute", "analysis,backtest"),
        "15m": ("stock_market", "basic", "sm_stock_minute", "analysis,backtest"),
        "30m": ("stock_market", "basic", "sm_stock_minute", "analysis,backtest"),
        "1h": ("stock_market", "basic", "sm_stock_minute", "analysis,backtest"),
        "1d": ("stock_market", "basic", "sm_stock_kline", "analysis,recommendation,backtest"),
        "1w": ("stock_market", "basic", "sm_stock_kline", "analysis"),
        "1mon": ("stock_market", "basic", "sm_stock_kline", "analysis"),
        "transactioncount1m": ("stock_flow", "feature_or_l2", "sm_stock_capital_flow_min", "capital,realtime"),
        "transactioncount1d": ("stock_flow", "feature_or_l2", "sm_stock_capital_flow_daily", "capital,recommendation"),
        "orderflow1m": ("stock_orderflow", "orderflow", "qmt_stock_order_flow", "capital,realtime"),
        "orderflow1d": ("stock_orderflow", "orderflow", "qmt_stock_order_flow", "capital,recommendation"),
        "northfinancechange1m": ("northbound", "vip", "qmt_northbound_flow", "market,realtime"),
        "northfinancechange1d": ("northbound", "vip", "qmt_northbound_flow", "market,recommendation"),
        "interactiveqa": ("interactive_qa", "vip", "qmt_interactive_qa", "event_risk"),
        "announcement": (
            "announcement", "vip", "st_pit_event_revision", "event_risk"
        ),
        "l2quote": ("level2", "level2", "qmt_stock_l2_quote", "realtime"),
        "l2quoteaux": ("level2", "level2", "qmt_stock_l2_quote", "realtime"),
        "l2order": ("level2", "level2", "qmt_stock_l2_order", "capital,realtime"),
        "l2transaction": ("level2", "level2", "qmt_stock_l2_transaction", "capital,realtime"),
        "l2transactioncount": ("level2", "level2", "qmt_stock_l2_transaction_count", "capital,realtime"),
        "l2orderqueue": ("level2", "level2", "qmt_stock_l2_order_queue", "capital,realtime"),
    }
    for period, (category, permission, table, consumer) in period_config.items():
        definitions.append(
            ApiDefinition(
                category=category,
                api_name="get_market_data_ex",
                period=period,
                required_permission=permission,
                target_table=table,
                consumer_module=consumer,
            )
        )
    return definitions


def api_definitions() -> list[ApiDefinition]:
    direct = [
        ApiDefinition("stock_info", "get_instrument_detail", target_table="si_all_code", consumer_module="stock_list,analysis"),
        ApiDefinition("stock_info", "get_instrument_detail_list", target_table="si_all_code", consumer_module="stock_list,analysis"),
        ApiDefinition("stock_info", "download_history_contracts", target_table="si_all_code", consumer_module="backtest"),
        ApiDefinition("stock_info", "download_his_st_data", required_permission="vip", target_table="qmt_stock_st_history", consumer_module="event_risk,backtest"),
        ApiDefinition("stock_info", "get_his_st_data", required_permission="vip", target_table="qmt_stock_st_history", consumer_module="event_risk,backtest"),
        ApiDefinition("stock_info", "get_divid_factors", target_table="sm_dividend", consumer_module="analysis,backtest"),
        ApiDefinition("stock_market", "get_market_data", target_table="sm_stock_kline", consumer_module="analysis"),
        ApiDefinition("stock_market", "get_local_data", target_table="sm_stock_kline", consumer_module="analysis,backtest"),
        ApiDefinition("stock_market", "get_full_tick", target_table="sm_stock_current", consumer_module="realtime"),
        ApiDefinition("stock_market", "get_full_kline", target_table="sm_stock_current", consumer_module="realtime"),
        ApiDefinition("stock_market", "subscribe_quote", target_table="sm_stock_current", consumer_module="realtime"),
        ApiDefinition("stock_market", "subscribe_whole_quote", target_table="sm_stock_current", consumer_module="market_overview"),
        ApiDefinition("stock_market", "unsubscribe_quote", consumer_module="realtime"),
        ApiDefinition("stock_market", "download_history_data", target_table="qmt_raw_manifest", consumer_module="sync,reconcile"),
        ApiDefinition("stock_market", "download_history_data2", target_table="qmt_raw_manifest", consumer_module="sync,reconcile"),
        ApiDefinition("trade_calendar", "download_holiday_data", target_table="si_trade_calendar", consumer_module="scheduler,recommendation"),
        ApiDefinition("trade_calendar", "get_trading_calendar", target_table="si_trade_calendar", consumer_module="scheduler,recommendation"),
        ApiDefinition("financial", "download_financial_data", target_table="si_stock_finance", consumer_module="analysis"),
        ApiDefinition("financial", "download_financial_data2", target_table="si_stock_finance", consumer_module="analysis"),
        ApiDefinition("financial", "get_financial_data", target_table="si_stock_finance", consumer_module="analysis,recommendation"),
        ApiDefinition("sector", "download_sector_data", document_url=INDUSTRY_DOC, target_table="qmt_raw_manifest", consumer_module="sync"),
        ApiDefinition("sector", "get_sector_list", document_url=INDUSTRY_DOC, target_table="si_concept_code_east", consumer_module="sector,analysis"),
        ApiDefinition("sector", "get_stock_list_in_sector", document_url=INDUSTRY_DOC, target_table="si_concept_constituent_east", consumer_module="sector,analysis"),
        ApiDefinition("index", "download_index_weight", document_url=INDEX_DOC, target_table="si_index_constituent", consumer_module="sync"),
        ApiDefinition("index", "get_index_weight", document_url=INDEX_DOC, target_table="si_index_constituent", consumer_module="market,backtest"),
        ApiDefinition("market_time", "get_market_time", execution_mode="embedded_only", target_table="", consumer_module="health"),
        ApiDefinition("dragon_tiger", "get_longhubang", execution_mode="embedded_only", target_table="st_a_list_daily", consumer_module="alist"),
        ApiDefinition("northbound_holding", "get_hkt_details", execution_mode="embedded_only", required_permission="vip", target_table="qmt_hkt_holding", consumer_module="analysis"),
    ]
    return direct + _market_periods()


CATALOG_TABLE_DDLS: dict[str, str] = {
    "qmt_api_registry": """
        CREATE TABLE IF NOT EXISTS qmt_api_registry (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            provider VARCHAR(32) NOT NULL,
            capability_key VARCHAR(160) NOT NULL,
            category VARCHAR(64) NOT NULL,
            api_name VARCHAR(96) NOT NULL,
            period VARCHAR(64) NOT NULL DEFAULT '',
            execution_mode VARCHAR(32) NOT NULL DEFAULT 'native',
            required_permission VARCHAR(64) NOT NULL DEFAULT 'basic',
            document_url VARCHAR(512) NOT NULL,
            target_table VARCHAR(128) NOT NULL DEFAULT '',
            consumer_module VARCHAR(256) NOT NULL DEFAULT '',
            enabled TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NULL,
            UNIQUE KEY uk_qmt_api_registry (provider, capability_key),
            KEY idx_qmt_api_category (category)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "qmt_api_capability": """
        CREATE TABLE IF NOT EXISTS qmt_api_capability (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            provider VARCHAR(32) NOT NULL,
            capability_key VARCHAR(160) NOT NULL,
            api_name VARCHAR(96) NOT NULL,
            period VARCHAR(64) NOT NULL DEFAULT '',
            capability_status VARCHAR(40) NOT NULL,
            available TINYINT(1) NULL,
            returned_rows BIGINT NOT NULL DEFAULT 0,
            returned_fields_json TEXT NULL,
            error_message TEXT NULL,
            connection_port INT NULL,
            sdk_module VARCHAR(512) NULL,
            sdk_version VARCHAR(128) NULL,
            probed_at DATETIME NOT NULL,
            UNIQUE KEY uk_qmt_api_capability (provider, capability_key),
            KEY idx_qmt_capability_status (capability_status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
}


CATALOG_TABLE_CONTRACTS: dict[str, FrozenTable] = {
    "qmt_api_registry": FrozenTable(
        ddl=CATALOG_TABLE_DDLS["qmt_api_registry"],
        columns=(
            ("id", FrozenColumn("bigint", False, extra="auto_increment")),
            ("provider", character_column("varchar(32)", nullable=False)),
            ("capability_key", character_column("varchar(160)", nullable=False)),
            ("category", character_column("varchar(64)", nullable=False)),
            ("api_name", character_column("varchar(96)", nullable=False)),
            ("period", character_column("varchar(64)", nullable=False, default="")),
            ("execution_mode", character_column("varchar(32)", nullable=False, default="native")),
            ("required_permission", character_column("varchar(64)", nullable=False, default="basic")),
            ("document_url", character_column("varchar(512)", nullable=False)),
            ("target_table", character_column("varchar(128)", nullable=False, default="")),
            ("consumer_module", character_column("varchar(256)", nullable=False, default="")),
            ("enabled", FrozenColumn("tinyint(1)", False, default="1")),
            ("created_at", FrozenColumn("timestamp", False, default="current_timestamp")),
            ("updated_at", FrozenColumn("datetime", True)),
        ),
        indexes={
            "PRIMARY": FrozenIndex(("id",), True),
            "uk_qmt_api_registry": FrozenIndex(("provider", "capability_key"), True),
            "idx_qmt_api_category": FrozenIndex(("category",), False),
        },
    ),
    "qmt_api_capability": FrozenTable(
        ddl=CATALOG_TABLE_DDLS["qmt_api_capability"],
        columns=(
            ("id", FrozenColumn("bigint", False, extra="auto_increment")),
            ("provider", character_column("varchar(32)", nullable=False)),
            ("capability_key", character_column("varchar(160)", nullable=False)),
            ("api_name", character_column("varchar(96)", nullable=False)),
            ("period", character_column("varchar(64)", nullable=False, default="")),
            ("capability_status", character_column("varchar(40)", nullable=False)),
            ("available", FrozenColumn("tinyint(1)", True)),
            ("returned_rows", FrozenColumn("bigint", False, default="0")),
            ("returned_fields_json", character_column("text", nullable=True)),
            ("error_message", character_column("text", nullable=True)),
            ("connection_port", FrozenColumn("int", True)),
            ("sdk_module", character_column("varchar(512)", nullable=True)),
            ("sdk_version", character_column("varchar(128)", nullable=True)),
            ("probed_at", FrozenColumn("datetime", False)),
        ),
        indexes={
            "PRIMARY": FrozenIndex(("id",), True),
            "uk_qmt_api_capability": FrozenIndex(("provider", "capability_key"), True),
            "idx_qmt_capability_status": FrozenIndex(("capability_status",), False),
        },
    ),
}


def validate_catalog_schema(engine: Engine, *, connection=None) -> dict[str, Any]:
    """Validate the catalog physical contract with SELECT statements only."""

    return validate_frozen_tables(
        engine,
        CATALOG_TABLE_CONTRACTS,
        context="QMT API catalog",
        connection=connection,
    )


def privileged_migrate_catalog_schema(engine: Engine) -> dict[str, Any]:
    """Create/validate catalog tables during a fenced privileged migration."""

    return privileged_migrate_frozen_tables(
        engine,
        CATALOG_TABLE_CONTRACTS,
        context="QMT API catalog",
    )


def _seed_payload(
    definitions: Iterable[ApiDefinition] | None = None,
) -> list[dict[str, Any]]:
    rows = list(definitions if definitions is not None else api_definitions())
    if not rows:
        raise RuntimeError("QMT catalog seed definitions cannot be empty")
    payload: list[dict[str, Any]] = []
    seen: set[str] = set()
    for definition in rows:
        if definition.capability_key in seen:
            raise RuntimeError(
                f"duplicate QMT catalog seed identity: {definition.capability_key}"
            )
        seen.add(definition.capability_key)
        item = asdict(definition)
        item.update({
            "provider": PROVIDER_ID,
            "capability_key": definition.capability_key,
            "enabled": 1,
        })
        payload.append(item)
    return sorted(payload, key=lambda row: str(row["capability_key"]))


def _seed_contract_hash_from_payload(payload: list[dict[str, Any]]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def catalog_seed_contract_hash(
    definitions: Iterable[ApiDefinition] | None = None,
) -> str:
    return _seed_contract_hash_from_payload(_seed_payload(definitions))


def _validate_catalog_seed_on_connection(
    connection,
    definitions: Iterable[ApiDefinition] | None = None,
) -> dict[str, Any]:
    expected_rows = _seed_payload(definitions)
    expected = {str(row["capability_key"]): row for row in expected_rows}
    rows = connection.execute(
        text(
            "SELECT provider, capability_key, category, api_name, period, "
            "execution_mode, required_permission, document_url, target_table, "
            "consumer_module, enabled FROM qmt_api_registry "
            "WHERE provider=:provider ORDER BY capability_key"
        ),
        {"provider": PROVIDER_ID},
    ).mappings().all()
    actual_rows = [dict(row) for row in rows]
    active: dict[str, dict[str, Any]] = {}
    inactive_count = 0
    for row in actual_rows:
        capability_key = str(row.get("capability_key") or "")
        if int(row.get("enabled") or 0) != 1:
            inactive_count += 1
            continue
        if capability_key in active:
            raise RuntimeError(f"duplicate active QMT catalog seed: {capability_key}")
        active[capability_key] = row
    if set(active) != set(expected):
        raise RuntimeError(
            "QMT catalog active seed identity differs: "
            f"missing={sorted(set(expected) - set(active))} "
            f"unexpected={sorted(set(active) - set(expected))}"
        )
    identity_fields = (
        "provider", "capability_key", "category", "api_name", "period",
        "execution_mode", "required_permission", "document_url", "target_table",
        "consumer_module",
    )
    for capability_key, expected_row in expected.items():
        actual = active[capability_key]
        differences = {
            field: (str(actual.get(field) or ""), str(expected_row.get(field) or ""))
            for field in identity_fields
            if str(actual.get(field) or "") != str(expected_row.get(field) or "")
        }
        if differences:
            raise RuntimeError(
                f"QMT catalog seed payload differs: {capability_key} {differences}"
            )
    return {
        "provider": PROVIDER_ID,
        "active_registry_rows": len(active),
        "inactive_registry_rows": inactive_count,
        "seed_contract_hash": _seed_contract_hash_from_payload(expected_rows),
        "seed_identity_verified": True,
        "read_only": True,
        "runtime_seed_required": False,
    }


def validate_catalog_registry_seed(
    engine: Engine,
    definitions: Iterable[ApiDefinition] | None = None,
    *,
    connection=None,
) -> dict[str, Any]:
    """Validate the immutable active seed identity using SELECT only."""

    if connection is not None:
        validate_catalog_schema(engine, connection=connection)
        return _validate_catalog_seed_on_connection(connection, definitions)
    validate_catalog_schema(engine)
    with engine.connect() as bound_connection:
        return _validate_catalog_seed_on_connection(bound_connection, definitions)


def privileged_seed_catalog_registry(
    engine: Engine,
    definitions: Iterable[ApiDefinition] | None = None,
) -> dict[str, Any]:
    """Install the active registry seed during a writer-fenced release phase.

    Removed API definitions are retained for auditability but disabled.  The
    active set and every active identity field must exactly match the frozen
    definitions before the migration succeeds.
    """

    validate_catalog_schema(engine)
    definition_rows = list(
        definitions if definitions is not None else api_definitions()
    )
    payload = _seed_payload(definition_rows)
    sql = text(
        """
        INSERT INTO qmt_api_registry (
            provider, capability_key, category, api_name, period, execution_mode,
            required_permission, document_url, target_table, consumer_module, enabled
        ) VALUES (
            :provider, :capability_key, :category, :api_name, :period, :execution_mode,
            :required_permission, :document_url, :target_table, :consumer_module, 1
        )
        ON DUPLICATE KEY UPDATE
            category=VALUES(category), api_name=VALUES(api_name), period=VALUES(period),
            execution_mode=VALUES(execution_mode), required_permission=VALUES(required_permission),
            document_url=VALUES(document_url), target_table=VALUES(target_table),
            consumer_module=VALUES(consumer_module), enabled=1, updated_at=NOW()
        """
    )
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE qmt_api_registry SET enabled=0, updated_at=NOW() "
                "WHERE provider=:provider"
            ),
            {"provider": PROVIDER_ID},
        )
        conn.execute(sql, payload)
        seed_result = _validate_catalog_seed_on_connection(conn, definition_rows)
    return {
        **seed_result,
        "seeded_registry_rows": len(payload),
        "privileged_seed": True,
        "read_only": False,
    }


CORE_PROBE_TO_REGISTRY_KEYS: dict[str, tuple[str, ...]] = {
    "sector_list": ("native:get_sector_list:-",),
    "stock_universe": ("native:get_stock_list_in_sector:-",),
    "index_universe": ("native:get_stock_list_in_sector:-",),
    "qmt_sector_indexes": ("native:get_stock_list_in_sector:-",),
    "stock_instrument": ("native:get_instrument_detail:-",),
    "index_instrument": ("native:get_instrument_detail:-",),
    "stock_full_tick": ("native:get_full_tick:-",),
    "index_full_tick": ("native:get_full_tick:-",),
    "index_weight": ("native:get_index_weight:-",),
    "trading_calendar": ("native:get_trading_calendar:-",),
    "stock_daily_bar": ("native:get_market_data_ex:1d",),
    "index_daily_bar": ("native:get_market_data_ex:1d",),
    "stock_minute_bar": ("native:get_market_data_ex:1m",),
    "index_minute_bar": ("native:get_market_data_ex:1m",),
    "stock_5m_bar": ("native:get_market_data_ex:5m",),
    "stock_15m_bar": ("native:get_market_data_ex:15m",),
    "stock_30m_bar": ("native:get_market_data_ex:30m",),
    "stock_1h_bar": ("native:get_market_data_ex:1h",),
    "stock_week_bar": ("native:get_market_data_ex:1w",),
    "stock_month_bar": ("native:get_market_data_ex:1mon",),
    "stock_tick_bar": ("native:get_market_data_ex:tick",),
    "stock_flow_daily": ("native:get_market_data_ex:transactioncount1d",),
    "stock_flow_min": ("native:get_market_data_ex:transactioncount1m",),
    "stock_orderflow_daily": ("native:get_market_data_ex:orderflow1d",),
    "stock_orderflow_min": ("native:get_market_data_ex:orderflow1m",),
    "northbound_flow_daily": ("native:get_market_data_ex:northfinancechange1d",),
    "northbound_flow_min": ("native:get_market_data_ex:northfinancechange1m",),
    "interactive_qa": ("native:get_market_data_ex:interactiveqa",),
    "announcement": ("native:get_market_data_ex:announcement",),
    "stock_l2_quote": ("native:get_market_data_ex:l2quote",),
    "stock_l2_quote_aux": ("native:get_market_data_ex:l2quoteaux",),
    "stock_l2_order": ("native:get_market_data_ex:l2order",),
    "stock_l2_transaction": ("native:get_market_data_ex:l2transaction",),
    "stock_l2_transaction_count": ("native:get_market_data_ex:l2transactioncount",),
    "stock_l2_order_queue": ("native:get_market_data_ex:l2orderqueue",),
}


def _capability_row_priority(row: dict[str, Any]) -> int:
    status = str(row.get("capability_status") or "")
    available = row.get("available")
    if available == 1 and status == "SUPPORTED":
        return 50
    if available == 1:
        return 40
    if status in {"NOT_AUTHORIZED", "UNSUPPORTED_CLIENT"}:
        return 30
    if status in {"NO_DATA", "FAILED"}:
        return 20
    if status.startswith("PENDING_"):
        return 10
    return 0


def _dedupe_capability_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = f"{row.get('provider')}::{row.get('capability_key')}"
        existing = deduped.get(key)
        if existing is None or _capability_row_priority(row) >= _capability_row_priority(existing):
            deduped[key] = row
    return list(deduped.values())


def save_capabilities(engine: Engine, capability_result: dict[str, Any], core_result: dict[str, Any]) -> int:
    rows: list[dict[str, Any]] = []
    connection_port = capability_result.get("connection_port") or core_result.get("connection_port")
    sdk_module = capability_result.get("sdk_module")
    sdk_version = capability_result.get("sdk_version")
    for item in capability_result.get("rows") or []:
        api_name = str(item.get("api_name") or "")
        rows.append(
            {
                "provider": PROVIDER_ID,
                "capability_key": f"native:{api_name}:-",
                "api_name": api_name,
                "period": "",
                "capability_status": "SDK_AVAILABLE" if item.get("available") else "SDK_UNSUPPORTED",
                "available": 1 if item.get("available") else 0,
                "returned_rows": 0,
                "returned_fields_json": "[]",
                "error_message": None,
                "connection_port": connection_port,
                "sdk_module": sdk_module,
                "sdk_version": sdk_version,
            }
        )
    for item in core_result.get("rows") or []:
        probe_name = str(item.get("probe_name") or "")
        status = str(item.get("status") or "UNKNOWN")
        available = 1 if status == "SUPPORTED" else 0
        common = {
            "capability_status": status,
            "available": available,
            "returned_rows": int(item.get("row_count") or 0),
            "returned_fields_json": json.dumps(item.get("fields") or [], ensure_ascii=False),
            "error_message": item.get("error"),
            "connection_port": connection_port,
            "sdk_module": sdk_module,
            "sdk_version": sdk_version,
        }
        rows.append(
            {
                "provider": PROVIDER_ID,
                "capability_key": f"probe:{probe_name}:-",
                "api_name": probe_name,
                "period": "",
                **common,
            }
        )
        for capability_key in CORE_PROBE_TO_REGISTRY_KEYS.get(probe_name, ()):
            _execution_mode, api_name, period = capability_key.split(":", 2)
            rows.append(
                {
                    "provider": PROVIDER_ID,
                    "capability_key": capability_key,
                    "api_name": api_name,
                    "period": "" if period == "-" else period,
                    **common,
                }
            )
    if not rows:
        return 0
    rows = _dedupe_capability_rows(rows)
    sql = text(
        """
        INSERT INTO qmt_api_capability (
            provider, capability_key, api_name, period, capability_status, available,
            returned_rows, returned_fields_json, error_message, connection_port,
            sdk_module, sdk_version, probed_at
        ) VALUES (
            :provider, :capability_key, :api_name, :period, :capability_status, :available,
            :returned_rows, :returned_fields_json, :error_message, :connection_port,
            :sdk_module, :sdk_version, NOW()
        )
        ON DUPLICATE KEY UPDATE
            capability_status=VALUES(capability_status), available=VALUES(available),
            returned_rows=VALUES(returned_rows), returned_fields_json=VALUES(returned_fields_json),
            error_message=VALUES(error_message), connection_port=VALUES(connection_port),
            sdk_module=VALUES(sdk_module), sdk_version=VALUES(sdk_version), probed_at=NOW()
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def complete_capability_ledger(engine: Engine) -> int:
    """Ensure every documented registry row has an explicit capability status.

    Native SDK existence alone does not prove each period/permission variant can
    return data, especially for L2/orderflow/VIP datasets.  Missing rows are
    therefore filled as pending/special-probe statuses instead of being silently
    absent from the ledger.
    """
    with engine.begin() as conn:
        normalized = int(
            conn.execute(
                text(
                    """
                    UPDATE qmt_api_capability c
                    JOIN qmt_api_registry r
                      ON r.provider = c.provider
                     AND r.capability_key = c.capability_key
                    SET c.capability_status = 'EMBEDDED_ONLY_PENDING',
                        c.available = 0,
                        c.error_message = 'Documented API is marked embedded_only; native SDK runtime cannot verify it.',
                        c.probed_at = NOW()
                    WHERE r.provider = :provider
                      AND r.enabled = 1
                      AND r.execution_mode = 'embedded_only'
                      AND c.capability_status IN ('PENDING_NATIVE_PROBE', 'PENDING_SAMPLE_PROBE')
                    """
                ),
                {"provider": PROVIDER_ID},
            ).rowcount
            or 0
        )
        native_available = {
            str(row[0])
            for row in conn.execute(
                text(
                    """
                    SELECT DISTINCT api_name
                    FROM qmt_api_capability
                    WHERE provider = :provider
                      AND capability_key LIKE 'native:%'
                      AND available = 1
                    """
                ),
                {"provider": PROVIDER_ID},
            ).fetchall()
        }
        registry_rows = conn.execute(
            text(
                """
                SELECT r.capability_key, r.api_name, r.period, r.required_permission, r.execution_mode
                FROM qmt_api_registry r
                LEFT JOIN qmt_api_capability c
                  ON c.provider = r.provider
                 AND c.capability_key = r.capability_key
                WHERE r.provider = :provider
                  AND r.enabled = 1
                  AND c.id IS NULL
                ORDER BY r.capability_key
                """
            ),
            {"provider": PROVIDER_ID},
        ).mappings().fetchall()
        if not registry_rows:
            return normalized

        payload: list[dict[str, Any]] = []
        for row in registry_rows:
            api_name = str(row["api_name"] or "")
            permission = str(row["required_permission"] or "basic")
            execution_mode = str(row.get("execution_mode") or "")
            sdk_exists = api_name in native_available
            if execution_mode == "embedded_only":
                status = "EMBEDDED_ONLY_PENDING"
                error_message = "Documented API is marked embedded_only; native SDK runtime cannot verify it."
            elif not sdk_exists:
                status = "PENDING_NATIVE_PROBE"
                error_message = "Documented API has not been confirmed in the current SDK runtime."
            elif permission in {"level2", "orderflow", "vip"}:
                status = "PENDING_PERMISSION_PROBE"
                error_message = f"SDK function exists; {permission} permission/sample probe is still required."
            elif row["period"]:
                status = "PENDING_PERIOD_PROBE"
                error_message = "SDK function exists; this period/dataset still requires a dedicated sample probe."
            else:
                status = "PENDING_SAMPLE_PROBE"
                error_message = "SDK function exists; sample return fields still need to be captured."
            payload.append(
                {
                    "provider": PROVIDER_ID,
                    "capability_key": str(row["capability_key"]),
                    "api_name": api_name,
                    "period": str(row["period"] or ""),
                    "capability_status": status,
                    "available": None,
                    "returned_rows": 0,
                    "returned_fields_json": "[]",
                    "error_message": error_message,
                }
            )

        conn.execute(
            text(
                """
                INSERT INTO qmt_api_capability (
                    provider, capability_key, api_name, period, capability_status,
                    available, returned_rows, returned_fields_json, error_message, probed_at
                ) VALUES (
                    :provider, :capability_key, :api_name, :period, :capability_status,
                    :available, :returned_rows, :returned_fields_json, :error_message, NOW()
                )
                ON DUPLICATE KEY UPDATE
                    capability_status = VALUES(capability_status),
                    available = VALUES(available),
                    returned_rows = VALUES(returned_rows),
                    returned_fields_json = VALUES(returned_fields_json),
                    error_message = VALUES(error_message),
                    probed_at = NOW()
                """
            ),
            payload,
        )
    return normalized + len(payload)
