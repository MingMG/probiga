"""Read-only, fail-closed QMT historical-data capability matrix.

An SDK method being present is not proof that a historical dataset is usable.
This assessor keeps three facts separate:

* the provider's latest, fresh probe result in ``qmt_api_capability``;
* an immutable ``EXACT`` history-coverage manifest for every required session;
* strategy eligibility, which requires both of the above.

Provider limitations are reported verbatim.  In particular, ``NO_DATA``,
``NOT_AUTHORIZED`` and ``UNSUPPORTED_CLIENT`` are healthy *evidence* of an
unavailable capability, never synthetic data and never strategy eligible.
The module performs SELECT statements only.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from integrations.bigqmt.spool import PROVIDER_ID as BIGQMT_PROVIDER_ID
from integrations.qmt.diagnostics import PROVIDER_ID as NATIVE_QMT_PROVIDER_ID
from server.common.qmt_history_coverage import (
    COVERAGE_EXACT,
    COVERAGE_INCOMPLETE,
    COVERAGE_TABLE,
    COVERAGE_UNAVAILABLE,
)


CAPABILITY_MATRIX_SCHEMA = "probiga.qmt-history-capability-matrix.v1"
QMT_PROVIDER = NATIVE_QMT_PROVIDER_ID
DEFAULT_CAPABILITY_MAX_AGE_SECONDS = 96 * 60 * 60

API_AVAILABLE = "AVAILABLE"
API_UNAVAILABLE = "UNAVAILABLE"
EVIDENCE_HEALTHY = "HEALTHY"
EVIDENCE_UNHEALTHY = "UNHEALTHY"
KNOWN_UNAVAILABLE_PROVIDER_STATUSES = frozenset(
    {"NO_DATA", "NOT_AUTHORIZED", "UNSUPPORTED_CLIENT"}
)
FINAL_PROVIDER_STATUSES = frozenset(
    {"SUPPORTED", *KNOWN_UNAVAILABLE_PROVIDER_STATUSES}
)

_HASH = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class QmtHistoryDatasetSpec:
    """A frozen provider/coverage contract for one strategy input."""

    dataset: str
    display_name: str
    category: str
    period: str
    capability_keys: tuple[str, ...]
    coverage_dataset: str
    point_in_time_required: bool
    strategy_feature_group: str
    capability_provider: str = NATIVE_QMT_PROVIDER_ID
    coverage_provider: str = NATIVE_QMT_PROVIDER_ID


QMT_HISTORY_DATASET_SPECS: tuple[QmtHistoryDatasetSpec, ...] = (
    QmtHistoryDatasetSpec(
        "stock_daily",
        "股票日线",
        "market_bar",
        "1d",
        (
            "native:get_market_data_ex:1d",
            "probe:stock_daily_bar:-",
        ),
        "stock_daily",
        False,
        "stock_price",
        coverage_provider=BIGQMT_PROVIDER_ID,
    ),
    QmtHistoryDatasetSpec(
        "stock_minute",
        "股票分钟线",
        "market_bar",
        "1m",
        (
            "native:get_market_data_ex:1m",
            "probe:stock_minute_bar:-",
        ),
        "stock_minute",
        False,
        "stock_intraday",
    ),
    QmtHistoryDatasetSpec(
        "index_daily",
        "指数日线",
        "market_bar",
        "1d",
        (
            "native:get_market_data_ex:1d",
            "probe:index_daily_bar:-",
        ),
        "index_daily",
        False,
        "market_regime",
    ),
    QmtHistoryDatasetSpec(
        "index_minute",
        "指数分钟线",
        "market_bar",
        "1m",
        (
            "native:get_market_data_ex:1m",
            "probe:index_minute_bar:-",
        ),
        "index_minute",
        False,
        "market_regime_intraday",
    ),
    QmtHistoryDatasetSpec(
        "sector_membership_pit",
        "板块历史成分（PIT）",
        "membership",
        "pit",
        (
            "native:get_sector_list:-",
            "native:get_stock_list_in_sector:-",
            "probe:qmt_sector_indexes:-",
        ),
        "sector_membership_pit",
        True,
        "sector_rotation",
    ),
    QmtHistoryDatasetSpec(
        "industry_membership_pit",
        "行业历史成分（PIT）",
        "membership",
        "pit",
        (
            "native:get_sector_list:-",
            "native:get_stock_list_in_sector:-",
            "probe:stock_universe:-",
        ),
        "industry_membership_pit",
        True,
        "industry_rotation",
    ),
    QmtHistoryDatasetSpec(
        "concept_membership_pit",
        "概念历史成分（PIT）",
        "membership",
        "pit",
        (
            "native:get_sector_list:-",
            "native:get_stock_list_in_sector:-",
            "probe:stock_universe:-",
        ),
        "concept_membership_pit",
        True,
        "concept_rotation",
    ),
    QmtHistoryDatasetSpec(
        "index_weight_pit",
        "指数历史权重（PIT）",
        "membership",
        "pit",
        (
            "native:get_index_weight:-",
            "probe:index_weight:-",
        ),
        "index_weight_pit",
        True,
        "index_constituent",
    ),
    QmtHistoryDatasetSpec(
        "stock_funds_flow_daily",
        "个股资金流（日）",
        "funds_flow",
        "transactioncount1d",
        (
            "native:get_market_data_ex:transactioncount1d",
            "probe:stock_flow_daily:-",
        ),
        "stock_funds_flow_daily",
        False,
        "capital_flow",
    ),
    QmtHistoryDatasetSpec(
        "stock_funds_flow_minute",
        "个股资金流（分钟）",
        "funds_flow",
        "transactioncount1m",
        (
            "native:get_market_data_ex:transactioncount1m",
            "probe:stock_flow_min:-",
        ),
        "stock_funds_flow_minute",
        False,
        "capital_flow_intraday",
    ),
    QmtHistoryDatasetSpec(
        "stock_orderflow_daily",
        "个股订单流（日）",
        "orderflow",
        "orderflow1d",
        (
            "native:get_market_data_ex:orderflow1d",
            "probe:stock_orderflow_daily:-",
        ),
        "stock_orderflow_daily",
        False,
        "orderflow",
    ),
    QmtHistoryDatasetSpec(
        "stock_orderflow_minute",
        "个股订单流（分钟）",
        "orderflow",
        "orderflow1m",
        (
            "native:get_market_data_ex:orderflow1m",
            "probe:stock_orderflow_min:-",
        ),
        "stock_orderflow_minute",
        False,
        "orderflow_intraday",
    ),
    QmtHistoryDatasetSpec(
        "northbound_flow_daily",
        "北向资金（日）",
        "northbound",
        "northfinancechange1d",
        (
            "native:get_market_data_ex:northfinancechange1d",
            "probe:northbound_flow_daily:-",
        ),
        "northbound_flow_daily",
        False,
        "northbound_flow",
    ),
    QmtHistoryDatasetSpec(
        "northbound_flow_minute",
        "北向资金（分钟）",
        "northbound",
        "northfinancechange1m",
        (
            "native:get_market_data_ex:northfinancechange1m",
            "probe:northbound_flow_min:-",
        ),
        "northbound_flow_minute",
        False,
        "northbound_flow_intraday",
    ),
    QmtHistoryDatasetSpec(
        "stock_l2_quote",
        "Level-2 行情",
        "level2",
        "l2quote",
        (
            "native:get_market_data_ex:l2quote",
            "probe:stock_l2_quote:-",
        ),
        "stock_l2_quote",
        False,
        "level2_quote",
    ),
    QmtHistoryDatasetSpec(
        "stock_l2_order",
        "Level-2 逐笔委托",
        "level2",
        "l2order",
        (
            "native:get_market_data_ex:l2order",
            "probe:stock_l2_order:-",
        ),
        "stock_l2_order",
        False,
        "level2_order",
    ),
    QmtHistoryDatasetSpec(
        "stock_l2_transaction",
        "Level-2 逐笔成交",
        "level2",
        "l2transaction",
        (
            "native:get_market_data_ex:l2transaction",
            "probe:stock_l2_transaction:-",
        ),
        "stock_l2_transaction",
        False,
        "level2_transaction",
    ),
    QmtHistoryDatasetSpec(
        "stock_l2_transaction_count",
        "Level-2 成交笔数",
        "level2",
        "l2transactioncount",
        (
            "native:get_market_data_ex:l2transactioncount",
            "probe:stock_l2_transaction_count:-",
        ),
        "stock_l2_transaction_count",
        False,
        "level2_transaction_count",
    ),
    QmtHistoryDatasetSpec(
        "stock_l2_order_queue",
        "Level-2 委托队列",
        "level2",
        "l2orderqueue",
        (
            "native:get_market_data_ex:l2orderqueue",
            "probe:stock_l2_order_queue:-",
        ),
        "stock_l2_order_queue",
        False,
        "level2_order_queue",
    ),
)

QMT_HISTORY_DATASET_KEYS = tuple(
    spec.dataset for spec in QMT_HISTORY_DATASET_SPECS
)
QMT_HISTORY_DATASET_SPEC_BY_KEY = {
    spec.dataset: spec for spec in QMT_HISTORY_DATASET_SPECS
}


def _rows(result: Any) -> list[dict[str, Any]]:
    mapped = result.mappings()
    values = mapped.all() if hasattr(mapped, "all") else list(mapped)
    return [dict(row) for row in values]


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _availability_flag(value: Any) -> bool | None:
    if value is True or value == 1:
        return True
    if value is False or value == 0:
        return False
    return None


def _iso_date(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        return date.fromisoformat(str(value or "").strip()[:10]).isoformat()
    except ValueError:
        return None


def _dedupe(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _normalize_required_dates(
    value: Mapping[str, Iterable[Any]] | None,
) -> tuple[dict[str, tuple[str, ...]], list[str]]:
    normalized: dict[str, tuple[str, ...]] = {}
    errors: list[str] = []
    if value is None:
        return normalized, errors
    if not isinstance(value, Mapping):
        return normalized, ["coverage_scope_not_mapping"]
    for dataset, raw_dates in value.items():
        key = str(dataset or "")
        if key not in QMT_HISTORY_DATASET_SPEC_BY_KEY:
            errors.append(f"coverage_scope_unknown_dataset:{key}")
            continue
        values: Iterable[Any]
        if isinstance(raw_dates, (str, date, datetime)):
            values = (raw_dates,)
        else:
            try:
                values = tuple(raw_dates)
            except TypeError:
                errors.append(f"coverage_scope_invalid:{key}")
                continue
        dates: list[str] = []
        invalid = False
        for item in values:
            parsed = _iso_date(item)
            if parsed is None:
                invalid = True
            else:
                dates.append(parsed)
        if invalid:
            errors.append(f"coverage_scope_invalid:{key}")
            continue
        normalized[key] = tuple(sorted(set(dates)))
    return normalized, errors


def _capability_detail(
    *,
    capability_key: str,
    candidates: list[dict[str, Any]],
    provider: str,
    maximum_age_seconds: int,
) -> dict[str, Any]:
    blockers: list[str] = []
    evidence_healthy = True
    api_available = False
    row = candidates[0] if len(candidates) == 1 else None
    if not candidates:
        blockers.append("capability_evidence_missing")
        evidence_healthy = False
    elif len(candidates) != 1:
        blockers.append("capability_evidence_ambiguous")
        evidence_healthy = False

    status_raw: str | None = None
    status_normalized: str | None = None
    age: int | None = None
    freshness = "MISSING" if row is None else "INVALID"
    available: bool | None = None
    if row is not None:
        status_raw = str(row.get("capability_status") or "")
        status_normalized = status_raw.strip().upper()
        age = _integer(row.get("probe_age_seconds"))
        available = _availability_flag(row.get("available"))
        if (
            str(row.get("provider") or "") != provider
            or str(row.get("capability_key") or "") != capability_key
        ):
            blockers.append("capability_identity_mismatch")
            evidence_healthy = False
        if age is None:
            blockers.append("capability_freshness_invalid")
            evidence_healthy = False
        elif age < 0:
            freshness = "FUTURE"
            blockers.append("capability_probe_future")
            evidence_healthy = False
        elif age > maximum_age_seconds:
            freshness = "STALE"
            blockers.append("capability_probe_stale")
            evidence_healthy = False
        else:
            freshness = "FRESH"

        if status_normalized == "SUPPORTED":
            if available is not True:
                blockers.append("capability_supported_flag_mismatch")
                evidence_healthy = False
            elif freshness == "FRESH":
                api_available = True
        elif status_normalized in KNOWN_UNAVAILABLE_PROVIDER_STATUSES:
            blockers.append(f"provider_status:{status_normalized}")
            if available is not False:
                blockers.append("capability_unavailable_flag_mismatch")
                evidence_healthy = False
        elif status_normalized == "FAILED":
            blockers.append("capability_probe_failed")
            evidence_healthy = False
        elif "PENDING" in (status_normalized or ""):
            blockers.append("capability_probe_pending")
            evidence_healthy = False
        else:
            blockers.append("capability_status_not_final")
            evidence_healthy = False

    return {
        "capability_key": capability_key,
        "provider_status": status_raw,
        "normalized_provider_status": status_normalized,
        "available": available,
        "api_available": api_available,
        "evidence_healthy": evidence_healthy,
        "freshness": freshness,
        "probe_age_seconds": age,
        "probed_at": row.get("probed_at") if row else None,
        "returned_rows": _integer(row.get("returned_rows")) if row else None,
        "error_message": row.get("error_message") if row else None,
        "connection_port": _integer(row.get("connection_port")) if row else None,
        "sdk_module": row.get("sdk_module") if row else None,
        "sdk_version": row.get("sdk_version") if row else None,
        "blockers": _dedupe(blockers),
    }


def _coverage_detail(
    *,
    spec: QmtHistoryDatasetSpec,
    candidates: list[dict[str, Any]],
    required_dates: tuple[str, ...] | None,
    provider: str,
    query_failed: bool,
) -> dict[str, Any]:
    blockers: list[str] = []
    evidence_healthy = not query_failed
    valid_exact_dates: set[str] = set()
    valid_incomplete_dates: set[str] = set()
    valid_unavailable_dates: set[str] = set()
    observed_dates: set[str] = set()
    status_counts = {
        COVERAGE_EXACT: 0,
        COVERAGE_INCOMPLETE: 0,
        COVERAGE_UNAVAILABLE: 0,
    }
    if query_failed:
        blockers.append("coverage_query_failed")

    for row in candidates:
        row_errors: list[str] = []
        dataset = str(row.get("dataset") or "")
        trade_date = _iso_date(row.get("trade_date"))
        period = str(row.get("period") or "")
        status = str(row.get("status") or "").strip().upper()
        strategy_flag = _availability_flag(row.get("strategy_eligible"))
        manifest_hash = str(row.get("manifest_hash") or "").strip().lower()
        captured_age = _integer(row.get("capture_age_seconds"))
        if (
            dataset != spec.coverage_dataset
            or str(row.get("provider") or "") != provider
        ):
            row_errors.append("coverage_identity_mismatch")
        if trade_date is None:
            row_errors.append("coverage_trade_date_invalid")
        if period != spec.period:
            row_errors.append("coverage_period_mismatch")
        if status not in status_counts:
            row_errors.append("coverage_status_invalid")
        if _HASH.fullmatch(manifest_hash) is None:
            row_errors.append("coverage_manifest_hash_invalid")
        if captured_age is None:
            row_errors.append("coverage_capture_time_invalid")
        elif captured_age < 0:
            row_errors.append("coverage_evidence_future")
        if (
            strategy_flag is None
            or (status == COVERAGE_EXACT) != strategy_flag
        ):
            row_errors.append("coverage_eligibility_flag_mismatch")
        if row_errors:
            blockers.extend(row_errors)
            evidence_healthy = False
            continue
        assert trade_date is not None
        observed_dates.add(trade_date)
        status_counts[status] += 1
        if status == COVERAGE_EXACT:
            valid_exact_dates.add(trade_date)
        elif status == COVERAGE_INCOMPLETE:
            valid_incomplete_dates.add(trade_date)
        elif status == COVERAGE_UNAVAILABLE:
            valid_unavailable_dates.add(trade_date)

    if required_dates is None or not required_dates:
        coverage_status = "UNASSESSED"
        missing_dates: list[str] = []
        blockers.append("coverage_scope_missing")
        exact = False
        required_count = 0
    else:
        required = set(required_dates)
        missing_dates = sorted(required - valid_exact_dates)
        required_count = len(required)
        exact = not missing_dates and evidence_healthy
        if exact:
            coverage_status = COVERAGE_EXACT
        elif valid_exact_dates & required:
            coverage_status = COVERAGE_INCOMPLETE
        elif valid_incomplete_dates & required:
            coverage_status = COVERAGE_INCOMPLETE
        elif valid_unavailable_dates & required:
            coverage_status = COVERAGE_UNAVAILABLE
        else:
            coverage_status = "MISSING"
        if missing_dates:
            blockers.append("coverage_exact_missing")

    return {
        "coverage_dataset": spec.coverage_dataset,
        "period": spec.period,
        "status": coverage_status,
        "exact": exact,
        "evidence_healthy": evidence_healthy,
        "required_trade_date_count": required_count,
        "exact_required_trade_date_count": (
            len(set(required_dates or ()) & valid_exact_dates)
        ),
        "missing_trade_dates": missing_dates,
        "observed_trade_date_count": len(observed_dates),
        "first_observed_trade_date": min(observed_dates) if observed_dates else None,
        "last_observed_trade_date": max(observed_dates) if observed_dates else None,
        "status_counts": status_counts,
        "blockers": _dedupe(blockers),
    }


def assess_qmt_history_capabilities(
    connection: Any,
    *,
    required_trade_dates_by_dataset: Mapping[str, Iterable[Any]] | None = None,
    capability_max_age_seconds: int = DEFAULT_CAPABILITY_MAX_AGE_SECONDS,
) -> tuple[bool, dict[str, Any]]:
    """Assess fixed QMT history inputs without mutating provider or DB state.

    ``required_trade_dates_by_dataset`` is deliberately explicit.  Omitting a
    dataset's scope leaves it ``UNASSESSED`` and strategy-ineligible; the
    assessor will not guess that one exact day proves an entire backtest.

    The returned boolean is evidence health, not "all datasets are available".
    A fresh, explicit provider ``NO_DATA`` is healthy evidence but closes every
    related strategy.  Missing, stale, future, failed, pending or ambiguous
    evidence makes the boolean false.
    """

    maximum_age = _integer(capability_max_age_seconds)
    global_errors: list[str] = []
    if maximum_age is None or maximum_age < 60:
        maximum_age = DEFAULT_CAPABILITY_MAX_AGE_SECONDS
        global_errors.append("capability_max_age_invalid")
    capability_providers = sorted(
        {spec.capability_provider for spec in QMT_HISTORY_DATASET_SPECS}
    )
    if capability_providers != [NATIVE_QMT_PROVIDER_ID]:
        global_errors.append("capability_provider_contract_invalid")
    provider_id = NATIVE_QMT_PROVIDER_ID

    required_dates, scope_errors = _normalize_required_dates(
        required_trade_dates_by_dataset
    )
    global_errors.extend(scope_errors)

    capability_keys = sorted(
        {
            key
            for spec in QMT_HISTORY_DATASET_SPECS
            for key in spec.capability_keys
        }
    )
    capability_params = {"provider": provider_id}
    capability_params.update(
        {f"key_{index}": key for index, key in enumerate(capability_keys)}
    )
    capability_placeholders = ",".join(
        f":key_{index}" for index in range(len(capability_keys))
    )
    try:
        capability_rows = _rows(
            connection.execute(
                text(
                    "SELECT provider, capability_key, capability_status, "
                    "available, returned_rows, error_message, connection_port, "
                    "sdk_module, sdk_version, probed_at, "
                    "TIMESTAMPDIFF(SECOND, probed_at, NOW()) "
                    "AS probe_age_seconds FROM qmt_api_capability "
                    "WHERE provider=:provider AND capability_key IN ("
                    f"{capability_placeholders}) ORDER BY capability_key, id"
                ),
                capability_params,
            )
        )
        capability_query_failed = False
    except Exception:
        capability_rows = []
        capability_query_failed = True
        global_errors.append("capability_ledger_query_failed")

    capabilities_by_key: dict[str, list[dict[str, Any]]] = {
        key: [] for key in capability_keys
    }
    for row in capability_rows:
        key = str(row.get("capability_key") or "")
        if key not in capabilities_by_key:
            global_errors.append("capability_query_returned_unexpected_key")
            continue
        capabilities_by_key[key].append(row)

    coverage_datasets = [
        spec.coverage_dataset for spec in QMT_HISTORY_DATASET_SPECS
    ]
    coverage_providers = sorted(
        {spec.coverage_provider for spec in QMT_HISTORY_DATASET_SPECS}
    )
    coverage_params = {
        f"coverage_provider_{index}": provider
        for index, provider in enumerate(coverage_providers)
    }
    coverage_params.update(
        {
            f"dataset_{index}": dataset
            for index, dataset in enumerate(coverage_datasets)
        }
    )
    coverage_placeholders = ",".join(
        f":dataset_{index}" for index in range(len(coverage_datasets))
    )
    coverage_provider_placeholders = ",".join(
        f":coverage_provider_{index}"
        for index in range(len(coverage_providers))
    )
    try:
        coverage_rows = _rows(
            connection.execute(
                text(
                    "SELECT manifest_hash, dataset, period, provider, "
                    "trade_date, status, strategy_eligible, captured_at, "
                    "TIMESTAMPDIFF(SECOND, captured_at, NOW()) "
                    f"AS capture_age_seconds FROM {COVERAGE_TABLE} "
                    "WHERE provider IN ("
                    f"{coverage_provider_placeholders}) AND dataset IN ("
                    f"{coverage_placeholders}) "
                    "ORDER BY dataset, trade_date, captured_at, manifest_hash"
                ),
                coverage_params,
            )
        )
        coverage_query_failed = False
    except Exception:
        coverage_rows = []
        coverage_query_failed = True
        global_errors.append("coverage_query_failed")

    coverage_by_dataset: dict[tuple[str, str], list[dict[str, Any]]] = {
        (spec.coverage_provider, spec.coverage_dataset): []
        for spec in QMT_HISTORY_DATASET_SPECS
    }
    for row in coverage_rows:
        dataset = str(row.get("dataset") or "")
        identity = (str(row.get("provider") or ""), dataset)
        if identity not in coverage_by_dataset:
            global_errors.append("coverage_query_returned_unexpected_dataset")
            continue
        coverage_by_dataset[identity].append(row)

    dataset_results: list[dict[str, Any]] = []
    for spec in QMT_HISTORY_DATASET_SPECS:
        capability_details = [
            _capability_detail(
                capability_key=key,
                candidates=capabilities_by_key.get(key, []),
                provider=spec.capability_provider,
                maximum_age_seconds=maximum_age,
            )
            for key in spec.capability_keys
        ]
        api_available = bool(capability_details) and all(
            item["api_available"] for item in capability_details
        )
        capability_evidence_healthy = (
            not capability_query_failed
            and all(item["evidence_healthy"] for item in capability_details)
        )
        coverage = _coverage_detail(
            spec=spec,
            candidates=coverage_by_dataset.get(
                (spec.coverage_provider, spec.coverage_dataset),
                [],
            ),
            required_dates=required_dates.get(spec.dataset),
            provider=spec.coverage_provider,
            query_failed=coverage_query_failed,
        )
        dataset_evidence_healthy = (
            capability_evidence_healthy and coverage["evidence_healthy"]
        )
        strategy_eligible = bool(
            dataset_evidence_healthy
            and api_available
            and coverage["exact"]
        )
        blockers = _dedupe(
            blocker
            for item in capability_details
            for blocker in item["blockers"]
        )
        blockers.extend(
            blocker
            for blocker in coverage["blockers"]
            if blocker not in blockers
        )
        dataset_results.append(
            {
                **asdict(spec),
                "status": API_AVAILABLE if strategy_eligible else API_UNAVAILABLE,
                "api_status": API_AVAILABLE if api_available else API_UNAVAILABLE,
                "evidence_status": (
                    EVIDENCE_HEALTHY
                    if dataset_evidence_healthy
                    else EVIDENCE_UNHEALTHY
                ),
                "evidence_healthy": dataset_evidence_healthy,
                "strategy_eligible": strategy_eligible,
                "capabilities": capability_details,
                "coverage": coverage,
                "blockers": blockers,
            }
        )

    evidence_healthy = not global_errors and all(
        item["evidence_healthy"] for item in dataset_results
    )
    eligible_count = sum(
        1 for item in dataset_results if item["strategy_eligible"]
    )
    available_api_count = sum(
        1 for item in dataset_results if item["api_status"] == API_AVAILABLE
    )
    return evidence_healthy, {
        "schema": CAPABILITY_MATRIX_SCHEMA,
        "capability_provider": provider_id,
        "coverage_providers": coverage_providers,
        "status": EVIDENCE_HEALTHY if evidence_healthy else EVIDENCE_UNHEALTHY,
        "evidence_healthy": evidence_healthy,
        "dataset_count": len(dataset_results),
        "api_available_dataset_count": available_api_count,
        "strategy_eligible_dataset_count": eligible_count,
        "strategy_ineligible_dataset_count": len(dataset_results) - eligible_count,
        "capability_max_age_seconds": maximum_age,
        "required_scope_dataset_count": sum(
            1 for value in required_dates.values() if value
        ),
        "errors": _dedupe(global_errors),
        "datasets": dataset_results,
    }


__all__ = [
    "API_AVAILABLE",
    "API_UNAVAILABLE",
    "CAPABILITY_MATRIX_SCHEMA",
    "DEFAULT_CAPABILITY_MAX_AGE_SECONDS",
    "EVIDENCE_HEALTHY",
    "EVIDENCE_UNHEALTHY",
    "FINAL_PROVIDER_STATUSES",
    "KNOWN_UNAVAILABLE_PROVIDER_STATUSES",
    "QMT_HISTORY_DATASET_KEYS",
    "QMT_HISTORY_DATASET_SPECS",
    "QMT_HISTORY_DATASET_SPEC_BY_KEY",
    "QMT_PROVIDER",
    "QmtHistoryDatasetSpec",
    "assess_qmt_history_capabilities",
]
