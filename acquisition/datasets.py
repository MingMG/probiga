"""Fixed products and existing table identities; no runtime registration."""
from datetime import time

from .models import DatasetSpec


# Upstream product lanes. A lane is a distinct surface whose outage pauses only
# that lane, so a stalled sector catalog cannot withhold licensed bars, and a
# minute backfill under local resource pressure cannot withhold daily bars.
# Lanes are declared per product and are never derived from the vendor name:
# one vendor can serve several lanes, and one lane can span asset classes.
FAILURE_DOMAINS = frozenset({
    "qmt.bars.daily",
    "qmt.bars.minute",
    "qmt.tick",
    "qmt.capital_flow",
    "qmt.reference",
    "eastmoney.finance",
    "eastmoney.alist",
    "eastmoney.notices",
})


DATASETS = {
    "stock_daily": DatasetSpec("stock_daily", "guojin_qmt", "sm_stock_kline", "history", "stock_code", ("stock_code", "trade_date", "k_type", "adjust_type"), "1d", ("none",), "stock", time(15, 30), persisted_source="gj_big_qmt_inner", failure_domain="qmt.bars.daily"),
    "stock_minute": DatasetSpec("stock_minute", "guojin_qmt", "sm_stock_minute", "history", "stock_code", ("stock_code", "trade_time"), "1m", ("none",), "stock", time(15, 30), persisted_source="gj_big_qmt_inner", failure_domain="qmt.bars.minute"),
    "index_daily": DatasetSpec("index_daily", "guojin_qmt", "sm_index_kline", "history", "index_code", ("index_code", "trade_date", "k_type"), "1d", ("none",), "index", time(15, 30), persisted_source="gj_big_qmt_inner", failure_domain="qmt.bars.daily"),
    "index_minute": DatasetSpec("index_minute", "guojin_qmt", "sm_index_minute", "history", "index_code", ("index_code", "trade_time"), "1m", ("none",), "index", time(15, 30), persisted_source="gj_big_qmt_inner", failure_domain="qmt.bars.minute"),
    "etf_daily": DatasetSpec("etf_daily", "guojin_qmt", "sm_etf_kline", "primary", "etf_code", ("etf_code", "trade_date", "k_type", "adjust_type"), "1d", ("none", "front"), "etf", time(15, 30), persisted_source="gj_big_qmt_inner", failure_domain="qmt.bars.daily"),
    "stock_current": DatasetSpec("stock_current", "guojin_qmt", "sm_stock_current", "primary", "stock_code", ("stock_code",), "tick", ("none",), "stock", time(9, 30), persisted_source="gj_big_qmt_inner", failure_domain="qmt.tick"),
    "index_current": DatasetSpec("index_current", "guojin_qmt", "sm_index_current", "primary", "index_code", ("index_code",), "tick", ("none",), "index", time(9, 30), persisted_source="gj_big_qmt_inner", failure_domain="qmt.tick"),
    "capital_flow_daily": DatasetSpec("capital_flow_daily", "guojin_qmt", "sm_stock_capital_flow_daily", "minute", "stock_code", ("stock_code", "trade_date"), "transactioncount1d", ("none",), "stock", time(15, 40), persisted_source="gj_big_qmt_inner", failure_domain="qmt.capital_flow"),
    "finance": DatasetSpec("finance", "eastmoney", "si_stock_finance", "primary", "stock_code", ("stock_code", "report_date"), "1d", ("none",), "stock", time(18), True, "eastmoney.finance.mainfinadata.direct", failure_domain="eastmoney.finance"),
    "alist_daily": DatasetSpec("alist_daily", "eastmoney", "st_a_list_daily", "primary", "stock_code", ("stock_code", "trade_date", "trade_id"), "1d", ("none",), "stock", time(18), True, "eastmoney", "trade_date", failure_domain="eastmoney.alist"),
    "alist_detail": DatasetSpec("alist_detail", "eastmoney", "st_a_list_info", "primary", "stock_code", ("stock_code", "trade_date", "trade_id", "operate_code", "operate_name", "report_side"), "1d", ("none",), "stock", time(18), True, "eastmoney", "trade_date", failure_domain="eastmoney.alist"),
    "notices": DatasetSpec("notices", "eastmoney", "si_notice_eastmoney", "primary", "stock_code", ("stock_code", "art_code"), "1d", ("none",), "stock", time(18), True, "eastmoney", "notice_date", failure_domain="eastmoney.notices"),
    "reference": DatasetSpec("reference", "guojin_qmt", "", "primary", "stock_code", (), "reference", ("none",), "reference", time(8, 30), persisted_source="gj_big_qmt_inner", failure_domain="qmt.reference"),
}


def _validate_declared_domains() -> None:
    """Fail the import when a lane is undeclared or spans two sources.

    Mirrors the fail-fast ownership validators in
    tools/qmt_host_ownership_contract.py: a lane that is not declared here is a
    programming error, not a runtime condition, and must not reach production
    as a silently merged failure domain.
    """
    owners: dict[str, str] = {}
    for name, spec in DATASETS.items():
        if spec.failure_domain not in FAILURE_DOMAINS:
            raise RuntimeError(f"undeclared failure domain for {name}")
        owner = owners.setdefault(spec.failure_domain, spec.source)
        if owner != spec.source:
            raise RuntimeError(f"failure domain spans two sources: {name}")


_validate_declared_domains()


def get_spec(name: str) -> DatasetSpec:
    try:
        return DATASETS[name]
    except KeyError as exc:
        raise ValueError(f"unsupported dataset: {name}") from exc
