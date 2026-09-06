"""Explicit installation configuration. Secrets stay in the caller's environment."""
from dataclasses import dataclass
from datetime import date
import json
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

from .datasets import get_spec
from server.common.config import get_kline_mysql_url, get_minute_mysql_url, get_mysql_url
from server.common.engine_factory import create_pooled_engine


DIRECT_ETF_WRITER_ERROR = (
    "direct etf_daily writer is disabled in this release; "
    "keep etf_forward_daily as the only ETF daily writer"
)


class DirectEtfWriterDisabled(ValueError):
    """Raised when the unreleased direct ETF writer is selected."""


def require_supported_writer_datasets(names):
    if "etf_daily" in set(names or ()):
        raise DirectEtfWriterDisabled(DIRECT_ETF_WRITER_ERROR)


@dataclass
class Config:
    data: dict
    path: Path

    @classmethod
    def load(cls, path):
        path = Path(path).resolve()
        with path.open(encoding="utf-8") as stream:
            data = json.load(stream)
        if not isinstance(data, dict):
            raise ValueError("configuration must be an object")
        date.fromisoformat(data["start_date"])
        if not Path(data["state_dir"]).is_absolute():
            raise ValueError("state_dir must be an explicit absolute private directory")
        datasets = data.get("datasets", [])
        for name in datasets:
            get_spec(name)
        require_supported_writer_datasets(datasets)
        return cls(data, path)

    @property
    def state_dir(self):
        return Path(self.data["state_dir"])

    def require_writes(self):
        if self.data.get("write_enabled") is not True:
            raise ValueError("writes are disabled; enable only after isolated tests and single-writer cutover")

    def engine(self, database):
        key = self.data.get("database_env", {}).get(database)
        value = os.environ.get(key or "", "")
        if not value:
            profile = self.data.get("database_profiles", {}).get(database)
            if profile == "primary":
                value = get_mysql_url(required=True)
            elif profile == "kline":
                value = get_kline_mysql_url()
            elif profile in {"minute", "market"}:
                value = get_minute_mysql_url()
            elif profile:
                raise ValueError(f"unsupported database profile: {database}")
        if not value:
            raise ValueError(f"database environment reference is missing: {database}")
        url = make_url(value)
        if url.get_backend_name() not in {"mysql", "sqlite"}:
            raise ValueError("unsupported database driver")
        kwargs = dict(pool_pre_ping=True)
        if url.get_backend_name() == "mysql":
            return create_pooled_engine(
                value,
                pool_config={"pool_size": 2, "max_overflow": 0, "pool_recycle": 300},
                pool_pre_ping=True,
                pool_timeout=5,
                connect_args={"connect_timeout": 5, "read_timeout": 30, "write_timeout": 30},
            )
        return create_engine(url, **kwargs)

    def normalization(self, catalog):
        factors = {}
        for item in self.data.get("unit_mappings", []):
            factors[(item["source_method"], item["period"], item["asset_class"])] = {
                "volume": item["volume_factor"], "amount": item["amount_factor"]}
        grids = {(item["asset_class"], item["code"]): item["times"]
                 for item in self.data.get("minute_grids", [])}
        # A named profile is explicitly selected per instrument/asset by the
        # installation, never silently inferred for every exchange.
        profiles = self.data.get("minute_profiles", {})
        assignments = self.data.get("minute_profile_assignments", {})
        for code, item in catalog.items():
            profile = assignments.get(code) or assignments.get(item.get("instrument_asset", item.get("asset_class", "")))
            if profile:
                item["minute_grid"] = profiles[profile]
        return dict(volume_factors=factors, minute_grids=grids, catalog=catalog)
