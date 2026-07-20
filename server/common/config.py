# -*- coding: utf-8 -*-
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(str(ROOT_DIR / ".env"), "/opt/ProBigA/.env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str | None = None
    mysql_url: str | None = None
    qmt_history_mysql_url: str | None = None
    minute_mysql_url: str | None = None
    minute_data_source: str = "legacy"
    minute_stock_table: str | None = None
    api_embedded_scheduler_enabled: bool = False
    api_scheduler_max_concurrent_tasks: int = 1
    api_scheduler_poll_seconds: int = 60
    api_mysql_pool_size: int = 3
    api_mysql_max_overflow: int = 1
    api_mysql_pool_recycle: int = 1800
    gj_qmt_home: str | None = None
    gj_qmt_exe: str | None = None
    gj_qmt_provider_id: str = "gj_qmt"
    qmt_ping_timeout: int = 8
    qmt_live_poll_enabled: bool = False
    qmt_live_poll_seconds: int = 5
    qmt_live_idle_sleep_seconds: int = 30
    qmt_live_trading_hours_only: bool = True
    qmt_live_candidate_limit: int = 60
    market_radar_enabled: bool = False
    market_radar_poll_seconds: int = 5
    market_radar_trading_hours_only: bool = True
    market_radar_stock_limit: int = 0
    market_radar_batch_size: int = 500
    market_radar_qmt_timeout: int = 120
    market_radar_sector_limit: int = 500
    market_radar_min_sector_members: int = 5
    market_radar_metadata_refresh_seconds: int = 900
    market_radar_event_cooldown_seconds: int = 60
    market_radar_alert_enabled: bool = False
    minute_mysql_pool_size: int = 2
    minute_mysql_max_overflow: int = 1
    minute_mysql_pool_recycle: int = 1800
    wecom_webhook_url: str | None = None
    wecom_news_webhook_url: str | None = None
    wecom_briefing_webhook_url: str | None = None
    deepseek_api_key: str | None = None
    deepseek_model: str = "deepseek-chat"


@lru_cache
def get_settings() -> Settings:
    return Settings()


def _bounded_int(value: int | None, *, default: int, minimum: int) -> int:
    try:
        return max(minimum, int(value))
    except (TypeError, ValueError):
        return default


def get_mysql_url(required: bool = True) -> str:
    """Return the configured MySQL URL used by data jobs and API modules."""
    settings = get_settings()
    url = (settings.mysql_url or settings.database_url or "").strip()
    if required and not url:
        raise RuntimeError("未配置 MYSQL_URL，请在 .env 中设置数据库连接串")
    return url


def get_minute_mysql_url() -> str:
    """Return the MySQL URL used by stock minute readers.

    Minute history can live outside the production database because the table is
    large.  When MINUTE_MYSQL_URL is unset, readers fall back to MYSQL_URL.
    """
    settings = get_settings()
    return (settings.minute_mysql_url or get_mysql_url(required=True)).strip()


def get_qmt_history_mysql_url(required: bool = True) -> str:
    """Return the local MySQL URL used for bulky Guojin QMT historical data.

    This intentionally does not fall back to MYSQL_URL directly. Historical QMT
    data is large and should live in a local/off-production database unless the
    operator explicitly points QMT_HISTORY_MYSQL_URL or MINUTE_MYSQL_URL there.
    """
    settings = get_settings()
    url = (settings.qmt_history_mysql_url or settings.minute_mysql_url or "").strip()
    if required and not url:
        raise RuntimeError("未配置 QMT_HISTORY_MYSQL_URL 或 MINUTE_MYSQL_URL，本地历史库禁止回退到生产 MYSQL_URL")
    return url


def get_api_mysql_pool_config() -> dict[str, int]:
    """Return small-server-friendly pool settings for the FastAPI process."""
    settings = get_settings()
    return {
        "pool_size": _bounded_int(settings.api_mysql_pool_size, default=3, minimum=1),
        "max_overflow": _bounded_int(settings.api_mysql_max_overflow, default=1, minimum=0),
        "pool_recycle": _bounded_int(settings.api_mysql_pool_recycle, default=1800, minimum=300),
    }


def get_minute_mysql_pool_config() -> dict[str, int]:
    """Return pool settings for minute-data readers."""
    settings = get_settings()
    return {
        "pool_size": _bounded_int(settings.minute_mysql_pool_size, default=2, minimum=1),
        "max_overflow": _bounded_int(settings.minute_mysql_max_overflow, default=1, minimum=0),
        "pool_recycle": _bounded_int(settings.minute_mysql_pool_recycle, default=1800, minimum=300),
    }


def get_scheduler_runtime_config() -> dict[str, int | bool]:
    """Return runtime limits for the embedded scheduler."""
    settings = get_settings()
    return {
        "embedded_enabled": bool(settings.api_embedded_scheduler_enabled),
        "max_concurrent_tasks": _bounded_int(settings.api_scheduler_max_concurrent_tasks, default=1, minimum=1),
        "poll_seconds": _bounded_int(settings.api_scheduler_poll_seconds, default=60, minimum=15),
    }


def get_qmt_live_runtime_config() -> dict[str, int | bool]:
    """Return runtime settings for the QMT intraday polling worker."""
    settings = get_settings()
    return {
        "enabled": bool(settings.qmt_live_poll_enabled),
        "poll_seconds": _bounded_int(settings.qmt_live_poll_seconds, default=5, minimum=2),
        "idle_sleep_seconds": _bounded_int(settings.qmt_live_idle_sleep_seconds, default=30, minimum=5),
        "trading_hours_only": bool(settings.qmt_live_trading_hours_only),
        "candidate_limit": _bounded_int(settings.qmt_live_candidate_limit, default=60, minimum=20),
    }


def get_market_radar_runtime_config() -> dict[str, int | bool]:
    settings = get_settings()
    return {
        "enabled": bool(settings.market_radar_enabled),
        "poll_seconds": _bounded_int(settings.market_radar_poll_seconds, default=5, minimum=2),
        "trading_hours_only": bool(settings.market_radar_trading_hours_only),
        "stock_limit": _bounded_int(settings.market_radar_stock_limit, default=0, minimum=0),
        "batch_size": _bounded_int(settings.market_radar_batch_size, default=500, minimum=50),
        "qmt_timeout": _bounded_int(settings.market_radar_qmt_timeout, default=120, minimum=30),
        "sector_limit": _bounded_int(settings.market_radar_sector_limit, default=500, minimum=50),
        "min_sector_members": _bounded_int(settings.market_radar_min_sector_members, default=5, minimum=3),
        "metadata_refresh_seconds": _bounded_int(settings.market_radar_metadata_refresh_seconds, default=900, minimum=60),
        "event_cooldown_seconds": _bounded_int(settings.market_radar_event_cooldown_seconds, default=60, minimum=10),
        "alert_enabled": bool(settings.market_radar_alert_enabled),
    }


def get_gj_qmt_config() -> dict[str, str | int | None]:
    """Return the Guojin QMT client and diagnostics settings."""
    settings = get_settings()
    return {
        "provider_id": (settings.gj_qmt_provider_id or "gj_qmt").strip() or "gj_qmt",
        "home": (settings.gj_qmt_home or "").strip() or None,
        "exe": (settings.gj_qmt_exe or "").strip() or None,
        "ping_timeout": _bounded_int(settings.qmt_ping_timeout, default=8, minimum=1),
    }


def get_wecom_webhook(kind: str = "default", required: bool = False) -> str:
    """Return a configured WeCom webhook URL.

    kind:
      - default: WECOM_WEBHOOK_URL
      - news: WECOM_NEWS_WEBHOOK_URL, fallback to default
      - briefing: WECOM_BRIEFING_WEBHOOK_URL, fallback to default
    """
    settings = get_settings()
    if kind == "news":
        url = settings.wecom_news_webhook_url or settings.wecom_webhook_url
    elif kind == "briefing":
        url = settings.wecom_briefing_webhook_url or settings.wecom_webhook_url
    else:
        url = settings.wecom_webhook_url
    url = (url or "").strip()
    if required and not url:
        raise RuntimeError(f"未配置企业微信机器人地址: {kind}")
    return url
