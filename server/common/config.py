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
    mysql_tls_required: bool = False
    mysql_ssl_ca: str | None = None
    current_mysql_url: str | None = None
    qmt_history_mysql_url: str | None = None
    kline_mysql_url: str | None = None
    minute_mysql_url: str | None = None
    minute_data_source: str = "legacy"
    data_source_kline: str = "adata"
    data_source_minute: str = "adata"
    data_source_current: str = "adata"
    minute_stock_table: str | None = None
    api_embedded_scheduler_enabled: bool = False
    api_qmt_live_runtime_enabled: bool = False
    api_scheduler_max_concurrent_tasks: int = 2
    api_scheduler_poll_seconds: int = 60
    api_mysql_pool_size: int = 3
    api_mysql_max_overflow: int = 1
    api_mysql_pool_recycle: int = 1800
    api_slow_request_ms: int = 1500
    api_slow_sql_ms: int = 500
    api_cache_max_entries: int = 512
    probiga_admin_auth_enabled: bool = True
    probiga_admin_token: str | None = None
    probiga_ai_bridge_token: str | None = None
    probiga_ai_bridge_lease_seconds: int = 900
    probiga_auth_session_hours: int = 24
    probiga_auth_refresh_after_hours: int = 20
    probiga_auth_max_failures: int = 5
    probiga_auth_lock_minutes: int = 15
    probiga_auth_cookie_secure: bool | None = None
    probiga_auth_registration_deadline: str | None = None
    gj_qmt_home: str | None = None
    gj_qmt_exe: str | None = None
    gj_qmt_provider_id: str = "gj_qmt"
    qmt_ping_timeout: int = 8
    qmt_live_poll_enabled: bool = False
    qmt_live_poll_seconds: int = 5
    qmt_live_idle_sleep_seconds: int = 30
    qmt_live_trading_hours_only: bool = True
    qmt_live_candidate_limit: int = 60
    live_quote_poll_enabled: bool | None = None
    live_quote_poll_seconds: int = 10
    live_quote_idle_sleep_seconds: int = 30
    live_quote_trading_hours_only: bool = True
    live_quote_candidate_limit: int = 60
    live_quote_index_poll_seconds: int = 60
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
    wecom_intraday_webhook_url: str | None = None
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


def get_mysql_tls_runtime_config() -> dict[str, str | bool | None]:
    """Return the single TLS policy used by every runtime MySQL engine.

    TLS material is intentionally configured outside database URLs.  The
    engine factory validates the CA path and rejects partial or caller-specific
    TLS overrides before opening a connection.
    """
    settings = get_settings()
    ssl_ca = (settings.mysql_ssl_ca or "").strip()
    return {
        "required": bool(settings.mysql_tls_required),
        "ssl_ca": ssl_ca or None,
    }


def get_minute_mysql_url() -> str:
    """Return the MySQL URL used by stock minute readers.

    Minute history can live outside the production database because the table is
    large.  When MINUTE_MYSQL_URL is unset, readers fall back to MYSQL_URL.
    """
    settings = get_settings()
    return (settings.minute_mysql_url or get_mysql_url(required=True)).strip()


def get_current_mysql_url() -> str:
    """Return the database used for the persisted current-quote tables.

    A Windows QMT collector can write locally while production reads the same
    tables over a reverse MySQL tunnel.  Keep this route separate from the
    primary business database so live quote traffic never writes production
    disk by accident.
    """
    settings = get_settings()
    return (
        settings.current_mysql_url
        or settings.minute_mysql_url
        or settings.kline_mysql_url
        or get_mysql_url(required=True)
    ).strip()


def get_kline_mysql_url() -> str:
    """Return the MySQL URL used by daily/index/concept K-line readers."""
    settings = get_settings()
    return (
        settings.kline_mysql_url
        or settings.qmt_history_mysql_url
        or settings.minute_mysql_url
        or get_mysql_url(required=True)
    ).strip()


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
        "max_concurrent_tasks": _bounded_int(settings.api_scheduler_max_concurrent_tasks, default=2, minimum=1),
        "poll_seconds": _bounded_int(settings.api_scheduler_poll_seconds, default=60, minimum=15),
    }


def get_api_lifespan_config() -> dict[str, bool]:
    """Return optional background runtimes owned by the FastAPI process."""
    settings = get_settings()
    return {
        "qmt_live_runtime_enabled": bool(settings.api_qmt_live_runtime_enabled),
    }


def get_api_observability_config() -> dict[str, int]:
    """Return lightweight API observability settings."""
    settings = get_settings()
    return {
        "slow_request_ms": _bounded_int(settings.api_slow_request_ms, default=1500, minimum=0),
        "slow_sql_ms": _bounded_int(settings.api_slow_sql_ms, default=500, minimum=0),
    }


def get_api_cache_config() -> dict[str, int]:
    """Return in-process cache limits for API hot paths."""
    settings = get_settings()
    return {
        "max_entries": _bounded_int(settings.api_cache_max_entries, default=512, minimum=32),
    }


def get_admin_auth_config() -> dict[str, str | bool | None]:
    """Return admin API protection settings."""
    settings = get_settings()
    token = (settings.probiga_admin_token or "").strip()
    return {
        "enabled": bool(settings.probiga_admin_auth_enabled),
        "token": token or None,
    }


def get_ai_bridge_config() -> dict[str, str | int | None]:
    """Return the separate credential and lease used by the local AI worker."""
    settings = get_settings()
    token = (settings.probiga_ai_bridge_token or "").strip()
    return {
        "token": token or None,
        "lease_seconds": _bounded_int(
            settings.probiga_ai_bridge_lease_seconds,
            default=900,
            minimum=60,
        ),
    }


def get_account_auth_config() -> dict[str, int | bool | None]:
    """Return username/password session settings.

    Sessions are deliberately short lived. The browser rotates the opaque
    session token before its 24-hour expiry instead of storing a reusable admin
    secret in localStorage.
    """
    settings = get_settings()
    session_hours = min(
        168,
        _bounded_int(settings.probiga_auth_session_hours, default=24, minimum=1),
    )
    refresh_after_hours = min(
        session_hours,
        _bounded_int(settings.probiga_auth_refresh_after_hours, default=20, minimum=1),
    )
    return {
        "session_hours": session_hours,
        "refresh_after_hours": refresh_after_hours,
        "max_failures": min(
            20,
            _bounded_int(settings.probiga_auth_max_failures, default=5, minimum=3),
        ),
        "lock_minutes": min(
            1440,
            _bounded_int(settings.probiga_auth_lock_minutes, default=15, minimum=1),
        ),
        "cookie_secure": settings.probiga_auth_cookie_secure,
        "registration_deadline": (settings.probiga_auth_registration_deadline or "").strip() or None,
    }


def get_live_quote_runtime_config() -> dict[str, int | bool]:
    """Return settings for the public-source intraday quote worker."""
    settings = get_settings()
    enabled = settings.live_quote_poll_enabled
    if enabled is None:
        enabled = settings.qmt_live_poll_enabled
    return {
        "enabled": bool(enabled),
        "poll_seconds": _bounded_int(settings.live_quote_poll_seconds, default=10, minimum=2),
        "idle_sleep_seconds": _bounded_int(settings.live_quote_idle_sleep_seconds, default=30, minimum=5),
        "trading_hours_only": bool(settings.live_quote_trading_hours_only),
        "candidate_limit": _bounded_int(settings.live_quote_candidate_limit, default=60, minimum=20),
        "index_poll_seconds": _bounded_int(settings.live_quote_index_poll_seconds, default=60, minimum=30),
    }


def get_qmt_live_runtime_config() -> dict[str, int | bool]:
    """Backward-compatible alias for the public-source live quote runtime."""
    return get_live_quote_runtime_config()


def get_market_radar_runtime_config() -> dict[str, int | bool]:
    """Return runtime settings for the QMT full-market anomaly radar.

    The radar deliberately has its own switch: a full-market quote scan is
    materially heavier than the existing tracked-stock live sync.
    """
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
        "metadata_refresh_seconds": _bounded_int(
            settings.market_radar_metadata_refresh_seconds, default=900, minimum=60
        ),
        "event_cooldown_seconds": _bounded_int(
            settings.market_radar_event_cooldown_seconds, default=60, minimum=10
        ),
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
      - intraday: WECOM_INTRADAY_WEBHOOK_URL, fallback to briefing/default
    """
    settings = get_settings()
    if kind == "news":
        url = settings.wecom_news_webhook_url or settings.wecom_webhook_url
    elif kind == "briefing":
        url = settings.wecom_briefing_webhook_url or settings.wecom_webhook_url
    elif kind == "intraday":
        url = (
            settings.wecom_intraday_webhook_url
            or settings.wecom_briefing_webhook_url
            or settings.wecom_webhook_url
        )
    else:
        url = settings.wecom_webhook_url
    url = (url or "").strip()
    if required and not url:
        raise RuntimeError(f"未配置企业微信机器人地址: {kind}")
    return url
