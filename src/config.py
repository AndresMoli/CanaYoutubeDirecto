from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Config:
    client_id: str
    client_secret: str
    refresh_token: str
    timezone: str
    default_privacy_status: str
    keyword_misa_10: str
    keyword_misa_12: str
    keyword_misa_20: str
    keyword_vela_21: str
    start_offset_days: int
    max_days_ahead: int
    stop_on_create_limit: bool
    rate_limit_retry_limit: int
    rate_limit_retry_base_seconds: float
    rate_limit_retry_max_seconds: float
    create_pause_seconds: float
    creation_mode: str = "api"
    studio_storage_state_path: str = ""
    studio_headless: bool = True
    studio_timeout_ms: int = 30000
    studio_slow_mo_ms: int = 0


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _get_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_str_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip()
    return normalized or default


def _get_float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _resolve_studio_storage_state_path() -> str:
    configured = _get_str_env("YT_STUDIO_STORAGE_STATE_PATH", "")
    if configured:
        return configured

    default_path = Path("storage_state.json")
    if default_path.is_file():
        return str(default_path)

    return ""


def load_config() -> Config:
    return Config(
        client_id=_require_env("YT_CLIENT_ID"),
        client_secret=_require_env("YT_CLIENT_SECRET"),
        refresh_token=_require_env("YT_REFRESH_TOKEN"),
        timezone=_get_str_env("YT_TIMEZONE", "Europe/Madrid"),
        default_privacy_status=_get_str_env("YT_DEFAULT_PRIVACY_STATUS", "unlisted"),
        keyword_misa_10=_get_str_env("YT_KEYWORD_MISA_10", "Misa 10h"),
        keyword_misa_12=_get_str_env("YT_KEYWORD_MISA_12", "Misa 12h"),
        keyword_misa_20=_get_str_env("YT_KEYWORD_MISA_20", "Misa 20h"),
        keyword_vela_21=_get_str_env("YT_KEYWORD_VELA_21", "Vela 21h"),
        start_offset_days=_get_int_env("YT_START_OFFSET_DAYS", 1),
        max_days_ahead=_get_int_env("YT_MAX_DAYS_AHEAD", 3650),
        stop_on_create_limit=_get_bool_env("YT_STOP_ON_CREATE_LIMIT", True),
        rate_limit_retry_limit=_get_int_env("YT_RATE_LIMIT_RETRY_LIMIT", 3),
        rate_limit_retry_base_seconds=_get_float_env("YT_RATE_LIMIT_RETRY_BASE_SECONDS", 1.0),
        rate_limit_retry_max_seconds=_get_float_env("YT_RATE_LIMIT_RETRY_MAX_SECONDS", 30.0),
        create_pause_seconds=_get_float_env("YT_CREATE_PAUSE_SECONDS", 0.2),
        creation_mode=_get_str_env("YT_CREATION_MODE", "studio_ui"),
        studio_storage_state_path=_resolve_studio_storage_state_path(),
        studio_headless=_get_bool_env("YT_STUDIO_HEADLESS", True),
        studio_timeout_ms=_get_int_env("YT_STUDIO_TIMEOUT_MS", 30000),
        studio_slow_mo_ms=_get_int_env("YT_STUDIO_SLOW_MO_MS", 0),
    )
