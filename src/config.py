from __future__ import annotations

from dataclasses import dataclass
import os


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


def load_config() -> Config:
    return Config(
        client_id=_require_env("YT_CLIENT_ID"),
        client_secret=_require_env("YT_CLIENT_SECRET"),
        refresh_token=_require_env("YT_REFRESH_TOKEN"),
        timezone=os.getenv("YT_TIMEZONE", "Europe/Madrid"),
        default_privacy_status=os.getenv("YT_DEFAULT_PRIVACY_STATUS", "unlisted"),
        keyword_misa_10=os.getenv("YT_KEYWORD_MISA_10", "Misa 10h"),
        keyword_misa_12=os.getenv("YT_KEYWORD_MISA_12", "Misa 12h"),
        keyword_misa_20=os.getenv("YT_KEYWORD_MISA_20", "Misa 20h"),
        keyword_vela_21=os.getenv("YT_KEYWORD_VELA_21", "Vela 21h"),
        start_offset_days=int(os.getenv("YT_START_OFFSET_DAYS", "1")),
        max_days_ahead=int(os.getenv("YT_MAX_DAYS_AHEAD", "3650")),
        stop_on_create_limit=_get_bool_env("YT_STOP_ON_CREATE_LIMIT", True),
    )
