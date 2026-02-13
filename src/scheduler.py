from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from io import BytesIO
import json
import random
from time import sleep
import sys
from typing import Any, Iterable, Optional
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from .config import Config
from .title_format import build_title


@dataclass(frozen=True)
class BroadcastTemplate:
    content_details: dict[str, Any]
    privacy_status: str
    bound_stream_id: Optional[str]
    description: str
    thumbnail_url: Optional[str]


@dataclass(frozen=True)
class BroadcastDefinition:
    prefix: str
    scheduled_time: time
    keyword: str
    default_description: str


DEFAULT_MISA_DESCRIPTION = (
    "Si quieres hacer un donativo a la Parroquia:\n"
    "https://smcana.es/donativos/\n"
    "Donativo Bizum ONG: 00104 o 38194 o 38341"
)

DEFAULT_VELA_DESCRIPTION = (
    "También puedes oírlas después en Spotify:\n"
    "https://open.spotify.com/show/1XitO8Ckw0kDvDTT9CuVp2"
)


class StopCreationLimit(Exception):
    def __init__(self, message: str, details: str | None = None):
        super().__init__(message)
        self.details = details


def _log(message: str) -> None:
    print(message, flush=True)


def _load_timezone(name: str) -> ZoneInfo:
    tz_name = (name or "").strip()
    if not tz_name:
        _log("WARN: timezone vacío, usando UTC.")
        return ZoneInfo("UTC")
    try:
        return ZoneInfo(tz_name)
    except Exception as exc:
        _log(f"WARN: timezone inválida '{tz_name}', usando UTC. ({exc})")
        return ZoneInfo("UTC")


def _rfc3339(dt: datetime) -> str:
    return dt.isoformat()


def _iter_broadcasts(youtube, page_size: int = 50) -> Iterable[dict[str, Any]]:
    page_token = None
    while True:
        request = youtube.liveBroadcasts().list(
            part="id,snippet,contentDetails,status",
            mine=True,
            maxResults=page_size,
            pageToken=page_token,
            broadcastType="all",
        )
        response = request.execute()
        for item in response.get("items", []):
            yield item
        page_token = response.get("nextPageToken")
        if not page_token:
            break


def find_broadcast_by_title_in_items(
    items: Iterable[dict[str, Any]], title: str
) -> Optional[dict[str, Any]]:
    for item in items:
        if item.get("snippet", {}).get("title") == title:
            return item
    return None


def _parse_scheduled_start(item: dict[str, Any], tz: ZoneInfo) -> Optional[datetime]:
    scheduled_start = item.get("snippet", {}).get("scheduledStartTime")
    if not scheduled_start:
        return None
    try:
        parsed = datetime.fromisoformat(scheduled_start.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
    return parsed.astimezone(tz)


def find_latest_scheduled_broadcast_in_items(
    items: Iterable[dict[str, Any]], keywords: Iterable[str], tz: ZoneInfo
) -> Optional[datetime]:
    latest: Optional[datetime] = None
    keyword_list = tuple(keywords)
    for item in items:
        title = item.get("snippet", {}).get("title", "")
        if not any(keyword in title for keyword in keyword_list):
            continue
        scheduled_start = _parse_scheduled_start(item, tz)
        if not scheduled_start:
            continue
        if latest is None or scheduled_start > latest:
            latest = scheduled_start
    return latest


def _pick_thumbnail_url(snippet: dict[str, Any]) -> Optional[str]:
    thumbnails = snippet.get("thumbnails", {})
    for key in ("maxres", "standard", "high", "medium", "default"):
        url = thumbnails.get(key, {}).get("url")
        if url:
            return url
    return None


def find_template_by_keyword_in_items(
    items: Iterable[dict[str, Any]], keyword: str
) -> Optional[BroadcastTemplate]:
    for item in items:
        title = item.get("snippet", {}).get("title", "")
        if keyword in title:
            content_details = item.get("contentDetails", {})
            status = item.get("status", {})
            snippet = item.get("snippet", {})
            return BroadcastTemplate(
                content_details=content_details,
                privacy_status=status.get("privacyStatus", "unlisted"),
                bound_stream_id=content_details.get("boundStreamId"),
                description=snippet.get("description", ""),
                thumbnail_url=_pick_thumbnail_url(snippet),
            )
    return None


def find_broadcast_by_title(youtube, title: str) -> Optional[dict[str, Any]]:
    return find_broadcast_by_title_in_items(_iter_broadcasts(youtube), title)


def find_latest_scheduled_broadcast(
    youtube, keywords: Iterable[str], tz: ZoneInfo
) -> Optional[datetime]:
    return find_latest_scheduled_broadcast_in_items(
        _iter_broadcasts(youtube), keywords, tz
    )


def find_template_by_keyword(youtube, keyword: str) -> Optional[BroadcastTemplate]:
    return find_template_by_keyword_in_items(_iter_broadcasts(youtube), keyword)


def _build_content_details(template: Optional[BroadcastTemplate]) -> dict[str, Any]:
    if not template:
        return {}
    allowed_fields = [
        "enableAutoStart",
        "enableAutoStop",
        "enableDvr",
        "recordFromStart",
        "latencyPreference",
        "monitorStream",
        "projection",
    ]
    return {
        key: template.content_details[key]
        for key in allowed_fields
        if key in template.content_details
    }


def _parse_error_reason(error: HttpError) -> tuple[str | None, str | None]:
    try:
        payload = json.loads(error.content.decode("utf-8"))
    except (json.JSONDecodeError, AttributeError):
        return None, None
    errors = payload.get("error", {}).get("errors", [])
    if errors:
        reason = errors[0].get("reason")
        message = errors[0].get("message") or payload.get("error", {}).get("message")
        return reason, message
    return None, payload.get("error", {}).get("message")


def _is_quota_or_limit_error(error: HttpError) -> tuple[bool, str | None]:
    reason, message = _parse_error_reason(error)
    if reason in {
        "quotaExceeded",
        "dailyLimitExceeded",
        "rateLimitExceeded",
        "userRateLimitExceeded",
        "userRequestsExceedRateLimit",
        "liveStreamingNotEnabled",
    }:
        return True, message or reason
    if error.resp.status in {403, 429}:
        if message and any(word in message.lower() for word in ["quota", "limit", "exceeded"]):
            return True, message
    return False, message


def _is_rate_limit_http_error(error: HttpError) -> tuple[bool, str | None]:
    reason, message = _parse_error_reason(error)
    if error.resp.status == 403 and reason in {
        "userRequestsExceedRateLimit",
        "rateLimitExceeded",
    }:
        return True, reason
    return False, message


def _create_broadcast(
    youtube,
    title: str,
    description: str,
    scheduled_start: datetime,
    template: Optional[BroadcastTemplate],
    default_privacy_status: str,
) -> dict[str, Any]:
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "scheduledStartTime": _rfc3339(scheduled_start),
        },
        "status": {
            "privacyStatus": template.privacy_status if template else default_privacy_status,
        },
        "contentDetails": _build_content_details(template),
    }
    request = youtube.liveBroadcasts().insert(
        part="snippet,contentDetails,status",
        body=body,
    )
    return request.execute()


def _with_rate_limit_retry(
    operation_name: str,
    title: str,
    retry_limit: int,
    base_seconds: float,
    max_seconds: float,
    operation,
):
    for attempt in range(retry_limit + 1):
        try:
            return operation()
        except HttpError as error:
            is_rate_limit, detail = _is_rate_limit_http_error(error)
            if not is_rate_limit:
                raise
            if attempt >= retry_limit:
                raise StopCreationLimit(
                    f"rate limit en {operation_name}",
                    details=detail or "userRequestsExceedRateLimit",
                )
            wait_seconds = min(max_seconds, base_seconds * (2**attempt)) + random.uniform(0, 0.5)
            _log(
                f"WARN: rate limit en {operation_name} para '{title}' "
                f"(intento {attempt + 1}/{retry_limit + 1}), reintentando en {wait_seconds:.2f}s."
            )
            sleep(wait_seconds)


def _create_broadcast_with_retry(
    youtube,
    title: str,
    description: str,
    scheduled_start: datetime,
    template: Optional[BroadcastTemplate],
    default_privacy_status: str,
    retry_limit: int,
    base_seconds: float,
    max_seconds: float,
) -> dict[str, Any]:
    return _with_rate_limit_retry(
        operation_name="liveBroadcasts.insert",
        title=title,
        retry_limit=retry_limit,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
        operation=lambda: _create_broadcast(
            youtube,
            title=title,
            description=description,
            scheduled_start=scheduled_start,
            template=template,
            default_privacy_status=default_privacy_status,
        ),
    )


def _ensure_template_for_keyword(
    youtube,
    broadcasts: list[dict[str, Any]],
    keyword: str,
    tz: ZoneInfo,
    default_privacy_status: str,
    retry_limit: int,
    base_seconds: float,
    max_seconds: float,
) -> Optional[BroadcastTemplate]:
    template = find_template_by_keyword_in_items(broadcasts, keyword)
    if template:
        return template

    template_title = keyword
    _log(
        f"TEMPLATE: '{keyword}' no encontrada. Creando emisión base con mismo nombre '{template_title}'."
    )

    try:
        created = _create_broadcast_with_retry(
            youtube,
            title=template_title,
            description="",
            scheduled_start=datetime.now(tz) + timedelta(minutes=10),
            template=None,
            default_privacy_status=default_privacy_status,
            retry_limit=retry_limit,
            base_seconds=base_seconds,
            max_seconds=max_seconds,
        )
    except (HttpError, StopCreationLimit) as error:
        detail = error.details if isinstance(error, StopCreationLimit) else _parse_error_reason(error)[0]
        if detail in {"userRequestsExceedRateLimit", "rateLimitExceeded"}:
            _log(
                "WARN: no se pudo crear la emisión base por rate limit; "
                "se continúa sin plantilla."
            )
            return None
        raise

    broadcasts.append(created)
    template = find_template_by_keyword_in_items([created], keyword)
    if template:
        _log(f"TEMPLATE: '{template_title}' creada para reutilización.")
    return template


def _bind_stream(youtube, broadcast_id: str, stream_id: str) -> None:
    request = youtube.liveBroadcasts().bind(
        part="id,contentDetails",
        id=broadcast_id,
        streamId=stream_id,
    )
    request.execute()


def _bind_stream_with_retry(
    youtube,
    broadcast_id: str,
    stream_id: str,
    title: str,
    retry_limit: int,
    base_seconds: float,
    max_seconds: float,
) -> None:
    _with_rate_limit_retry(
        operation_name="liveBroadcasts.bind",
        title=title,
        retry_limit=retry_limit,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
        operation=lambda: _bind_stream(youtube, broadcast_id, stream_id),
    )


def _set_thumbnail_from_url(youtube, video_id: str, thumbnail_url: str) -> None:
    with urlopen(thumbnail_url) as response:
        content_type = response.headers.get_content_type()
        data = response.read()
    media = MediaIoBaseUpload(
        BytesIO(data),
        mimetype=content_type or "image/jpeg",
        resumable=False,
    )
    youtube.thumbnails().set(videoId=video_id, media_body=media).execute()


def _copy_thumbnail_if_available(youtube, broadcast_id: str, template: Optional[BroadcastTemplate]) -> None:
    if not template or not template.thumbnail_url:
        return
    if not hasattr(youtube, "thumbnails"):
        return
    try:
        _set_thumbnail_from_url(youtube, broadcast_id, template.thumbnail_url)
        _log(f"THUMBNAIL: broadcast {broadcast_id} <- {template.thumbnail_url}")
    except Exception as error:
        _log(f"WARN: no se pudo copiar portada en {broadcast_id}: {error}")


def _log_status_list(title: str, items: list[str]) -> None:
    _log(f"STATUS: {title} ({len(items)})")
    for item in items:
        _log(f"  - {item}")


def _log_summary(
    planned: list[str],
    created: list[str],
    existing: list[str],
    failed: list[str],
) -> None:
    _log_status_list("planificadas para crear", planned)
    _log_status_list("creadas", created)
    _log_status_list("ya existían", existing)
    _log_status_list("fallidas", failed)


def run_scheduler(youtube, config: Config) -> int:
    tz = _load_timezone(config.timezone)
    today = datetime.now(tz).date()
    definitions = [
        BroadcastDefinition(config.keyword_misa_10, time(10, 0), config.keyword_misa_10, DEFAULT_MISA_DESCRIPTION),
        BroadcastDefinition(config.keyword_misa_12, time(12, 0), config.keyword_misa_12, DEFAULT_MISA_DESCRIPTION),
        BroadcastDefinition(config.keyword_misa_20, time(20, 0), config.keyword_misa_20, DEFAULT_MISA_DESCRIPTION),
        BroadcastDefinition(config.keyword_vela_21, time(21, 0), config.keyword_vela_21, DEFAULT_VELA_DESCRIPTION),
    ]
    start_date = today + timedelta(days=config.start_offset_days)
    broadcasts = list(_iter_broadcasts(youtube))
    _log(f"START: procesando desde {start_date.isoformat()} (sin saltar huecos).")
    max_days_ahead = min(config.max_days_ahead, 7)
    end_date = today + timedelta(days=max_days_ahead)
    if start_date > end_date:
        _log(
            "SKIP: no hay días pendientes (inicio "
            f"{start_date.isoformat()} > fin {end_date.isoformat()})."
        )
        return 0
    total_days = (end_date - start_date).days + 1
    templates: dict[str, Optional[BroadcastTemplate]] = {}
    for definition in definitions:
        if definition.keyword not in templates:
            templates[definition.keyword] = _ensure_template_for_keyword(
                youtube,
                broadcasts,
                definition.keyword,
                tz,
                config.default_privacy_status,
                config.rate_limit_retry_limit,
                config.rate_limit_retry_base_seconds,
                config.rate_limit_retry_max_seconds,
            )
            if templates[definition.keyword]:
                _log(f"TEMPLATE: '{definition.keyword}' encontrada.")
            else:
                _log(f"TEMPLATE: '{definition.keyword}' no encontrada, usando defaults.")

    shared_stream_id = next((t.bound_stream_id for t in templates.values() if t and t.bound_stream_id), None)

    planned: list[str] = []
    created_titles: list[str] = []
    existing_titles: list[str] = []
    failed: list[str] = []

    for offset in range(total_days):
        target_date = start_date + timedelta(days=offset)
        _log(f"DAY: procesando {target_date.isoformat()}")
        for definition in definitions:
            if (
                definition.keyword == config.keyword_vela_21
                and target_date.weekday() != 3
            ):
                continue
            scheduled_start = datetime.combine(target_date, definition.scheduled_time, tz)
            title = build_title(definition.prefix, target_date)
            existing = find_broadcast_by_title_in_items(broadcasts, title)
            if existing:
                _log(f"SKIP: ya existe '{title}' (id={existing.get('id')})")
                existing_titles.append(title)
                continue
            planned.append(title)
            template = templates.get(definition.keyword)
            description = template.description if template and template.description else definition.default_description
            try:
                created = _create_broadcast_with_retry(
                    youtube,
                    title=title,
                    description=description,
                    scheduled_start=scheduled_start,
                    template=template,
                    default_privacy_status=config.default_privacy_status,
                    retry_limit=config.rate_limit_retry_limit,
                    base_seconds=config.rate_limit_retry_base_seconds,
                    max_seconds=config.rate_limit_retry_max_seconds,
                )
                _log(f"CREATED: '{title}' (id={created.get('id')})")
                created_titles.append(title)
                broadcasts.append(created)
                stream_id = shared_stream_id
                if stream_id:
                    _bind_stream_with_retry(
                        youtube,
                        created.get("id"),
                        stream_id,
                        title,
                        config.rate_limit_retry_limit,
                        config.rate_limit_retry_base_seconds,
                        config.rate_limit_retry_max_seconds,
                    )
                    _log(f"BIND: broadcast {created.get('id')} -> stream {stream_id}")
                _copy_thumbnail_if_available(youtube, created.get("id"), template)
                sleep(config.create_pause_seconds)
            except StopCreationLimit as limit_error:
                if config.stop_on_create_limit:
                    detail_text = limit_error.details or "rateLimitExceeded"
                    _log(f"STOP: límite alcanzado ({detail_text})")
                    _log_summary(planned, created_titles, existing_titles, failed)
                    return 0
                failed.append(f"{title} (rate limit: {limit_error.details or 'sin detalle'})")
            except HttpError as error:
                is_limit, detail = _is_quota_or_limit_error(error)
                if is_limit and config.stop_on_create_limit:
                    detail_text = detail or "API limit"
                    _log(f"STOP: límite alcanzado ({detail_text})")
                    _log_summary(planned, created_titles, existing_titles, failed)
                    return 0
                _log(f"ERROR: fallo creando '{title}'")
                reason, message = _parse_error_reason(error)
                if reason or message:
                    failed.append(f"{title} ({reason or 'error'}: {message or 'sin detalle'})")
                else:
                    failed.append(title)
                _log_summary(planned, created_titles, existing_titles, failed)
                raise
    _log("DONE: reached max days ahead without limit.")
    _log_summary(planned, created_titles, existing_titles, failed)
    return 0


def main(youtube, config: Config) -> None:
    try:
        exit_code = run_scheduler(youtube, config)
    except Exception as exc:
        _log(f"FATAL: {exc}")
        sys.exit(1)
    sys.exit(exit_code)
