from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import json
import sys
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

from googleapiclient.errors import HttpError

from .config import Config
from .title_format import build_title


@dataclass(frozen=True)
class BroadcastTemplate:
    content_details: dict[str, Any]
    privacy_status: str
    bound_stream_id: Optional[str]


@dataclass(frozen=True)
class BroadcastDefinition:
    prefix: str
    scheduled_time: time
    keyword: str


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


def find_broadcast_by_title(youtube, title: str) -> Optional[dict[str, Any]]:
    for item in _iter_broadcasts(youtube):
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


def find_latest_scheduled_broadcast(
    youtube, keywords: Iterable[str], tz: ZoneInfo
) -> Optional[datetime]:
    latest: Optional[datetime] = None
    keyword_list = tuple(keywords)
    for item in _iter_broadcasts(youtube):
        title = item.get("snippet", {}).get("title", "")
        if not any(keyword in title for keyword in keyword_list):
            continue
        scheduled_start = _parse_scheduled_start(item, tz)
        if not scheduled_start:
            continue
        if latest is None or scheduled_start > latest:
            latest = scheduled_start
    return latest


def find_template_by_keyword(youtube, keyword: str) -> Optional[BroadcastTemplate]:
    request = youtube.liveBroadcasts().list(
        part="id,snippet,contentDetails,status",
        mine=True,
        maxResults=50,
        broadcastType="all",
    )
    response = request.execute()
    for item in response.get("items", []):
        title = item.get("snippet", {}).get("title", "")
        if keyword in title:
            content_details = item.get("contentDetails", {})
            status = item.get("status", {})
            return BroadcastTemplate(
                content_details=content_details,
                privacy_status=status.get("privacyStatus", "unlisted"),
                bound_stream_id=content_details.get("boundStreamId"),
            )
    return None


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
        "liveStreamingNotEnabled",
    }:
        return True, message or reason
    if error.resp.status in {403, 429}:
        if message and any(word in message.lower() for word in ["quota", "limit", "exceeded"]):
            return True, message
    return False, message


def _create_broadcast(
    youtube,
    title: str,
    scheduled_start: datetime,
    template: Optional[BroadcastTemplate],
    default_privacy_status: str,
) -> dict[str, Any]:
    body = {
        "snippet": {
            "title": title,
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


def _bind_stream(youtube, broadcast_id: str, stream_id: str) -> None:
    request = youtube.liveBroadcasts().bind(
        part="id,contentDetails",
        id=broadcast_id,
        streamId=stream_id,
    )
    request.execute()


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
        BroadcastDefinition(config.keyword_misa_10, time(10, 0), config.keyword_misa_10),
        BroadcastDefinition(config.keyword_misa_12, time(12, 0), config.keyword_misa_12),
        BroadcastDefinition(config.keyword_misa_20, time(20, 0), config.keyword_misa_20),
        BroadcastDefinition(config.keyword_vela_21, time(21, 0), config.keyword_vela_21),
    ]
    start_date = today + timedelta(days=config.start_offset_days)
    base_start = datetime.combine(start_date, time.min, tz)
    latest_scheduled = find_latest_scheduled_broadcast(
        youtube, (definition.keyword for definition in definitions), tz
    )
    if latest_scheduled and latest_scheduled > base_start:
        start_date = latest_scheduled.date()
        start_after = latest_scheduled
        _log(
            "START: ajustando inicio al siguiente hueco tras "
            f"{latest_scheduled.isoformat()}"
        )
    else:
        start_after = base_start
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
            templates[definition.keyword] = find_template_by_keyword(
                youtube, definition.keyword
            )
            if templates[definition.keyword]:
                _log(f"TEMPLATE: '{definition.keyword}' encontrada.")
            else:
                _log(f"TEMPLATE: '{definition.keyword}' no encontrada, usando defaults.")

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
            if target_date == start_after.date() and scheduled_start <= start_after:
                continue
            title = build_title(definition.prefix, target_date)
            existing = find_broadcast_by_title(youtube, title)
            if existing:
                _log(f"SKIP: ya existe '{title}' (id={existing.get('id')})")
                existing_titles.append(title)
                continue
            planned.append(title)
            template = templates.get(definition.keyword)
            try:
                created = _create_broadcast(
                    youtube,
                    title=title,
                    scheduled_start=scheduled_start,
                    template=template,
                    default_privacy_status=config.default_privacy_status,
                )
                _log(f"CREATED: '{title}' (id={created.get('id')})")
                created_titles.append(title)
                stream_id = template.bound_stream_id if template else None
                if stream_id:
                    _bind_stream(youtube, created.get("id"), stream_id)
                    _log(f"BIND: broadcast {created.get('id')} -> stream {stream_id}")
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
