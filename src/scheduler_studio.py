from __future__ import annotations

from datetime import datetime, time, timedelta

from .config import Config
from .scheduler import (
    BroadcastDefinition,
    BroadcastTemplate,
    DEFAULT_MISA_DESCRIPTION,
    DEFAULT_VELA_DESCRIPTION,
    _ensure_template_for_keyword,
    _iter_broadcasts,
    _list_scheduled_broadcasts,
    _load_timezone,
    _log,
    _log_status_list,
    _log_summary,
    _rfc3339,
    find_scheduled_broadcast_for_slot_in_items,
)
from .studio_creator import StudioBroadcastCreator, StudioCreationError
from .title_format import build_title


def run_scheduler_studio(youtube, config: Config) -> int:
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
    _log(f"START(STUDIO): procesando desde {start_date.isoformat()} (sin saltar huecos).")
    max_days_ahead = min(config.max_days_ahead, 11)
    end_date = today + timedelta(days=max_days_ahead)
    if start_date > end_date:
        _log(
            "SKIP: no hay días pendientes (inicio "
            f"{start_date.isoformat()} > fin {end_date.isoformat()})."
        )
        return 0
    total_days = (end_date - start_date).days + 1
    _log_status_list("emisiones programadas detectadas", _list_scheduled_broadcasts(broadcasts, tz))

    templates: dict[str, BroadcastTemplate | None] = {}
    for definition in definitions:
        if definition.keyword not in templates:
            templates[definition.keyword] = _ensure_template_for_keyword(
                broadcasts,
                definition.keyword,
            )
            if templates[definition.keyword]:
                _log(f"TEMPLATE: '{definition.keyword}' encontrada.")


    planned: list[str] = []
    created_titles: list[str] = []
    existing_titles: list[str] = []
    failed: list[str] = []

    creator = StudioBroadcastCreator(
        storage_state_path=config.studio_storage_state_path,
        headless=config.studio_headless,
        timeout_ms=config.studio_timeout_ms,
        slow_mo_ms=config.studio_slow_mo_ms,
    )

    with creator:
        _log("MODE: creación vía YouTube Studio (Playwright).")
        for offset in range(total_days):
            target_date = start_date + timedelta(days=offset)
            _log(f"DAY: procesando {target_date.isoformat()}")
            for definition in definitions:
                if definition.keyword == config.keyword_vela_21 and target_date.weekday() != 3:
                    continue

                scheduled_start = datetime.combine(target_date, definition.scheduled_time, tz)
                title = build_title(definition.prefix, target_date)
                existing = find_scheduled_broadcast_for_slot_in_items(
                    broadcasts,
                    title=title,
                    keyword=definition.keyword,
                    scheduled_start=scheduled_start,
                    tz=tz,
                )
                if existing:
                    _log(f"SKIP: ya existe '{title}' (id={existing.get('id')})")
                    existing_titles.append(title)
                    continue

                planned.append(title)
                try:
                    creator.create_with_previous_settings(
                        title=title,
                        scheduled_start=scheduled_start,
                        template_keyword=definition.keyword,
                    )
                    created = {
                        "id": f"studio-{len(created_titles) + 1}",
                        "snippet": {
                            "title": title,
                            "scheduledStartTime": _rfc3339(scheduled_start),
                        },
                    }
                    created_titles.append(title)
                    broadcasts.append(created)
                    _log(f"CREATED(STUDIO): '{title}'")
                except StudioCreationError as studio_error:
                    failed.append(f"{title} (studio error: {studio_error})")
                    _log_summary(planned, created_titles, existing_titles, failed)
                    raise

    _log("DONE(STUDIO): reached max days ahead without limit.")
    _log_summary(planned, created_titles, existing_titles, failed)
    return 0
