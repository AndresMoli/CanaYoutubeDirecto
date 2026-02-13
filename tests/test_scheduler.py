from __future__ import annotations

import unittest
from datetime import datetime, timedelta
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from googleapiclient.errors import HttpError

from src.config import Config
from src.scheduler import DEFAULT_MISA_DESCRIPTION, DEFAULT_VELA_DESCRIPTION, run_scheduler
from src.title_format import build_title


class _FakeRequest:
    def __init__(self, payload):
        self._payload = payload

    def execute(self):
        return self._payload


class _FakeLiveBroadcasts:
    def __init__(self, items):
        self._items = items
        self.inserted_bodies = []
        self.bound_streams = []

    def list(self, **_kwargs):
        return _FakeRequest({"items": self._items})

    def insert(self, **kwargs):
        body = kwargs["body"]
        self.inserted_bodies.append(body)
        title = body["snippet"]["title"]
        return _FakeRequest({"id": f"created-{len(self.inserted_bodies)}", "snippet": {"title": title}})

    def bind(self, **kwargs):
        self.bound_streams.append((kwargs.get("id"), kwargs.get("streamId")))
        return _FakeRequest({})


class _FakeYoutube:
    def __init__(self, items):
        self._live = _FakeLiveBroadcasts(items)

    def liveBroadcasts(self):
        return self._live




class _AlwaysRateLimitLiveBroadcasts(_FakeLiveBroadcasts):
    def insert(self, **_kwargs):
        payload = {
            "error": {
                "errors": [{"reason": "userRequestsExceedRateLimit", "message": "Quota exceeded"}],
                "message": "Quota exceeded",
            }
        }
        raise HttpError(SimpleNamespace(status=403, reason="Forbidden"), json.dumps(payload).encode("utf-8"))


class _AlwaysRateLimitYoutube(_FakeYoutube):
    def __init__(self, items):
        self._live = _AlwaysRateLimitLiveBroadcasts(items)


class SchedulerTests(unittest.TestCase):
    def test_creates_without_template_and_without_skipping_start_day(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()
        tomorrow = today + timedelta(days=1)
        # Existing broadcast in the future should not shift start date.
        future_item = {
            "id": "future",
            "snippet": {
                "title": "Misa 10h - 31 de diciembre",
                "scheduledStartTime": datetime.combine(today + timedelta(days=10), datetime.min.time(), tz).isoformat(),
            },
            "contentDetails": {},
            "status": {"privacyStatus": "unlisted"},
        }

        youtube = _FakeYoutube([future_item])
        config = Config(
            client_id="id",
            client_secret="secret",
            refresh_token="token",
            timezone="UTC",
            default_privacy_status="unlisted",
            keyword_misa_10="Misa 10h",
            keyword_misa_12="Misa 12h",
            keyword_misa_20="Misa 20h",
            keyword_vela_21="Vela 21h",
            start_offset_days=1,
            max_days_ahead=1,
            stop_on_create_limit=True,
            rate_limit_retry_limit=3,
            rate_limit_retry_base_seconds=0.0,
            rate_limit_retry_max_seconds=0.0,
            create_pause_seconds=0.0,
        )

        exit_code = run_scheduler(youtube, config)
        self.assertEqual(exit_code, 0)

        created_titles = [body["snippet"]["title"] for body in youtube._live.inserted_bodies]
        self.assertIn(build_title("Misa 10h", tomorrow), created_titles)
        self.assertIn(build_title("Misa 12h", tomorrow), created_titles)
        self.assertIn(build_title("Misa 20h", tomorrow), created_titles)

        self.assertIn("Misa 12h", created_titles)
        self.assertIn("Misa 20h", created_titles)
        self.assertIn("Vela 21h", created_titles)

    def test_rate_limit_exits_zero_after_retries(self) -> None:
        youtube = _AlwaysRateLimitYoutube([])
        config = Config(
            client_id="id",
            client_secret="secret",
            refresh_token="token",
            timezone="UTC",
            default_privacy_status="unlisted",
            keyword_misa_10="Misa 10h",
            keyword_misa_12="Misa 12h",
            keyword_misa_20="Misa 20h",
            keyword_vela_21="Vela 21h",
            start_offset_days=1,
            max_days_ahead=1,
            stop_on_create_limit=True,
            rate_limit_retry_limit=1,
            rate_limit_retry_base_seconds=0.0,
            rate_limit_retry_max_seconds=0.0,
            create_pause_seconds=0.0,
        )

        exit_code = run_scheduler(youtube, config)

        self.assertEqual(exit_code, 0)

    def test_uses_template_description_and_shared_stream_binding(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()
        tomorrow = today + timedelta(days=1)
        template_item = {
            "id": "template-10",
            "snippet": {
                "title": "Misa 10h plantilla",
                "description": "Descripción misa 10h",
                "scheduledStartTime": datetime.combine(today, datetime.min.time(), tz).isoformat(),
            },
            "contentDetails": {"boundStreamId": "stream-shared"},
            "status": {"privacyStatus": "unlisted"},
        }

        youtube = _FakeYoutube([template_item])
        config = Config(
            client_id="id",
            client_secret="secret",
            refresh_token="token",
            timezone="UTC",
            default_privacy_status="unlisted",
            keyword_misa_10="Misa 10h",
            keyword_misa_12="Misa 12h",
            keyword_misa_20="Misa 20h",
            keyword_vela_21="Vela 21h",
            start_offset_days=1,
            max_days_ahead=1,
            stop_on_create_limit=True,
            rate_limit_retry_limit=1,
            rate_limit_retry_base_seconds=0.0,
            rate_limit_retry_max_seconds=0.0,
            create_pause_seconds=0.0,
        )

        run_scheduler(youtube, config)

        created_bodies = youtube._live.inserted_bodies
        scheduled_bodies = [
            body for body in created_bodies if body["snippet"]["title"] not in {"Misa 12h", "Misa 20h", "Vela 21h"}
        ]
        misa_10_title = build_title("Misa 10h", tomorrow)
        misa_12_title = build_title("Misa 12h", tomorrow)
        misa_20_title = build_title("Misa 20h", tomorrow)

        description_by_title = {body["snippet"]["title"]: body["snippet"]["description"] for body in scheduled_bodies}
        self.assertEqual(description_by_title[misa_10_title], "Descripción misa 10h")
        self.assertEqual(description_by_title[misa_12_title], DEFAULT_MISA_DESCRIPTION)
        self.assertEqual(description_by_title[misa_20_title], DEFAULT_MISA_DESCRIPTION)

        for _broadcast_id, stream_id in youtube._live.bound_streams:
            self.assertEqual(stream_id, "stream-shared")

        vela_title = build_title("Vela 21h", tomorrow)
        if tomorrow.weekday() == 3:
            self.assertEqual(description_by_title[vela_title], DEFAULT_VELA_DESCRIPTION)


if __name__ == "__main__":
    unittest.main()
