from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src.config import Config
from src.scheduler import run_scheduler
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

    def list(self, **_kwargs):
        return _FakeRequest({"items": self._items})

    def insert(self, **kwargs):
        body = kwargs["body"]
        self.inserted_bodies.append(body)
        title = body["snippet"]["title"]
        return _FakeRequest({"id": f"created-{len(self.inserted_bodies)}", "snippet": {"title": title}})

    def bind(self, **_kwargs):
        return _FakeRequest({})


class _FakeYoutube:
    def __init__(self, items):
        self._live = _FakeLiveBroadcasts(items)

    def liveBroadcasts(self):
        return self._live


class SchedulerTests(unittest.TestCase):
    def test_creates_without_template_and_without_skipping_start_day(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()
        tomorrow = today + timedelta(days=1)
        # Existing broadcast in the future should not shift start date.
        future_item = {
            "id": "future",
            "snippet": {
                "title": "Misa de 10h - 31 de diciembre",
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
            keyword_misa_10="Misa de 10h",
            keyword_misa_12="Misa de 12h",
            keyword_misa_20="Misa de 20h",
            keyword_vela_21="Vela 21h",
            start_offset_days=1,
            max_days_ahead=1,
            stop_on_create_limit=True,
        )

        exit_code = run_scheduler(youtube, config)
        self.assertEqual(exit_code, 0)

        created_titles = [body["snippet"]["title"] for body in youtube._live.inserted_bodies]
        self.assertIn(build_title("Misa de 10h", tomorrow), created_titles)
        self.assertIn(build_title("Misa de 12h", tomorrow), created_titles)
        self.assertIn(build_title("Misa de 20h", tomorrow), created_titles)

        self.assertIn("Misa de 12h", created_titles)
        self.assertIn("Misa de 20h", created_titles)
        self.assertIn("Vela 21h", created_titles)


if __name__ == "__main__":
    unittest.main()
