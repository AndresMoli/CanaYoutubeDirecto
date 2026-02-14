from __future__ import annotations

import unittest
from unittest.mock import patch
from datetime import datetime, timedelta
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from googleapiclient.errors import HttpError

from src.config import Config
from src.scheduler import (
    DEFAULT_MISA_DESCRIPTION,
    DEFAULT_VELA_DESCRIPTION,
    _iter_broadcasts,
    run_scheduler,
)
from src.title_format import build_title


class _FakeRequest:
    def __init__(self, payload):
        self._payload = payload

    def execute(self):
        return self._payload


class _FakeLiveBroadcasts:
    def __init__(self, items):
        self._items = items
        self._created_by_id = {}
        self.inserted_bodies = []
        self.bound_streams = []
        self.deleted_ids = []
        self.updated_bodies = []
        self.force_insert_chat_enabled = False
        self.force_list_chat_enabled = False

    def list(self, **kwargs):
        broadcast_id = kwargs.get("id")
        if broadcast_id:
            item = self._created_by_id.get(broadcast_id)
            if item:
                listed_item = dict(item)
                listed_content = dict(listed_item.get("contentDetails", {}))
                if self.force_list_chat_enabled:
                    listed_content["enableLiveChat"] = True
                listed_item["contentDetails"] = listed_content
                return _FakeRequest({"items": [listed_item]})
            return _FakeRequest({"items": []})
        return _FakeRequest({"items": self._items})

    def insert(self, **kwargs):
        body = kwargs["body"]
        self.inserted_bodies.append(body)
        title = body["snippet"]["title"]
        created_payload = {
            "id": f"created-{len(self.inserted_bodies)}",
            "snippet": {"title": title},
        }
        if self.force_insert_chat_enabled:
            created_payload["contentDetails"] = {
                "enableLiveChat": True,
                "enableLiveChatReplay": True,
                "enableLiveChatSummary": True,
            }
        self._created_by_id[created_payload["id"]] = created_payload
        return _FakeRequest(created_payload)

    def bind(self, **kwargs):
        self.bound_streams.append((kwargs.get("id"), kwargs.get("streamId")))
        return _FakeRequest({})

    def delete(self, **kwargs):
        self.deleted_ids.append(kwargs.get("id"))
        return _FakeRequest({})

    def update(self, **kwargs):
        body = kwargs["body"]
        self.updated_bodies.append(body)
        item = self._created_by_id.get(body.get("id"))
        if item:
            item["contentDetails"] = {
                **item.get("contentDetails", {}),
                **body.get("contentDetails", {}),
            }
        return _FakeRequest(kwargs["body"])


class _FakeYoutube:
    def __init__(self, items):
        self._live = _FakeLiveBroadcasts(items)

    def liveBroadcasts(self):
        return self._live


class _FakeThumbnails:
    def __init__(self):
        self.calls = []

    def set(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeRequest({})


class _ThumbnailUploadYoutube(_FakeYoutube):
    def __init__(self, items):
        super().__init__(items)
        self._thumbs = _FakeThumbnails()

    def thumbnails(self):
        return self._thumbs



class _FakeStudioCreator:
    def __init__(self, **_kwargs):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def create_with_previous_settings(self, **kwargs):
        self.calls.append(kwargs)


class _NoThumbnailUploadYoutube(_FakeYoutube):
    def thumbnails(self):
        raise AssertionError("No debe intentar subir miniaturas")


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


class _Retry503Request:
    def __init__(self, payload):
        self._payload = payload
        self._attempts = 0

    def execute(self):
        self._attempts += 1
        if self._attempts == 1:
            payload = {
                "error": {
                    "errors": [{"reason": "SERVICE_UNAVAILABLE", "message": "The service is currently unavailable."}],
                    "message": "The service is currently unavailable.",
                }
            }
            raise HttpError(
                SimpleNamespace(status=503, reason="Service Unavailable"),
                json.dumps(payload).encode("utf-8"),
            )
        return self._payload


class _Retry503LiveBroadcasts(_FakeLiveBroadcasts):
    def list(self, **kwargs):
        broadcast_id = kwargs.get("id")
        if broadcast_id:
            return super().list(**kwargs)
        return _Retry503Request({"items": self._items})


class _Retry503Youtube(_FakeYoutube):
    def __init__(self, items):
        self._live = _Retry503LiveBroadcasts(items)


class SchedulerTests(unittest.TestCase):
    @patch("src.scheduler.sleep", return_value=None)
    def test_iter_broadcasts_retries_on_service_unavailable(self, _sleep_mock) -> None:
        youtube = _Retry503Youtube([{"id": "ok", "snippet": {"title": "Misa 10h"}}])

        broadcasts = list(_iter_broadcasts(youtube))

        self.assertEqual(len(broadcasts), 1)
        self.assertEqual(broadcasts[0]["id"], "ok")

    def test_caps_schedule_window_to_fifteen_days(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()
        template_items = [
            {
                "id": f"template-{keyword}",
                "snippet": {
                    "title": f"{keyword} plantilla",
                    "description": f"{keyword} desc",
                    "scheduledStartTime": datetime.combine(today, datetime.min.time(), tz).isoformat(),
                },
                "contentDetails": {},
                "status": {"privacyStatus": "unlisted"},
            }
            for keyword in ("Misa 10h", "Misa 12h", "Misa 20h", "Vela 21h")
        ]

        youtube = _FakeYoutube(template_items)
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
            max_days_ahead=30,
            stop_on_create_limit=True,
            rate_limit_retry_limit=1,
            rate_limit_retry_base_seconds=0.0,
            rate_limit_retry_max_seconds=0.0,
            create_pause_seconds=0.0,
        )

        run_scheduler(youtube, config)

        scheduled_dates = {
            datetime.fromisoformat(body["snippet"]["scheduledStartTime"]).date()
            for body in youtube._live.inserted_bodies
            if "scheduledStartTime" in body["snippet"]
        }
        self.assertIn(today + timedelta(days=11), scheduled_dates)
        self.assertNotIn(today + timedelta(days=12), scheduled_dates)

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

    def test_studio_mode_skips_api_insert_and_uses_ui_creator(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()
        template_item = {
            "id": "template-10",
            "snippet": {
                "title": "Misa 10h histórica",
                "description": "Descripción emitida",
                "scheduledStartTime": datetime.combine(today, datetime.min.time(), tz).isoformat(),
                "actualEndTime": datetime.combine(today, datetime.min.time(), tz).isoformat(),
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
            creation_mode="studio_ui",
            studio_storage_state_path="fake.json",
        )

        with patch("src.scheduler_studio.StudioBroadcastCreator", _FakeStudioCreator):
            run_scheduler(youtube, config)

        self.assertEqual(len(youtube._live.inserted_bodies), 0)

    def test_does_not_upload_thumbnail_when_reusing_metadata(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()
        template_item = {
            "id": "template-10",
            "snippet": {
                "title": "Misa 10h histórica",
                "description": "Descripción emitida",
                "scheduledStartTime": datetime.combine(today, datetime.min.time(), tz).isoformat(),
                "actualEndTime": datetime.combine(today, datetime.min.time(), tz).isoformat(),
                "thumbnails": {"high": {"url": "https://example.org/thumb.jpg"}},
            },
            "contentDetails": {"boundStreamId": "stream-shared"},
            "status": {"privacyStatus": "unlisted"},
        }

        youtube = _NoThumbnailUploadYoutube([template_item])
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

        tomorrow = today + timedelta(days=1)
        misa_10_title = build_title("Misa 10h", tomorrow)
        description_by_title = {
            body["snippet"]["title"]: body["snippet"]["description"]
            for body in youtube._live.inserted_bodies
            if "scheduledStartTime" in body["snippet"]
        }
        self.assertEqual(description_by_title[misa_10_title], "Descripción emitida")

    def test_uses_latest_emitted_template_for_same_keyword(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()

        old_emitted_10 = {
            "id": "old-emitted-10",
            "snippet": {
                "title": "Misa 10h antigua",
                "description": "Desc vieja 10",
                "scheduledStartTime": datetime.combine(today - timedelta(days=5), datetime.min.time(), tz).isoformat(),
                "actualEndTime": datetime.combine(today - timedelta(days=5), datetime.min.time(), tz).isoformat(),
            },
            "contentDetails": {"boundStreamId": "stream-emitted"},
            "status": {"privacyStatus": "unlisted"},
        }
        latest_emitted_10 = {
            "id": "latest-emitted-10",
            "snippet": {
                "title": "Misa 10h última",
                "description": "Desc última 10",
                "scheduledStartTime": datetime.combine(today - timedelta(days=1), datetime.min.time(), tz).isoformat(),
                "actualEndTime": datetime.combine(today - timedelta(days=1), datetime.min.time(), tz).isoformat(),
            },
            "contentDetails": {"boundStreamId": "stream-emitted"},
            "status": {"privacyStatus": "unlisted"},
        }
        latest_emitted_12 = {
            "id": "latest-emitted-12",
            "snippet": {
                "title": "Misa 12h última",
                "description": "Desc última 12",
                "scheduledStartTime": datetime.combine(today - timedelta(days=1), datetime.min.time(), tz).isoformat(),
                "actualEndTime": datetime.combine(today - timedelta(days=1), datetime.min.time(), tz).isoformat(),
            },
            "contentDetails": {},
            "status": {"privacyStatus": "unlisted"},
        }

        youtube = _NoThumbnailUploadYoutube([old_emitted_10, latest_emitted_10, latest_emitted_12])
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

        tomorrow = today + timedelta(days=1)
        misa_10_title = build_title("Misa 10h", tomorrow)
        misa_12_title = build_title("Misa 12h", tomorrow)
        description_by_title = {
            body["snippet"]["title"]: body["snippet"]["description"]
            for body in youtube._live.inserted_bodies
            if "scheduledStartTime" in body["snippet"]
        }
        self.assertEqual(description_by_title[misa_10_title], "Desc última 10")
        self.assertEqual(description_by_title[misa_12_title], "Desc última 12")

    def test_copies_category_audience_and_chat_settings_from_latest_emitted(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()
        template_item = {
            "id": "latest-emitted-10",
            "snippet": {
                "title": "Misa 10h última",
                "description": "Desc última 10",
                "categoryId": "29",
                "scheduledStartTime": datetime.combine(today - timedelta(days=1), datetime.min.time(), tz).isoformat(),
                "actualEndTime": datetime.combine(today - timedelta(days=1), datetime.min.time(), tz).isoformat(),
            },
            "contentDetails": {
                "enableLowLatency": True,
                "enableDvr": False,
                "enableLiveChat": True,
                "enableLiveChatReplay": True,
                "enableLiveChatSummary": True,
                "boundStreamId": "stream-emitted",
            },
            "status": {
                "privacyStatus": "unlisted",
                "selfDeclaredMadeForKids": False,
            },
            "monetizationDetails": {
                "adsMonetizationStatus": "off",
                "cuepointSchedule": {"enabled": True},
            },
        }

        youtube = _NoThumbnailUploadYoutube([template_item])
        config = Config(
            client_id="id",
            client_secret="secret",
            refresh_token="token",
            timezone="UTC",
            default_privacy_status="private",
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

        tomorrow = today + timedelta(days=1)
        misa_10_title = build_title("Misa 10h", tomorrow)
        body_by_title = {
            body["snippet"]["title"]: body
            for body in youtube._live.inserted_bodies
            if "scheduledStartTime" in body["snippet"]
        }
        misa_10_body = body_by_title[misa_10_title]
        self.assertEqual(misa_10_body["snippet"]["categoryId"], "29")
        self.assertFalse(misa_10_body["status"]["selfDeclaredMadeForKids"])
        self.assertTrue(misa_10_body["contentDetails"]["enableLowLatency"])
        self.assertFalse(misa_10_body["contentDetails"]["enableDvr"])
        self.assertEqual(misa_10_body["monetizationDetails"]["adsMonetizationStatus"], "on")
        self.assertFalse(misa_10_body["monetizationDetails"]["cuepointSchedule"]["enabled"])
        self.assertFalse(misa_10_body["contentDetails"]["enableLiveChat"])
        self.assertTrue(misa_10_body["contentDetails"]["enableLiveChatReplay"])
        self.assertTrue(misa_10_body["contentDetails"]["enableLiveChatSummary"])

    def test_skips_creation_when_same_slot_exists_even_with_different_title(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()
        tomorrow = today + timedelta(days=1)
        existing_slot = {
            "id": "existing-misa-12",
            "snippet": {
                "title": "Misa 12h - Evento ya creado manualmente",
                "description": "Desc previa",
                "scheduledStartTime": datetime.combine(tomorrow, datetime.min.time().replace(hour=12), tz).isoformat(),
            },
            "contentDetails": {},
            "status": {"privacyStatus": "unlisted"},
        }

        youtube = _FakeYoutube([existing_slot])
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

        created_titles = [body["snippet"]["title"] for body in youtube._live.inserted_bodies]
        self.assertNotIn(build_title("Misa 12h", tomorrow), created_titles)

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

    def test_uploads_thumbnail_for_each_created_broadcast(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()
        template_items = [
            {
                "id": "template-10",
                "snippet": {
                    "title": "Misa 10h plantilla",
                    "description": "Desc 10",
                    "scheduledStartTime": datetime.combine(today, datetime.min.time(), tz).isoformat(),
                    "thumbnails": {"high": {"url": "https://example.org/10.jpg"}},
                },
                "contentDetails": {},
                "status": {"privacyStatus": "unlisted"},
            },
            {
                "id": "template-12",
                "snippet": {
                    "title": "Misa 12h plantilla",
                    "description": "Desc 12",
                    "scheduledStartTime": datetime.combine(today, datetime.min.time(), tz).isoformat(),
                    "thumbnails": {"high": {"url": "https://example.org/12.jpg"}},
                },
                "contentDetails": {},
                "status": {"privacyStatus": "unlisted"},
            },
            {
                "id": "template-20",
                "snippet": {
                    "title": "Misa 20h plantilla",
                    "description": "Desc 20",
                    "scheduledStartTime": datetime.combine(today, datetime.min.time(), tz).isoformat(),
                    "thumbnails": {"high": {"url": "https://example.org/20.jpg"}},
                },
                "contentDetails": {},
                "status": {"privacyStatus": "unlisted"},
            },
        ]

        youtube = _ThumbnailUploadYoutube(template_items)
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
            max_days_ahead=2,
            stop_on_create_limit=True,
            rate_limit_retry_limit=1,
            rate_limit_retry_base_seconds=0.0,
            rate_limit_retry_max_seconds=0.0,
            create_pause_seconds=0.0,
        )

        class _FakeHeaders:
            @staticmethod
            def get_content_type():
                return "image/jpeg"

        class _FakeResponse:
            headers = _FakeHeaders()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            @staticmethod
            def read():
                return b"img"

        with patch("src.scheduler.urlopen", return_value=_FakeResponse()):
            run_scheduler(youtube, config)

        self.assertGreaterEqual(len(youtube._thumbs.calls), 6)

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

    def test_deletes_broadcast_if_thumbnail_cannot_be_replicated(self) -> None:
        tz = ZoneInfo("UTC")
        today = datetime.now(tz).date()
        template_item = {
            "id": "latest-emitted-10",
            "snippet": {
                "title": "Misa 10h última",
                "description": "Desc última 10",
                "scheduledStartTime": datetime.combine(today - timedelta(days=1), datetime.min.time(), tz).isoformat(),
                "actualEndTime": datetime.combine(today - timedelta(days=1), datetime.min.time(), tz).isoformat(),
                "thumbnails": {"high": {"url": "https://example.org/fail.jpg"}},
            },
            "contentDetails": {"boundStreamId": "stream-emitted"},
            "status": {"privacyStatus": "unlisted"},
        }

        youtube = _ThumbnailUploadYoutube([template_item])
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

        with patch("src.scheduler.urlopen", side_effect=RuntimeError("download failed")):
            run_scheduler(youtube, config)

        self.assertGreaterEqual(len(youtube._live.deleted_ids), 1)

    def test_updates_created_broadcast_when_chat_is_still_enabled(self) -> None:
        youtube = _FakeYoutube([])
        youtube._live.force_insert_chat_enabled = True

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

        self.assertGreaterEqual(len(youtube._live.updated_bodies), 1)
        for body in youtube._live.updated_bodies:
            self.assertFalse(body["contentDetails"]["enableLiveChat"])
            self.assertEqual(set(body["contentDetails"].keys()), {"enableLiveChat"})

    def test_updates_created_broadcast_when_verification_detects_live_chat_enabled(self) -> None:
        youtube = _FakeYoutube([])
        youtube._live.force_list_chat_enabled = True

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

        self.assertGreaterEqual(len(youtube._live.updated_bodies), 1)
        for body in youtube._live.updated_bodies:
            self.assertFalse(body["contentDetails"]["enableLiveChat"])


if __name__ == "__main__":
    unittest.main()
