import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from src.studio_creator import StudioBroadcastCreator, StudioCreationError


class _FakePage:
    def set_default_timeout(self, timeout_ms):
        self.timeout_ms = timeout_ms


class _FakeContext:
    def new_page(self):
        return _FakePage()

    def close(self):
        return None


class _FakeBrowser:
    def new_context(self, **kwargs):
        self.kwargs = kwargs
        return _FakeContext()

    def close(self):
        return None


class _FakeChromium:
    def launch(self, **kwargs):
        self.kwargs = kwargs
        return _FakeBrowser()


class _FakePlaywright:
    def __init__(self):
        self.chromium = _FakeChromium()

    def stop(self):
        return None


class _FakeSyncPlaywrightFactory:
    def start(self):
        return _FakePlaywright()


class StudioCreatorTests(unittest.TestCase):
    def _fake_playwright_modules(self):
        playwright_module = types.ModuleType("playwright")
        sync_api_module = types.ModuleType("playwright.sync_api")
        sync_api_module.Error = Exception
        sync_api_module.sync_playwright = lambda: _FakeSyncPlaywrightFactory()
        playwright_module.sync_api = sync_api_module
        return {
            "playwright": playwright_module,
            "playwright.sync_api": sync_api_module,
        }

    def test_empty_storage_path_fails_with_clear_error(self):
        creator = StudioBroadcastCreator(
            storage_state_path="   ",
            headless=True,
            timeout_ms=30000,
            slow_mo_ms=0,
        )
        with self.assertRaises(StudioCreationError) as error:
            creator.__enter__()

        self.assertIn("Falta YT_STUDIO_STORAGE_STATE_PATH", str(error.exception))

    def test_directory_with_single_json_is_accepted(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "sesion.json"
            json_path.write_text(json.dumps({"cookies": [], "origins": []}), encoding="utf-8")

            creator = StudioBroadcastCreator(
                storage_state_path=temp_dir,
                headless=True,
                timeout_ms=30000,
                slow_mo_ms=0,
            )
            with patch.dict(sys.modules, self._fake_playwright_modules()):
                with creator:
                    self.assertEqual(creator._storage_state_path, json_path)

    def test_invalid_json_file_fails_before_playwright(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "storage_state.json"
            json_path.write_text("{not valid json", encoding="utf-8")

            creator = StudioBroadcastCreator(
                storage_state_path=str(json_path),
                headless=True,
                timeout_ms=30000,
                slow_mo_ms=0,
            )
            with self.assertRaises(StudioCreationError) as error:
                creator.__enter__()

        self.assertIn("no contiene JSON válido", str(error.exception))


if __name__ == "__main__":
    unittest.main()
