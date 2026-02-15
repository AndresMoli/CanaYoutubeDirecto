import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.config import load_config


class ConfigTests(unittest.TestCase):
    def test_empty_keyword_env_uses_defaults(self) -> None:
        env = {
            "YT_CLIENT_ID": "id",
            "YT_CLIENT_SECRET": "secret",
            "YT_REFRESH_TOKEN": "token",
            "YT_TIMEZONE": " ",
            "YT_KEYWORD_MISA_10": "",
            "YT_KEYWORD_MISA_12": "   ",
            "YT_KEYWORD_MISA_20": "",
            "YT_KEYWORD_VELA_21": "",
        }
        with patch.dict(os.environ, env, clear=True):
            config = load_config()

        self.assertEqual(config.timezone, "Europe/Madrid")
        self.assertEqual(config.keyword_misa_10, "Misa 10h")
        self.assertEqual(config.keyword_misa_12, "Misa 12h")
        self.assertEqual(config.keyword_misa_20, "Misa 20h")
        self.assertEqual(config.keyword_vela_21, "Vela 21h")
        self.assertEqual(config.creation_mode, "studio_ui")
        self.assertTrue(config.studio_log_screenshots)
        self.assertEqual(config.studio_log_screenshots_dir, "studio_logs")

    def test_storage_state_falls_back_to_default_file(self) -> None:
        env = {
            "YT_CLIENT_ID": "id",
            "YT_CLIENT_SECRET": "secret",
            "YT_REFRESH_TOKEN": "token",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")
            current_dir = os.getcwd()
            try:
                os.chdir(temp_dir)
                with patch.dict(os.environ, env, clear=True):
                    config = load_config()
            finally:
                os.chdir(current_dir)

        self.assertEqual(config.studio_storage_state_path, "storage_state.json")


if __name__ == "__main__":
    unittest.main()
