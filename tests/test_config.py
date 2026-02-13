import os
import unittest
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


if __name__ == "__main__":
    unittest.main()
