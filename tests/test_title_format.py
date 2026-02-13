from datetime import date
import unittest

from src.title_format import build_title


class TitleFormatTests(unittest.TestCase):
    def test_build_title_includes_weekday_and_month(self) -> None:
        self.assertEqual(
            build_title("Misa 12h", date(2026, 2, 14)),
            "Misa 12h - Sábado 14 de febrero",
        )


if __name__ == "__main__":
    unittest.main()
