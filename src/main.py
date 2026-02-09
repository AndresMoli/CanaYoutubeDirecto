from __future__ import annotations

from .config import load_config
from .scheduler import main as scheduler_main
from .youtube_client import build_youtube_client


def run() -> None:
    config = load_config()
    youtube = build_youtube_client(config)
    scheduler_main(youtube, config)


if __name__ == "__main__":
    run()
