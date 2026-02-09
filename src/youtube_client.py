from __future__ import annotations

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from .config import Config


def build_youtube_client(config: Config):
    credentials = Credentials(
        token=None,
        refresh_token=config.refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=config.client_id,
        client_secret=config.client_secret,
        scopes=["https://www.googleapis.com/auth/youtube"],
    )
    credentials.refresh(Request())
    return build("youtube", "v3", credentials=credentials)
