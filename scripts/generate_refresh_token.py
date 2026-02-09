from __future__ import annotations

import os

from google_auth_oauthlib.flow import InstalledAppFlow


def main() -> None:
    client_id = os.getenv("YT_CLIENT_ID")
    client_secret = os.getenv("YT_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise ValueError("Set YT_CLIENT_ID and YT_CLIENT_SECRET to generate token.")

    flow = InstalledAppFlow.from_client_config(
        {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        },
        scopes=["https://www.googleapis.com/auth/youtube"],
    )
    credentials = flow.run_console()
    print("Refresh token:")
    print(credentials.refresh_token)


if __name__ == "__main__":
    main()
