"""Manages Strava API token, including loading, saving, and refreshing."""

import json
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
from core.config import settings
from pydantic import BaseModel


class StravaToken(BaseModel):
    access_token: str
    refresh_token: str
    expires_at: int  # Unix timestamp


@dataclass
class TokenManager:
    """Manages Strava API token, including loading, saving, and refreshing.

    :raises MissingStravaTokenError: raised when json does not exist.
    """

    client_id: str
    client_secret: str
    cache_path: Path = settings.TOKEN_JSON

    def save_token(self, token_data: dict) -> None:
        """Saves generated Strava token as a .json file.

        :param token_data: dict with Strava token details.
        :type token_data: dict
        """
        Path(self.cache_path.parent).mkdir(exist_ok=True, parents=True)
        self.cache_path.write_text(json.dumps(token_data, indent=4))

    def load_token(self) -> StravaToken | None:
        """Loads a Strava token from `self.cache_path` and returns a `StravaToken` object, or None.

        :return: Strava tokens
        :rtype: StravaToken | None
        """
        if not self.cache_path.exists():
            return None
        with self.cache_path.open() as f:
            return StravaToken(**json.load(f))

    def get_valid_token(self) -> str:
        token = self.load_token()
        if not token:
            raise MissingStravaTokenError

        # If token expires in less than 5 minutes, refresh it
        if token.expires_at < (time.time() + 300):
            print("Strava API Token expired or expiring soon. Refreshing...")
            return self.refresh_token(token.refresh_token)

        return token.access_token

    def refresh_token(self, refresh_token: str) -> str:
        response = httpx.post(
            "https://www.strava.com/oauth/token",
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
        )
        new_token_data = response.json()
        self.save_token(new_token_data)
        return new_token_data["access_token"]


class MissingStravaTokenError(Exception):
    """Missing Strava Token File. credentials. Expected at `settings.TOKEN_JSON`."""
