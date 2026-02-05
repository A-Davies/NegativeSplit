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
    client_id: str
    client_secret: str
    cache_path: Path = settings.TOKEN_JSON

    def save_token(self, token_data: dict) -> None:
        Path(self.cache_path.parent).mkdir(exist_ok=True, parents=True)
        with open(self.cache_path, "w") as f:
            json.dump(token_data, f)

    def load_token(self) -> StravaToken | None:
        if not self.cache_path.exists():
            return None
        with open(self.cache_path) as f:
            return StravaToken(**json.load(f))

    def get_valid_token(self) -> str:
        token = self.load_token()
        if not token:
            raise Exception("No token found. Please run the OAuth login flow.")

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


def main():

    pass

    # manager = TokenManager(client_id="YOUR_ID", client_secret="YOUR_SECRET")

    # # This will either return the current token or
    # # automatically call Strava to get a new one.
    # token = manager.get_valid_token()

    # # Now use it in a request
    # headers = {"Authorization": f"Bearer {token}"}
    # # ... make your API call ...
