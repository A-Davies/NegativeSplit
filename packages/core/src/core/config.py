import os
from pathlib import Path

from dotenv import load_dotenv


class StravaCredentialsError(Exception):
    """Missing Strava credentials. Please set STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET in your .env file."""


class Config:
    # Find the Workspace Root (Climb up from this file)
    # config.py -> core -> src -> core -> projects -> NegativeSplit
    ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent

    # Define Data Subdirectories
    DATA_DIR = ROOT_DIR / "data"
    RAW_DIR = DATA_DIR / "raw"
    GPX_DIR = RAW_DIR / "gpx"
    PROCESSED_DIR = DATA_DIR / "processed"
    CACHE_DIR = DATA_DIR / "cache"

    # Specific File Paths
    RAW_ACTIVITES_PATH = PROCESSED_DIR / "raw_activities.csv"
    PROCESSED_ACTIVITIES_PATH = PROCESSED_DIR / "processed_activities.parquet"
    TOKEN_JSON = CACHE_DIR / "token.json"

    # Strava API keys, get from environment
    # Load .env file into environment variables
    load_dotenv()
    STRAVA_CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
    STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")

    if not STRAVA_CLIENT_ID or not STRAVA_CLIENT_SECRET:
        raise StravaCredentialsError

    STRAVA_API_BASE = "https://www.strava.com/api/v3"

    @classmethod
    def setup_folders(cls) -> None:
        """Call this once to ensure your data tree exists."""
        for path in [cls.GPX_DIR, cls.PROCESSED_DIR, cls.CACHE_DIR]:
            path.mkdir(parents=True, exist_ok=True)


DISTANCE_TARGETS = {
    "year": {
        "2026": 2500,
        "default": 2000,
    },
    "month": {
        "2026-01": 150,
        "2026-02": 200,
        "2026-03": 250,
        "2026-04": 300,
        "default": 150,
    },
}

# Create a singleton instance for easy import
settings = Config()
