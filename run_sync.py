"""script will run a strava sync to make sure all activities are saved locally."""

from core.config import settings
from strava_api.sync_service import StravaSync
from strava_api.token_manager import TokenManager

# TODO - currently not working on work laptop on work wifi, need to test at home


def main() -> None:
    """Uses StravaSync to update all local data from Strava."""
    # Initialize Folders
    print("📁 Setting up data directories...")
    settings.setup_folders()

    # Setup Credentials
    STRAVA_CLIENT_ID = settings.STRAVA_CLIENT_ID
    STRAVA_CLIENT_SECRET = settings.STRAVA_CLIENT_SECRET
    assert isinstance(STRAVA_CLIENT_ID, str)
    assert isinstance(STRAVA_CLIENT_SECRET, str)

    # Initialize the Engine
    print("🔑 Authenticating with Strava...")
    token_manager = TokenManager(STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET)
    syncer = StravaSync(token_manager=token_manager)

    # Run the Sync
    print("🔄 Starting Sync Loop...")
    syncer.sync()
    print("✅ Finished! Check the 'data' folder for your GPX and Parquet files.")


if __name__ == "__main__":
    main()
