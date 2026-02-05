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
    strava_client_id = settings.STRAVA_CLIENT_ID
    strava_client_secret = settings.STRAVA_CLIENT_SECRET
    assert isinstance(strava_client_id, str)
    assert isinstance(strava_client_secret, str)

    # Initialize the Engine
    print("🔑 Authenticating with Strava...")
    token_manager = TokenManager(strava_client_id, strava_client_secret)
    syncer = StravaSync(token_manager=token_manager)

    # Run the Sync
    print("🔄 Starting Sync Loop...")
    syncer.sync()
    print("✅ Finished! Check the 'data' folder for your GPX and Parquet files.")


if __name__ == "__main__":
    main()
