from strava_api.token_manager import TokenManager
from strava_api.sync_service import StravaSync
from core.config import settings

# TODO - currently not working on work laptop on work wifi, need to test at home


def main():
    # Initialize Folders
    print("📁 Setting up data directories...")
    settings.setup_folders()

    # Setup Credentials
    STRAVA_CLIENT_ID = settings.STRAVA_CLIENT_ID
    STRAVA_CLIENT_SECRET = settings.STRAVA_CLIENT_SECRET

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
