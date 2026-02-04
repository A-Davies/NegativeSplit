from strava_api.token_manager import TokenManager
from strava_api.sync_service import StravaSync
from core.config import settings


def main():
    # 1. Initialize Folders
    print("📁 Setting up data directories...")
    settings.setup_folders()

    # 2. Setup Credentials
    # Replace with your actual credentials from the Strava Dashboard
    MY_CLIENT_ID = ""
    MY_CLIENT_SECRET = ""

    # 3. Initialize the Engine
    print("🔑 Authenticating with Strava...")
    token_manager = TokenManager(MY_CLIENT_ID, MY_CLIENT_SECRET)
    syncer = StravaSync(token_manager=token_manager)

    # 4. Run the Sync
    print("🔄 Starting Sync Loop...")
    syncer.sync()
    print("✅ Finished! Check the 'data' folder for your GPX and Parquet files.")


if __name__ == "__main__":
    main()
