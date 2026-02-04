from pathlib import Path


class Config:
    # 1. Find the Workspace Root (Climb up from this file)
    # config.py -> core -> src -> core -> projects -> NegativeSplit
    ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent

    # 2. Define Data Subdirectories
    DATA_DIR = ROOT_DIR / "data"
    RAW_DIR = DATA_DIR / "raw"
    GPX_DIR = RAW_DIR / "gpx"
    PROCESSED_DIR = DATA_DIR / "processed"
    CACHE_DIR = DATA_DIR / "cache"

    # 3. Specific File Paths
    PARQUET_PATH = PROCESSED_DIR / "activities.parquet"
    TOKEN_JSON = CACHE_DIR / "token.json"

    @classmethod
    def setup_folders(cls):
        """Call this once to ensure your data tree exists."""
        for path in [cls.GPX_DIR, cls.PROCESSED_DIR, cls.CACHE_DIR]:
            path.mkdir(parents=True, exist_ok=True)


# Create a singleton instance for easy import
settings = Config()
