"""Strava API synchronization service for fetching and storing activity data."""

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pandas as pd
from core.config import settings
from gpxpy import gpx
from tqdm import tqdm

from strava_api.token_manager import TokenManager


@dataclass
class StravaSync:
    """Manages synchronization of Strava activities with local storage.

    Handles API communication, rate limiting, GPX file creation, and
    local database updates.
    """

    token_manager: TokenManager
    base_dir: Path = settings.DATA_DIR
    gpx_dir: Path = settings.GPX_DIR
    raw_activities_path: Path = settings.RAW_ACTIVITES_PATH
    # Track usage to keep the UI informed later
    rate_limit_usage: dict = field(default_factory=dict)

    def _update_rate_limits(self, headers: httpx.Headers) -> None:
        """Parse Strava rate limit headers: 'usage, limit'."""
        if "X-Ratelimit-Usage" in headers:
            usage = headers["X-Ratelimit-Usage"].split(",")
            limit = headers["X-Ratelimit-Limit"].split(",")
            self.rate_limit_usage = {
                "15min": {"used": int(usage[0]), "limit": int(limit[0])},
                "daily": {"used": int(usage[1]), "limit": int(limit[1])},
            }
            # Safety: If we've used 90% of our 15-min quota, take a longer break
            if self.rate_limit_usage["15min"]["used"] > (
                self.rate_limit_usage["15min"]["limit"] * 0.9
            ):
                print("⚠️ Rate limit 90% reached. Sleeping for 30s...")
                time.sleep(30)

    def get_local_activity_ids(self) -> set[str]:
        """Returns a set of activity IDs sourced from the Parquet database."""
        if not self.raw_activities_path.exists():
            return set()

        # Efficiently load ONLY the ID column
        df_ids = pd.read_csv(self.raw_activities_path, usecols=["id"])

        # Convert to strings and return as a set for O(1) lookups
        return set(df_ids["id"].astype(str).tolist())

    def sync(self) -> None:
        """Orchestrates the synchronization process.

        Fetches new activities, downloads their streams, creates GPX files,
        and updates the local database.
        """
        token = self.token_manager.get_valid_token()
        headers = {"Authorization": f"Bearer {token}"}

        # Create the client ONCE for the whole sync session
        with httpx.Client(http2=True, timeout=10.0) as client:
            new_activities = self.fetch_new_activity_list(client, headers)
            if not new_activities:
                return

            pbar = tqdm(new_activities, desc="🔄 Syncing Strava", unit="act")

            for activity in pbar:
                activity_name = activity.get("name", "Unknown Activity")
                activity_id = activity.get("id")
                pbar.set_postfix_str(f"Run: {activity_name}")

                gpx_path = self.gpx_dir / f"{activity_id}.gpx"

                if gpx_path.exists():
                    continue

                try:
                    streams = self.get_streams(client, activity["id"], token)
                    if streams:
                        gpx_data = self.create_gpx(
                            streams, activity["start_date"], activity_name
                        )
                        if gpx_data:
                            gpx_path.write_text(gpx_data)
                    # Respect the API
                    time.sleep(0.5)
                except httpx.HTTPStatusError as e:
                    print(f"❌ Failed to fetch streams for {activity['id']}: {e}")
                    continue
            print("\nAll activities synced.")

        # Update the Parquet/CSV database
        self.update_local_db(new_activities)

    def fetch_new_activity_list(
        self, client: httpx.Client, headers: dict[str, str]
    ) -> list[dict]:
        """Fetches a list of new activities from Strava.

        Iterates through activity pages until it finds an activity that already
        exists locally or runs out of pages.

        Args:
            client: The HTTP client to use for requests.
            headers: The headers to include in the requests (must contain auth).

        Returns:
            A list of dictionaries representing the new activities found.
        """
        local_ids = self.get_local_activity_ids()
        new_activities = []

        # 1. Fetch activities from Strava (Page by page)
        page = 1
        while True:
            print(f"📡 Fetching Strava activities page {page}...")
            resp = client.get(
                f"{settings.STRAVA_API_BASE}/athlete/activities",
                headers=headers,
                params={"page": page, "per_page": 50},
            )
            activities = resp.json()
            if not activities:
                break  # Stop if no more data

            # 2. Filter for only NEW activities
            found_old_activity = False
            for act in activities:
                act_id = str(act["id"])
                if act_id in local_ids:
                    found_old_activity = True  # We've hit data we already have
                    continue
                new_activities.append(act)

            if found_old_activity:
                break  # Stop loop if we've caught up to existing data
            page += 1

        if not new_activities:
            print("✨ Everything is already up to date!")
            return []

        return new_activities

    def update_local_db(self, new_data: list) -> None:
        """Updates the local CSV database with new activity data.

        Merges the new activities with the existing dataset, removing duplicates
        based on activity ID, and saves the result to disk.

        Args:
            new_data: A list of dictionaries containing the new activity data.
        """
        new_df = pd.DataFrame(new_data)
        if self.raw_activities_path.exists():
            old_df = pd.read_csv(self.raw_activities_path)
            final_df = pd.concat([new_df, old_df]).drop_duplicates(subset=["id"])
        else:
            final_df = new_df

        final_df.to_csv(self.raw_activities_path, index=False)

        print("💾 Local database updated.")

    def get_streams(
        self, client: httpx.Client, activity_id: int, access_token: str
    ) -> dict | None:
        """Fetches detailed stream data (GPS, heart rate, etc.) for a specific activity.

        Args:
            client: The HTTP client to use for requests.
            activity_id: The ID of the activity to fetch.
            access_token: The valid access token for authentication.

        Returns:
            A dictionary containing the stream data, or None if the activity
            is not found (404).
        """
        url = f"{settings.STRAVA_API_BASE}/activities/{activity_id}/streams"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {
            "keys": "time,latlng,altitude,heartrate,velocity_smooth,grade_smooth,moving",
            "key_by_type": "true",
        }

        resp = client.get(url, headers=headers, params=params)
        self._update_rate_limits(resp.headers)

        if resp.status_code == httpx.codes.NOT_FOUND:
            return None

        resp.raise_for_status()
        return resp.json()

    def create_gpx(
        self, streams: dict, start_time_string: str, activity_name: str
    ) -> str | None:
        """Converts Strava stream data into a GPX XML string.

        Args:
            streams: The dictionary of stream data returned by the API.
            start_time_string: The ISO 8601 start time string of the activity.
            activity_name: The name of the activity (used for logging).

        Returns:
            A string containing the GPX XML data, or None if GPS data is missing.
        """
        # Safety check: Manual entries or gym workouts won't have 'latlng'
        if "latlng" not in streams or not streams["latlng"].get("data"):
            print(
                f"⏭️ No GPS data found for this activity. Skipping GPX creation - {activity_name}"
            )
            return None

        gpx_data = gpx.GPX()
        segment = gpx.GPXTrackSegment()

        # Strava dates come in ISO 8601: "2026-02-04T12:00:00Z"
        start_time = datetime.strptime(start_time_string, "%Y-%m-%dT%H:%M:%SZ")

        # Extract lists safely
        latlngs = streams["latlng"]["data"]
        altitudes = streams.get("altitude", {}).get("data", [0] * len(latlngs))
        times = streams.get("time", {}).get("data", range(len(latlngs)))

        for latlng, alt, seconds in zip(latlngs, altitudes, times, strict=True):
            point = gpx.GPXTrackPoint(
                latitude=latlng[0],
                longitude=latlng[1],
                elevation=alt,
                time=start_time + timedelta(seconds=seconds),
            )
            segment.points.append(point)

        track = gpx.GPXTrack()
        track.segments.append(segment)
        gpx_data.tracks.append(track)

        return gpx_data.to_xml()
