import pandas as pd  # type: ignore
import httpx
import time
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from gpxpy import gpx
from strava_api.token_manager import TokenManager
from core.config import settings


@dataclass
class StravaSync:
    token_manager: TokenManager
    base_dir: Path = settings.DATA_DIR
    gpx_dir: Path = settings.GPX_DIR
    parquet_path: Path = settings.PARQUET_PATH
    # Track usage to keep the UI informed later
    rate_limit_usage: dict = field(default_factory=dict)

    def _update_rate_limits(self, headers):
        """Parse Strava rate limit headers: 'usage, limit'"""
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
        """Returns a set of activity IDs already saved as GPX."""
        return {f.stem for f in self.gpx_dir.glob("*.gpx")}

    def sync(self):
        token = self.token_manager.get_valid_token()
        headers = {"Authorization": f"Bearer {token}"}

        # Create the client ONCE for the whole sync session
        with httpx.Client(http2=True, timeout=10.0) as client:
            new_activities = self.fetch_new_activity_list(
                client, headers
            )  # Assume this returns a list
            if not new_activities:
                return

            print(f"🚀 Syncing {len(new_activities)} new activities...")
            for activity in new_activities:
                # TODO - add a counter and activity name
                try:
                    streams = self.get_streams(client, activity["id"], token)
                    if streams:
                        gpx_data = self.create_gpx(streams, activity["start_date"])
                        if gpx_data:
                            gpx_path = self.gpx_dir / f"{activity['id']}.gpx"
                            gpx_path.write_text(gpx_data)
                    # Respect the API
                    time.sleep(0.5)
                except httpx.HTTPStatusError as e:
                    print(f"❌ Failed to fetch streams for {activity['id']}: {e}")
                    continue

        # Update the Parquet/CSV database
        self.update_local_db(new_activities)

    def fetch_new_activity_list(self, client: httpx.Client, headers: dict):

        local_ids = self.get_local_activity_ids()
        new_activities = []

        # 1. Fetch activities from Strava (Page by page)
        page = 1
        while True:
            print(f"📡 Fetching Strava activities page {page}...")
            resp = client.get(
                "https://www.strava.com/api/v3/athlete/activities",
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
            return
        else:
            return new_activities

    def update_local_db(self, new_data):
        new_df = pd.DataFrame(new_data)
        if self.parquet_path.exists():
            old_df = pd.read_parquet(self.parquet_path)
            final_df = pd.concat([new_df, old_df]).drop_duplicates(subset=["id"])
        else:
            final_df = new_df

        final_df.to_parquet(self.parquet_path, engine="pyarrow", index=False)
        final_df.to_csv(self.parquet_path.with_suffix(".csv"), index=False)
        print("💾 Local database updated.")

    def get_streams(self, client: httpx.Client, activity_id: int, access_token: str):
        url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {
            "keys": "time,latlng,altitude,heartrate,velocity_smooth,grade_smooth,moving",
            "key_by_type": "true",
        }

        resp = client.get(url, headers=headers, params=params)
        self._update_rate_limits(resp.headers)

        if resp.status_code == 404:
            return None

        resp.raise_for_status()
        return resp.json()

    def create_gpx(self, streams: dict, start_time_string: str):
        # Safety check: Manual entries or gym workouts won't have 'latlng'
        if "latlng" not in streams or not streams["latlng"].get("data"):
            print("⏭️ No GPS data found for this activity. Skipping GPX creation.")
            return False

        gpx_data = gpx.GPX()
        segment = gpx.GPXTrackSegment()

        # Strava dates come in ISO 8601: "2026-02-04T12:00:00Z"
        start_time = datetime.strptime(start_time_string, "%Y-%m-%dT%H:%M:%SZ")

        # Extract lists safely
        latlngs = streams["latlng"]["data"]
        altitudes = streams.get("altitude", {}).get("data", [0] * len(latlngs))
        times = streams.get("time", {}).get("data", range(len(latlngs)))

        for latlng, alt, seconds in zip(latlngs, altitudes, times):
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
