"""This module processes the raw Strava activity data into a cleaned parquet file."""

import ast
from pathlib import Path

import numpy as np
import pandas as pd
from core.config import DISTANCE_TARGETS, settings
from core.parquet_schema import STRAVA_COLUMN_TYPES


def calculate_target_relatives(
    df: pd.DataFrame, distance_targets: dict
) -> pd.DataFrame:
    """Calculates the relative target performance for both monthly and yearly targets for a set of dates.

    :param df: dataframe containing at least a 'date' and 'distance_km' column.
    :type df: pd.DataFrame
    :param distance_targets: dict with monthly and yearly targets.
    :type distance_targets: dict
    :return: dataframe with additional columns showing relative targeet performances for month and year.
    :rtype: pd.DataFrame
    """
    # 1. Setup Data & Targets
    # Convert string keys to Period keys ('M' for months, 'A' for years)
    month_targets = {
        pd.Period(k, freq="M"): v
        for k, v in distance_targets["month"].items()
        if k != "default"
    }
    year_targets = {
        pd.Period(k, freq="Y"): v
        for k, v in distance_targets["year"].items()
        if k != "default"
    }

    month_default = distance_targets["month"].get("default")
    year_default = distance_targets["year"].get("default")

    # 2. Pre-process Dates (Sort is vital for cumsum!)
    df = df.sort_values("date")
    df["distance_km"] = df["distance"] / 1000

    # 3. Monthly Calculations
    m_periods = df["date"].dt.to_period("M")  # type: ignore
    df["pct_of_month"] = df["date"].dt.day / df["date"].dt.days_in_month  # type: ignore
    df["month_target"] = m_periods.map(month_targets).fillna(month_default)
    df["monthly_cumulative"] = df.groupby(m_periods)["distance_km"].cumsum()

    # 4. Yearly Calculations
    y_periods = df["date"].dt.to_period("Y")  # 'A' is for Annual #type: ignore
    # Day of year (1-366) / Total days in that year
    day_of_year = df["date"].dt.dayofyear  # type: ignore
    days_in_year = np.where(df["date"].dt.is_leap_year, 366, 365)  # type: ignore
    df["pct_of_year"] = day_of_year / days_in_year

    df["year_target"] = y_periods.map(year_targets).fillna(year_default)
    df["yearly_cumulative"] = df.groupby(y_periods)["distance_km"].cumsum()

    # 5. Final Relative Calculations
    # Monthly Relative
    df["month_expected"] = df["month_target"] * df["pct_of_month"]
    df["month_ahead_behind"] = df["monthly_cumulative"] - df["month_expected"]

    # Yearly Relative
    df["year_expected"] = df["year_target"] * df["pct_of_year"]
    df["year_ahead_behind"] = df["yearly_cumulative"] - df["year_expected"]

    return df.sort_values("start_date_time", ascending=False)


def safe_eval_for_coord_list(list_lat_lon: str) -> list[float | None]:
    """Safely evaluates a string representation of a list of coordinates [lat, lon].

    :param list_lat_lon: string representation of a list of lat, log, e.g. "[37.7749, -122.4194]"
    :type list_lat_lon: str
    :return: list of [lat, lon] or [None, None]
    :rtype: list[float | None]
    """
    try:
        return ast.literal_eval(list_lat_lon)
    except (ValueError, SyntaxError):
        return [None, None]


def process_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    df[["start_lat", "start_lon"]] = pd.DataFrame(
        df["start_latlng"].apply(safe_eval_for_coord_list).tolist(), index=df.index
    )
    df[["end_lat", "end_lon"]] = pd.DataFrame(
        df["end_latlng"].apply(safe_eval_for_coord_list).tolist(), index=df.index
    )
    return df


def add_rolling_day_data(
    window_size: int, distance_column: pd.Series
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    """Creates rolling sums and differences for a given window size and distance column.

    :param window_size: size of the rolling window in days (e.g. 7 for weekly, 30 for monthly)
    :type window_size: int
    :param distance_column: the column containing distance data to perform rolling calculations on
    :type distance_column: pd.Series
    :return: a tuple of the rolling sum, rolling mean per day, rolling sum difference, and rolling mean difference
    :rtype: tuple[pd.Series, pd.Series, pd.Series, pd.Series]
    """
    window_distance = distance_column.rolling(window=f"{window_size}D").sum()
    window_distance_per_day = window_distance / window_size
    window_distance_diff = window_distance.diff()
    window_distance_day_diff = window_distance_per_day.diff()
    return (
        window_distance,
        window_distance_per_day,
        window_distance_diff,
        window_distance_day_diff,
    )


def process_data_df(
    raw_activities_path: Path = settings.RAW_ACTIVITES_PATH,
    processed_activities_path: Path = settings.PROCESSED_ACTIVITIES_PATH,
    parquet_column_types: dict[str, str] = STRAVA_COLUMN_TYPES,
) -> pd.DataFrame:

    raw_df = pd.read_csv(raw_activities_path)

    processed_df = raw_df[raw_df["type"] == "Run"]

    processed_df["start_date_time"] = pd.to_datetime(
        processed_df["start_date"], utc=True, format="mixed"
    ).dt.tz_localize(None)

    processed_df = process_coordinates(processed_df)

    processed_df = processed_df[
        [
            "name",
            "distance",
            "moving_time",
            "total_elevation_gain",
            "device_name",
            "id",
            "gear_id",
            "average_cadence",
            "average_heartrate",
            "max_heartrate",
            "start_date_time",
            "start_lat",
            "start_lon",
            "end_lat",
            "end_lon",
        ]
    ]

    # 3. Clean up the schema to match the new columns
    # Ensure we don't try to 'astype' columns we just dropped
    final_schema = {
        k: v for k, v in parquet_column_types.items() if k in processed_df.columns
    }

    # 4. Apply Types
    processed_df = processed_df.astype(final_schema)

    processed_df["start_date_time"] = pd.to_datetime(processed_df["start_date_time"])

    # 2. Extract the Date (this remains a datetime-like object)
    processed_df["date"] = pd.to_datetime(processed_df["start_date_time"].dt.date)

    # 3. Extract the Time
    processed_df["time"] = processed_df["start_date_time"].dt.time

    processed_df = processed_df.sort_values("start_date_time", ascending=True)

    # convert distance to km
    processed_df["distance_km"] = processed_df["distance"] / 1000

    # add pace (min/km)
    processed_df["pace_min_km"] = (processed_df["moving_time"] / 60) / (
        processed_df["distance_km"]
    )

    # calculate rolling sums
    processed_df = processed_df.set_index("start_date_time")
    for window_size in [7, 30, 90]:
        (
            processed_df[f"vol_{window_size}d"],
            processed_df[f"vol_{window_size}d_per_day"],
            processed_df[f"vol_{window_size}d_diff"],
            processed_df[f"vol_{window_size}d_day_diff"],
        ) = add_rolling_day_data(
            window_size=window_size, distance_column=processed_df["distance_km"]
        )

    processed_df = calculate_target_relatives(processed_df, DISTANCE_TARGETS)

    # Flip back to newest first for the table
    processed_df = processed_df.reset_index()
    processed_df = processed_df.sort_values("start_date_time", ascending=False)

    processed_df.to_parquet(processed_activities_path)
    processed_df.to_csv(processed_activities_path.with_suffix(".csv"))

    return processed_df


if __name__ == "__main__":
    _ = process_data_df()
