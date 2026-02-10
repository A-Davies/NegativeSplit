import flet as ft
import pandas as pd
from core.config import settings

# TODO - something is going wrong with the athelte column in the acitvities files


# --- Formatting Helpers ---
def format_duration(seconds):
    td = pd.to_timedelta(seconds, unit="s")
    hrs, rem = divmod(int(td.total_seconds()), 3600)
    mins, secs = divmod(rem, 60)
    return f"{hrs}:{mins:02d}:{secs:02d}" if hrs > 0 else f"{mins:02d}:{secs:02d}"


def format_pace(pace_float):
    if pd.isna(pace_float) or pace_float <= 0:
        return "--:--"
    return f"{int(pace_float)}:{int((pace_float % 1) * 60):02d}"


# --- Data Loading & Vectorized Calculation ---
def load_and_process_data():
    try:
        df = pd.read_parquet(settings.PROCESSED_ACTIVITIES_PATH)
        df = df[df["type"] == "Run"].copy()
        df = df.reset_index()
        df["date"] = pd.to_datetime(df["date"])
        # Ensure it's sorted by date for rolling calculations
        df = df.sort_values("date", ascending=True)

        # 2. Vectorized Rolling Sums using index-based windows
        # We set the index to 'date' so .rolling('7D') knows how to measure time
        df = df.set_index("date")

        dist_km = df["distance"] / 1000
        df["vol_7d"] = dist_km.rolling(window="7D").sum()
        df["vol_30d"] = dist_km.rolling(window="30D").sum()
        df["vol_90d"] = dist_km.rolling(window="90D").sum()

        # 3. Move 'date' back to a column and sort for the UI (newest first)
        df = df.reset_index()

        # Flip back to newest first for the table
        df = df.sort_values("date", ascending=False)
        df["pace_min_km"] = (df["moving_time"] / 60) / (df["distance"] / 1000)
        return df
    except Exception as e:
        print(f"Data Error: {e}")
        return pd.DataFrame()


def handle_sync():
    pass


def main(page: ft.Page):
    page.title = "NegativeSplit Dashboard"
    page.theme_mode = ft.ThemeMode.DARK
    page.window_full_screen = True
    page.padding = 20

    full_df = pd.read_parquet(settings.PROCESSED_ACTIVITIES_PATH)

    # --- UI Components ---
    details_content = ft.Column(visible=False, spacing=15)
    details_panel = ft.Container(
        content=details_content,
        width=450,
        bgcolor=ft.Colors.BLUE_GREY_900,
        border_radius=15,
        padding=25,
    )

    table = ft.DataTable(
        expand=True,
        show_checkbox_column=False,
        columns=[
            ft.DataColumn(label=ft.Text("Date")),
            ft.DataColumn(label=ft.Text("Title")),
            ft.DataColumn(label=ft.Text("Time")),
            ft.DataColumn(label=ft.Text("Distance (km)")),
            ft.DataColumn(label=ft.Text("Pace (min/km)")),
        ],
    )

    # --- Sync Click Handler ---
    def on_row_click(row_data):
        details_content.visible = True

        trend_sizes = [14, 10]
        trend_7_day = [
            [
                ft.Text(
                    f"{'+' if col > 0 else ""}{col:.2f} km",
                    size=t_size,
                    color=(ft.Colors.RED if col < 0 else ft.Colors.GREEN),
                ),
            ]
            for col, t_size in zip(
                [row_data["vol_7d_diff"], row_data["vol_7d_day_diff"]],
                trend_sizes,
                strict=True,
            )
        ]

        trend_30_day = [
            [
                ft.Text(
                    f"{'+' if col > 0 else ""}{col:.2f} km",
                    size=t_size,
                    color=(ft.Colors.RED if col < 0 else ft.Colors.GREEN),
                ),
            ]
            for col, t_size in zip(
                [row_data["vol_30d_diff"], row_data["vol_30d_day_diff"]],
                trend_sizes,
                strict=True,
            )
        ]

        trend_90_day = [
            [
                ft.Text(
                    f"{'+' if col > 0 else ""}{col:.2f} km",
                    size=t_size,
                    color=(ft.Colors.RED if col < 0 else ft.Colors.GREEN),
                ),
            ]
            for col, t_size in zip(
                [row_data["vol_90d_diff"], row_data["vol_90d_day_diff"]],
                trend_sizes,
                strict=True,
            )
        ]

        target_relative = {
            k: ft.Text(
                f"{'+' if row_data[f"{k}_ahead_behind"] > 0 else ""}{row_data[f"{k}_ahead_behind"]:.2f} km",
                size=14,
                color=(
                    ft.Colors.RED
                    if row_data[f"{k}_ahead_behind"] < 0
                    else ft.Colors.GREEN
                ),
            )
            for k in ["month", "year"]
        }

        details_content.controls = [
            ft.Text(
                row_data["name"], size=24, weight="bold", color=ft.Colors.ORANGE_400
            ),
            ft.Text(
                row_data["date"].strftime("%A, %d %B %Y"),
                color=ft.Colors.GREY_400,
            ),
            ft.Divider(height=10, color=ft.Colors.WHITE10),
            ft.Row(
                [
                    ft.Column(
                        [
                            ft.Text("Distance (km)", size=10),
                            ft.Text(
                                f"{row_data['distance']/1000:.2f}",
                                size=16,
                                weight="bold",
                            ),
                            ft.Divider(height=10, color=ft.Colors.WHITE10),
                            ft.Text("Mean Heartrate (bpm)", size=10),
                            ft.Text(
                                f"{int(row_data['average_heartrate'])}",
                                size=16,
                                weight="bold",
                            ),
                        ]
                    ),
                    ft.Column(
                        [
                            ft.Text("Pace (min/km)", size=10),
                            ft.Text(
                                format_pace(row_data["pace_min_km"]),
                                size=16,
                                weight="bold",
                            ),
                            ft.Divider(height=10, color=ft.Colors.WHITE10),
                            ft.Text("Total Elevation (m)", size=10),
                            ft.Text(
                                f"{int(row_data['total_elevation_gain'])}",
                                size=16,
                                weight="bold",
                            ),
                        ]
                    ),
                    ft.Column(
                        [
                            ft.Text("Time", size=10),
                            ft.Text(
                                format_duration(row_data["moving_time"]),
                                size=16,
                                weight="bold",
                            ),
                            ft.Divider(height=10, color=ft.Colors.WHITE10),
                            ft.Text("Time", size=10),
                            ft.Text(
                                format_duration(row_data["moving_time"]),
                                size=16,
                                weight="bold",
                            ),
                        ]
                    ),
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            ),
            ft.Divider(height=10, color=ft.Colors.WHITE10),
            ft.Text(
                "Training Volume",
                size=14,
                color=ft.Colors.BLUE_200,
                weight="bold",
            ),
            ft.Row(
                [
                    ft.Column(
                        [
                            ft.Text("Last 7 Days", size=10),
                            ft.Text(
                                f"{row_data["vol_7d"]:.2f} km",
                                size=14,
                                weight="bold",
                                color=ft.Colors.CYAN_200,
                            ),
                            ft.Row(
                                controls=trend_7_day[0],
                                vertical_alignment=ft.CrossAxisAlignment.CENTER,  # Keeps them level
                            ),
                            ft.Text(
                                f"{row_data["vol_7d_per_day"]:.2f} km / day",
                                size=10,
                                weight="bold",
                                color=ft.Colors.CYAN_200,
                            ),
                            ft.Row(
                                controls=trend_7_day[1],
                                vertical_alignment=ft.CrossAxisAlignment.CENTER,  # Keeps them level
                            ),
                        ]
                    ),
                    ft.Column(
                        [
                            ft.Text("Last 30 Days", size=10),
                            ft.Text(
                                f"{row_data["vol_30d"]:.2f} km",
                                size=14,
                                weight="bold",
                                color=ft.Colors.CYAN_200,
                            ),
                            ft.Row(
                                controls=trend_30_day[0],
                                vertical_alignment=ft.CrossAxisAlignment.CENTER,  # Keeps them level
                            ),
                            ft.Text(
                                f"{row_data["vol_30d_per_day"]:.2f} km / day",
                                size=10,
                                weight="bold",
                                color=ft.Colors.CYAN_200,
                            ),
                            ft.Row(
                                controls=trend_30_day[1],
                                vertical_alignment=ft.CrossAxisAlignment.CENTER,  # Keeps them level
                            ),
                        ]
                    ),
                    ft.Column(
                        [
                            ft.Text("Last 90 Days", size=10),
                            ft.Text(
                                f"{row_data["vol_90d"]:.2f} km",
                                size=14,
                                weight="bold",
                                color=ft.Colors.CYAN_200,
                            ),
                            ft.Row(
                                controls=trend_90_day[0],
                                vertical_alignment=ft.CrossAxisAlignment.CENTER,  # Keeps them level
                            ),
                            ft.Text(
                                f"{row_data["vol_90d_per_day"]:.2f} km / day",
                                size=10,
                                weight="bold",
                                color=ft.Colors.CYAN_200,
                            ),
                            ft.Row(
                                controls=trend_90_day[1],
                                vertical_alignment=ft.CrossAxisAlignment.CENTER,  # Keeps them level
                            ),
                        ]
                    ),
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            ),
            ft.Divider(height=10, color=ft.Colors.WHITE10),
            ft.Text(
                "Training Targets",
                size=14,
                color=ft.Colors.BLUE_200,
                weight="bold",
            ),
            ft.Row(
                controls=[
                    ft.Column(
                        [ft.Text("Month Target ()", size=10), target_relative["month"]],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    ft.Column(
                        [ft.Text("Year Target ()", size=10), target_relative["year"]],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            ),
            ft.Divider(height=10, color=ft.Colors.WHITE10),
        ]
        page.update()

    # Build Table Rows
    for _, row in full_df.iterrows():
        table.rows.append(
            ft.DataRow(
                cells=[
                    ft.DataCell(ft.Text(row["date"].strftime("%d/%m/%Y"))),
                    ft.DataCell(ft.Text(row["name"])),
                    ft.DataCell(ft.Text(format_duration(row["moving_time"]))),
                    ft.DataCell(ft.Text(f"{row['distance']/1000:.2f}")),
                    ft.DataCell(ft.Text(format_pace(row["pace_min_km"]))),
                ],
                on_select_change=lambda e, r=row: on_row_click(r),
            )
        )

    page.add(
        ft.Row(
            [
                ft.Column(
                    [
                        ft.Row(
                            [
                                ft.Text("NegativeSplit", size=32, weight="bold"),
                                ft.VerticalDivider(width=20),
                                ft.Button(
                                    "Sync Data",
                                    icon=ft.Icons.SYNC,
                                    on_click=handle_sync,
                                ),
                            ],
                            alignment=ft.MainAxisAlignment.START,
                        ),
                        ft.Container(
                            content=ft.Column([table], scroll=ft.ScrollMode.ALWAYS),
                            expand=True,
                            border=ft.Border.all(1, ft.Colors.WHITE10),
                            border_radius=10,
                        ),
                    ],
                    expand=True,
                ),
                details_panel,
            ],
            expand=True,
        )
    )


if __name__ == "__main__":
    ft.run(main)
