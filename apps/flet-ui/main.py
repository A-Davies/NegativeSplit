import flet as ft
import strava_api
import running_analysis


def main(page: ft.Page):
    # --- 2026 THEME & WINDOW CONFIG ---
    page.title = "NegativeSplit Dashboard"
    page.theme_mode = ft.ThemeMode.DARK
    page.window.width = 450
    page.window.height = 600
    page.window.resizable = True

    # NEW: In v1.0, explicit padding helps with Mac rendering issues
    page.padding = 20
    page.spacing = 20

    # Logic Handshake
    try:
        s_msg = strava_api.get_status()
        a_msg = running_analysis.analyze_sample()
    except Exception as e:
        s_msg = "Link Error"
        a_msg = str(e)

    # --- MODERN DECLARATIVE LAYOUT ---
    # We use a 'Card' inside a 'Column' to ensure it pops against the grey background
    handshake_card = ft.Card(
        content=ft.Container(
            content=ft.Column(
                [
                    ft.ListTile(
                        leading=ft.Icon(
                            ft.Icons.DIRECTIONS_RUN, color=ft.Colors.ORANGE
                        ),
                        title=ft.Text("NegativeSplit v1.0", weight="bold"),
                        subtitle=ft.Text("System Handshake"),
                    ),
                    ft.Divider(),
                    ft.Row(
                        [
                            ft.Icon(
                                ft.Icons.CHECK_CIRCLE_OUTLINE, color=ft.Colors.GREEN
                            ),
                            ft.Text(s_msg),
                        ]
                    ),
                    ft.Row(
                        [
                            ft.Icon(ft.Icons.ANALYTICS_OUTLINED, color=ft.Colors.BLUE),
                            ft.Text(a_msg),
                        ]
                    ),
                ],
                tight=True,
            ),
            padding=20,
        )
    )

    page.add(handshake_card)

    # NEW: v1.0 often needs an explicit 'update' if content is added after init
    # but for most apps, it's automatic. We add it here just to be safe.
    page.update()


if __name__ == "__main__":
    # Ensure you use 'ft.app' with the target
    ft.app(target=main)
