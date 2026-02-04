- Make the project folder and sub projects
```
mkdir NegativeSplit
cd NegativeSplit
uv init
```

- define the workspace, open the newly created pyproject.toml and add this block to the bottom

```
[tool.uv.workspace]
members = ["packages/*", "apps/*"]
```

- create your subprojects

```
uv init --lib packages/strava-api
uv init --lib packages/running-analysis
uv init --app apps/flet-ui
```

- Linking the packages

Add the packages to your dependencies.
Open your apps/flet-ui/pyproject.toml

add to the bottom

```
[tool.uv.sources]
strava-api = { workspace = true, editable = true }
running-analysis = { workspace = true, editable = true }
```

then run uv sync


