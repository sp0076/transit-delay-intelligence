# BART Transit Delay Intelligence

Transit delay analytics MVP that ingests BART GTFS static + GTFS-Realtime + weather, aligns realtime trip tails to schedule using best-offset matching, and serves KPI/risk dashboards in Streamlit.

## What this repo includes

- Clean runbook and caveats (this README)
- Architecture diagram: [docs/architecture.md](docs/architecture.md)
- Dashboard screenshots: [docs/dashboard_screenshots](docs/dashboard_screenshots)
- SQL samples: [sql/ad_hoc_checks/core_analysis_samples.sql](sql/ad_hoc_checks/core_analysis_samples.sql)
- Business recommendations: [docs/findings_summary.md](docs/findings_summary.md)
- Curated notebooks only (3 active notebooks, no archive clutter)

## Data sources

| Source | What it provides | Format |
|---|---|---|
| BART GTFS Static (511) | routes, stops, trips, stop_times | GTFS ZIP/CSV |
| BART GTFS-Realtime | trip updates with arrival timestamps | Protobuf |
| Open-Meteo | hourly precipitation/temperature/windspeed | JSON REST |

## Pipeline overview

1. Ingest static GTFS into `raw.*`
2. Ingest GTFS-RT TripUpdates into `raw.gtfs_rt_trip_updates`
3. Ingest weather into `raw.weather_hourly`
4. Build best-offset trip alignment (`staging.best_trip_offsets`, `staging.rt_static_alignment_best_offset`)
5. Build matched delay fact layer (`staging.delay_preview_best_offset`)
6. Build KPI marts (`marts.route_performance`, `marts.stop_performance`, `marts.route_hour_performance`)
7. Build weather + events + risk marts (`marts.weather_impact`, `marts.event_window_impact`, `marts.disruption_association`, `marts.risk_forecast_next_24h`)

## Dashboard pages (max 4)

- Executive overview
- Route performance
- Weather and events impact
- Prediction / risk view (next 24h)

Run:

```bash
.venv/bin/streamlit run dashboard.py
```

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# required env vars in .env
# DB_PATH=./data/transit_delay.duckdb
# API_511_KEY=<your-key>
# WEATHER_LAT=37.7749
# WEATHER_LON=-122.4194
# BART_GTFS_RT_URL=http://api.bart.gov/gtfsrt/tripupdate.aspx

python3 -m ingestion.load_gtfs_static
python3 -m ingestion.ingest_gtfs_rt
python3 -m ingestion.ingest_weather

python3 build_best_offset_alignment_preview.py
python3 build_delay_preview_best_offset.py
python3 build_kpi_summary.py
python3 build_weather_enriched_delay.py
python3 build_impact_and_risk_marts.py

python3 check_kpi_summary.py
python3 check_alignment_coverage.py
python3 check_weather_enriched_delay.py
```

## Caveats (must disclose)

- Matched-trip analytics only; unmatched RT trip IDs are excluded from route-level KPI attribution.
- Best-offset alignment is an MVP approximation and can introduce optimism bias.
- Event/disruption impact marts are schema-ready; insight quality depends on loading real event/alert windows.
- Risk view is heuristic (rule-based), not a trained forecast model.

## Notebook hygiene

- Active notebooks: `01_eda.ipynb`, `02_route_stop_analysis.ipynb`, `03_weather_impact.ipynb`
- Keep only decision-relevant notebooks in `notebooks/`
- Move experiments/scratch work out of the repo before publishing
