# BART Transit Delay Intelligence

An exploratory data pipeline and dashboard for analyzing BART transit delays using GTFS-Realtime data matched to static GTFS schedules, enriched with weather observations.

**MVP status: exploration-ready, not production-grade. Read the coverage and limitations sections before interpreting any numbers.**

---

## Project Summary

This project ingests BART realtime arrival data and aligns it to the published static schedule using a per-trip best-offset sequence alignment method. It computes delay metrics at the route, stop, and hour level, prepares weather-joined enrichment tables, and serves results through a Streamlit exploration dashboard.

The key problem solved: GTFS-RT stop IDs and stop_sequence fields do not directly match static GTFS data for BART. Rather than abandoning the join, the pipeline implements a dynamic offset search that treats RT feeds as partial trip tails and selects the schedule position that minimizes timing error.

---

## Data Sources

| Source | Content | Format |
|--------|---------|--------|
| BART GTFS Static | Routes, stops, trips, stop times | ZIP / CSV |
| BART GTFS-Realtime | Trip updates with arrival timestamps | Protobuf |
| Open-Meteo API | Hourly precipitation, temperature, windspeed | JSON / REST |

All data lands in a local DuckDB database at `data/transit_delay.duckdb`.

---

## Methodology

### Alignment

1. Match RT `trip_id` to static `trip_id` (74 of 84 RT trips matched; 10 unmatched)
2. Assign ordinal sequence positions to RT stop updates and static stop times independently
3. For each matched trip, search candidate sequence offsets from 0 to `(static_stops - rt_stops)`
4. Score each offset by `MEDIAN(ABS(actual_arrival - scheduled_arrival))`
5. Select the offset with the lowest median absolute timing error per trip
6. Join RT to static using `static_seq = rt_seq + best_offset`
7. Compute `delay_minutes = actual_arrival_local - scheduled_arrival_local`

### Offset Distribution

| Offset bucket | % of delay layer rows |
|---------------|----------------------|
| 0 (trip start captured) | 42.0% |
| 1–5 stops | 28.8% |
| 6–10 stops | 17.0% |
| 11+ stops | 12.2% |

High offsets are not noise — they reflect that RT feeds capture later portions of trips.

### Weather Enrichment

Weather joined from `raw.weather_hourly` on the local hour of `actual_arrival_local_ts`. Current data is dry-only (no rain events in the captured window). Wet/dry comparison is not yet possible.

---

## Key Findings

On the matched-trip, best-offset-aligned MVP slice (1,056 aligned stop events across 74 matched trips and 10 routes), most aligned observations fell within a delay range of −16 to +6 minutes, with a median of 1 minute. Yellow-S and Red-N show the highest average delays within matched coverage.

**These numbers should not be read as full-network BART performance.** Coverage caveats listed below apply.

---

## Coverage and Limitations

- **Trip ID match rate: 88.1%** (74 of 84 distinct RT trip IDs matched to static)
- **RT row match rate: 98.7%** (1,056 of 1,070 RT rows with arrival timestamps matched)
- **10 unmatched trip IDs** exist in `staging.unmatched_rt_trip_ids`; their IDs (668–683) do not match any BART mainline trip in the static feed, suggesting shuttle or special service
- Route-level KPI tables show `pct_rt_rows_matched = 100%` because unmatched trips have no route assignment and are excluded before aggregation — not counted as low-coverage
- Alignment is optimism-biased by construction: selecting offset by minimizing timing error produces tighter delay distributions than a naive join
- Current data covers a single dry afternoon (2026-03-10 ~17:00–20:00 local). Weather sensitivity analysis requires multi-day data with precipitation

Full methodology: see `docs/METHODOLOGY_LIMITATIONS.md`

---

## Dashboard

```bash
.venv/bin/streamlit run dashboard.py
```

Three pages:

- **Network / Route Overview** — route average delay, on-time rate, P95, coverage table
- **Stop & Hour Drilldown** — worst stops, hourly trends, filterable by route
- **Weather Impact** — precipitation bucket breakdown (dry-only notice until rain data is available)

### Dashboard Screenshots

- Network / Route Overview: [docs/dashboard_screenshots/network_route_overview.png](docs/dashboard_screenshots/network_route_overview.png)
- Stop & Hour Drilldown: [docs/dashboard_screenshots/stop_hour_drilldown.png](docs/dashboard_screenshots/stop_hour_drilldown.png)
- Weather Impact: [docs/dashboard_screenshots/weather_impact.png](docs/dashboard_screenshots/weather_impact.png)

Inline preview:

![Network / Route Overview](docs/dashboard_screenshots/network_route_overview.png)
![Stop & Hour Drilldown](docs/dashboard_screenshots/stop_hour_drilldown.png)
![Weather Impact](docs/dashboard_screenshots/weather_impact.png)

### Architecture + SQL Samples

- Architecture diagram: [docs/architecture.md](docs/architecture.md)
- SQL samples: [sql/ad_hoc_checks/core_analysis_samples.sql](sql/ad_hoc_checks/core_analysis_samples.sql)

Use real browser screenshots before publishing:
- Page 1 with caveat banner visible
- Page 2 with route filter visible
- Page 3 with dry-only notice visible

---

## Repo Structure

```
├── ingestion/                  # GTFS static, RT, and weather ingestion scripts
├── data/
│   └── transit_delay.duckdb    # Local DuckDB database
├── docs/
│   ├── METHODOLOGY_LIMITATIONS.md
│   ├── MVP_DELAY_LAYER.md
│   ├── architecture.md
│   └── dashboard_screenshots/
├── sql/
│   └── ad_hoc_checks/
│       └── core_analysis_samples.sql
├── dbt/                        # dbt project (not used in MVP pipeline)
├── notebooks/                  # EDA notebooks
├── build_best_offset_alignment_preview.py
├── build_delay_preview_best_offset.py
├── build_kpi_summary.py
├── build_weather_enriched_delay.py
├── check_alignment_coverage.py
├── check_delay_preview_best_offset.py
├── check_kpi_summary.py
├── check_weather_enriched_delay.py
└── dashboard.py
```

---

## How to Run

```bash
# 1. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set environment
cp .env.example .env
# ensure required keys are set (at minimum: DB_PATH, BART_GTFS_RT_URL, API_511_KEY)

# 4. Ingest data
python3 -m ingestion.load_gtfs_static
python3 -m ingestion.ingest_gtfs_rt
python3 -m ingestion.ingest_weather

# 5. Build alignment and delay layer
python3 build_best_offset_alignment_preview.py
python3 build_delay_preview_best_offset.py

# 6. Build KPI and weather tables
python3 build_kpi_summary.py
python3 build_weather_enriched_delay.py

# 7. Validate
python3 check_kpi_summary.py
python3 check_alignment_coverage.py

# 8. Run dashboard
.venv/bin/streamlit run dashboard.py
```

Note: This project was developed on a constrained local environment; if installation fails, check available disk space before retrying package installation.

---

## Required Dashboard Labels

Any output from this project must include:

> Based on matched GTFS-RT trips only (88.1% of raw trip IDs). Alignment uses per-trip best-offset approximation. Exploratory analysis — not audited operational performance reporting.

---

## Known Limitations

- Coverage is incomplete at the trip-ID level (88.1% matched); 10 RT trip IDs are excluded from route KPIs because they do not map to static route metadata.
- Route-level coverage values are conditional on matched trips only; they are not full-network coverage metrics.
- Best-offset alignment is an approximation optimized for timing fit and can bias delay distributions toward tighter ranges.
- Weather analysis is currently dry-only for this data slice; rainy-period conclusions are not available yet.
- Results are exploratory MVP outputs and should not be presented as audited BART operational performance.
