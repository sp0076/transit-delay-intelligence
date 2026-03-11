# Architecture

```mermaid
flowchart TD
	A1[BART GTFS Static API] --> B1[ingestion.load_gtfs_static]
	A2[BART GTFS-Realtime API] --> B2[ingestion.ingest_gtfs_rt]
	A3[Open-Meteo API] --> B3[ingestion.ingest_weather]

	B1 --> C1[(raw.gtfs_stops/routes/trips/stop_times)]
	B2 --> C2[(raw.gtfs_rt_trip_updates)]
	B3 --> C3[(raw.weather_hourly)]

	C1 --> D1[build_best_offset_alignment_preview.py]
	C2 --> D1
	D1 --> E1[(staging.best_trip_offsets)]
	D1 --> E2[(staging.rt_static_alignment_best_offset)]

	E2 --> D2[build_delay_preview_best_offset.py]
	D2 --> F1[(staging.delay_preview_best_offset)]

	F1 --> D3[build_kpi_summary.py]
	D3 --> G1[(marts.route_performance)]
	D3 --> G2[(marts.stop_performance)]
	D3 --> G3[(marts.route_hour_performance)]

	F1 --> D4[build_weather_enriched_delay.py]
	C3 --> D4
	D4 --> G4[(marts.weather_impact)]
	D4 --> G5[(marts.route_weather_impact)]

	F1 --> D5[build_impact_and_risk_marts.py]
	G3 --> D5
	C3 --> D5
	H1[(staging.event_windows)] --> D5
	H2[(staging.service_alert_windows)] --> D5
	D5 --> G6[(marts.hour_day_reliability)]
	D5 --> G7[(marts.event_window_impact)]
	D5 --> G8[(marts.disruption_association)]
	D5 --> G9[(marts.risk_forecast_next_24h)]

	G1 --> I1[dashboard.py]
	G2 --> I1
	G3 --> I1
	G4 --> I1
	G7 --> I1
	G8 --> I1
	G9 --> I1
```

## Design notes

- Storage engine: local DuckDB (`data/transit_delay.duckdb`)
- Modeling pattern: raw → staging → marts
- Critical logic: per-trip best-offset alignment to match RT trip tails to static stop sequences
- Consumer: Streamlit dashboard (4 pages)
