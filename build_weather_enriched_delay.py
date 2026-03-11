#!/usr/bin/env python3
"""
Build weather-enriched delay layer and weather impact marts tables.
"""

import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
db_path = os.getenv("DB_PATH", "./data/transit_delay.duckdb")
conn = duckdb.connect(db_path)

print("Building weather-enriched delay tables...")

conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
conn.execute("CREATE SCHEMA IF NOT EXISTS marts")

print("1) Building staging.delay_weather_enriched...")
conn.execute("DROP TABLE IF EXISTS staging.delay_weather_enriched")
conn.execute(
    """
    CREATE TABLE staging.delay_weather_enriched AS
    WITH delay_rows AS (
        SELECT
            d.*,
            date_trunc('hour', d.actual_arrival_local_ts) AS actual_arrival_local_hour
        FROM staging.delay_preview_best_offset d
    ),
    weather_rows AS (
        SELECT
            date_trunc(
                'hour',
                COALESCE(
                    try_strptime(observation_datetime, '%Y-%m-%d %H:%M:%S'),
                    try_strptime(observation_datetime, '%Y-%m-%dT%H:%M:%S'),
                    try_strptime(observation_datetime, '%Y-%m-%d %H:%M'),
                    try_strptime(observation_datetime, '%Y-%m-%dT%H:%M')
                )
            ) AS weather_local_hour,
            precipitation_mm,
            temperature_c,
            windspeed_kmh
        FROM raw.weather_hourly
    )
    SELECT
        d.*,
        d.actual_arrival_local_hour,
        w.precipitation_mm,
        CASE WHEN COALESCE(w.precipitation_mm, 0) > 0 THEN TRUE ELSE FALSE END AS is_raining,
        CASE
            WHEN w.precipitation_mm IS NULL THEN 'unknown'
            WHEN w.precipitation_mm = 0 THEN 'dry'
            WHEN w.precipitation_mm > 0 AND w.precipitation_mm < 1 THEN 'light_rain'
            WHEN w.precipitation_mm >= 1 AND w.precipitation_mm < 5 THEN 'moderate_rain'
            ELSE 'heavy_rain'
        END AS precipitation_bucket,
        w.temperature_c,
        w.windspeed_kmh
    FROM delay_rows d
    LEFT JOIN weather_rows w
      ON d.actual_arrival_local_hour = w.weather_local_hour
    """
)

print("2) Building marts.weather_impact...")
conn.execute("DROP TABLE IF EXISTS marts.weather_impact")
conn.execute(
    """
    CREATE TABLE marts.weather_impact AS
    SELECT
        precipitation_bucket,
        is_raining,
        COUNT(*) AS stop_event_count,
        COUNT(DISTINCT trip_id) AS trip_count,
        ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
        ROUND(MEDIAN(delay_minutes), 2) AS median_delay_minutes,
        ROUND(QUANTILE_CONT(delay_minutes, 0.95), 2) AS p95_delay_minutes,
        ROUND(100.0 * COUNT(CASE WHEN delay_minutes <= 5 THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS on_time_rate
    FROM staging.delay_weather_enriched
    GROUP BY precipitation_bucket, is_raining
    ORDER BY precipitation_bucket
    """
)

print("3) Building marts.route_weather_impact...")
conn.execute("DROP TABLE IF EXISTS marts.route_weather_impact")
conn.execute(
    """
    CREATE TABLE marts.route_weather_impact AS
    SELECT
        route_id,
        precipitation_bucket,
        is_raining,
        COUNT(*) AS stop_event_count,
        COUNT(DISTINCT trip_id) AS trip_count,
        ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
        ROUND(MEDIAN(delay_minutes), 2) AS median_delay_minutes,
        ROUND(100.0 * COUNT(CASE WHEN delay_minutes <= 5 THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS on_time_rate,
        ROUND(AVG(temperature_c), 2) AS avg_temperature_c,
        ROUND(AVG(windspeed_kmh), 2) AS avg_windspeed_kmh
    FROM staging.delay_weather_enriched
    GROUP BY route_id, precipitation_bucket, is_raining
    ORDER BY route_id, precipitation_bucket
    """
)

delay_rows = conn.execute("SELECT COUNT(*) FROM staging.delay_preview_best_offset").fetchone()[0]
enriched_rows = conn.execute("SELECT COUNT(*) FROM staging.delay_weather_enriched").fetchone()[0]
weather_matched = conn.execute(
    "SELECT COUNT(*) FROM staging.delay_weather_enriched WHERE precipitation_bucket <> 'unknown'"
).fetchone()[0]

print("\nBuild complete:")
print(f"  staging.delay_preview_best_offset rows:   {delay_rows}")
print(f"  staging.delay_weather_enriched rows:      {enriched_rows}")
print(f"  rows with weather match:                  {weather_matched}")
print(f"  rows with unknown weather:                {enriched_rows - weather_matched}")

conn.close()
