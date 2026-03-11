#!/usr/bin/env python3
"""
Check weather-enriched delay layer and weather impact summary tables.
"""

import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
db_path = os.getenv("DB_PATH", "./data/transit_delay.duckdb")
conn = duckdb.connect(db_path)

print("WEATHER-ENRICHED DELAY CHECK")
print("=" * 72)

core = conn.execute(
    """
    SELECT
        COUNT(*) AS total_rows,
        COUNT(CASE WHEN precipitation_bucket <> 'unknown' THEN 1 END) AS weather_matched_rows,
        COUNT(CASE WHEN precipitation_bucket = 'unknown' THEN 1 END) AS weather_unknown_rows,
        ROUND(100.0 * COUNT(CASE WHEN precipitation_bucket <> 'unknown' THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS weather_match_rate,
        COUNT(CASE WHEN is_raining THEN 1 END) AS rainy_rows,
        ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
        ROUND(MEDIAN(delay_minutes), 2) AS median_delay_minutes
    FROM staging.delay_weather_enriched
    """
).fetchone()

print("\nJoin quality")
print(f"  total_rows:            {core[0]}")
print(f"  weather_matched_rows:  {core[1]}")
print(f"  weather_unknown_rows:  {core[2]}")
print(f"  weather_match_rate:    {core[3]}%")
print(f"  rainy_rows:            {core[4]}")
print(f"  avg_delay_minutes:     {core[5]}")
print(f"  median_delay_minutes:  {core[6]}")

print("\nPrecipitation bucket impact")
bucket_rows = conn.execute(
    """
    SELECT
        precipitation_bucket,
        stop_event_count,
        trip_count,
        avg_delay_minutes,
        median_delay_minutes,
        p95_delay_minutes,
        on_time_rate
    FROM marts.weather_impact
    ORDER BY precipitation_bucket
    """
).fetchall()
for row in bucket_rows:
    print(
        f"  {row[0]:14} rows={row[1]:4} trips={row[2]:3} avg={row[3]:5.2f} "
        f"median={row[4]:5.2f} p95={row[5]:5.2f} on_time={row[6]:5.1f}%"
    )

dry_rain = conn.execute(
    """
    WITH summary AS (
        SELECT
            CASE WHEN is_raining THEN 'rainy' ELSE 'dry' END AS weather_type,
            COUNT(*) AS rows,
            ROUND(AVG(delay_minutes), 2) AS avg_delay,
            ROUND(100.0 * COUNT(CASE WHEN delay_minutes <= 5 THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS on_time_rate
        FROM staging.delay_weather_enriched
        WHERE precipitation_bucket <> 'unknown'
        GROUP BY 1
    )
    SELECT weather_type, rows, avg_delay, on_time_rate
    FROM summary
    ORDER BY weather_type
    """
).fetchall()

print("\nDry vs rainy")
for row in dry_rain:
    print(f"  {row[0]:5} rows={row[1]:4} avg_delay={row[2]:5.2f} on_time={row[3]:5.1f}%")

route_sensitivity = conn.execute(
    """
    WITH by_type AS (
        SELECT
            route_id,
            CASE WHEN is_raining THEN 'rainy' ELSE 'dry' END AS weather_type,
            COUNT(*) AS rows,
            AVG(delay_minutes) AS avg_delay
        FROM staging.delay_weather_enriched
        WHERE precipitation_bucket <> 'unknown'
        GROUP BY route_id, weather_type
    ),
    pivoted AS (
        SELECT
            route_id,
            MAX(CASE WHEN weather_type = 'dry' THEN rows END) AS dry_rows,
            MAX(CASE WHEN weather_type = 'rainy' THEN rows END) AS rainy_rows,
            MAX(CASE WHEN weather_type = 'dry' THEN avg_delay END) AS dry_avg_delay,
            MAX(CASE WHEN weather_type = 'rainy' THEN avg_delay END) AS rainy_avg_delay
        FROM by_type
        GROUP BY route_id
    )
    SELECT
        route_id,
        COALESCE(dry_rows, 0) AS dry_rows,
        COALESCE(rainy_rows, 0) AS rainy_rows,
        ROUND(dry_avg_delay, 2) AS dry_avg_delay,
        ROUND(rainy_avg_delay, 2) AS rainy_avg_delay,
        ROUND(rainy_avg_delay - dry_avg_delay, 2) AS rainy_minus_dry_delay
    FROM pivoted
    WHERE dry_rows > 0 AND rainy_rows > 0
    ORDER BY rainy_minus_dry_delay DESC
    LIMIT 10
    """
).fetchall()

print("\nTop route weather sensitivity (rainy - dry delay)")
if not route_sensitivity:
    print("  Not enough mixed weather rows per route to compute sensitivity.")
else:
    for row in route_sensitivity:
        print(
            f"  {row[0]:10} dry_rows={row[1]:4} rainy_rows={row[2]:4} "
            f"dry_avg={row[3]:5.2f} rainy_avg={row[4]:5.2f} delta={row[5]:+5.2f}"
        )

print("\n" + "=" * 72)
print("Weather check complete.")

conn.close()
