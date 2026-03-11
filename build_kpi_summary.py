#!/usr/bin/env python3
"""
Build KPI summary tables from MVP delay layer.
Creates aggregated performance metrics for routes, stops, and time periods.
"""

import duckdb
from dotenv import load_dotenv
import os

load_dotenv()
db_path = os.getenv("DB_PATH", "./data/transit_delay.duckdb")
conn = duckdb.connect(db_path)

print("Building KPI summary tables from staging.delay_preview_best_offset...\n")

# Create marts schema if it doesn't exist
print("0. Creating marts schema...")
try:
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
    print("   ✓ marts schema ready")
except Exception as e:
    print(f"   ✗ Error: {e}")

# 1. ROUTE PERFORMANCE
print("1. Building marts.route_performance...")
query_route = """
DROP TABLE IF EXISTS marts.route_performance;

CREATE TABLE marts.route_performance AS
WITH raw_by_route AS (
    SELECT
        t.route_id,
        COUNT(*) AS raw_rt_rows,
        COUNT(DISTINCT r.trip_id) AS raw_rt_trips
    FROM raw.gtfs_rt_trip_updates r
    JOIN raw.gtfs_trips t
      ON CAST(t.trip_id AS VARCHAR) = r.trip_id
    WHERE r.arrival_time_unix IS NOT NULL
    GROUP BY t.route_id
),
matched_by_route AS (
    SELECT
        route_id,
        COUNT(DISTINCT trip_id) AS trip_count,
        COUNT(*) AS stop_event_count,
        ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
        ROUND(MEDIAN(delay_minutes), 2) AS median_delay_minutes,
        ROUND(QUANTILE_CONT(delay_minutes, 0.95), 2) AS p95_delay_minutes,
        ROUND(100.0 * COUNT(CASE WHEN delay_minutes <= 5 THEN 1 END) / COUNT(*), 1) AS on_time_rate,
        COUNT(*) AS coverage_rows,
        COUNT(DISTINCT trip_id) AS coverage_trips
    FROM staging.delay_preview_best_offset
    GROUP BY route_id
)
SELECT
    m.route_id,
    m.trip_count,
    m.stop_event_count,
    m.avg_delay_minutes,
    m.median_delay_minutes,
    m.p95_delay_minutes,
    m.on_time_rate,
    m.coverage_rows,
    m.coverage_trips,
    ROUND(100.0 * m.coverage_rows / NULLIF(r.raw_rt_rows, 0), 1) AS pct_rt_rows_matched
FROM matched_by_route m
LEFT JOIN raw_by_route r
  ON r.route_id = m.route_id
ORDER BY avg_delay_minutes DESC, route_id ASC
"""

try:
    for query in query_route.split(";"):
        if query.strip():
            conn.execute(query)
    result = conn.execute("SELECT COUNT(*) as count FROM marts.route_performance").fetchall()
    print(f"   ✓ Created marts.route_performance ({result[0][0]} routes)")
except Exception as e:
    print(f"   ✗ Error: {e}")

# 2. STOP PERFORMANCE
print("2. Building marts.stop_performance...")
query_stop = """
DROP TABLE IF EXISTS marts.stop_performance;

CREATE TABLE marts.stop_performance AS
WITH route_cov AS (
    SELECT route_id, coverage_rows, coverage_trips, pct_rt_rows_matched
    FROM marts.route_performance
)
SELECT
    d.static_stop_id,
    d.stop_name,
    d.route_id,
    COUNT(*) as stop_event_count,
    ROUND(AVG(d.delay_minutes), 2) as avg_delay_minutes,
    ROUND(MEDIAN(d.delay_minutes), 2) as median_delay_minutes,
    ROUND(100.0 * COUNT(CASE WHEN d.delay_minutes <= 5 THEN 1 END) / COUNT(*), 1) as on_time_rate,
    COALESCE(rc.coverage_rows, 0) AS coverage_rows,
    COALESCE(rc.coverage_trips, 0) AS coverage_trips,
    COALESCE(rc.pct_rt_rows_matched, 0.0) AS pct_rt_rows_matched
FROM staging.delay_preview_best_offset d
LEFT JOIN route_cov rc
  ON rc.route_id = d.route_id
GROUP BY d.static_stop_id, d.stop_name, d.route_id, rc.coverage_rows, rc.coverage_trips, rc.pct_rt_rows_matched
ORDER BY avg_delay_minutes DESC, stop_name ASC
"""

try:
    for query in query_stop.split(";"):
        if query.strip():
            conn.execute(query)
    result = conn.execute("SELECT COUNT(*) as count FROM marts.stop_performance").fetchall()
    print(f"   ✓ Created marts.stop_performance ({result[0][0]} route-stop combinations)")
except Exception as e:
    print(f"   ✗ Error: {e}")

# 3. ROUTE HOUR PERFORMANCE
print("3. Building marts.route_hour_performance...")
query_hour = """
DROP TABLE IF EXISTS marts.route_hour_performance;

CREATE TABLE marts.route_hour_performance AS
WITH raw_by_route_hour AS (
    SELECT
        t.route_id,
        EXTRACT(HOUR FROM (to_timestamp(r.arrival_time_unix) AT TIME ZONE 'America/Los_Angeles')) AS hour_of_day,
        COUNT(*) AS raw_rt_rows,
        COUNT(DISTINCT r.trip_id) AS raw_rt_trips
    FROM raw.gtfs_rt_trip_updates r
    JOIN raw.gtfs_trips t
      ON CAST(t.trip_id AS VARCHAR) = r.trip_id
    WHERE r.arrival_time_unix IS NOT NULL
    GROUP BY t.route_id, EXTRACT(HOUR FROM (to_timestamp(r.arrival_time_unix) AT TIME ZONE 'America/Los_Angeles'))
),
matched_by_route_hour AS (
    SELECT
        route_id,
        EXTRACT(HOUR FROM actual_arrival_local_ts) as hour_of_day,
        COUNT(*) as stop_event_count,
        COUNT(DISTINCT trip_id) as trip_count,
        ROUND(AVG(delay_minutes), 2) as avg_delay_minutes,
        ROUND(MEDIAN(delay_minutes), 2) as median_delay_minutes,
        ROUND(100.0 * COUNT(CASE WHEN delay_minutes <= 5 THEN 1 END) / COUNT(*), 1) as on_time_rate,
        COUNT(*) AS coverage_rows,
        COUNT(DISTINCT trip_id) AS coverage_trips
    FROM staging.delay_preview_best_offset
    GROUP BY route_id, EXTRACT(HOUR FROM actual_arrival_local_ts)
)
SELECT
    m.route_id,
    m.hour_of_day,
    m.stop_event_count,
    m.trip_count,
    m.avg_delay_minutes,
    m.median_delay_minutes,
    m.on_time_rate,
    m.coverage_rows,
    m.coverage_trips,
    ROUND(100.0 * m.coverage_rows / NULLIF(r.raw_rt_rows, 0), 1) AS pct_rt_rows_matched
FROM matched_by_route_hour m
LEFT JOIN raw_by_route_hour r
  ON r.route_id = m.route_id
 AND r.hour_of_day = m.hour_of_day
ORDER BY route_id ASC, hour_of_day ASC
"""

try:
    for query in query_hour.split(";"):
        if query.strip():
            conn.execute(query)
    result = conn.execute("SELECT COUNT(*) as count FROM marts.route_hour_performance").fetchall()
    print(f"   ✓ Created marts.route_hour_performance ({result[0][0]} route-hour combinations)")
except Exception as e:
    print(f"   ✗ Error: {e}")

# 4. UNMATCHED RT TRIPS TABLE
print("4. Building staging.unmatched_rt_trip_ids...")
try:
    conn.execute("DROP TABLE IF EXISTS staging.unmatched_rt_trip_ids")
    conn.execute(
        """
        CREATE TABLE staging.unmatched_rt_trip_ids AS
        SELECT
            r.trip_id,
            COUNT(*) AS raw_row_count,
            MIN(r.poll_timestamp) AS first_poll_ts,
            MAX(r.poll_timestamp) AS last_poll_ts
        FROM raw.gtfs_rt_trip_updates r
        WHERE r.trip_id IS NOT NULL
          AND r.trip_id NOT IN (
              SELECT DISTINCT trip_id FROM staging.delay_preview_best_offset
          )
        GROUP BY r.trip_id
        ORDER BY raw_row_count DESC
        """
    )
    result = conn.execute("SELECT COUNT(*) FROM staging.unmatched_rt_trip_ids").fetchone()[0]
    print(f"   ✓ Created staging.unmatched_rt_trip_ids ({result} excluded trip IDs)")
except Exception as e:
    print(f"   ✗ Error: {e}")

print("\nAll KPI tables built successfully.")
conn.close()
