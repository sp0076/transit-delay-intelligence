#!/usr/bin/env python3
"""
Build additional impact and risk marts for EDA questions:
- hour/day unreliability
- event window impact
- disruption association
- next 24h risk view

This script is resilient when event/alert seed tables are missing by creating empty defaults.
"""

import os
import duckdb
from dotenv import load_dotenv

load_dotenv()
db_path = os.getenv("DB_PATH", "./data/transit_delay.duckdb")
conn = duckdb.connect(db_path)

print("Building impact and risk marts...")
conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
conn.execute("CREATE SCHEMA IF NOT EXISTS marts")

print("1) Ensuring event/disruption source tables exist...")
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS staging.event_windows (
        event_id VARCHAR,
        event_name VARCHAR,
        event_type VARCHAR,
        region VARCHAR,
        window_start_local_ts TIMESTAMP,
        window_end_local_ts TIMESTAMP,
        severity VARCHAR
    )
    """
)
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS staging.service_alert_windows (
        alert_id VARCHAR,
        alert_type VARCHAR,
        region VARCHAR,
        window_start_local_ts TIMESTAMP,
        window_end_local_ts TIMESTAMP,
        severity VARCHAR
    )
    """
)

print("2) Building marts.hour_day_reliability...")
conn.execute("DROP TABLE IF EXISTS marts.hour_day_reliability")
conn.execute(
    """
    CREATE TABLE marts.hour_day_reliability AS
    SELECT
        route_id,
        strftime(actual_arrival_local_ts, '%w')::INTEGER AS day_of_week_num,
        CASE strftime(actual_arrival_local_ts, '%w')::INTEGER
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_of_week,
        EXTRACT(HOUR FROM actual_arrival_local_ts)::INTEGER AS hour_of_day,
        COUNT(*) AS stop_event_count,
        COUNT(DISTINCT trip_id) AS trip_count,
        ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
        ROUND(MEDIAN(delay_minutes), 2) AS median_delay_minutes,
        ROUND(QUANTILE_CONT(delay_minutes, 0.95), 2) AS p95_delay_minutes,
        ROUND(100.0 * SUM(CASE WHEN delay_minutes > 5 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS delay_probability_pct,
        ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 5 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS on_time_rate
    FROM staging.delay_preview_best_offset
    GROUP BY 1, 2, 3, 4
    ORDER BY avg_delay_minutes DESC, stop_event_count DESC
    """
)

print("3) Building marts.event_window_impact...")
conn.execute("DROP TABLE IF EXISTS marts.event_window_impact")
conn.execute(
    """
    CREATE TABLE marts.event_window_impact AS
    WITH joined AS (
        SELECT
            d.*,
            e.event_id,
            e.event_type,
            CASE WHEN e.event_id IS NULL THEN FALSE ELSE TRUE END AS in_event_window
        FROM staging.delay_weather_enriched d
        LEFT JOIN staging.event_windows e
          ON d.actual_arrival_local_ts >= e.window_start_local_ts
         AND d.actual_arrival_local_ts <= e.window_end_local_ts
    )
    SELECT
        COALESCE(event_type, 'none') AS event_type,
        in_event_window,
        COUNT(*) AS stop_event_count,
        COUNT(DISTINCT trip_id) AS trip_count,
        ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
        ROUND(MEDIAN(delay_minutes), 2) AS median_delay_minutes,
        ROUND(100.0 * SUM(CASE WHEN delay_minutes > 5 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS delay_probability_pct,
        ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 5 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS on_time_rate
    FROM joined
    GROUP BY 1, 2
    ORDER BY in_event_window DESC, avg_delay_minutes DESC
    """
)

print("4) Building marts.disruption_association...")
conn.execute("DROP TABLE IF EXISTS marts.disruption_association")
conn.execute(
    """
    CREATE TABLE marts.disruption_association AS
    WITH joined AS (
        SELECT
            d.*,
            a.alert_id,
            a.alert_type,
            CASE WHEN a.alert_id IS NULL THEN FALSE ELSE TRUE END AS in_alert_window
        FROM staging.delay_weather_enriched d
        LEFT JOIN staging.service_alert_windows a
          ON d.actual_arrival_local_ts >= a.window_start_local_ts
         AND d.actual_arrival_local_ts <= a.window_end_local_ts
    )
    SELECT
        COALESCE(alert_type, 'none') AS alert_type,
        in_alert_window,
        COUNT(*) AS stop_event_count,
        COUNT(DISTINCT trip_id) AS trip_count,
        ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
        ROUND(MEDIAN(delay_minutes), 2) AS median_delay_minutes,
        ROUND(100.0 * SUM(CASE WHEN delay_minutes > 5 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS delay_probability_pct,
        ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 5 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS on_time_rate,
        ROUND(
            100.0 * SUM(CASE WHEN in_alert_window THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            1
        ) AS pct_rows_associated_with_alerts
    FROM joined
    GROUP BY 1, 2
    ORDER BY in_alert_window DESC, avg_delay_minutes DESC
    """
)

print("5) Building marts.risk_forecast_next_24h...")
conn.execute("DROP TABLE IF EXISTS marts.risk_forecast_next_24h")
conn.execute(
    """
    CREATE TABLE marts.risk_forecast_next_24h AS
    WITH horizon AS (
        SELECT
            ts,
            EXTRACT(HOUR FROM ts)::INTEGER AS hour_of_day
        FROM generate_series(
            date_trunc('hour', now()),
            date_trunc('hour', now()) + INTERVAL '23 hour',
            INTERVAL 1 HOUR
        ) AS t(ts)
    ),
    route_baseline AS (
        SELECT
            route_id,
            hour_of_day,
            avg_delay_minutes,
            (100.0 - on_time_rate) AS delay_probability_pct,
            stop_event_count
        FROM marts.route_hour_performance
    ),
    weather_future AS (
        SELECT
            date_trunc(
                'hour',
                COALESCE(
                    try_strptime(observation_datetime, '%Y-%m-%d %H:%M:%S'),
                    try_strptime(observation_datetime, '%Y-%m-%dT%H:%M:%S'),
                    try_strptime(observation_datetime, '%Y-%m-%d %H:%M'),
                    try_strptime(observation_datetime, '%Y-%m-%dT%H:%M')
                )
            ) AS weather_hour,
            precipitation_mm,
            windspeed_kmh,
            temperature_c
        FROM raw.weather_hourly
    ),
    scored AS (
        SELECT
            h.ts AS forecast_hour_local_ts,
            b.route_id,
            b.avg_delay_minutes AS baseline_avg_delay_minutes,
            b.delay_probability_pct AS baseline_delay_probability_pct,
            b.stop_event_count AS baseline_support_rows,
            COALESCE(w.precipitation_mm, 0) AS precipitation_mm,
            COALESCE(w.windspeed_kmh, 0) AS windspeed_kmh,
            w.temperature_c,
            (
                COALESCE(b.avg_delay_minutes, 0)
                + CASE
                    WHEN COALESCE(w.precipitation_mm, 0) >= 5 THEN 6
                    WHEN COALESCE(w.precipitation_mm, 0) >= 1 THEN 3
                    WHEN COALESCE(w.precipitation_mm, 0) > 0 THEN 1.5
                    ELSE 0
                  END
                + CASE
                    WHEN COALESCE(w.windspeed_kmh, 0) >= 35 THEN 2
                    WHEN COALESCE(w.windspeed_kmh, 0) >= 20 THEN 1
                    ELSE 0
                  END
            ) AS projected_delay_minutes
        FROM horizon h
        JOIN route_baseline b
                    ON b.hour_of_day = h.hour_of_day
        LEFT JOIN weather_future w
          ON w.weather_hour = h.ts
    )
    SELECT
        forecast_hour_local_ts,
        route_id,
        ROUND(baseline_avg_delay_minutes, 2) AS baseline_avg_delay_minutes,
        ROUND(projected_delay_minutes, 2) AS projected_delay_minutes,
        ROUND(
            LEAST(100, GREATEST(0, baseline_delay_probability_pct + (projected_delay_minutes - baseline_avg_delay_minutes) * 2.5)),
            1
        ) AS projected_delay_probability_pct,
        baseline_support_rows,
        precipitation_mm,
        windspeed_kmh,
        ROUND(temperature_c, 2) AS temperature_c,
        CASE
            WHEN projected_delay_minutes >= 15 THEN 'high'
            WHEN projected_delay_minutes >= 8 THEN 'medium'
            ELSE 'low'
        END AS risk_level
    FROM scored
    ORDER BY projected_delay_minutes DESC, forecast_hour_local_ts ASC
    """
)

summary = conn.execute(
    """
    SELECT
        (SELECT COUNT(*) FROM marts.hour_day_reliability) AS hour_day_rows,
        (SELECT COUNT(*) FROM marts.event_window_impact) AS event_rows,
        (SELECT COUNT(*) FROM marts.disruption_association) AS disruption_rows,
        (SELECT COUNT(*) FROM marts.risk_forecast_next_24h) AS risk_rows
    """
).fetchone()

print("\nBuild complete:")
print(f"  marts.hour_day_reliability:      {summary[0]} rows")
print(f"  marts.event_window_impact:       {summary[1]} rows")
print(f"  marts.disruption_association:    {summary[2]} rows")
print(f"  marts.risk_forecast_next_24h:    {summary[3]} rows")

conn.close()
