#!/usr/bin/env python3
"""
Validate KPI summary tables.
Spot-checks route, stop, and hour-level performance metrics.
"""

import duckdb
from dotenv import load_dotenv
import os

load_dotenv()
db_path = os.getenv("DB_PATH", "./data/transit_delay.duckdb")
conn = duckdb.connect(db_path)

print("=" * 70)
print("KPI SUMMARY TABLES VALIDATION")
print("=" * 70)

# 0. GLOBAL COVERAGE SUMMARY
print("\n0. GLOBAL COVERAGE SUMMARY")
print("-" * 70)

coverage = conn.execute(
    """
    WITH raw_rows AS (
        SELECT COUNT(*) AS total_raw_rt_rows
        FROM raw.gtfs_rt_trip_updates
        WHERE arrival_time_unix IS NOT NULL
    ),
    matched_rows AS (
        SELECT COUNT(*) AS matched_rt_rows
        FROM staging.delay_preview_best_offset
    ),
    raw_trips AS (
        SELECT COUNT(DISTINCT trip_id) AS raw_trip_ids
        FROM raw.gtfs_rt_trip_updates
        WHERE trip_id IS NOT NULL
    ),
    matched_trips AS (
        SELECT COUNT(DISTINCT trip_id) AS matched_trip_ids
        FROM staging.delay_preview_best_offset
    )
    SELECT
        r.total_raw_rt_rows,
        m.matched_rt_rows,
        r.total_raw_rt_rows - m.matched_rt_rows AS unmatched_rt_rows,
        ROUND(100.0 * m.matched_rt_rows / NULLIF(r.total_raw_rt_rows, 0), 1) AS pct_rt_rows_matched,
        t.raw_trip_ids,
        mt.matched_trip_ids,
        t.raw_trip_ids - mt.matched_trip_ids AS unmatched_trip_ids,
        ROUND(100.0 * mt.matched_trip_ids / NULLIF(t.raw_trip_ids, 0), 1) AS pct_trip_ids_matched
    FROM raw_rows r
    CROSS JOIN matched_rows m
    CROSS JOIN raw_trips t
    CROSS JOIN matched_trips mt
    """
).fetchone()

print(f"  total_raw_rt_rows:       {coverage[0]}")
print(f"  matched_rt_rows:         {coverage[1]}")
print(f"  unmatched_rt_rows:       {coverage[2]}")
print(f"  pct_rt_rows_matched:     {coverage[3]}%")
print(f"  raw_trip_ids:            {coverage[4]}")
print(f"  matched_trip_ids:        {coverage[5]}")
print(f"  unmatched_trip_ids:      {coverage[6]}")
print(f"  pct_trip_ids_matched:    {coverage[7]}%")
print()
print("  COVERAGE CAVEAT: Route-level coverage (pct_rt_rows_matched) is computed")
print("  only over matched trips assigned to a route. Global coverage is lower")
print("  because unmatched RT trip IDs do not map to route metadata and are")
print("  excluded from route KPIs entirely.")

# 1. OFFSET DISTRIBUTION SUMMARY
print("\n1. OFFSET DISTRIBUTION SUMMARY")
print("-" * 70)

offset_buckets = conn.execute(
    """
    SELECT
        CASE
            WHEN best_offset = 0      THEN '0'
            WHEN best_offset <= 5     THEN '1-5'
            WHEN best_offset <= 10    THEN '6-10'
            ELSE '11+'
        END AS bucket,
        COUNT(*) AS rows,
        COUNT(DISTINCT trip_id) AS trips,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_rows
    FROM staging.delay_preview_best_offset
    GROUP BY 1
    ORDER BY CASE bucket
        WHEN '0' THEN 1 WHEN '1-5' THEN 2 WHEN '6-10' THEN 3 ELSE 4
    END
    """
).fetchall()

print("  best_offset=0 means RT was captured from trip start.")
print("  Higher offsets mean RT represents a tail subset of the static schedule.")
print()
for bucket, rows, trips, pct in offset_buckets:
    print(f"  offset {bucket:>4}: rows={rows:4}  trips={trips:3}  pct_rows={pct:5.1f}%")

# 2. UNMATCHED TRIP AUDIT
print("\n2. UNMATCHED TRIP AUDIT")
print("-" * 70)

unmatched = conn.execute(
    """
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
    LIMIT 10
    """
).fetchall()

if not unmatched:
    print("  ✓ No unmatched RT trip IDs found.")
else:
    print(f"  Showing up to 10 excluded RT trip IDs:")
    for row in unmatched:
        print(f"  trip_id={row[0]:>12}  raw_rows={row[1]:3}  first_poll={row[2]}  last_poll={row[3]}")

# 3. ROUTE PERFORMANCE CHECK
print("\n3. ROUTE PERFORMANCE")
print("-" * 70)

route_stats = conn.execute("""
    SELECT
        COUNT(*) as route_count,
        ROUND(AVG(avg_delay_minutes), 2) as mean_avg_delay,
        ROUND(MAX(avg_delay_minutes), 2) as worst_route_avg_delay,
        ROUND(MIN(avg_delay_minutes), 2) as best_route_avg_delay,
        ROUND(AVG(on_time_rate), 1) as mean_on_time_rate,
        SUM(stop_event_count) as total_stop_events
    FROM marts.route_performance
""").fetchall()[0]

print(f"Routes analyzed: {route_stats[0]}")
print(f"Mean avg delay across routes: {route_stats[1]} min")
print(f"Worst route avg delay: {route_stats[2]} min")
print(f"Best route avg delay: {route_stats[3]} min")
print(f"Mean on-time rate: {route_stats[4]}%")
print(f"Total stop events: {route_stats[5]}")

print("\nTop 5 worst-performing routes:")
worst_routes = conn.execute("""
    SELECT route_id, trip_count, stop_event_count, avg_delay_minutes, on_time_rate
    FROM marts.route_performance
    ORDER BY avg_delay_minutes DESC
    LIMIT 5
""").fetchall()

for i, row in enumerate(worst_routes, 1):
    print(f"  {i}. {row[0]:15} | Trips: {row[1]:3} | Events: {row[2]:4} | Avg delay: {row[3]:5.1f} min | On-time: {row[4]:5.1f}%")

print("\nTop 5 best-performing routes:")
best_routes = conn.execute("""
    SELECT route_id, trip_count, stop_event_count, avg_delay_minutes, on_time_rate
    FROM marts.route_performance
    ORDER BY avg_delay_minutes ASC
    LIMIT 5
""").fetchall()

for i, row in enumerate(best_routes, 1):
    print(f"  {i}. {row[0]:15} | Trips: {row[1]:3} | Events: {row[2]:4} | Avg delay: {row[3]:5.1f} min | On-time: {row[4]:5.1f}%")

# 4. STOP PERFORMANCE CHECK
print("\n4. STOP PERFORMANCE")
print("-" * 70)

stop_stats = conn.execute("""
    SELECT
        COUNT(*) as route_stop_combinations,
        COUNT(DISTINCT static_stop_id) as unique_stops,
        COUNT(DISTINCT route_id) as routes_in_stop_table,
        ROUND(AVG(avg_delay_minutes), 2) as mean_avg_delay,
        ROUND(MAX(avg_delay_minutes), 2) as worst_stop_avg_delay,
        SUM(stop_event_count) as total_stop_events
    FROM marts.stop_performance
""").fetchall()[0]

print(f"Route-stop combinations: {stop_stats[0]}")
print(f"Unique stops: {stop_stats[1]}")
print(f"Routes: {stop_stats[2]}")
print(f"Mean delay across all stop-route pairs: {stop_stats[3]} min")
print(f"Worst stop avg delay: {stop_stats[4]} min")
print(f"Total stop events: {stop_stats[5]}")

print("\nTop 5 worst stops (across all routes):")
worst_stops = conn.execute("""
    SELECT static_stop_id, stop_name, route_id, stop_event_count, avg_delay_minutes, on_time_rate
    FROM marts.stop_performance
    ORDER BY avg_delay_minutes DESC
    LIMIT 5
""").fetchall()

for i, row in enumerate(worst_stops, 1):
    print(f"  {i}. {row[1]:30} | Route: {row[2]:10} | Events: {row[3]:3} | Avg: {row[4]:5.1f} min | On-time: {row[5]:5.1f}%")

# 5. ROUTE HOUR PERFORMANCE CHECK
print("\n5. ROUTE HOUR PERFORMANCE")
print("-" * 70)

hour_stats = conn.execute("""
    SELECT
        COUNT(*) as route_hour_combinations,
        COUNT(DISTINCT route_id) as routes_in_hour_table,
        COUNT(DISTINCT hour_of_day) as hours_covered,
        ROUND(AVG(avg_delay_minutes), 2) as mean_avg_delay,
        ROUND(MAX(avg_delay_minutes), 2) as peak_hour_avg_delay,
        ROUND(MIN(avg_delay_minutes), 2) as best_hour_avg_delay
    FROM marts.route_hour_performance
""").fetchall()[0]

print(f"Route-hour combinations: {hour_stats[0]}")
print(f"Routes: {hour_stats[1]}")
print(f"Hours covered: {hour_stats[2]}")
print(f"Mean delay: {hour_stats[3]} min")
print(f"Peak hour avg delay: {hour_stats[4]} min")
print(f"Best hour avg delay: {hour_stats[5]} min")

print("\nPeak hours (worst avg delays, any route):")
peak_hours = conn.execute("""
    SELECT hour_of_day, route_id, stop_event_count, avg_delay_minutes, on_time_rate
    FROM marts.route_hour_performance
    ORDER BY avg_delay_minutes DESC
    LIMIT 5
""").fetchall()

for i, row in enumerate(peak_hours, 1):
    hour = int(row[0])
    print(f"  {i}. {hour:02d}:00         | Route: {row[1]:10} | Events: {row[2]:3} | Avg: {row[3]:5.1f} min | On-time: {row[4]:5.1f}%")

# 6. CROSS-CHECK TOTALS
print("\n6. CROSS-VALIDATION")
print("-" * 70)

delay_total = conn.execute("SELECT COUNT(*) FROM staging.delay_preview_best_offset").fetchall()[0][0]
route_total = conn.execute("SELECT SUM(stop_event_count) FROM marts.route_performance").fetchall()[0][0]
stop_total = conn.execute("SELECT SUM(stop_event_count) FROM marts.stop_performance").fetchall()[0][0]
hour_total = conn.execute("SELECT SUM(stop_event_count) FROM marts.route_hour_performance").fetchall()[0][0]

print(f"Delay layer total rows: {delay_total}")
print(f"Route performance total events: {route_total} {'✓' if route_total == delay_total else '✗ MISMATCH'}")
print(f"Stop performance total events: {stop_total} {'✓' if stop_total == delay_total else '✗ MISMATCH'}")
print(f"Hour performance total events: {hour_total} {'✓' if hour_total == delay_total else '✗ MISMATCH'}")

# 7. NULL VALUE CHECK
print("\n7. NULL VALUE CHECK")
print("-" * 70)

nulls_route = conn.execute("""
    SELECT
        COUNT(CASE WHEN route_id IS NULL THEN 1 END) as null_route_id,
        COUNT(CASE WHEN avg_delay_minutes IS NULL THEN 1 END) as null_avg_delay,
        COUNT(CASE WHEN on_time_rate IS NULL THEN 1 END) as null_on_time_rate
    FROM marts.route_performance
""").fetchall()[0]

print(f"Route table nulls: {nulls_route[0] + nulls_route[1] + nulls_route[2]} total")
if nulls_route[0] + nulls_route[1] + nulls_route[2] == 0:
    print("  ✓ No nulls detected")
else:
    print(f"  ✗ Found nulls: route_id={nulls_route[0]}, avg_delay={nulls_route[1]}, on_time_rate={nulls_route[2]}")

print("\n" + "=" * 70)
print("Validation complete.")
print("=" * 70)
print()
print("NOTE: All KPI metrics cover matched RT trips only (88.1% of raw trip IDs).")
print("      Global RT row coverage is 98.7%. Excluded trips have no route mapping.")
print("      Do not report route on-time rates as full-network performance.")

conn.close()
