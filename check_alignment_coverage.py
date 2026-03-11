#!/usr/bin/env python3
"""
Coverage and alignment QA checks for the MVP delay layer.
Focuses on what was matched vs excluded, and where offset usage is highest.
"""

import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
db_path = os.getenv("DB_PATH", "./data/transit_delay.duckdb")
conn = duckdb.connect(db_path)

print("ALIGNMENT COVERAGE CHECK")
print("=" * 72)

summary = conn.execute(
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

print("\nCore Coverage")
print(f"  total_raw_rt_rows:        {summary[0]}")
print(f"  matched_rt_rows:          {summary[1]}")
print(f"  unmatched_rt_rows:        {summary[2]}")
print(f"  %_rt_rows_matched:        {summary[3]}%")
print(f"  raw_trip_ids:             {summary[4]}")
print(f"  matched_trip_ids:         {summary[5]}")
print(f"  unmatched_trip_ids:       {summary[6]}")
print(f"  %_trip_ids_matched:       {summary[7]}%")

print("\nRows by best_offset bucket")
offset_buckets = conn.execute(
    """
    SELECT
        CASE
            WHEN best_offset = 0 THEN '0'
            WHEN best_offset BETWEEN 1 AND 5 THEN '1-5'
            WHEN best_offset BETWEEN 6 AND 10 THEN '6-10'
            ELSE '11+'
        END AS offset_bucket,
        COUNT(*) AS row_count,
        COUNT(DISTINCT trip_id) AS trip_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_rows
    FROM staging.delay_preview_best_offset
    GROUP BY 1
    ORDER BY CASE offset_bucket
        WHEN '0' THEN 1
        WHEN '1-5' THEN 2
        WHEN '6-10' THEN 3
        ELSE 4
    END
    """
).fetchall()
for bucket, rows, trips, pct in offset_buckets:
    print(f"  {bucket:>4}: rows={rows:4}, trips={trips:3}, pct_rows={pct:5.1f}%")

print("\nRoutes with highest non-zero offset usage")
high_offset_routes = conn.execute(
    """
    SELECT
        route_id,
        COUNT(*) AS matched_rows,
        ROUND(AVG(best_offset), 2) AS avg_best_offset,
        COUNT(CASE WHEN best_offset > 0 THEN 1 END) AS nonzero_offset_rows,
        ROUND(
            100.0 * COUNT(CASE WHEN best_offset > 0 THEN 1 END) / NULLIF(COUNT(*), 0),
            1
        ) AS pct_nonzero_offset_rows
    FROM staging.delay_preview_best_offset
    GROUP BY route_id
    ORDER BY pct_nonzero_offset_rows DESC, avg_best_offset DESC, matched_rows DESC
    LIMIT 10
    """
).fetchall()
for row in high_offset_routes:
    print(
        f"  {row[0]:10}  matched_rows={row[1]:4}  avg_offset={row[2]:5.2f}  "
        f"nonzero_rows={row[3]:4}  pct_nonzero={row[4]:5.1f}%"
    )

print("\nRoutes with lowest coverage")
lowest_coverage = conn.execute(
    """
    WITH raw_by_route AS (
        SELECT
            t.route_id,
            COUNT(*) AS raw_rt_rows,
            COUNT(DISTINCT r.trip_id) AS raw_trip_ids
        FROM raw.gtfs_rt_trip_updates r
        JOIN raw.gtfs_trips t
          ON CAST(t.trip_id AS VARCHAR) = r.trip_id
        WHERE r.arrival_time_unix IS NOT NULL
        GROUP BY t.route_id
    ),
    matched_by_route AS (
        SELECT
            route_id,
            COUNT(*) AS matched_rows,
            COUNT(DISTINCT trip_id) AS matched_trip_ids
        FROM staging.delay_preview_best_offset
        GROUP BY route_id
    )
    SELECT
        rr.route_id,
        rr.raw_rt_rows,
        COALESCE(mr.matched_rows, 0) AS matched_rows,
        rr.raw_rt_rows - COALESCE(mr.matched_rows, 0) AS unmatched_rows,
        ROUND(100.0 * COALESCE(mr.matched_rows, 0) / NULLIF(rr.raw_rt_rows, 0), 1) AS pct_rows_matched,
        rr.raw_trip_ids,
        COALESCE(mr.matched_trip_ids, 0) AS matched_trip_ids,
        ROUND(100.0 * COALESCE(mr.matched_trip_ids, 0) / NULLIF(rr.raw_trip_ids, 0), 1) AS pct_trip_ids_matched
    FROM raw_by_route rr
    LEFT JOIN matched_by_route mr
      ON mr.route_id = rr.route_id
    ORDER BY pct_rows_matched ASC, rr.raw_rt_rows DESC
    LIMIT 10
    """
).fetchall()
for row in lowest_coverage:
    print(
        f"  {row[0]:10} raw_rows={row[1]:4} matched_rows={row[2]:4} unmatched={row[3]:4} "
        f"pct_rows={row[4]:5.1f}% raw_trips={row[5]:3} matched_trips={row[6]:3} pct_trips={row[7]:5.1f}%"
    )

excluded_trip_count = conn.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT r.trip_id
        FROM raw.gtfs_rt_trip_updates r
        WHERE r.trip_id IS NOT NULL
        EXCEPT
        SELECT DISTINCT trip_id
        FROM staging.delay_preview_best_offset
    ) x
    """
).fetchone()[0]
print(f"\nUnmatched RT trips excluded from final layer: {excluded_trip_count}")

print("\n" + "=" * 72)
print("Coverage check complete.")

conn.close()
