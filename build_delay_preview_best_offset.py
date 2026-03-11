#!/usr/bin/env python3
"""
Build delay preview using best-offset alignment.
Computes final delay metrics from staging.rt_static_alignment_best_offset.
"""

import duckdb
from dotenv import load_dotenv
import os

load_dotenv()
db_path = os.getenv("DB_PATH", "./data/transit_delay.duckdb")
conn = duckdb.connect(db_path)

print("Building staging.delay_preview_best_offset...")

query = """
CREATE TABLE staging.delay_preview_best_offset AS
WITH base AS (
    SELECT trip_id, route_id, direction_id, service_id, best_offset, rt_seq,
           rt_stop_id, static_stop_id, stop_name, actual_arrival_ts, scheduled_arrival_hms,
           actual_arrival_ts AT TIME ZONE 'America/Los_Angeles' AS actual_arrival_local_ts,
           CAST(actual_arrival_ts AT TIME ZONE 'America/Los_Angeles' AS DATE) AS service_date_local,
           CAST(split_part(scheduled_arrival_hms, ':', 1) AS INTEGER) AS sched_hour,
           CAST(split_part(scheduled_arrival_hms, ':', 2) AS INTEGER) AS sched_minute,
           CAST(split_part(scheduled_arrival_hms, ':', 3) AS INTEGER) AS sched_second
    FROM staging.rt_static_alignment_best_offset
    WHERE actual_arrival_ts IS NOT NULL AND scheduled_arrival_hms IS NOT NULL
),
scheduled AS (
    SELECT *,
           CAST(service_date_local AS TIMESTAMP)
               + sched_hour * INTERVAL 1 HOUR + sched_minute * INTERVAL 1 MINUTE + sched_second * INTERVAL 1 SECOND
               AS scheduled_arrival_local_ts
    FROM base
)
SELECT trip_id, route_id, direction_id, service_id, best_offset, rt_seq, rt_stop_id, static_stop_id, stop_name,
       service_date_local, actual_arrival_local_ts, scheduled_arrival_local_ts,
       date_diff('minute', scheduled_arrival_local_ts, actual_arrival_local_ts) AS delay_minutes
FROM scheduled
"""

try:
    conn.execute("DROP TABLE IF EXISTS staging.delay_preview_best_offset")
    conn.execute(query)
    result = conn.execute("SELECT COUNT(*) as count FROM staging.delay_preview_best_offset").fetchall()
    print(f"staging.delay_preview_best_offset created: {result[0][0]} rows")
    print("Done.")
finally:
    conn.close()
