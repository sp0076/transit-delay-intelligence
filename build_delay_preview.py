from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("Building staging.delay_preview...")

conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
conn.execute("DROP TABLE IF EXISTS staging.delay_preview")

conn.execute("""
CREATE TABLE staging.delay_preview AS
WITH base AS (
    SELECT
        trip_id,
        route_id,
        direction_id,
        service_id,
        rt_seq,
        rt_stop_id,
        static_stop_id,
        stop_name,
        actual_arrival_ts,
        scheduled_arrival_hms,

        -- convert actual arrival to local Bay Area time
        actual_arrival_ts AT TIME ZONE 'America/Los_Angeles' AS actual_arrival_local_ts,

        -- derive local service date from actual arrival
        CAST(actual_arrival_ts AT TIME ZONE 'America/Los_Angeles' AS DATE) AS service_date_local,

        CAST(split_part(scheduled_arrival_hms, ':', 1) AS INTEGER) AS sched_hour,
        CAST(split_part(scheduled_arrival_hms, ':', 2) AS INTEGER) AS sched_minute,
        CAST(split_part(scheduled_arrival_hms, ':', 3) AS INTEGER) AS sched_second
    FROM staging.rt_static_alignment_preview
    WHERE actual_arrival_ts IS NOT NULL
      AND scheduled_arrival_hms IS NOT NULL
),
scheduled AS (
    SELECT
        *,
        CAST(service_date_local AS TIMESTAMP)
            + sched_hour * INTERVAL 1 HOUR
            + sched_minute * INTERVAL 1 MINUTE
            + sched_second * INTERVAL 1 SECOND
            AS scheduled_arrival_local_ts
    FROM base
)
SELECT
    trip_id,
    route_id,
    direction_id,
    service_id,
    rt_seq,
    rt_stop_id,
    static_stop_id,
    stop_name,
    service_date_local,
    actual_arrival_local_ts,
    scheduled_arrival_local_ts,
    date_diff('minute', scheduled_arrival_local_ts, actual_arrival_local_ts) AS delay_minutes
FROM scheduled
""")

count = conn.execute("""
    SELECT COUNT(*) FROM staging.delay_preview
""").fetchone()[0]

print(f"staging.delay_preview: {count:,} rows")
print("Done.")

conn.close()
