from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("Building staging.rt_static_alignment_preview...")

conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
conn.execute("DROP TABLE IF EXISTS staging.rt_static_alignment_preview")

conn.execute("""
CREATE TABLE staging.rt_static_alignment_preview AS
WITH rt AS (
    SELECT
        r.trip_id,
        r.stop_id AS rt_stop_id,
        r.arrival_time_unix,
        r.departure_time_unix,
        r.poll_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY r.trip_id
            ORDER BY r.arrival_time_unix
        ) AS rt_seq
    FROM raw.gtfs_rt_trip_updates r
),
st AS (
    SELECT
        s.trip_id,
        CAST(s.stop_id AS VARCHAR) AS static_stop_id,
        s.arrival_time AS scheduled_arrival_hms,
        s.departure_time AS scheduled_departure_hms,
        ROW_NUMBER() OVER (
            PARTITION BY s.trip_id
            ORDER BY s.arrival_time, s.departure_time
        ) AS static_seq
    FROM raw.gtfs_stop_times s
),
trip_meta AS (
    SELECT DISTINCT
        trip_id,
        route_id,
        direction_id,
        service_id
    FROM raw.gtfs_trips
),
stop_meta AS (
    SELECT DISTINCT
        CAST(stop_id AS VARCHAR) AS static_stop_id,
        stop_name
    FROM raw.gtfs_stops
)
SELECT
    rt.trip_id,
    tm.route_id,
    tm.direction_id,
    tm.service_id,
    rt.rt_seq,
    st.static_seq,
    rt.rt_stop_id,
    st.static_stop_id,
    sm.stop_name,
    rt.arrival_time_unix,
    rt.departure_time_unix,
    to_timestamp(rt.arrival_time_unix) AS actual_arrival_ts,
    to_timestamp(rt.departure_time_unix) AS actual_departure_ts,
    st.scheduled_arrival_hms,
    st.scheduled_departure_hms,
    rt.poll_timestamp
FROM rt
INNER JOIN st
    ON rt.trip_id = st.trip_id
   AND rt.rt_seq = st.static_seq
LEFT JOIN trip_meta tm
    ON rt.trip_id = tm.trip_id
LEFT JOIN stop_meta sm
    ON st.static_stop_id = sm.static_stop_id
""")

count = conn.execute("""
    SELECT COUNT(*) FROM staging.rt_static_alignment_preview
""").fetchone()[0]

print(f"staging.rt_static_alignment_preview: {count:,} rows")
print("Done.")

conn.close()
