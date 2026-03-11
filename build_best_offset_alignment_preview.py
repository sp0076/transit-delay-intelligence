from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("Building staging.best_trip_offsets...")
conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
conn.execute("DROP TABLE IF EXISTS staging.best_trip_offsets")

conn.execute("""
CREATE TABLE staging.best_trip_offsets AS
WITH rt AS (
    SELECT
        trip_id,
        arrival_time_unix,
        to_timestamp(arrival_time_unix) AT TIME ZONE 'America/Los_Angeles' AS actual_arrival_local_ts,
        CAST(to_timestamp(arrival_time_unix) AT TIME ZONE 'America/Los_Angeles' AS DATE) AS service_date_local,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY arrival_time_unix
        ) AS rt_seq,
        COUNT(*) OVER (PARTITION BY trip_id) AS rt_count
    FROM raw.gtfs_rt_trip_updates
),
st AS (
    SELECT
        trip_id,
        CAST(stop_id AS VARCHAR) AS static_stop_id,
        arrival_time AS scheduled_arrival_hms,
        departure_time AS scheduled_departure_hms,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY arrival_time, departure_time
        ) AS static_seq,
        COUNT(*) OVER (PARTITION BY trip_id) AS static_count
    FROM raw.gtfs_stop_times
),
trip_candidates AS (
    SELECT DISTINCT
        rt.trip_id,
        rt.rt_count,
        st.static_count
    FROM (
        SELECT trip_id, COUNT(*) AS rt_count
        FROM raw.gtfs_rt_trip_updates
        GROUP BY trip_id
    ) rt
    INNER JOIN (
        SELECT trip_id, COUNT(*) AS static_count
        FROM raw.gtfs_stop_times
        GROUP BY trip_id
    ) st
      ON rt.trip_id = st.trip_id
    WHERE rt.trip_id IN (
        SELECT DISTINCT r.trip_id
        FROM raw.gtfs_rt_trip_updates r
        INNER JOIN raw.gtfs_trips t
          ON r.trip_id = t.trip_id
    )
      AND st.static_count >= rt.rt_count
),
offsets AS (
    SELECT
        tc.trip_id,
        tc.rt_count,
        tc.static_count,
        gs.seq_offset
    FROM trip_candidates tc
    CROSS JOIN generate_series(0, tc.static_count - tc.rt_count) AS gs(seq_offset)
),
aligned AS (
    SELECT
        o.trip_id,
        o.seq_offset,
        rt.rt_seq,
        rt.actual_arrival_local_ts,
        rt.service_date_local,
        st.static_stop_id,
        st.scheduled_arrival_hms,
        CAST(split_part(st.scheduled_arrival_hms, ':', 1) AS INTEGER) AS sched_hour,
        CAST(split_part(st.scheduled_arrival_hms, ':', 2) AS INTEGER) AS sched_minute,
        CAST(split_part(st.scheduled_arrival_hms, ':', 3) AS INTEGER) AS sched_second
    FROM offsets o
    INNER JOIN rt
      ON o.trip_id = rt.trip_id
    INNER JOIN st
      ON o.trip_id = st.trip_id
         AND st.static_seq = rt.rt_seq + o.seq_offset
),
timed AS (
    SELECT
        trip_id,
                seq_offset,
        rt_seq,
        static_stop_id,
        actual_arrival_local_ts,
        CAST(service_date_local AS TIMESTAMP)
            + sched_hour * INTERVAL 1 HOUR
            + sched_minute * INTERVAL 1 MINUTE
            + sched_second * INTERVAL 1 SECOND
            AS scheduled_arrival_local_ts
    FROM aligned
),
scored AS (
    SELECT
        trip_id,
        seq_offset,
        COUNT(*) AS aligned_rows,
        MEDIAN(ABS(date_diff('minute', scheduled_arrival_local_ts, actual_arrival_local_ts))) AS median_abs_error_min,
        AVG(ABS(date_diff('minute', scheduled_arrival_local_ts, actual_arrival_local_ts))) AS avg_abs_error_min
    FROM timed
    GROUP BY trip_id, seq_offset
),
best AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY median_abs_error_min ASC, avg_abs_error_min ASC, seq_offset ASC
        ) AS rn
    FROM scored
)
SELECT
    trip_id,
    seq_offset AS best_offset,
    aligned_rows,
    median_abs_error_min,
    avg_abs_error_min
FROM best
WHERE rn = 1
""")

print("Building staging.rt_static_alignment_best_offset...")
conn.execute("DROP TABLE IF EXISTS staging.rt_static_alignment_best_offset")

conn.execute("""
CREATE TABLE staging.rt_static_alignment_best_offset AS
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
    bo.best_offset,
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
FROM staging.best_trip_offsets bo
INNER JOIN rt
    ON bo.trip_id = rt.trip_id
INNER JOIN st
    ON bo.trip_id = st.trip_id
   AND st.static_seq = rt.rt_seq + bo.best_offset
LEFT JOIN trip_meta tm
    ON rt.trip_id = tm.trip_id
LEFT JOIN stop_meta sm
    ON st.static_stop_id = sm.static_stop_id
""")

rows = conn.execute("""
SELECT COUNT(*), COUNT(DISTINCT trip_id)
FROM staging.rt_static_alignment_best_offset
""").fetchone()

print(f"staging.best_trip_offsets trip count: {conn.execute('SELECT COUNT(*) FROM staging.best_trip_offsets').fetchone()[0]}")
print(f"staging.rt_static_alignment_best_offset rows: {rows[0]}")
print(f"staging.rt_static_alignment_best_offset distinct trips: {rows[1]}")
print("Done.")

conn.close()
