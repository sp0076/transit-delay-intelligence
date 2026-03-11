from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("Building staging.rt_static_alignment_preview using per-trip best offsets...")

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
        ) AS rt_seq,
        COUNT(*) OVER (PARTITION BY r.trip_id) AS rt_count,
        (
            EXTRACT(HOUR FROM to_timestamp(r.arrival_time_unix) AT TIME ZONE 'America/Los_Angeles') * 3600
            + EXTRACT(MINUTE FROM to_timestamp(r.arrival_time_unix) AT TIME ZONE 'America/Los_Angeles') * 60
            + EXTRACT(SECOND FROM to_timestamp(r.arrival_time_unix) AT TIME ZONE 'America/Los_Angeles')
        ) AS rt_arrival_sec_of_day
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
        ) AS static_seq,
        COUNT(*) OVER (PARTITION BY s.trip_id) AS static_count,
        (
            CAST(split_part(s.arrival_time, ':', 1) AS INTEGER) * 3600
            + CAST(split_part(s.arrival_time, ':', 2) AS INTEGER) * 60
            + CAST(split_part(s.arrival_time, ':', 3) AS INTEGER)
        ) AS sched_arrival_sec
    FROM raw.gtfs_stop_times s
),
trip_candidates AS (
    SELECT DISTINCT
        rt.trip_id,
        rt.rt_count,
        st.static_count
    FROM rt
    INNER JOIN st
      ON rt.trip_id = st.trip_id
    WHERE st.static_count >= rt.rt_count
),
offsets AS (
    SELECT
        tc.trip_id,
        gs.seq_offset
    FROM trip_candidates tc
    CROSS JOIN generate_series(0, tc.static_count - tc.rt_count) AS gs(seq_offset)
),
aligned_for_scoring AS (
    SELECT
        o.trip_id,
        o.seq_offset,
        rt.rt_seq,
        st.static_seq,
        abs(
            rt.rt_arrival_sec_of_day
            - (st.sched_arrival_sec % 86400)
        ) AS raw_abs_diff_seconds
    FROM offsets o
    INNER JOIN rt
      ON o.trip_id = rt.trip_id
    INNER JOIN st
      ON o.trip_id = st.trip_id
     AND st.static_seq = rt.rt_seq + o.seq_offset
),
scored_offsets AS (
    SELECT
        trip_id,
        seq_offset,
        COUNT(*) AS aligned_rows,
        AVG(
            CASE
                WHEN raw_abs_diff_seconds <= 43200 THEN raw_abs_diff_seconds
                ELSE 86400 - raw_abs_diff_seconds
            END
        ) AS avg_abs_diff_seconds
    FROM aligned_for_scoring
    GROUP BY trip_id, seq_offset
),
best_offset AS (
    SELECT
        trip_id,
        seq_offset,
        aligned_rows,
        avg_abs_diff_seconds,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY avg_abs_diff_seconds ASC, aligned_rows DESC, seq_offset ASC
        ) AS rn
    FROM scored_offsets
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
    bo.seq_offset AS best_seq_offset,
    bo.aligned_rows AS offset_aligned_rows,
    bo.avg_abs_diff_seconds AS offset_avg_abs_diff_seconds,
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
FROM best_offset bo
INNER JOIN rt
    ON bo.trip_id = rt.trip_id
   AND bo.rn = 1
INNER JOIN st
    ON bo.trip_id = st.trip_id
   AND st.static_seq = rt.rt_seq + bo.seq_offset
LEFT JOIN trip_meta tm
    ON rt.trip_id = tm.trip_id
LEFT JOIN stop_meta sm
    ON st.static_stop_id = sm.static_stop_id
""")

count = conn.execute("""
    SELECT COUNT(*) FROM staging.rt_static_alignment_preview
""").fetchone()[0]

offset_summary = conn.execute("""
    SELECT
        COUNT(DISTINCT trip_id) AS trips,
        COUNT(DISTINCT CASE WHEN best_seq_offset > 0 THEN trip_id END) AS trips_with_nonzero_offset
    FROM staging.rt_static_alignment_preview
""").fetchone()

print(f"staging.rt_static_alignment_preview: {count:,} rows")
print(f"trips: {offset_summary[0]}, trips_with_nonzero_offset: {offset_summary[1]}")
print("Done.")

conn.close()
