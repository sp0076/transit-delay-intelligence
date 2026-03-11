from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("BEST OFFSET BY TIME CHECK\n")

query = """
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
    seq_offset,
    aligned_rows,
    median_abs_error_min,
    avg_abs_error_min
FROM best
WHERE rn = 1
ORDER BY median_abs_error_min ASC, avg_abs_error_min ASC, trip_id
LIMIT 100
"""

rows = conn.execute(query).fetchall()

print("Best offset per trip (time-based):")
for row in rows:
    print(row)

print("\nTrips where best offset is NOT zero:")
rows = conn.execute("""
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
        CAST(service_date_local AS TIMESTAMP)
            + sched_hour * INTERVAL 1 HOUR
            + sched_minute * INTERVAL 1 MINUTE
            + sched_second * INTERVAL 1 SECOND
            AS scheduled_arrival_local_ts,
        actual_arrival_local_ts
    FROM aligned
),
scored AS (
    SELECT
        trip_id,
        seq_offset,
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
    seq_offset,
    median_abs_error_min,
    avg_abs_error_min
FROM best
WHERE rn = 1
  AND seq_offset <> 0
ORDER BY median_abs_error_min ASC, avg_abs_error_min ASC, trip_id
LIMIT 100
""").fetchall()

for row in rows:
    print(row)

conn.close()
