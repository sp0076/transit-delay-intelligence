from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("BEST OFFSET ALIGNMENT CHECK\n")

rows = conn.execute("""
WITH rt AS (
    SELECT
        trip_id,
        arrival_time_unix,
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
        arrival_time,
        departure_time,
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
),
offsets AS (
    SELECT
        tc.trip_id,
        tc.rt_count,
        tc.static_count,
        gs.seq_offset
    FROM trip_candidates tc
    CROSS JOIN generate_series(0, tc.static_count - tc.rt_count) AS gs(seq_offset)
    WHERE tc.static_count >= tc.rt_count
),
aligned AS (
    SELECT
        o.trip_id,
        o.seq_offset,
        rt.rt_seq,
        st.static_seq,
        rt.arrival_time_unix,
        st.arrival_time
    FROM offsets o
    INNER JOIN rt
      ON o.trip_id = rt.trip_id
    INNER JOIN st
      ON o.trip_id = st.trip_id
     AND st.static_seq = rt.rt_seq + o.seq_offset
),
scored AS (
    SELECT
        trip_id,
        seq_offset,
        COUNT(*) AS aligned_rows
    FROM aligned
    GROUP BY trip_id, seq_offset
),
best AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY aligned_rows DESC, seq_offset ASC
        ) AS rn
    FROM scored
)
SELECT
    trip_id,
    seq_offset,
    aligned_rows
FROM best
WHERE rn = 1
ORDER BY trip_id
LIMIT 50
""").fetchall()

print("Best offset per trip (based on row-count fit):")
for row in rows:
    print(row)

conn.close()
