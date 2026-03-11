from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("SEQUENCE ALIGNMENT CHECK\n")

# 1) Find matched trip_ids that exist in both RT and static trips
print("1. Candidate trip_ids that exist in both RT and static GTFS\n")
rows = conn.execute("""
    SELECT DISTINCT r.trip_id
    FROM raw.gtfs_rt_trip_updates r
    INNER JOIN raw.gtfs_trips t
      ON r.trip_id = t.trip_id
    ORDER BY r.trip_id
    LIMIT 10
""").fetchall()

trip_ids = [row[0] for row in rows]

for trip_id in trip_ids:
    print(trip_id)

# 2) For a few trips, compare RT ordinal order vs static stop_times ordinal order
print("\n2. Compare RT ordered stops to static ordered stops\n")

for trip_id in trip_ids[:3]:
    print(f"\n--- trip_id = {trip_id} ---")

    query = f"""
    WITH rt AS (
        SELECT
            trip_id,
            stop_id AS rt_stop_id,
            arrival_time_unix,
            departure_time_unix,
            ROW_NUMBER() OVER (
                PARTITION BY trip_id
                ORDER BY arrival_time_unix
            ) AS rt_seq
        FROM raw.gtfs_rt_trip_updates
        WHERE trip_id = '{trip_id}'
    ),
    st AS (
        SELECT
            s.trip_id,
            CAST(s.stop_id AS VARCHAR) AS static_stop_id,
            s.arrival_time,
            s.departure_time,
            ROW_NUMBER() OVER (
                PARTITION BY s.trip_id
                ORDER BY s.arrival_time, s.departure_time
            ) AS static_seq
        FROM raw.gtfs_stop_times s
        WHERE s.trip_id = '{trip_id}'
    ),
    enriched AS (
        SELECT
            st.trip_id,
            st.static_seq,
            st.static_stop_id,
            gs.stop_name,
            rt.rt_stop_id,
            rt.arrival_time_unix,
            rt.departure_time_unix
        FROM st
        LEFT JOIN rt
          ON st.trip_id = rt.trip_id
         AND st.static_seq = rt.rt_seq
        LEFT JOIN raw.gtfs_stops gs
                    ON CAST(st.static_stop_id AS VARCHAR) = CAST(gs.stop_id AS VARCHAR)
    )
    SELECT *
    FROM enriched
    ORDER BY static_seq
    LIMIT 25
    """

    rows = conn.execute(query).fetchall()
    for row in rows:
        print(row)

# 3) Check count alignment per trip
print("\n3. Count alignment between RT rows and static stop_times rows\n")

rows = conn.execute("""
WITH rt_counts AS (
    SELECT trip_id, COUNT(*) AS rt_count
    FROM raw.gtfs_rt_trip_updates
    GROUP BY trip_id
),
st_counts AS (
    SELECT trip_id, COUNT(*) AS static_count
    FROM raw.gtfs_stop_times
    GROUP BY trip_id
)
SELECT
    r.trip_id,
    r.rt_count,
    s.static_count,
    (r.rt_count = s.static_count) AS exact_match
FROM rt_counts r
INNER JOIN st_counts s
  ON r.trip_id = s.trip_id
ORDER BY exact_match DESC, r.trip_id
LIMIT 50
""").fetchall()

for row in rows:
    print(row)

# 4) Recover route_id from trip_id
print("\n4. Recover route_id from raw.gtfs_trips\n")

rows = conn.execute("""
SELECT DISTINCT
    r.trip_id,
    t.route_id,
    t.direction_id,
    t.service_id
FROM raw.gtfs_rt_trip_updates r
LEFT JOIN raw.gtfs_trips t
  ON r.trip_id = t.trip_id
WHERE t.trip_id IS NOT NULL
LIMIT 20
""").fetchall()

for row in rows:
    print(row)

conn.close()
