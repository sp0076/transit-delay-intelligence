from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("JOIN FEASIBILITY CHECK\n")

# 1) Can RT trip_ids map to static gtfs_trips?
print("1. Trip ID match to raw.gtfs_trips")
rows = conn.execute("""
WITH rt AS (
    SELECT DISTINCT trip_id
    FROM raw.gtfs_rt_trip_updates
),
static AS (
    SELECT DISTINCT trip_id
    FROM raw.gtfs_trips
)
SELECT
    (SELECT COUNT(*) FROM rt) AS rt_trip_ids,
    (SELECT COUNT(*) FROM static) AS static_trip_ids,
    (
        SELECT COUNT(*)
        FROM rt
        INNER JOIN static USING (trip_id)
    ) AS matched_trip_ids
""").fetchone()

print(f"rt_trip_ids: {rows[0]}")
print(f"static_trip_ids: {rows[1]}")
print(f"matched_trip_ids: {rows[2]}")

print("\nSample matched trip -> route:")
rows = conn.execute("""
SELECT DISTINCT r.trip_id, t.route_id, t.direction_id, t.service_id
FROM raw.gtfs_rt_trip_updates r
LEFT JOIN raw.gtfs_trips t
  ON r.trip_id = t.trip_id
WHERE t.trip_id IS NOT NULL
LIMIT 20
""").fetchall()

for row in rows:
    print(row)

# 2) Do RT stop_ids exist in static stops?
print("\n2. Stop ID match to raw.gtfs_stops")
rows = conn.execute("""
WITH rt AS (
    SELECT DISTINCT stop_id
    FROM raw.gtfs_rt_trip_updates
),
static AS (
    SELECT DISTINCT stop_id
    FROM raw.gtfs_stops
)
SELECT
    (SELECT COUNT(*) FROM rt) AS rt_stop_ids,
    (SELECT COUNT(*) FROM static) AS static_stop_ids,
    (
        SELECT COUNT(*)
        FROM rt
        INNER JOIN static USING (stop_id)
    ) AS matched_stop_ids
""").fetchone()

print(f"rt_stop_ids: {rows[0]}")
print(f"static_stop_ids: {rows[1]}")
print(f"matched_stop_ids: {rows[2]}")

print("\nSample unmatched RT stop_ids:")
rows = conn.execute("""
SELECT DISTINCT r.stop_id
FROM raw.gtfs_rt_trip_updates r
LEFT JOIN raw.gtfs_stops s
  ON r.stop_id = s.stop_id
WHERE s.stop_id IS NULL
LIMIT 20
""").fetchall()

for row in rows:
    print(row)

# 3) Is (trip_id, stop_id) unique enough in static stop_times?
print("\n3. Static stop_times uniqueness for (trip_id, stop_id)")
rows = conn.execute("""
WITH pairs AS (
    SELECT trip_id, stop_id, COUNT(*) AS row_count
    FROM raw.gtfs_stop_times
    GROUP BY trip_id, stop_id
)
SELECT
    COUNT(*) AS total_pairs,
    SUM(CASE WHEN row_count = 1 THEN 1 ELSE 0 END) AS unique_pairs,
    SUM(CASE WHEN row_count > 1 THEN 1 ELSE 0 END) AS duplicate_pairs
FROM pairs
""").fetchone()

print(f"total_pairs: {rows[0]}")
print(f"unique_pairs: {rows[1]}")
print(f"duplicate_pairs: {rows[2]}")

print("\nSample duplicate (trip_id, stop_id) pairs from static:")
rows = conn.execute("""
SELECT trip_id, stop_id, COUNT(*) AS row_count
FROM raw.gtfs_stop_times
GROUP BY trip_id, stop_id
HAVING COUNT(*) > 1
ORDER BY row_count DESC
LIMIT 20
""").fetchall()

for row in rows:
    print(row)

# 4) Can RT rows join to static stop_times on (trip_id, stop_id)?
print("\n4. RT join coverage on (trip_id, stop_id)")
rows = conn.execute("""
WITH rt_pairs AS (
    SELECT DISTINCT trip_id, stop_id
    FROM raw.gtfs_rt_trip_updates
),
static_pairs AS (
    SELECT DISTINCT trip_id, stop_id
    FROM raw.gtfs_stop_times
)
SELECT
    (SELECT COUNT(*) FROM rt_pairs) AS rt_pairs,
    (
        SELECT COUNT(*)
        FROM rt_pairs
        INNER JOIN static_pairs USING (trip_id, stop_id)
    ) AS matched_pairs
""").fetchone()

print(f"rt_pairs: {rows[0]}")
print(f"matched_pairs: {rows[1]}")

print("\nSample RT rows with joined static schedule info:")
rows = conn.execute("""
SELECT
    r.trip_id,
    r.stop_id,
    t.route_id,
    s.arrival_time,
    s.departure_time
FROM raw.gtfs_rt_trip_updates r
LEFT JOIN raw.gtfs_trips t
  ON r.trip_id = t.trip_id
LEFT JOIN raw.gtfs_stop_times s
  ON r.trip_id = s.trip_id
 AND r.stop_id = s.stop_id
LIMIT 20
""").fetchall()

for row in rows:
    print(row)

conn.close()