from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("STOP ID MAPPING CHECK\n")

print("1. Static stops columns preview")
rows = conn.execute("""
    SELECT *
    FROM raw.gtfs_stops
    LIMIT 5
""").fetchall()

for row in rows:
    print(row)

print("\n2. Static stops table schema")
rows = conn.execute("""
    DESCRIBE raw.gtfs_stops
""").fetchall()

for row in rows:
    print(row)

print("\n3. Distinct RT stop_id samples")
rows = conn.execute("""
    SELECT DISTINCT stop_id
    FROM raw.gtfs_rt_trip_updates
    ORDER BY stop_id
    LIMIT 50
""").fetchall()

for row in rows:
    print(row)

print("\n4. Check if RT stop_id matches static stop_code")
try:
    rows = conn.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT r.stop_id
            FROM raw.gtfs_rt_trip_updates r
            INNER JOIN raw.gtfs_stops s
              ON r.stop_id = s.stop_code
        )
    """).fetchone()
    print(f"matched_on_stop_code: {rows[0]}")
except Exception as e:
    print("stop_code check failed:", e)

print("\n5. Sample static stop values for stop_id / stop_code / stop_name")
try:
    rows = conn.execute("""
        SELECT stop_id, stop_code, stop_name
        FROM raw.gtfs_stops
        LIMIT 30
    """).fetchall()
    for row in rows:
        print(row)
except Exception as e:
    print("static stop sample failed:", e)

conn.close()
