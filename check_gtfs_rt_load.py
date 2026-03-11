from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("Checking raw.gtfs_rt_trip_updates...\n")

count = conn.execute("SELECT COUNT(*) FROM raw.gtfs_rt_trip_updates").fetchone()[0]
print(f"raw.gtfs_rt_trip_updates: {count:,} rows")

rows = conn.execute("""
    SELECT trip_id, route_id, stop_id, stop_sequence, arrival_time_unix, departure_time_unix, poll_timestamp
    FROM raw.gtfs_rt_trip_updates
    ORDER BY poll_timestamp DESC
    LIMIT 10
""").fetchall()

print("\nSample rows:")
for row in rows:
    print(row)

conn.close()
