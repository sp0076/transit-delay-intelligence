from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("GTFS-RT profile\n")

queries = {
    "total_rows": "SELECT COUNT(*) FROM raw.gtfs_rt_trip_updates",
    "distinct_trip_ids": "SELECT COUNT(DISTINCT trip_id) FROM raw.gtfs_rt_trip_updates",
    "distinct_route_ids": "SELECT COUNT(DISTINCT route_id) FROM raw.gtfs_rt_trip_updates",
    "null_route_ids": "SELECT COUNT(*) FROM raw.gtfs_rt_trip_updates WHERE route_id IS NULL",
    "null_stop_ids": "SELECT COUNT(*) FROM raw.gtfs_rt_trip_updates WHERE stop_id IS NULL",
    "null_arrival_time_unix": "SELECT COUNT(*) FROM raw.gtfs_rt_trip_updates WHERE arrival_time_unix IS NULL",
    "null_departure_time_unix": "SELECT COUNT(*) FROM raw.gtfs_rt_trip_updates WHERE departure_time_unix IS NULL",
}

for name, query in queries.items():
    value = conn.execute(query).fetchone()[0]
    print(f"{name}: {value}")

print("\nTop stop_sequence values:")
rows = conn.execute("""
    SELECT stop_sequence, COUNT(*) AS row_count
    FROM raw.gtfs_rt_trip_updates
    GROUP BY stop_sequence
    ORDER BY row_count DESC
    LIMIT 20
""").fetchall()

for row in rows:
    print(row)

print("\nSample distinct trip_id / stop_id pairs:")
rows = conn.execute("""
    SELECT trip_id, stop_id, arrival_time_unix, departure_time_unix
    FROM raw.gtfs_rt_trip_updates
    LIMIT 20
""").fetchall()

for row in rows:
    print(row)

conn.close()