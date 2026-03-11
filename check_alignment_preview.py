from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("ALIGNMENT PREVIEW CHECK\n")

rows = conn.execute("""
SELECT COUNT(*) FROM staging.rt_static_alignment_preview
""").fetchone()
print(f"total_rows: {rows[0]}")

rows = conn.execute("""
SELECT COUNT(DISTINCT trip_id) FROM staging.rt_static_alignment_preview
""").fetchone()
print(f"distinct_trip_ids: {rows[0]}")

rows = conn.execute("""
SELECT COUNT(DISTINCT route_id) FROM staging.rt_static_alignment_preview
""").fetchone()
print(f"distinct_route_ids: {rows[0]}")

rows = conn.execute("""
SELECT COUNT(*) FROM staging.rt_static_alignment_preview
WHERE route_id IS NULL
""").fetchone()
print(f"null_route_ids: {rows[0]}")

print("\nSample rows:")
rows = conn.execute("""
SELECT
    trip_id,
    route_id,
    rt_seq,
    rt_stop_id,
    static_stop_id,
    stop_name,
    actual_arrival_ts,
    scheduled_arrival_hms
FROM staging.rt_static_alignment_preview
ORDER BY trip_id, rt_seq
LIMIT 30
""").fetchall()

for row in rows:
    print(row)

print("\nTrips with most aligned rows:")
rows = conn.execute("""
SELECT
    trip_id,
    route_id,
    COUNT(*) AS aligned_rows
FROM staging.rt_static_alignment_preview
GROUP BY trip_id, route_id
ORDER BY aligned_rows DESC, trip_id
LIMIT 20
""").fetchall()

for row in rows:
    print(row)

conn.close()
