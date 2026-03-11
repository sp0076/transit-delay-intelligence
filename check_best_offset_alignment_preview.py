from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("BEST OFFSET ALIGNMENT PREVIEW CHECK\n")

rows = conn.execute("""
SELECT COUNT(*) FROM staging.rt_static_alignment_best_offset
""").fetchone()
print(f"total_rows: {rows[0]}")

rows = conn.execute("""
SELECT COUNT(DISTINCT trip_id) FROM staging.rt_static_alignment_best_offset
""").fetchone()
print(f"distinct_trip_ids: {rows[0]}")

rows = conn.execute("""
SELECT COUNT(DISTINCT route_id) FROM staging.rt_static_alignment_best_offset
""").fetchone()
print(f"distinct_route_ids: {rows[0]}")

print("\nBest offsets:")
rows = conn.execute("""
SELECT trip_id, best_offset, COUNT(*) AS aligned_rows
FROM staging.rt_static_alignment_best_offset
GROUP BY trip_id, best_offset
ORDER BY best_offset DESC, trip_id
LIMIT 50
""").fetchall()

for row in rows:
    print(row)

print("\nSample aligned rows:")
rows = conn.execute("""
SELECT
    trip_id,
    route_id,
    best_offset,
    rt_seq,
    static_seq,
    rt_stop_id,
    static_stop_id,
    stop_name,
    actual_arrival_ts,
    scheduled_arrival_hms
FROM staging.rt_static_alignment_best_offset
ORDER BY trip_id, rt_seq
LIMIT 40
""").fetchall()

for row in rows:
    print(row)

conn.close()
