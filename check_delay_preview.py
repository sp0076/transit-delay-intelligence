from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("DELAY PREVIEW CHECK\n")

rows = conn.execute("""
SELECT COUNT(*) FROM staging.delay_preview
""").fetchone()
print(f"total_rows: {rows[0]}")

rows = conn.execute("""
SELECT
    MIN(delay_minutes),
    AVG(delay_minutes),
    MEDIAN(delay_minutes),
    MAX(delay_minutes)
FROM staging.delay_preview
""").fetchone()

print(f"min_delay: {rows[0]}")
print(f"avg_delay: {rows[1]}")
print(f"median_delay: {rows[2]}")
print(f"max_delay: {rows[3]}")

rows = conn.execute("""
SELECT COUNT(*) FROM staging.delay_preview
WHERE delay_minutes > 60
""").fetchone()
print(f"rows_over_60_min: {rows[0]}")

rows = conn.execute("""
SELECT COUNT(*) FROM staging.delay_preview
WHERE delay_minutes < -10
""").fetchone()
print(f"rows_below_minus_10_min: {rows[0]}")

print("\nSample rows:")
rows = conn.execute("""
SELECT
    trip_id,
    route_id,
    rt_seq,
    stop_name,
    scheduled_arrival_local_ts,
    actual_arrival_local_ts,
    delay_minutes
FROM staging.delay_preview
ORDER BY trip_id, rt_seq
LIMIT 30
""").fetchall()

for row in rows:
    print(row)

print("\nWorst delays:")
rows = conn.execute("""
SELECT
    trip_id,
    route_id,
    rt_seq,
    stop_name,
    scheduled_arrival_local_ts,
    actual_arrival_local_ts,
    delay_minutes
FROM staging.delay_preview
ORDER BY delay_minutes DESC
LIMIT 20
""").fetchall()

for row in rows:
    print(row)

conn.close()
