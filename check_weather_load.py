from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

print("Checking raw.weather_hourly...\n")

count = conn.execute("SELECT COUNT(*) FROM raw.weather_hourly").fetchone()[0]
print(f"raw.weather_hourly: {count:,} rows")

rows = conn.execute("""
    SELECT observation_datetime, precipitation_mm, temperature_c, windspeed_kmh
    FROM raw.weather_hourly
    ORDER BY observation_datetime
    LIMIT 10
""").fetchall()

print("\nSample rows:")
for row in rows:
    print(row)

conn.close()