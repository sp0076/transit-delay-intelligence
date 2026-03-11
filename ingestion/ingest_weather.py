import os
from datetime import date, timedelta, datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

from ingestion.utils.db_client import get_duckdb_connection

load_dotenv()

WEATHER_LAT = os.getenv("WEATHER_LAT")
WEATHER_LON = os.getenv("WEATHER_LON")
TIMEZONE = os.getenv("TIMEZONE", "America/Los_Angeles")

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_weather_dataframe(days_back: int = 7) -> pd.DataFrame:
    if not WEATHER_LAT or not WEATHER_LON:
        raise ValueError("WEATHER_LAT and WEATHER_LON must be set in .env")

    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)

    params = {
        "latitude": WEATHER_LAT,
        "longitude": WEATHER_LON,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": "precipitation,temperature_2m,windspeed_10m",
        "timezone": TIMEZONE,
    }

    response = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    hourly = data.get("hourly")
    if not hourly:
        raise ValueError("No hourly weather data returned")

    df = pd.DataFrame(
        {
            "observation_datetime": hourly["time"],
            "precipitation_mm": hourly["precipitation"],
            "temperature_c": hourly["temperature_2m"],
            "windspeed_kmh": hourly["windspeed_10m"],
        }
    )

    df["latitude"] = float(WEATHER_LAT)
    df["longitude"] = float(WEATHER_LON)
    df["loaded_at"] = datetime.now(timezone.utc).isoformat()

    return df


def create_raw_schema(conn) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")


def load_weather_to_duckdb(conn, df: pd.DataFrame) -> None:
    conn.register("temp_weather_hourly", df)

    conn.execute("DROP TABLE IF EXISTS raw.weather_hourly")
    conn.execute("""
        CREATE TABLE raw.weather_hourly AS
        SELECT * FROM temp_weather_hourly
    """)

    conn.unregister("temp_weather_hourly")


def print_weather_summary(conn) -> None:
    count = conn.execute("SELECT COUNT(*) FROM raw.weather_hourly").fetchone()[0]
    min_dt, max_dt = conn.execute("""
        SELECT MIN(observation_datetime), MAX(observation_datetime)
        FROM raw.weather_hourly
    """).fetchone()

    print(f"raw.weather_hourly: {count:,} rows")
    print(f"observation_datetime min={min_dt} max={max_dt}")


def main() -> None:
    print("Downloading weather data from Open-Meteo...")
    df = fetch_weather_dataframe(days_back=7)
    print(f"Fetched {len(df):,} hourly weather rows.")

    conn = get_duckdb_connection()
    try:
        create_raw_schema(conn)
        load_weather_to_duckdb(conn, df)
        print_weather_summary(conn)
        print("Weather load completed successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
