import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

from ingestion.utils.db_client import get_duckdb_connection

load_dotenv()

GTFS_RT_URL = os.getenv("BART_GTFS_RT_URL", "http://api.bart.gov/gtfsrt/tripupdate.aspx")


def fetch_trip_updates() -> pd.DataFrame:
    response = requests.get(GTFS_RT_URL, timeout=60)
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    rows = []
    polled_at = datetime.now(timezone.utc).isoformat()

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip = trip_update.trip

        trip_id = trip.trip_id if trip.HasField("trip_id") else None
        route_id = trip.route_id if trip.HasField("route_id") else None

        for stu in trip_update.stop_time_update:
            arrival_time = None
            departure_time = None

            if stu.HasField("arrival") and stu.arrival.HasField("time"):
                arrival_time = stu.arrival.time

            if stu.HasField("departure") and stu.departure.HasField("time"):
                departure_time = stu.departure.time

            rows.append(
                {
                    "trip_id": trip_id,
                    "route_id": route_id,
                    "stop_id": stu.stop_id,
                    "stop_sequence": stu.stop_sequence,
                    "arrival_time_unix": arrival_time,
                    "departure_time_unix": departure_time,
                    "poll_timestamp": polled_at,
                    "loaded_at": polled_at,
                }
            )

    return pd.DataFrame(rows)


def create_raw_schema(conn) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")


def append_trip_updates(conn, df: pd.DataFrame) -> None:
    if df.empty:
        print("No GTFS-RT rows returned.")
        return

    conn.register("temp_gtfs_rt", df)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.gtfs_rt_trip_updates AS
        SELECT * FROM temp_gtfs_rt WHERE 1=0
    """)

    conn.execute("""
        INSERT INTO raw.gtfs_rt_trip_updates
        SELECT * FROM temp_gtfs_rt
    """)

    conn.unregister("temp_gtfs_rt")


def print_summary(conn) -> None:
    count = conn.execute("SELECT COUNT(*) FROM raw.gtfs_rt_trip_updates").fetchone()[0]
    min_poll, max_poll = conn.execute("""
        SELECT MIN(poll_timestamp), MAX(poll_timestamp)
        FROM raw.gtfs_rt_trip_updates
    """).fetchone()

    print(f"raw.gtfs_rt_trip_updates: {count:,} rows")
    print(f"poll_timestamp min={min_poll} max={max_poll}")


def main() -> None:
    print("Downloading BART GTFS-Realtime TripUpdates...")
    df = fetch_trip_updates()
    print(f"Fetched {len(df):,} trip update rows.")

    conn = get_duckdb_connection()
    try:
        create_raw_schema(conn)
        append_trip_updates(conn, df)
        print_summary(conn)
        print("GTFS-Realtime load completed successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
