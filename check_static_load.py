import os

from dotenv import load_dotenv

from ingestion.utils.db_client import get_duckdb_connection


def main() -> None:
    load_dotenv()

    db_path = os.getenv("DB_PATH")
    if not db_path:
        raise ValueError("DB_PATH is not set in .env")

    conn = get_duckdb_connection()

    tables = [
        "gtfs_stops",
        "gtfs_routes",
        "gtfs_trips",
        "gtfs_stop_times",
    ]

    print("Checking raw GTFS tables...")
    for table in tables:
        exists = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'raw' AND table_name = '{table}'
            """
        ).fetchone()[0]

        if exists == 0:
            print(f"raw.{table}: MISSING")
            continue

        row_count, min_loaded_at, max_loaded_at = conn.execute(
            f"""
            SELECT COUNT(*), MIN(loaded_at), MAX(loaded_at)
            FROM raw.{table}
            """
        ).fetchone()

        print(
            f"raw.{table}: {row_count} rows | loaded_at min={min_loaded_at} max={max_loaded_at}"
        )

    conn.close()


if __name__ == "__main__":
    main()
