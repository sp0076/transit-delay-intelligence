import io
import os
import zipfile

import pandas as pd
import requests
from dotenv import load_dotenv

from ingestion.utils.db_client import get_duckdb_connection


GTFS_URL = "http://api.511.org/transit/datafeeds"
OPERATOR_ID = "BA"
FILE_TO_TABLE = {
	"stops.txt": "gtfs_stops",
	"routes.txt": "gtfs_routes",
	"trips.txt": "gtfs_trips",
	"stop_times.txt": "gtfs_stop_times",
}


def download_gtfs_zip(api_key: str) -> bytes:
	params = {"api_key": api_key, "operator_id": OPERATOR_ID}
	response = requests.get(GTFS_URL, params=params, timeout=60)
	response.raise_for_status()
	return response.content


def load_one_file(zip_ref: zipfile.ZipFile, conn, file_name: str, table_name: str) -> int:
	if file_name not in zip_ref.namelist():
		raise FileNotFoundError(f"{file_name} not found in GTFS zip")

	with zip_ref.open(file_name) as file_obj:
		df = pd.read_csv(file_obj)

	df["loaded_at"] = pd.Timestamp.utcnow()
	conn.register("tmp_gtfs_df", df)
	conn.execute(f"CREATE OR REPLACE TABLE raw.{table_name} AS SELECT * FROM tmp_gtfs_df")
	conn.unregister("tmp_gtfs_df")

	return conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]


def main() -> None:
	load_dotenv()
	api_key = os.getenv("API_511_KEY")

	if not api_key:
		raise ValueError("API_511_KEY is not set in .env")

	print("Downloading BART GTFS static feed from 511...")
	zip_bytes = download_gtfs_zip(api_key)

	conn = get_duckdb_connection()
	conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

	with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_ref:
		print("Loading files into DuckDB raw schema...")
		for file_name, table_name in FILE_TO_TABLE.items():
			row_count = load_one_file(zip_ref, conn, file_name, table_name)
			print(f"raw.{table_name}: {row_count} rows")

	conn.close()

	print("Done.")


if __name__ == "__main__":
	main()
