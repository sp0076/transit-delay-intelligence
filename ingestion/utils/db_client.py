import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

def get_duckdb_connection():
    db_path = os.getenv("DB_PATH")
    if not db_path:
        raise ValueError("DB_PATH is not set in .env")
    return duckdb.connect(db_path)
