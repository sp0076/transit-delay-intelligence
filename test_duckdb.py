from ingestion.utils.db_client import get_duckdb_connection

conn = get_duckdb_connection()

conn.execute("""
CREATE TABLE IF NOT EXISTS test_table (
    id INTEGER,
    name VARCHAR
)
""")

conn.execute("DELETE FROM test_table")
conn.execute("INSERT INTO test_table VALUES (1, 'shruti')")

rows = conn.execute("SELECT * FROM test_table").fetchall()
print(rows)

conn.close()
