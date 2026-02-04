from pathlib import Path
import duckdb
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "db" / "mortgage.duckdb"
DATA_DIR = ROOT / "data"

def main():
    print("✅ Repo root:", ROOT)
    print("✅ Data dir exists:", DATA_DIR.exists())
    print("✅ DuckDB path:", DB_PATH)

    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE TABLE IF NOT EXISTS sanity_check (ts TIMESTAMP DEFAULT now())")
    con.execute("INSERT INTO sanity_check DEFAULT VALUES")
    row_count = con.execute("SELECT COUNT(*) FROM sanity_check").fetchone()[0]
    con.close()

    print("✅ DuckDB write/read works. Rows in sanity_check =", row_count)

    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    print("✅ Polars works:\n", df)

if __name__ == "__main__":
    main()
