from pathlib import Path
import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "db" / "mortgage.duckdb"
OUT_DIR = REPO_ROOT / "data"

SQL_VIEWS_PATH = REPO_ROOT / "scripts" / "02_create_views.sql"

def main():
    OUT_DIR.mkdir(exist_ok=True)

    con = duckdb.connect(str(DB_PATH))

    # Ensure base table exists (your 01 script already creates it, but this is safe)
    con.execute(f"CREATE OR REPLACE TABLE loan_tape AS SELECT * FROM read_parquet('{OUT_DIR / 'loan_tape.parquet'}')")

    # Create/replace views
    con.execute(SQL_VIEWS_PATH.read_text())

    # Export outputs for Excel / reporting
    con.execute(
        f"COPY (SELECT * FROM v_state_summary) TO '{OUT_DIR / 'state_summary.csv'}' (HEADER, DELIMITER ',')"
    )
    con.execute(
        f"COPY (SELECT * FROM v_fico_ltv_bands) TO '{OUT_DIR / 'fico_ltv_bands.csv'}' (HEADER, DELIMITER ',')"
    )

    print("✅ Exported:", OUT_DIR / "state_summary.csv")
    print("✅ Exported:", OUT_DIR / "fico_ltv_bands.csv")

    con.close()

if __name__ == "__main__":
    main()
