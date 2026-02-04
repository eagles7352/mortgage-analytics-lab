from __future__ import annotations

import duckdb

from src.config import DATA_DIR, DB_PATH
from src.loan_tape import make_loans


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    loans = make_loans()
    out_path = DATA_DIR / "loan_tape.parquet"
    loans.write_parquet(out_path)

    print(f"✅ Wrote {loans.height:,} loans → {out_path}")

    # Load into DuckDB (table will live in db/mortgage.duckdb, which we ignore in git)
    con = duckdb.connect(str(DB_PATH))
    con.execute(
        "CREATE OR REPLACE TABLE loan_tape AS SELECT * FROM read_parquet(?)",
        [str(out_path)],
    )

    # A few quick rollups (these become recruiter-friendly outputs)
    print("\n--- Portfolio rollups (DuckDB SQL) ---")

    wac = con.execute(
        "SELECT SUM(note_rate * orig_balance) / SUM(orig_balance) AS wac FROM loan_tape"
    ).fetchone()[0]
    print(f"WAC (weighted avg coupon): {wac:.3f}%")

    top_states = con.execute(
        """
        SELECT state, COUNT(*) AS n, SUM(orig_balance) AS bal
        FROM loan_tape
        GROUP BY 1
        ORDER BY bal DESC
        LIMIT 5
        """
    ).fetchall()

    print("\nTop 5 states by balance:")
    for s, n, bal in top_states:
        print(f"  {s}: loans={n:,} balance=${bal:,.0f}")

    bands = con.execute(
        """
        SELECT CASE
                   WHEN fico < 660 THEN 'subprime'
                   WHEN fico < 720 THEN 'near_prime'
                   WHEN fico < 780 THEN 'prime'
                   ELSE 'super_prime'
                   END  AS fico_band,
               CASE
                   WHEN ltv <= 60 THEN '<=60'
                   WHEN ltv <= 80 THEN '60-80'
                   WHEN ltv <= 90 THEN '80-90'
                   ELSE '90+'
                   END  AS ltv_band,
               COUNT(*) AS n
        FROM loan_tape
        GROUP BY 1, 2
        ORDER BY n DESC LIMIT 10
        """
    ).fetchall()

    print("\nMost common FICO/LTV bands (top 10):")
    for fb, lb, n in bands:
        print(f"  {fb:12s} | {lb:5s} | {n:,}")

    con.close()
    print("\n✅ DuckDB table created: loan_tape")


if __name__ == "__main__":
    main()
