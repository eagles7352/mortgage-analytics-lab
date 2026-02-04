from __future__ import annotations

from pathlib import Path
import numpy as np
import polars as pl
import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB_PATH = ROOT / "db" / "mortgage.duckdb"

def make_loans(n: int = 50_000, seed: int = 42) -> pl.DataFrame:
    rng = np.random.default_rng(seed)

    states = np.array(["NJ", "NY", "PA", "CT", "MA", "MD", "VA", "NC", "FL", "CA", "TX"])
    terms = np.array([180, 240, 360])  # months: 15/20/30y

    fico = np.clip(rng.normal(720, 45, n).round().astype(int), 560, 850)
    ltv = np.clip(rng.normal(78, 12, n), 30, 97).round(1)

    # rate loosely correlated with fico/ltv (higher fico/lower ltv -> lower rate)
    base = rng.normal(6.25, 0.75, n)
    rate = base - (fico - 720) * 0.004 + (ltv - 80) * 0.015
    rate = np.clip(rate, 2.5, 12.0).round(3)

    orig_bal = np.clip(rng.lognormal(mean=np.log(275_000), sigma=0.55, size=n), 40_000, 1_500_000).round(0).astype(int)

    df = pl.DataFrame(
        {
            "loan_id": np.arange(1, n + 1, dtype=np.int64),
            "state": rng.choice(states, size=n, replace=True),
            "term_months": rng.choice(terms, size=n, replace=True),
            "fico": fico,
            "ltv": ltv,
            "note_rate": rate,
            "orig_balance": orig_bal,
        }
    )

    # Simple derived buckets for reporting
    df = df.with_columns(
        [
            pl.when(pl.col("fico") < 660).then(pl.lit("subprime"))
            .when(pl.col("fico") < 720).then(pl.lit("near_prime"))
            .when(pl.col("fico") < 780).then(pl.lit("prime"))
            .otherwise(pl.lit("super_prime"))
            .alias("fico_band"),

            pl.when(pl.col("ltv") <= 60).then(pl.lit("<=60"))
            .when(pl.col("ltv") <= 80).then(pl.lit("60-80"))
            .when(pl.col("ltv") <= 90).then(pl.lit("80-90"))
            .otherwise(pl.lit("90+"))
            .alias("ltv_band"),
        ]
    )

    return df

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    loans = make_loans()
    out_path = DATA_DIR / "loan_tape.parquet"
    loans.write_parquet(out_path)

    print(f"✅ Wrote {loans.height:,} loans → {out_path}")

    # Load into DuckDB (table will live in db/mortgage.duckdb, which we ignore in git)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE OR REPLACE TABLE loan_tape AS SELECT * FROM read_parquet(?)", [str(out_path)])

    # A few quick rollups (these become recruiter-friendly outputs)
    print("\n--- Portfolio rollups (DuckDB SQL) ---")

    wac = con.execute("SELECT SUM(note_rate * orig_balance) / SUM(orig_balance) AS wac FROM loan_tape").fetchone()[0]
    print(f"WAC (weighted avg coupon): {wac:.3f}%")

    top_states = con.execute("""
        SELECT state, COUNT(*) AS n, SUM(orig_balance) AS bal
        FROM loan_tape
        GROUP BY 1
        ORDER BY bal DESC
        LIMIT 5
    """).fetchall()
    print("\nTop 5 states by balance:")
    for s, n, bal in top_states:
        print(f"  {s}: loans={n:,} balance=${bal:,.0f}")

    bands = con.execute("""
        SELECT fico_band, ltv_band, COUNT(*) AS n
        FROM loan_tape
        GROUP BY 1,2
        ORDER BY n DESC
        LIMIT 10
    """).fetchall()
    print("\nMost common FICO/LTV bands (top 10):")
    for fb, lb, n in bands:
        print(f"  {fb:12s} | {lb:5s} | {n:,}")

    con.close()
    print("\n✅ DuckDB table created: loan_tape")

if __name__ == "__main__":
    main()
