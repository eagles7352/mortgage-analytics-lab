from __future__ import annotations

import numpy as np
import polars as pl


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

    orig_bal = np.clip(
        rng.lognormal(mean=np.log(275_000), sigma=0.55, size=n),
        40_000,
        1_500_000
    ).round(0).astype(int)

    return pl.DataFrame(
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
