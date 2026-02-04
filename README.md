# Mortgage Analytics Lab (Python + SQL + Parquet)

Portfolio-style analytics repo built to showcase an end-to-end workflow you’d use in pricing, portfolio analytics, and performance monitoring—especially in mortgage / insurance / structured products contexts.

This project is intentionally structured like a real analytics “starter framework”:
- **Python** for data engineering + modeling automation  
- **SQL** for analysis, reporting, and QA checks  
- **Parquet** for efficient columnar storage  
- **DuckDB** for fast local analytics + reproducible queries  

---

## What this project demonstrates

### Recruiter / Hiring Manager highlights
- Loan-level dataset pipeline: generate → validate → store → query
- Reproducible, reviewable structure (scripts + modular `src/`)
- SQL-based summarization suitable for monitoring / dashboards
- Clean dependency management (`requirements.txt` + local `.venv`)
- Strong repo hygiene (data + local DB artifacts ignored, folders preserved)

### Technical highlights
- Python 3.13 project using a local virtual environment (`.venv`)
- Libraries: Pandas / Polars, DuckDB, PyArrow (Parquet)
- DuckDB table creation + “sanity check” inserts/reads (quick verification that the stack works)
- Organized to expand into:
  - cashflow modeling (amortization, prepay/default assumptions)
  - scenario analysis + stress testing
  - portfolio rollups and KPI reporting

---

## Project structure

