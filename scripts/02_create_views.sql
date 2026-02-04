-- scripts/02_create_views.sql
-- Run inside DuckDB after loan_tape is loaded.

CREATE OR REPLACE VIEW v_state_summary AS
SELECT
  state,
  COUNT(*) AS loan_count,
  SUM(orig_balance) AS total_balance,
  AVG(note_rate) AS avg_rate,
  AVG(fico) AS avg_fico,
  AVG(ltv) AS avg_ltv
FROM loan_tape
GROUP BY state
ORDER BY total_balance DESC;

CREATE OR REPLACE VIEW v_fico_ltv_bands AS
SELECT
  CASE
    WHEN fico < 660 THEN 'subprime'
    WHEN fico < 720 THEN 'near_prime'
    WHEN fico < 780 THEN 'prime'
    ELSE 'super_prime'
  END AS fico_band,
  CASE
    WHEN ltv <= 60 THEN '<=60'
    WHEN ltv <= 80 THEN '60-80'
    WHEN ltv <= 90 THEN '80-90'
    ELSE '90+'
  END AS ltv_band,
  COUNT(*) AS n
FROM loan_tape
GROUP BY 1, 2
ORDER BY n DESC;

