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
  fico_band,
  ltv_band,
  COUNT(*) AS loan_count,
  SUM(orig_balance) AS total_balance,
  AVG(note_rate) AS avg_rate
FROM loan_tape
GROUP BY fico_band, ltv_band
ORDER BY total_balance DESC;
