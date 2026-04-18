-- =============================================================================
-- Custom DMF: Distribution drift — mean drift z-score
--
-- Returns the z-score of the most recent date's mean metric_value relative to
-- the historical mean of the entire table (excluding the latest date).
--
-- Interpretation:
--   |return value| <= 2  → normal
--   |return value| >  2  → warn (meaningful distribution shift)
--   NULL                 → insufficient history or zero variance
--
-- Applied to fct_metric_drift to detect when the overall value distribution
-- has shifted relative to historical norms.
-- =============================================================================

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_MEAN_DRIFT(
    ARG_T TABLE(metric_date DATE, metric_value FLOAT)
)
RETURNS FLOAT
AS
$$
    WITH latest_date AS (
        SELECT MAX(metric_date) AS dt FROM ARG_T
    ),
    historical AS (
        SELECT
            AVG(metric_value)    AS hist_mean,
            STDDEV(metric_value) AS hist_std
        FROM ARG_T
        WHERE metric_date < (SELECT dt FROM latest_date)
    ),
    latest_avg AS (
        SELECT AVG(metric_value) AS latest_mean
        FROM ARG_T
        WHERE metric_date = (SELECT dt FROM latest_date)
    )
    SELECT
        CASE
            WHEN h.hist_std > 0
            THEN ROUND((l.latest_mean - h.hist_mean) / h.hist_std, 4)
            ELSE NULL
        END
    FROM historical h, latest_avg l
$$;

-- Assign to fct_metric_drift
ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_MEAN_DRIFT
    ON (metric_date, metric_value);
