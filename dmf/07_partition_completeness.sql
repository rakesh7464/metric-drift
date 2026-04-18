-- =============================================================================
-- Custom DMF: Date partition completeness
--
-- Returns the count of gaps > 1 day in the metric_date series.
-- A gap means at least one expected daily partition has no data.
--
-- The seed data has a deliberate 5-day gap (2026-03-15 to 2026-03-19) to
-- demonstrate this check firing. Expected return value: 1 (one gap detected).
--
-- Applied to both the seed table and fct_metric_drift.
-- =============================================================================

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_DATE_GAPS(
    ARG_T TABLE(metric_date DATE)
)
RETURNS NUMBER
AS
$$
    WITH ordered AS (
        SELECT DISTINCT
            metric_date,
            LAG(metric_date) OVER (ORDER BY metric_date) AS prev_date
        FROM ARG_T
    )
    SELECT COUNT_IF(DATEDIFF(day, prev_date, metric_date) > 1)
    FROM ordered
    WHERE prev_date IS NOT NULL
$$;

-- Assign to seed table (checks raw data completeness)
ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_DATE_GAPS
    ON (metric_date);

-- Assign to mart (checks that gaps don't propagate downstream)
ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_DATE_GAPS
    ON (metric_date);
