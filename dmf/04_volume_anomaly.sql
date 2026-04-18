-- =============================================================================
-- Custom DMF: Volume anomaly detection
--
-- Compares the most recent date's row count against the historical per-date
-- average. Returns 1 if the latest date's count is > 2 standard deviations
-- from the mean (anomalous), 0 if normal.
--
-- Applied to fct_metric_drift — each date should have exactly 6 rows
-- (3 metrics × 2 segments). Any deviation indicates a pipeline issue.
-- =============================================================================

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_VOLUME_ANOMALY(
    ARG_T TABLE(metric_date DATE)
)
RETURNS NUMBER
AS
$$
    WITH daily_counts AS (
        SELECT metric_date, COUNT(*) AS row_count
        FROM ARG_T
        GROUP BY metric_date
    ),
    latest_date AS (
        SELECT MAX(metric_date) AS dt FROM daily_counts
    ),
    historical_stats AS (
        -- Exclude the latest date to avoid comparing today to itself
        SELECT
            AVG(row_count)    AS avg_count,
            STDDEV(row_count) AS stddev_count
        FROM daily_counts
        WHERE metric_date < (SELECT dt FROM latest_date)
    ),
    latest_count AS (
        SELECT row_count
        FROM daily_counts
        WHERE metric_date = (SELECT dt FROM latest_date)
    )
    SELECT
        CASE
            WHEN h.stddev_count > 0
                 AND ABS((l.row_count - h.avg_count) / h.stddev_count) > 2
            THEN 1
            ELSE 0
        END
    FROM historical_stats h, latest_count l
$$;

-- Assign to fct_metric_drift (fires on every dbt run via TRIGGER_ON_CHANGES)
ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_VOLUME_ANOMALY
    ON (metric_date);
