-- =============================================================================
-- Custom DMF: Cardinality check on metric_name
--
-- Expected: exactly 3 distinct metric names (revenue, conversion_rate, active_users)
-- The injected "sessions" rows in the seed will cause this to return 4 → fail
-- =============================================================================

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_METRIC_NAME_CARDINALITY(
    ARG_T TABLE(metric_name VARCHAR)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(DISTINCT metric_name) FROM ARG_T
$$;

-- Assign to seed table
ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_METRIC_NAME_CARDINALITY
    ON (metric_name);
