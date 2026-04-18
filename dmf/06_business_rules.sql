-- =============================================================================
-- Custom DMFs: Business rule enforcement
--
-- These are the continuous, always-on equivalents of the dbt accepted_values
-- and custom tests already present in _marts.yml. They run on a schedule
-- rather than only at dbt build time.
-- =============================================================================

-- drift_severity must be one of: critical, warning, watch, normal
-- Returns count of rows with unexpected values (expected: 0)
CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_SEVERITY_VALID(
    ARG_T TABLE(drift_severity VARCHAR)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT_IF(
        drift_severity NOT IN ('critical', 'warning', 'watch', 'normal')
    )
    FROM ARG_T
$$;

-- z_score must be within a plausible range: -10 to +10
-- Values outside this range almost certainly indicate a data error, not a real anomaly
-- Returns count of implausible rows (expected: 0)
CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_ZSCORE_PLAUSIBLE(
    ARG_T TABLE(z_score FLOAT)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT_IF(ABS(z_score) > 10)
    FROM ARG_T
$$;

-- z_score readiness: count rows where z_score IS NULL
-- In this project, z_score is NULL when rolling_14d_count < 7 (insufficient history)
-- Returns count of rows without a z_score (warn, not fail — expected during ramp-up)
CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_Z_SCORE_READINESS(
    ARG_T TABLE(z_score FLOAT)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT_IF(z_score IS NULL) FROM ARG_T
$$;

-- Assign to fct_metric_drift
ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_SEVERITY_VALID
    ON (drift_severity);

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_ZSCORE_PLAUSIBLE
    ON (z_score);

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_Z_SCORE_READINESS
    ON (z_score);
