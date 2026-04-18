-- =============================================================================
-- Consolidated DMF assignment — DEV schema
--
-- Run once after `dbt seed` + `dbt run` in dev:
--   1. Open a Snowflake worksheet
--   2. Set role to ACCOUNTADMIN
--   3. Execute this file
--
-- Re-run if you drop and recreate the project tables (e.g., after dbt clean).
-- Custom DMFs are created in METRIC_DRIFT.DEV schema.
-- Tables live in METRIC_DRIFT.DEV_SEEDS and METRIC_DRIFT.DEV_MARTS.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE METRIC_DRIFT;

-- ---------------------------------------------------------------------------
-- Step 1: Create custom DMFs
-- ---------------------------------------------------------------------------

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_METRIC_NAME_CARDINALITY(
    ARG_T TABLE(metric_name VARCHAR)
)
RETURNS NUMBER
AS $$ SELECT COUNT(DISTINCT metric_name) FROM ARG_T $$;

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_CONVERSION_RATE_VALID(
    ARG_T TABLE(metric_name VARCHAR, metric_value FLOAT)
)
RETURNS NUMBER
AS $$
    SELECT COUNT_IF(
        metric_name = 'conversion_rate' AND (metric_value < 0 OR metric_value > 1)
    ) FROM ARG_T
$$;

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_REVENUE_NON_NEGATIVE(
    ARG_T TABLE(metric_name VARCHAR, metric_value FLOAT)
)
RETURNS NUMBER
AS $$
    SELECT COUNT_IF(metric_name = 'revenue' AND metric_value < 0) FROM ARG_T
$$;

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_ACTIVE_USERS_POSITIVE(
    ARG_T TABLE(metric_name VARCHAR, metric_value FLOAT)
)
RETURNS NUMBER
AS $$
    SELECT COUNT_IF(metric_name = 'active_users' AND metric_value <= 0) FROM ARG_T
$$;

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_VOLUME_ANOMALY(
    ARG_T TABLE(metric_date DATE)
)
RETURNS NUMBER
AS $$
    WITH daily_counts AS (
        SELECT metric_date, COUNT(*) AS row_count FROM ARG_T GROUP BY metric_date
    ),
    latest_date AS (SELECT MAX(metric_date) AS dt FROM daily_counts),
    historical_stats AS (
        SELECT AVG(row_count) AS avg_count, STDDEV(row_count) AS stddev_count
        FROM daily_counts WHERE metric_date < (SELECT dt FROM latest_date)
    ),
    latest_count AS (
        SELECT row_count FROM daily_counts
        WHERE metric_date = (SELECT dt FROM latest_date)
    )
    SELECT CASE
        WHEN h.stddev_count > 0
             AND ABS((l.row_count - h.avg_count) / h.stddev_count) > 2 THEN 1
        ELSE 0
    END FROM historical_stats h, latest_count l
$$;

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_MEAN_DRIFT(
    ARG_T TABLE(metric_date DATE, metric_value FLOAT)
)
RETURNS FLOAT
AS $$
    WITH latest_date AS (SELECT MAX(metric_date) AS dt FROM ARG_T),
    historical AS (
        SELECT AVG(metric_value) AS hist_mean, STDDEV(metric_value) AS hist_std
        FROM ARG_T WHERE metric_date < (SELECT dt FROM latest_date)
    ),
    latest_avg AS (
        SELECT AVG(metric_value) AS latest_mean
        FROM ARG_T WHERE metric_date = (SELECT dt FROM latest_date)
    )
    SELECT CASE WHEN h.hist_std > 0
        THEN ROUND((l.latest_mean - h.hist_mean) / h.hist_std, 4)
        ELSE NULL END
    FROM historical h, latest_avg l
$$;

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_SEVERITY_VALID(
    ARG_T TABLE(drift_severity VARCHAR)
)
RETURNS NUMBER
AS $$
    SELECT COUNT_IF(
        drift_severity NOT IN ('critical', 'warning', 'watch', 'normal')
    ) FROM ARG_T
$$;

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_ZSCORE_PLAUSIBLE(
    ARG_T TABLE(z_score FLOAT)
)
RETURNS NUMBER
AS $$ SELECT COUNT_IF(ABS(z_score) > 10) FROM ARG_T $$;

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_Z_SCORE_READINESS(
    ARG_T TABLE(z_score FLOAT)
)
RETURNS NUMBER
AS $$ SELECT COUNT_IF(z_score IS NULL) FROM ARG_T $$;

CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_DATE_GAPS(
    ARG_T TABLE(metric_date DATE)
)
RETURNS NUMBER
AS $$
    WITH ordered AS (
        SELECT DISTINCT metric_date,
               LAG(metric_date) OVER (ORDER BY metric_date) AS prev_date
        FROM ARG_T
    )
    SELECT COUNT_IF(DATEDIFF(day, prev_date, metric_date) > 1)
    FROM ordered WHERE prev_date IS NOT NULL
$$;

-- ---------------------------------------------------------------------------
-- Step 2: Assign system DMFs to seed table
-- ---------------------------------------------------------------------------

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (metric_value);

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (metric_date);

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN ON (metric_value);

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX ON (metric_value);

-- ---------------------------------------------------------------------------
-- Step 3: Assign custom DMFs to seed table
-- ---------------------------------------------------------------------------

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_METRIC_NAME_CARDINALITY
    ON (metric_name);

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_CONVERSION_RATE_VALID
    ON (metric_name, metric_value);

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_REVENUE_NON_NEGATIVE
    ON (metric_name, metric_value);

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_ACTIVE_USERS_POSITIVE
    ON (metric_name, metric_value);

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_DATE_GAPS
    ON (metric_date);

-- Seed table runs on a schedule (only changes on dbt seed)
ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    SET DATA_METRIC_SCHEDULE = '60 MINUTE';

-- ---------------------------------------------------------------------------
-- Step 4: Assign system DMFs to mart
-- ---------------------------------------------------------------------------

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (z_score);

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (drift_severity);

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT
    ON (metric_date, metric_name, segment);

-- ---------------------------------------------------------------------------
-- Step 5: Assign custom DMFs to mart
-- ---------------------------------------------------------------------------

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_VOLUME_ANOMALY
    ON (metric_date);

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_MEAN_DRIFT
    ON (metric_date, metric_value);

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_SEVERITY_VALID
    ON (drift_severity);

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_ZSCORE_PLAUSIBLE
    ON (z_score);

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_Z_SCORE_READINESS
    ON (z_score);

ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_DATE_GAPS
    ON (metric_date);

-- Mart fires on every dbt run (table is fully rebuilt each time)
ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
