-- =============================================================================
-- System DMFs: NULL_COUNT, ROW_COUNT, FRESHNESS, DUPLICATE_COUNT, MIN, MAX
-- Applied to the seed table and fct_metric_drift mart.
--
-- Run AFTER dbt seed + dbt run in dev:
--   snowsql or Snowflake worksheet, role = ACCOUNTADMIN
--
-- Schema naming: dbt resolves +schema: seeds → DEV_SEEDS, +schema: marts → DEV_MARTS
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Seed table: RAW_DAILY_METRICS
-- ---------------------------------------------------------------------------

-- Null check on metric_value — catches corrupt upstream feeds
ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    ON (metric_value);

-- Row count — expected: 540+ rows; drop signals partial load
ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT
    ON ();

-- Freshness on metric_date — will show stale (seed ends 2026-04-05); intentional
ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS
    ON (metric_date);

-- Min/max on metric_value — bounds tracking for the full dataset
ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MIN
    ON (metric_value);

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.MAX
    ON (metric_value);

-- Schedule: every 60 minutes (seed only changes on dbt seed)
ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    SET DATA_METRIC_SCHEDULE = '60 MINUTE';

-- ---------------------------------------------------------------------------
-- Mart: FCT_METRIC_DRIFT
-- ---------------------------------------------------------------------------

-- Null check on z_score — continuous equivalent of dbt not_null test
ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    ON (z_score);

-- Null check on drift_severity — ensures classification logic ran for all rows
ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
    ON (drift_severity);

-- Duplicate check on composite grain (metric_date, metric_name, segment)
ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT
    ON (metric_date, metric_name, segment);

-- Schedule: fire immediately after every dbt run (table is fully rebuilt each time)
ALTER TABLE METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
