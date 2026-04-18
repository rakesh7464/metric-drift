-- =============================================================================
-- Custom DMFs: Value range validation
--
-- Each DMF returns a COUNT of violating rows (expected: 0).
-- Non-zero result = data quality failure.
--
-- Violations injected in seed data:
--   - conversion_rate = 1.15 on 2026-03-01 (product) → DMF_CONVERSION_RATE_VALID returns 1
--   - revenue = -500 on 2026-03-05 (all) → DMF_REVENUE_NON_NEGATIVE returns 1
--   - active_users = 0 on 2026-03-10 (all) → DMF_ACTIVE_USERS_POSITIVE returns 1
-- =============================================================================

-- conversion_rate must be between 0 and 1 (it's a ratio, not a percentage)
CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_CONVERSION_RATE_VALID(
    ARG_T TABLE(metric_name VARCHAR, metric_value FLOAT)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT_IF(
        metric_name = 'conversion_rate'
        AND (metric_value < 0 OR metric_value > 1)
    )
    FROM ARG_T
$$;

-- revenue cannot be negative
CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_REVENUE_NON_NEGATIVE(
    ARG_T TABLE(metric_name VARCHAR, metric_value FLOAT)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT_IF(metric_name = 'revenue' AND metric_value < 0)
    FROM ARG_T
$$;

-- active_users must be > 0 (zero users implies a pipeline failure, not a real value)
CREATE OR REPLACE DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_ACTIVE_USERS_POSITIVE(
    ARG_T TABLE(metric_name VARCHAR, metric_value FLOAT)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT_IF(metric_name = 'active_users' AND metric_value <= 0)
    FROM ARG_T
$$;

-- Assign all three to the seed table
ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_CONVERSION_RATE_VALID
    ON (metric_name, metric_value);

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_REVENUE_NON_NEGATIVE
    ON (metric_name, metric_value);

ALTER TABLE METRIC_DRIFT.DEV_SEEDS.RAW_DAILY_METRICS
    ADD DATA METRIC FUNCTION METRIC_DRIFT.DEV.DMF_ACTIVE_USERS_POSITIVE
    ON (metric_name, metric_value);
