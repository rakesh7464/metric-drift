-- ============================================================
-- DRIFT LOG TABLE + SCHEDULED TASKS + ALERT
-- Sets up the automated drift detection pipeline.
-- ============================================================

-- ── Drift Log Table ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS CAMBRIA_DB.GOLD.DRIFT_LOG (
    run_date        DATE,
    source          STRING,   -- 'SLABS' or 'FAB'
    "Overall Severity"                    STRING,
    "Region"                              STRING,
    "Current Window"                      STRING,
    "Prior Window"                        STRING,
    -- Add remaining columns to match your drift query output
    inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Slabs Task — weekdays at 6am CT ────────────────────────
CREATE OR REPLACE TASK cambria_metric_drift_slabs_daily
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 6 * * 1-5 America/Chicago'
AS
  INSERT INTO CAMBRIA_DB.GOLD.DRIFT_LOG
  SELECT CURRENT_DATE AS run_date, 'SLABS' AS source, *
  FROM (
    -- paste metric_drift_slabs.sql contents here
    SELECT 1 -- placeholder
  );

-- ── Fab Task — weekdays at 6am CT ──────────────────────────
CREATE OR REPLACE TASK cambria_metric_drift_fab_daily
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 6 * * 1-5 America/Chicago'
AS
  INSERT INTO CAMBRIA_DB.GOLD.DRIFT_LOG
  SELECT CURRENT_DATE AS run_date, 'FAB' AS source, *
  FROM (
    -- paste metric_drift_fab.sql contents here
    SELECT 1 -- placeholder
  );

-- ── Resume Tasks ───────────────────────────────────────────
ALTER TASK cambria_metric_drift_slabs_daily RESUME;
ALTER TASK cambria_metric_drift_fab_daily   RESUME;

-- ── Alert on CRITICAL Results ──────────────────────────────
CREATE OR REPLACE ALERT cambria_drift_critical_alert
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 7 * * 1-5 America/Chicago'
  IF (EXISTS (
    SELECT 1 FROM CAMBRIA_DB.GOLD.DRIFT_LOG
    WHERE "Overall Severity" = 'CRITICAL'
    AND   run_date = CURRENT_DATE
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'cambria-data-alerts',
      'rakesh@cambria.com',
      'Metric Drift Alert — CRITICAL',
      'One or more regions have drifted critically. Review Sigma dashboard.'
    );
