# metric-drift

dbt project that detects when business metrics drift from expected ranges using z-score analysis on rolling windows.

## How it works

Synthetic daily metrics flow through three dbt layers:

1. **Staging** — cleans raw seed data, adds `is_weekend` flag
2. **Intermediate** — computes 14-day rolling average, stddev, and z-score per metric
3. **Marts** — classifies drift severity and flags alerts

### Severity thresholds

| Severity | Z-score |
|----------|---------|
| critical | \|z\| >= 3.0 |
| warning  | \|z\| >= 2.0 |
| watch    | \|z\| >= 1.5 |
| normal   | \|z\| < 1.5 |

## Example output

Running against the included seed data (90 days, 3 metrics), the mart produces **6 drift alerts**:

| metric | day | value | 14d avg | z-score | severity |
|--------|-----|-------|---------|---------|----------|
| revenue | 31 | 32,400 | 51,386 | -2.95 | critical |
| revenue | 45 | 61,500 | 53,250 | 2.25 | warning |
| active_users | 31 | 7,200 | 12,221 | -2.75 | critical |
| active_users | 45 | 15,200 | 12,736 | 2.18 | warning |
| conversion_rate | 31 | 0.018 | 0.034 | -2.62 | critical |
| conversion_rate | 45 | 0.042 | 0.035 | 2.10 | warning |

Day 31 has an intentional dip across all metrics; day 45 has a spike.

## DMF quality layer

On top of dbt's build-time tests, this project uses Snowflake **Data Metric Functions (DMFs)** for continuous, post-deployment data quality monitoring — the same pattern used by tools like Monte Carlo and Bigeye, built natively on Snowflake.

dbt tests run assertions at build time. DMFs run on a schedule (or on every data change) and record results into `SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS`. The `fct_dmf_quality_log` mart reads that history and classifies each measurement as `pass`, `warn`, or `fail`.

### Checks implemented

| Category | DMF | What it catches |
|---|---|---|
| Completeness | `SNOWFLAKE.CORE.NULL_COUNT` | Unexpected NULLs in key columns |
| Uniqueness | `SNOWFLAKE.CORE.DUPLICATE_COUNT` | Duplicate rows on composite grain |
| Freshness | `SNOWFLAKE.CORE.FRESHNESS` | Stale data (no new rows loaded) |
| Volume | `SNOWFLAKE.CORE.ROW_COUNT` | Empty tables |
| Bounds | `SNOWFLAKE.CORE.MIN` / `MAX` | Numeric range tracking |
| Cardinality | `DMF_METRIC_NAME_CARDINALITY` | Unexpected new metric_name values |
| Value ranges | `DMF_CONVERSION_RATE_VALID` | conversion_rate outside [0, 1] |
| Value ranges | `DMF_REVENUE_NON_NEGATIVE` | Negative revenue |
| Value ranges | `DMF_ACTIVE_USERS_POSITIVE` | Zero or negative active_users |
| Volume anomaly | `DMF_VOLUME_ANOMALY` | Row count > 2σ from historical mean |
| Distribution drift | `DMF_MEAN_DRIFT` | Mean z-score shift vs historical |
| Business rules | `DMF_SEVERITY_VALID` | Invalid drift_severity values |
| Business rules | `DMF_ZSCORE_PLAUSIBLE` | Z-scores outside ±10 (data error) |
| Readiness | `DMF_Z_SCORE_READINESS` | Rows lacking enough history for z-score |
| Partitioning | `DMF_DATE_GAPS` | Gaps > 1 day in the date series |

The seed data contains deliberate violations (negative revenue, out-of-range conversion_rate, zero active_users, an unexpected metric name, and a 5-day date gap) so `fct_dmf_quality_log` demonstrates real `fail` and `warn` results.

DMF definitions live in `dmf/`. Run `dmf/08_assign_dmfs_dev.sql` once in a Snowflake worksheet after `dbt seed` + `dbt run`. CI assigns DMFs automatically via GitHub Actions.

## Tech stack

- dbt-core 1.11 + dbt-snowflake
- Snowflake (Enterprise — required for DMFs)
- Python 3.12

## Quick setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install dbt-core dbt-snowflake

# configure ~/.dbt/profiles.yml with your Snowflake credentials
# profile name: metric_drift

dbt seed      # load synthetic data (540 rows, 2 segments, deliberate violations)
dbt run       # build models
dbt test      # run schema tests

# then in a Snowflake worksheet: execute dmf/08_assign_dmfs_dev.sql
# run dbt run again to trigger TRIGGER_ON_CHANGES on the mart
# query fct_dmf_quality_log to see DMF results
```

## Project structure

```
models/
  staging/       stg_daily_metrics        — clean seed data
  intermediate/  int_metric_rolling_stats  — 14-day rolling stats + z-score
  marts/         fct_metric_drift          — drift classification + alerts
                 fct_drift_alerts          — alert events only
                 fct_drift_insights        — Cortex AI explanations
                 fct_dmf_quality_log       — DMF quality monitoring history
dmf/
  01_system_dmfs.sql                       — system DMF assignments
  02_cardinality.sql                       — cardinality DMF
  03_value_ranges.sql                      — value range DMFs
  04_volume_anomaly.sql                    — volume anomaly DMF
  05_distribution_drift.sql               — mean drift DMF
  06_business_rules.sql                    — business rule DMFs
  07_partition_completeness.sql           — date gap DMF
  08_assign_dmfs_dev.sql                  — run once in dev
  09_assign_dmfs_ci.sql                   — used by GitHub Actions
seeds/
  raw_daily_metrics.csv                    — ~540 rows, 3 metrics, 2 segments
```
