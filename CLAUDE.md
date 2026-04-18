# metric-drift

## What
dbt project that detects when business metrics drift from expected ranges using z-score analysis on rolling windows.

## Stack
- dbt-core 1.11.7 + dbt-snowflake 1.11.3 (pinned in `requirements.txt`)
- Snowflake (trial account)
- Python 3.12 venv in `.venv/`

## Conventions
- Activate venv before running dbt: `source .venv/bin/activate`
- Snowflake password is via env var `SNOWFLAKE_PASSWORD` — never hardcode
- Model layers: staging → intermediate → marts
- Seeds provide synthetic data (3 metrics × 2 segments × ~90 days = ~540 rows) — no real company data
- Local dev uses `DEV` schema; CI uses `CI` schema

## Profile
- Profile name: `metric_drift` (in `~/.dbt/profiles.yml`)
- Account: parwfuh-btb55676
- User: rakpatel

## CI
- GitHub Actions workflow in `.github/workflows/dbt-ci.yml`
- Runs on push/PR to main: dbt debug → seed → run → DMF assignment → test
- Snowflake password stored as GitHub secret `SNOWFLAKE_PASSWORD`

## Models
- `stg_daily_metrics` (view) — casts types, adds day_of_week/is_weekend
- `int_metric_rolling_stats` (view) — 14-day rolling avg/stddev, z-score
- `fct_metric_drift` (table) — classifies drift severity (critical/warning/watch/normal), direction, pct deviation
- `fct_drift_alerts` (table) — drift events only, enriched with day-over-day change and consecutive alert context
- `fct_drift_insights` (table) — AI-generated plain-English explanations via Snowflake Cortex COMPLETE (snowflake-arctic model)
- `fct_dmf_quality_log` (table) — reads Snowflake DMF measurement history; requires DMFs to be assigned first (see `dmf/`)

## Seed data
- ~540 rows: 3 metrics × 2 segments (all, product) × ~90 days
- Deliberate violations injected: negative revenue (2026-03-05), conversion_rate=1.15 (2026-03-01 product), active_users=0 (2026-03-10), metric_name="sessions" (2 rows), 5-day date gap (2026-03-15 to 2026-03-19)

## DMF Layer
- SQL files in `dmf/` define 15 Data Metric Functions (9 custom + 6 system DMFs)
- Coverage: cardinality, value ranges, volume anomaly, distribution drift, business rules, z-score readiness, partition completeness, NULL/duplicate/freshness/row count
- Run `dmf/08_assign_dmfs_dev.sql` once in Snowflake after `dbt seed` + `dbt run`
- CI assigns DMFs automatically via `dmf/09_assign_dmfs_ci.sql` (GitHub Actions step after dbt run)
- `fct_dmf_quality_log` builds empty if DMFs not yet assigned — expected, all tests pass
- DMF assignment is idempotent: re-running raises non-fatal "already exists" errors (handled in CI script)
