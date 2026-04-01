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
- Seeds provide synthetic data (3 metrics × ~46 days = 138 rows) — no real company data
- Local dev uses `DEV` schema; CI uses `CI` schema

## Profile
- Profile name: `metric_drift` (in `~/.dbt/profiles.yml`)
- Account: parwfuh-btb55676
- User: rakpatel

## CI
- GitHub Actions workflow in `.github/workflows/dbt-ci.yml`
- Runs on push/PR to main: dbt debug → seed → run → test
- Snowflake password stored as GitHub secret `SNOWFLAKE_PASSWORD`

## Models
- `stg_daily_metrics` (view) — casts types, adds day_of_week/is_weekend
- `int_metric_rolling_stats` (view) — 14-day rolling avg/stddev, z-score
- `fct_metric_drift` (table) — classifies drift severity (critical/warning/watch/normal), direction, pct deviation
- `fct_drift_alerts` (table) — drift events only, enriched with day-over-day change and consecutive alert context
- `fct_drift_insights` (table) — AI-generated plain-English explanations via Snowflake Cortex COMPLETE (snowflake-arctic model)
