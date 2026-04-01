# metric-drift

## What
dbt project that detects when business metrics drift from expected ranges using z-score analysis on rolling windows.

## Stack
- dbt-core 1.11 + dbt-snowflake
- Snowflake (trial account)
- Python 3.12 venv in `.venv/`

## Conventions
- Activate venv before running dbt: `source .venv/bin/activate`
- Snowflake password is via env var `SNOWFLAKE_PASSWORD` — never hardcode
- Model layers: staging → intermediate → marts
- Seeds provide synthetic data — no real company data

## Profile
- Profile name: `metric_drift` (in `~/.dbt/profiles.yml`)
- Account: parwfuh-btb55676
- User: rakpatel
