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

## Tech stack

- dbt-core 1.11 + dbt-snowflake
- Snowflake
- Python 3.12

## Quick setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install dbt-core dbt-snowflake

# configure ~/.dbt/profiles.yml with your Snowflake credentials
# profile name: metric_drift

dbt seed      # load synthetic data
dbt run       # build models
dbt test      # run schema tests
```

## Project structure

```
models/
  staging/       stg_daily_metrics        — clean seed data
  intermediate/  int_metric_rolling_stats  — 14-day rolling stats + z-score
  marts/         fct_metric_drift          — drift classification + alerts
seeds/
  raw_daily_metrics.csv                    — 90 days × 3 metrics (synthetic)
```
