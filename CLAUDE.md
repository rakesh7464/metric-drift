# Metric Drift Detector

## Purpose
Automated alert system that detects volume metric drift across Cambria's slab and fabricated shipment data by Sales Region. Flags CRITICAL / WARNING / STABLE severity.

## Architecture
- Pure SQL (Snowflake dialect) — no Python
- Drift queries compare two rolling time windows (default: WoW)
- Results log to `CAMBRIA_DB.GOLD.DRIFT_LOG` via Snowflake Tasks
- Snowflake Alert triggers email on CRITICAL drift
- Cortex Code skill definition (`skill.yaml`) is a secondary feature

## Key Files
- `queries/metric_drift_slabs.sql` — Slab drift by SFAccountRegionName
- `queries/metric_drift_fab.sql` — Fab drift by SF Account Sales Region Name
- `queries/drift_log_setup.sql` — Log table, scheduled tasks, and alert setup
- `skill.yaml` — Cortex Code skill definition

## Source Tables
- `PROD_VIEWS_SALES.SALES_WITH_DOLLARS.SLAB_SALES_SHIPMENTS`
- `PROD_VIEWS_SALES.SALES_WITH_DOLLARS.FABRICATED_SALES_SHIPMENTS`
- `PROD_VIEWS_SALES.SALESFORCE.ACCOUNTS_NIGHTLY` (Commercial/Residential classification)

## Conventions
- To change cadence: edit only the `date_params` CTE in each SQL file
- Classification logic mirrors `volume_report_daily_materialized`
