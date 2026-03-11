# 📊 Metric Drift Detector v2.0
**Cambria Commercial Analytics | Cortex Code Skill**

---

## What It Does

Compares two rolling time windows across Cambria's slab and fabricated shipment data, grouped by Sales Region. Flags Ordered and Shipped volume drift (Total + Commercial + Residential) as CRITICAL / WARNING / STABLE — surfacing pipeline issues or business shifts **before** they reach Sigma dashboards or executive reports.

---

## Source Objects

| File | Source Table | Join | Group By |
|---|---|---|---|
| `metric_drift_slabs.sql` | `PROD_VIEWS_SALES.SALES_WITH_DOLLARS.SLAB_SALES_SHIPMENTS` | `ACCOUNTS_NIGHTLY` via `SFAccountID` | `SFAccountRegionName` |
| `metric_drift_fab.sql` | `PROD_VIEWS_SALES.SALES_WITH_DOLLARS.FABRICATED_SALES_SHIPMENTS` | `ACCOUNTS_NIGHTLY` via `PurchasingAccountID` | `SF Account Sales Region Name` |

Both files use `PROD_VIEWS_SALES.SALESFORCE.ACCOUNTS_NIGHTLY` to resolve Commercial/Residential classification — exactly matching the logic in `volume_report_daily_materialized`.

---

## Metrics Tracked

| Metric | Slabs | Fab |
|---|---|---|
| Total Ordered | Slabs Ordered | SQFT Ordered |
| Commercial Ordered | ✅ | ✅ |
| Residential Ordered | ✅ | ✅ |
| Total Shipped | Slabs Shipped | SQFT Shipped |
| Commercial Shipped | ✅ | ✅ |
| Residential Shipped | ✅ | ✅ |

---

## Severity Labels

| Label | Default Trigger |
|---|---|
| 🔴 CRITICAL | ≥ 30% drift on any metric |
| 🟡 WARNING | ≥ 15% drift on any metric |
| 🟢 STABLE | < 15% on all metrics |

Overall row severity is driven by the **worst single metric** on that row.

---

## How Dynamic Dates Work

All windows are computed in the `date_params` CTE at the top of each file:

```sql
date_params AS (
    SELECT
        DATEADD(day, -7,  CURRENT_DATE)::DATE   AS current_start,  -- WoW default
        DATEADD(day, -1,  CURRENT_DATE)::DATE   AS current_end,
        DATEADD(day, -14, CURRENT_DATE)::DATE   AS prior_start,
        DATEADD(day, -8,  CURRENT_DATE)::DATE   AS prior_end,
        15  AS threshold_pct,
        30  AS critical_pct
)
```

To switch cadence, edit **only `date_params`**. Commented alternatives for MTD and rolling 30-day are already in each file.

---

## Files

```
metric_drift_detector/
├── skill.yaml                  # Cortex Code skill definition
├── metric_drift_slabs.sql      # Slabs drift — SFAccountRegionName
├── metric_drift_fab.sql        # Fab drift — SF Account Sales Region Name
└── README.md
```

---

## Usage

### Cortex Code CLI
```bash
cortex run metric_drift_detector --view SLABS
cortex run metric_drift_detector --view FAB
```

### Snowflake Worksheet
Open either SQL file and run directly — dates auto-compute from `CURRENT_DATE`.

---

## Automate with Snowflake Tasks

```sql
-- Slabs — runs weekdays at 6am CT
CREATE OR REPLACE TASK cambria_metric_drift_slabs_daily
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 6 * * 1-5 America/Chicago'
AS
EXECUTE IMMEDIATE $$
  -- paste metric_drift_slabs.sql contents here
$$;

-- Fab — same schedule
CREATE OR REPLACE TASK cambria_metric_drift_fab_daily
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 6 * * 1-5 America/Chicago'
AS
EXECUTE IMMEDIATE $$
  -- paste metric_drift_fab.sql contents here
$$;

ALTER TASK cambria_metric_drift_slabs_daily RESUME;
ALTER TASK cambria_metric_drift_fab_daily   RESUME;
```

> **Tip:** Insert results into a log table first, then alert on it:
> ```sql
> INSERT INTO CAMBRIA_DB.GOLD.DRIFT_LOG
> SELECT CURRENT_DATE AS run_date, 'SLABS' AS source, * FROM ( <slabs query> );
> ```

---

## Order Classification Logic

Both files mirror the exact classification logic from `volume_report_daily_materialized`:

1. If `"OrderJobClassification"` contains `'Commercial'` → Commercial
2. If `"OrderJobClassification"` contains `'Residential'` → Residential
3. If neither, fall back to `"Trade Type"` from `ACCOUNTS_NIGHTLY`
4. Otherwise → unclassified (excluded from Commercial/Residential splits)
