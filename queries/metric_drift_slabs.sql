-- ============================================================
-- METRIC DRIFT DETECTOR — SLABS
-- Cambria Commercial Analytics | Snowflake Cortex Code Skill
-- ============================================================
-- SOURCE:  PROD_VIEWS_SALES.SALES_WITH_DOLLARS.SLAB_SALES_SHIPMENTS
-- GROUPS:  SFAccountRegionName
-- METRICS: Ordered & Shipped (Total, Commercial, Residential)
--
-- DEFAULT CADENCE: Week-over-Week (rolling 7-day windows)
--   Current:  CURRENT_DATE - 7  →  CURRENT_DATE - 1
--   Prior:    CURRENT_DATE - 14 →  CURRENT_DATE - 8
--
-- TO CHANGE CADENCE: Edit the date_params CTE only.
-- ============================================================

WITH

-- ── STEP 1: Date Parameters (edit only this CTE) ─────────────
date_params AS (
    SELECT
        -- Current window: last 7 complete days
        DATEADD(day, -7,  CURRENT_DATE)::DATE   AS current_start,
        DATEADD(day, -1,  CURRENT_DATE)::DATE   AS current_end,

        -- Prior window: the 7 days before that
        DATEADD(day, -14, CURRENT_DATE)::DATE   AS prior_start,
        DATEADD(day, -8,  CURRENT_DATE)::DATE   AS prior_end,

        -- Thresholds
        15                                       AS threshold_pct,   -- WARNING
        30                                       AS critical_pct     -- CRITICAL

        -- ── Alternative Cadences ──────────────────────────────
        -- MTD vs Prior MTD:
        --   DATE_TRUNC('month', CURRENT_DATE)                         AS current_start,
        --   DATEADD(day, -1, CURRENT_DATE)                            AS current_end,
        --   DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE))     AS prior_start,
        --   DATEADD(day, -1, DATE_TRUNC('month', CURRENT_DATE))       AS prior_end,

        -- Rolling 30-day vs prior 30-day:
        --   DATEADD(day, -30, CURRENT_DATE)   AS current_start,
        --   DATEADD(day, -1,  CURRENT_DATE)   AS current_end,
        --   DATEADD(day, -60, CURRENT_DATE)   AS prior_start,
        --   DATEADD(day, -31, CURRENT_DATE)   AS prior_end,
),

-- ── STEP 2: Resolve Order Classification from Accounts ────────
-- Mirrors the classification logic used in volume_report_daily_materialized
cte_accounts AS (
    SELECT DISTINCT
        "Account ID",
        "Trade Type"
    FROM PROD_VIEWS_SALES.SALESFORCE.ACCOUNTS_NIGHTLY
    WHERE "Trade Type" IS NOT NULL
),

-- ── STEP 3: Current Window — Slabs Ordered ───────────────────
current_ordered AS (
    SELECT
        d.current_start,
        d.current_end,
        s."SFAccountRegionName",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(s."OrderJobClassification", 'Commercial') THEN s."OrderLineOrderedQuantityinStockUOM"
            WHEN CONTAINS(s."OrderJobClassification", 'Residential') THEN NULL
            WHEN NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Commercial' THEN s."OrderLineOrderedQuantityinStockUOM"
        END))                                       AS "Commercial Slabs Ordered",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(s."OrderJobClassification", 'Residential') THEN s."OrderLineOrderedQuantityinStockUOM"
            WHEN CONTAINS(s."OrderJobClassification", 'Commercial') THEN NULL
            WHEN NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Residential' THEN s."OrderLineOrderedQuantityinStockUOM"
        END))                                       AS "Residential Slabs Ordered",
        ZEROIFNULL(SUM(s."OrderLineOrderedQuantityinStockUOM"))
                                                    AS "Slabs Ordered"
    FROM PROD_VIEWS_SALES.SALES_WITH_DOLLARS.SLAB_SALES_SHIPMENTS s
    LEFT JOIN cte_accounts a ON s."SFAccountID" = a."Account ID"
    CROSS JOIN date_params d
    WHERE DATE(s."OrderDate") BETWEEN d.current_start AND d.current_end
    GROUP BY 1, 2, 3
),

-- ── STEP 4: Current Window — Slabs Shipped ───────────────────
current_shipped AS (
    SELECT
        d.current_start,
        d.current_end,
        s."SFAccountRegionName",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(s."OrderJobClassification", 'Commercial') THEN s."OrderLineShippedQuantityinStockUOM"
            WHEN CONTAINS(s."OrderJobClassification", 'Residential') THEN NULL
            WHEN NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Commercial' THEN s."OrderLineShippedQuantityinStockUOM"
        END))                                       AS "Commercial Slabs Shipped",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(s."OrderJobClassification", 'Residential') THEN s."OrderLineShippedQuantityinStockUOM"
            WHEN CONTAINS(s."OrderJobClassification", 'Commercial') THEN NULL
            WHEN NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Residential' THEN s."OrderLineShippedQuantityinStockUOM"
        END))                                       AS "Residential Slabs Shipped",
        ZEROIFNULL(SUM(s."OrderLineShippedQuantityinStockUOM"))
                                                    AS "Slabs Shipped"
    FROM PROD_VIEWS_SALES.SALES_WITH_DOLLARS.SLAB_SALES_SHIPMENTS s
    LEFT JOIN cte_accounts a ON s."SFAccountID" = a."Account ID"
    CROSS JOIN date_params d
    WHERE DATE(s."OrderLineLastActualShipDateTime") BETWEEN d.current_start AND d.current_end
    GROUP BY 1, 2, 3
),

-- ── STEP 5: Prior Window — Slabs Ordered ─────────────────────
prior_ordered AS (
    SELECT
        d.prior_start,
        d.prior_end,
        s."SFAccountRegionName",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(s."OrderJobClassification", 'Commercial') THEN s."OrderLineOrderedQuantityinStockUOM"
            WHEN CONTAINS(s."OrderJobClassification", 'Residential') THEN NULL
            WHEN NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Commercial' THEN s."OrderLineOrderedQuantityinStockUOM"
        END))                                       AS "Commercial Slabs Ordered",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(s."OrderJobClassification", 'Residential') THEN s."OrderLineOrderedQuantityinStockUOM"
            WHEN CONTAINS(s."OrderJobClassification", 'Commercial') THEN NULL
            WHEN NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Residential' THEN s."OrderLineOrderedQuantityinStockUOM"
        END))                                       AS "Residential Slabs Ordered",
        ZEROIFNULL(SUM(s."OrderLineOrderedQuantityinStockUOM"))
                                                    AS "Slabs Ordered"
    FROM PROD_VIEWS_SALES.SALES_WITH_DOLLARS.SLAB_SALES_SHIPMENTS s
    LEFT JOIN cte_accounts a ON s."SFAccountID" = a."Account ID"
    CROSS JOIN date_params d
    WHERE DATE(s."OrderDate") BETWEEN d.prior_start AND d.prior_end
    GROUP BY 1, 2, 3
),

-- ── STEP 6: Prior Window — Slabs Shipped ─────────────────────
prior_shipped AS (
    SELECT
        d.prior_start,
        d.prior_end,
        s."SFAccountRegionName",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(s."OrderJobClassification", 'Commercial') THEN s."OrderLineShippedQuantityinStockUOM"
            WHEN CONTAINS(s."OrderJobClassification", 'Residential') THEN NULL
            WHEN NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Commercial' THEN s."OrderLineShippedQuantityinStockUOM"
        END))                                       AS "Commercial Slabs Shipped",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(s."OrderJobClassification", 'Residential') THEN s."OrderLineShippedQuantityinStockUOM"
            WHEN CONTAINS(s."OrderJobClassification", 'Commercial') THEN NULL
            WHEN NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(s."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Residential' THEN s."OrderLineShippedQuantityinStockUOM"
        END))                                       AS "Residential Slabs Shipped",
        ZEROIFNULL(SUM(s."OrderLineShippedQuantityinStockUOM"))
                                                    AS "Slabs Shipped"
    FROM PROD_VIEWS_SALES.SALES_WITH_DOLLARS.SLAB_SALES_SHIPMENTS s
    LEFT JOIN cte_accounts a ON s."SFAccountID" = a."Account ID"
    CROSS JOIN date_params d
    WHERE DATE(s."OrderLineLastActualShipDateTime") BETWEEN d.prior_start AND d.prior_end
    GROUP BY 1, 2, 3
),

-- ── STEP 7: Combine All Windows Per Region ────────────────────
drift_raw AS (
    SELECT
        COALESCE(co."SFAccountRegionName", po."SFAccountRegionName",
                 cs."SFAccountRegionName", ps."SFAccountRegionName")
                                                    AS "SFAccountRegionName",

        -- Window labels
        po.prior_start::STRING   || ' → ' || po.prior_end::STRING     AS "Prior Window",
        co.current_start::STRING || ' → ' || co.current_end::STRING   AS "Current Window",

        -- Slabs Ordered
        ZEROIFNULL(po."Slabs Ordered")              AS "Prior Slabs Ordered",
        ZEROIFNULL(co."Slabs Ordered")              AS "Current Slabs Ordered",
        ROUND(DIV0(co."Slabs Ordered" - po."Slabs Ordered",
                   po."Slabs Ordered") * 100, 2)    AS "Slabs Ordered Drift %",

        ZEROIFNULL(po."Commercial Slabs Ordered")   AS "Prior Commercial Slabs Ordered",
        ZEROIFNULL(co."Commercial Slabs Ordered")   AS "Current Commercial Slabs Ordered",
        ROUND(DIV0(co."Commercial Slabs Ordered" - po."Commercial Slabs Ordered",
                   po."Commercial Slabs Ordered") * 100, 2)
                                                    AS "Commercial Slabs Ordered Drift %",

        ZEROIFNULL(po."Residential Slabs Ordered")  AS "Prior Residential Slabs Ordered",
        ZEROIFNULL(co."Residential Slabs Ordered")  AS "Current Residential Slabs Ordered",
        ROUND(DIV0(co."Residential Slabs Ordered" - po."Residential Slabs Ordered",
                   po."Residential Slabs Ordered") * 100, 2)
                                                    AS "Residential Slabs Ordered Drift %",

        -- Slabs Shipped
        ZEROIFNULL(ps."Slabs Shipped")              AS "Prior Slabs Shipped",
        ZEROIFNULL(cs."Slabs Shipped")              AS "Current Slabs Shipped",
        ROUND(DIV0(cs."Slabs Shipped" - ps."Slabs Shipped",
                   ps."Slabs Shipped") * 100, 2)    AS "Slabs Shipped Drift %",

        ZEROIFNULL(ps."Commercial Slabs Shipped")   AS "Prior Commercial Slabs Shipped",
        ZEROIFNULL(cs."Commercial Slabs Shipped")   AS "Current Commercial Slabs Shipped",
        ROUND(DIV0(cs."Commercial Slabs Shipped" - ps."Commercial Slabs Shipped",
                   ps."Commercial Slabs Shipped") * 100, 2)
                                                    AS "Commercial Slabs Shipped Drift %",

        ZEROIFNULL(ps."Residential Slabs Shipped")  AS "Prior Residential Slabs Shipped",
        ZEROIFNULL(cs."Residential Slabs Shipped")  AS "Current Residential Slabs Shipped",
        ROUND(DIV0(cs."Residential Slabs Shipped" - ps."Residential Slabs Shipped",
                   ps."Residential Slabs Shipped") * 100, 2)
                                                    AS "Residential Slabs Shipped Drift %"

    FROM prior_ordered   po
    FULL OUTER JOIN current_ordered  co ON po."SFAccountRegionName" = co."SFAccountRegionName"
    FULL OUTER JOIN prior_shipped    ps ON po."SFAccountRegionName" = ps."SFAccountRegionName"
    FULL OUTER JOIN current_shipped  cs ON po."SFAccountRegionName" = cs."SFAccountRegionName"
),

-- ── STEP 8: Severity Labeling ─────────────────────────────────
drift_labeled AS (
    SELECT
        dr.*,
        dp.threshold_pct,
        dp.critical_pct,

        -- Per-metric severity helpers
        CASE WHEN ABS("Slabs Ordered Drift %")               >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("Slabs Ordered Drift %")               >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                            AS "Slabs Ordered Severity",

        CASE WHEN ABS("Slabs Shipped Drift %")               >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("Slabs Shipped Drift %")               >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                            AS "Slabs Shipped Severity",

        CASE WHEN ABS("Commercial Slabs Ordered Drift %")    >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("Commercial Slabs Ordered Drift %")    >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                            AS "Commercial Ordered Severity",

        CASE WHEN ABS("Commercial Slabs Shipped Drift %")    >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("Commercial Slabs Shipped Drift %")    >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                            AS "Commercial Shipped Severity",

        CASE WHEN ABS("Residential Slabs Ordered Drift %")   >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("Residential Slabs Ordered Drift %")   >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                            AS "Residential Ordered Severity",

        CASE WHEN ABS("Residential Slabs Shipped Drift %")   >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("Residential Slabs Shipped Drift %")   >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                            AS "Residential Shipped Severity",

        -- Overall: worst single metric drives the row
        CASE
            WHEN GREATEST(
                ABS("Slabs Ordered Drift %"),
                ABS("Slabs Shipped Drift %"),
                ABS("Commercial Slabs Ordered Drift %"),
                ABS("Commercial Slabs Shipped Drift %"),
                ABS("Residential Slabs Ordered Drift %"),
                ABS("Residential Slabs Shipped Drift %")
            ) >= dp.critical_pct  THEN '🔴 CRITICAL'
            WHEN GREATEST(
                ABS("Slabs Ordered Drift %"),
                ABS("Slabs Shipped Drift %"),
                ABS("Commercial Slabs Ordered Drift %"),
                ABS("Commercial Slabs Shipped Drift %"),
                ABS("Residential Slabs Ordered Drift %"),
                ABS("Residential Slabs Shipped Drift %")
            ) >= dp.threshold_pct THEN '🟡 WARNING'
            ELSE '🟢 STABLE'
        END                                                  AS "Overall Severity"

    FROM drift_raw dr
    CROSS JOIN date_params dp
)

-- ── STEP 9: Final Output ──────────────────────────────────────
SELECT
    "Overall Severity",
    "SFAccountRegionName"                           AS "Region",
    "Current Window",
    "Prior Window",

    -- Slabs Ordered
    "Prior Slabs Ordered",
    "Current Slabs Ordered",
    "Slabs Ordered Drift %",
    "Slabs Ordered Severity",

    "Prior Commercial Slabs Ordered",
    "Current Commercial Slabs Ordered",
    "Commercial Slabs Ordered Drift %",
    "Commercial Ordered Severity",

    "Prior Residential Slabs Ordered",
    "Current Residential Slabs Ordered",
    "Residential Slabs Ordered Drift %",
    "Residential Ordered Severity",

    -- Slabs Shipped
    "Prior Slabs Shipped",
    "Current Slabs Shipped",
    "Slabs Shipped Drift %",
    "Slabs Shipped Severity",

    "Prior Commercial Slabs Shipped",
    "Current Commercial Slabs Shipped",
    "Commercial Slabs Shipped Drift %",
    "Commercial Shipped Severity",

    "Prior Residential Slabs Shipped",
    "Current Residential Slabs Shipped",
    "Residential Slabs Shipped Drift %",
    "Residential Shipped Severity"

FROM drift_labeled
ORDER BY
    CASE "Overall Severity"
        WHEN '🔴 CRITICAL' THEN 1
        WHEN '🟡 WARNING'  THEN 2
        ELSE 3
    END,
    ABS("Slabs Shipped Drift %") DESC;
