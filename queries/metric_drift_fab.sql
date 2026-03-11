-- ============================================================
-- METRIC DRIFT DETECTOR — FAB (FABRICATED)
-- Cambria Commercial Analytics | Snowflake Cortex Code Skill
-- ============================================================
-- SOURCE:  PROD_VIEWS_SALES.SALES_WITH_DOLLARS.FABRICATED_SALES_SHIPMENTS
-- GROUPS:  SF Account Sales Region Name
-- METRICS: Ordered & Shipped SQFT (Total, Commercial, Residential)
-- FILTER:  "Is Sales Order Flag" = 'TRUE'  (mirrors DDL logic)
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
        DATEADD(day, -7,  CURRENT_DATE)::DATE   AS current_start,
        DATEADD(day, -1,  CURRENT_DATE)::DATE   AS current_end,
        DATEADD(day, -14, CURRENT_DATE)::DATE   AS prior_start,
        DATEADD(day, -8,  CURRENT_DATE)::DATE   AS prior_end,
        15                                       AS threshold_pct,
        30                                       AS critical_pct

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
cte_accounts AS (
    SELECT DISTINCT
        "Account ID",
        "Trade Type"
    FROM PROD_VIEWS_SALES.SALESFORCE.ACCOUNTS_NIGHTLY
    WHERE "Trade Type" IS NOT NULL
),

-- ── STEP 3: Current Window — FAB Ordered ─────────────────────
current_ordered AS (
    SELECT
        d.current_start,
        d.current_end,
        f."SF Account Sales Region Name",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(f."OrderJobClassification", 'Commercial') THEN f."OrderLineOrderedQuantity"
            WHEN CONTAINS(f."OrderJobClassification", 'Residential') THEN NULL
            WHEN NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Commercial' THEN f."OrderLineOrderedQuantity"
        END))                                       AS "Commercial SQFT Ordered",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(f."OrderJobClassification", 'Residential') THEN f."OrderLineOrderedQuantity"
            WHEN CONTAINS(f."OrderJobClassification", 'Commercial') THEN NULL
            WHEN NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Residential' THEN f."OrderLineOrderedQuantity"
        END))                                       AS "Residential SQFT Ordered",
        ZEROIFNULL(SUM(f."OrderLineOrderedQuantity"))
                                                    AS "SQFT Ordered"
    FROM PROD_VIEWS_SALES.SALES_WITH_DOLLARS.FABRICATED_SALES_SHIPMENTS f
    LEFT JOIN cte_accounts a ON f."PurchasingAccountID" = a."Account ID"
    CROSS JOIN date_params d
    WHERE f."Is Sales Order Flag" = 'TRUE'
      AND DATE(f."OrderHeaderDateTime") BETWEEN d.current_start AND d.current_end
    GROUP BY 1, 2, 3
),

-- ── STEP 4: Current Window — FAB Shipped ─────────────────────
current_shipped AS (
    SELECT
        d.current_start,
        d.current_end,
        f."SF Account Sales Region Name",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(f."OrderJobClassification", 'Commercial') THEN f."OrderLineShippedQuantity"
            WHEN CONTAINS(f."OrderJobClassification", 'Residential') THEN NULL
            WHEN NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Commercial' THEN f."OrderLineShippedQuantity"
        END))                                       AS "Commercial SQFT Shipped",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(f."OrderJobClassification", 'Residential') THEN f."OrderLineShippedQuantity"
            WHEN CONTAINS(f."OrderJobClassification", 'Commercial') THEN NULL
            WHEN NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Residential' THEN f."OrderLineShippedQuantity"
        END))                                       AS "Residential SQFT Shipped",
        ZEROIFNULL(SUM(f."OrderLineShippedQuantity"))
                                                    AS "SQFT Shipped"
    FROM PROD_VIEWS_SALES.SALES_WITH_DOLLARS.FABRICATED_SALES_SHIPMENTS f
    LEFT JOIN cte_accounts a ON f."PurchasingAccountID" = a."Account ID"
    CROSS JOIN date_params d
    WHERE f."Is Sales Order Flag" = 'TRUE'
      AND DATE(f."OrderLineLastActualShipDateTime") BETWEEN d.current_start AND d.current_end
    GROUP BY 1, 2, 3
),

-- ── STEP 5: Prior Window — FAB Ordered ───────────────────────
prior_ordered AS (
    SELECT
        d.prior_start,
        d.prior_end,
        f."SF Account Sales Region Name",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(f."OrderJobClassification", 'Commercial') THEN f."OrderLineOrderedQuantity"
            WHEN CONTAINS(f."OrderJobClassification", 'Residential') THEN NULL
            WHEN NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Commercial' THEN f."OrderLineOrderedQuantity"
        END))                                       AS "Commercial SQFT Ordered",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(f."OrderJobClassification", 'Residential') THEN f."OrderLineOrderedQuantity"
            WHEN CONTAINS(f."OrderJobClassification", 'Commercial') THEN NULL
            WHEN NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Residential' THEN f."OrderLineOrderedQuantity"
        END))                                       AS "Residential SQFT Ordered",
        ZEROIFNULL(SUM(f."OrderLineOrderedQuantity"))
                                                    AS "SQFT Ordered"
    FROM PROD_VIEWS_SALES.SALES_WITH_DOLLARS.FABRICATED_SALES_SHIPMENTS f
    LEFT JOIN cte_accounts a ON f."PurchasingAccountID" = a."Account ID"
    CROSS JOIN date_params d
    WHERE f."Is Sales Order Flag" = 'TRUE'
      AND DATE(f."OrderHeaderDateTime") BETWEEN d.prior_start AND d.prior_end
    GROUP BY 1, 2, 3
),

-- ── STEP 6: Prior Window — FAB Shipped ───────────────────────
prior_shipped AS (
    SELECT
        d.prior_start,
        d.prior_end,
        f."SF Account Sales Region Name",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(f."OrderJobClassification", 'Commercial') THEN f."OrderLineShippedQuantity"
            WHEN CONTAINS(f."OrderJobClassification", 'Residential') THEN NULL
            WHEN NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Commercial' THEN f."OrderLineShippedQuantity"
        END))                                       AS "Commercial SQFT Shipped",
        ZEROIFNULL(SUM(CASE
            WHEN CONTAINS(f."OrderJobClassification", 'Residential') THEN f."OrderLineShippedQuantity"
            WHEN CONTAINS(f."OrderJobClassification", 'Commercial') THEN NULL
            WHEN NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Commercial')
             AND NOT CONTAINS(COALESCE(f."OrderJobClassification", ''), 'Residential')
             AND a."Trade Type" = 'Residential' THEN f."OrderLineShippedQuantity"
        END))                                       AS "Residential SQFT Shipped",
        ZEROIFNULL(SUM(f."OrderLineShippedQuantity"))
                                                    AS "SQFT Shipped"
    FROM PROD_VIEWS_SALES.SALES_WITH_DOLLARS.FABRICATED_SALES_SHIPMENTS f
    LEFT JOIN cte_accounts a ON f."PurchasingAccountID" = a."Account ID"
    CROSS JOIN date_params d
    WHERE f."Is Sales Order Flag" = 'TRUE'
      AND DATE(f."OrderLineLastActualShipDateTime") BETWEEN d.prior_start AND d.prior_end
    GROUP BY 1, 2, 3
),

-- ── STEP 7: Combine All Windows Per Region ────────────────────
drift_raw AS (
    SELECT
        COALESCE(co."SF Account Sales Region Name", po."SF Account Sales Region Name",
                 cs."SF Account Sales Region Name", ps."SF Account Sales Region Name")
                                                    AS "SF Account Sales Region Name",

        po.prior_start::STRING   || ' → ' || po.prior_end::STRING     AS "Prior Window",
        co.current_start::STRING || ' → ' || co.current_end::STRING   AS "Current Window",

        -- SQFT Ordered
        ZEROIFNULL(po."SQFT Ordered")               AS "Prior SQFT Ordered",
        ZEROIFNULL(co."SQFT Ordered")               AS "Current SQFT Ordered",
        ROUND(DIV0(co."SQFT Ordered" - po."SQFT Ordered",
                   po."SQFT Ordered") * 100, 2)     AS "SQFT Ordered Drift %",

        ZEROIFNULL(po."Commercial SQFT Ordered")    AS "Prior Commercial SQFT Ordered",
        ZEROIFNULL(co."Commercial SQFT Ordered")    AS "Current Commercial SQFT Ordered",
        ROUND(DIV0(co."Commercial SQFT Ordered" - po."Commercial SQFT Ordered",
                   po."Commercial SQFT Ordered") * 100, 2)
                                                    AS "Commercial SQFT Ordered Drift %",

        ZEROIFNULL(po."Residential SQFT Ordered")   AS "Prior Residential SQFT Ordered",
        ZEROIFNULL(co."Residential SQFT Ordered")   AS "Current Residential SQFT Ordered",
        ROUND(DIV0(co."Residential SQFT Ordered" - po."Residential SQFT Ordered",
                   po."Residential SQFT Ordered") * 100, 2)
                                                    AS "Residential SQFT Ordered Drift %",

        -- SQFT Shipped
        ZEROIFNULL(ps."SQFT Shipped")               AS "Prior SQFT Shipped",
        ZEROIFNULL(cs."SQFT Shipped")               AS "Current SQFT Shipped",
        ROUND(DIV0(cs."SQFT Shipped" - ps."SQFT Shipped",
                   ps."SQFT Shipped") * 100, 2)     AS "SQFT Shipped Drift %",

        ZEROIFNULL(ps."Commercial SQFT Shipped")    AS "Prior Commercial SQFT Shipped",
        ZEROIFNULL(cs."Commercial SQFT Shipped")    AS "Current Commercial SQFT Shipped",
        ROUND(DIV0(cs."Commercial SQFT Shipped" - ps."Commercial SQFT Shipped",
                   ps."Commercial SQFT Shipped") * 100, 2)
                                                    AS "Commercial SQFT Shipped Drift %",

        ZEROIFNULL(ps."Residential SQFT Shipped")   AS "Prior Residential SQFT Shipped",
        ZEROIFNULL(cs."Residential SQFT Shipped")   AS "Current Residential SQFT Shipped",
        ROUND(DIV0(cs."Residential SQFT Shipped" - ps."Residential SQFT Shipped",
                   ps."Residential SQFT Shipped") * 100, 2)
                                                    AS "Residential SQFT Shipped Drift %"

    FROM prior_ordered   po
    FULL OUTER JOIN current_ordered  co ON po."SF Account Sales Region Name" = co."SF Account Sales Region Name"
    FULL OUTER JOIN prior_shipped    ps ON po."SF Account Sales Region Name" = ps."SF Account Sales Region Name"
    FULL OUTER JOIN current_shipped  cs ON po."SF Account Sales Region Name" = cs."SF Account Sales Region Name"
),

-- ── STEP 8: Severity Labeling ─────────────────────────────────
drift_labeled AS (
    SELECT
        dr.*,
        dp.threshold_pct,
        dp.critical_pct,

        CASE WHEN ABS("SQFT Ordered Drift %")               >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("SQFT Ordered Drift %")               >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                           AS "SQFT Ordered Severity",

        CASE WHEN ABS("SQFT Shipped Drift %")               >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("SQFT Shipped Drift %")               >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                           AS "SQFT Shipped Severity",

        CASE WHEN ABS("Commercial SQFT Ordered Drift %")    >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("Commercial SQFT Ordered Drift %")    >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                           AS "Commercial Ordered Severity",

        CASE WHEN ABS("Commercial SQFT Shipped Drift %")    >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("Commercial SQFT Shipped Drift %")    >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                           AS "Commercial Shipped Severity",

        CASE WHEN ABS("Residential SQFT Ordered Drift %")   >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("Residential SQFT Ordered Drift %")   >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                           AS "Residential Ordered Severity",

        CASE WHEN ABS("Residential SQFT Shipped Drift %")   >= dp.critical_pct  THEN '🔴 CRITICAL'
             WHEN ABS("Residential SQFT Shipped Drift %")   >= dp.threshold_pct THEN '🟡 WARNING'
             ELSE '🟢 STABLE' END                           AS "Residential Shipped Severity",

        CASE
            WHEN GREATEST(
                ABS("SQFT Ordered Drift %"),
                ABS("SQFT Shipped Drift %"),
                ABS("Commercial SQFT Ordered Drift %"),
                ABS("Commercial SQFT Shipped Drift %"),
                ABS("Residential SQFT Ordered Drift %"),
                ABS("Residential SQFT Shipped Drift %")
            ) >= dp.critical_pct  THEN '🔴 CRITICAL'
            WHEN GREATEST(
                ABS("SQFT Ordered Drift %"),
                ABS("SQFT Shipped Drift %"),
                ABS("Commercial SQFT Ordered Drift %"),
                ABS("Commercial SQFT Shipped Drift %"),
                ABS("Residential SQFT Ordered Drift %"),
                ABS("Residential SQFT Shipped Drift %")
            ) >= dp.threshold_pct THEN '🟡 WARNING'
            ELSE '🟢 STABLE'
        END                                                 AS "Overall Severity"

    FROM drift_raw dr
    CROSS JOIN date_params dp
)

-- ── STEP 9: Final Output ──────────────────────────────────────
SELECT
    "Overall Severity",
    "SF Account Sales Region Name"                  AS "Region",
    "Current Window",
    "Prior Window",

    -- SQFT Ordered
    "Prior SQFT Ordered",
    "Current SQFT Ordered",
    "SQFT Ordered Drift %",
    "SQFT Ordered Severity",

    "Prior Commercial SQFT Ordered",
    "Current Commercial SQFT Ordered",
    "Commercial SQFT Ordered Drift %",
    "Commercial Ordered Severity",

    "Prior Residential SQFT Ordered",
    "Current Residential SQFT Ordered",
    "Residential SQFT Ordered Drift %",
    "Residential Ordered Severity",

    -- SQFT Shipped
    "Prior SQFT Shipped",
    "Current SQFT Shipped",
    "SQFT Shipped Drift %",
    "SQFT Shipped Severity",

    "Prior Commercial SQFT Shipped",
    "Current Commercial SQFT Shipped",
    "Commercial SQFT Shipped Drift %",
    "Commercial Shipped Severity",

    "Prior Residential SQFT Shipped",
    "Current Residential SQFT Shipped",
    "Residential SQFT Shipped Drift %",
    "Residential Shipped Severity"

FROM drift_labeled
ORDER BY
    CASE "Overall Severity"
        WHEN '🔴 CRITICAL' THEN 1
        WHEN '🟡 WARNING'  THEN 2
        ELSE 3
    END,
    ABS("SQFT Shipped Drift %") DESC;
