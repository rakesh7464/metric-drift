"""
Check Glossary page — plain-English reference for all DMF checks.
"""

import streamlit as st
import pandas as pd


CHECKS = [
    # ── System DMFs ──────────────────────────────────────────────────────────
    {
        "type":        "System",
        "check":       "NULL_COUNT",
        "table":       "RAW_DAILY_METRICS",
        "what_it_does":"Counts rows where any tracked column is NULL.",
        "passes_when": "Returns 0 — no NULLs present.",
        "fails_when":  "Returns > 0 — one or more NULL values found.",
        "why_it_matters": "NULL metric values cause silent failures downstream — rolling averages, z-scores, and drift classification all produce wrong results when inputs are missing.",
    },
    {
        "type":        "System",
        "check":       "DUPLICATE_COUNT",
        "table":       "RAW_DAILY_METRICS",
        "what_it_does":"Counts duplicate rows in the table.",
        "passes_when": "Returns 0 — all rows are unique.",
        "fails_when":  "Returns > 0 — duplicate rows detected.",
        "why_it_matters": "Duplicates inflate metric values and distort averages, leading to false drift alerts or masking real ones.",
    },
    {
        "type":        "System",
        "check":       "ROW_COUNT",
        "table":       "RAW_DAILY_METRICS",
        "what_it_does":"Counts total rows in the table.",
        "passes_when": "Returns > 0 — table has data.",
        "fails_when":  "Returns 0 — table is empty.",
        "why_it_matters": "An empty table means the pipeline didn't load any data. Every downstream model and check would produce meaningless results.",
    },
    {
        "type":        "System",
        "check":       "FRESHNESS",
        "table":       "RAW_DAILY_METRICS",
        "what_it_does":"Measures how recently the table was updated.",
        "passes_when": "Data was loaded within the expected window.",
        "fails_when":  "Table hasn't been updated recently — data is stale.",
        "why_it_matters": "Stale data means yesterday's (or last week's) numbers are being served as current. Drift alerts become meaningless if the underlying data isn't fresh.",
    },
    {
        "type":        "System",
        "check":       "MIN",
        "table":       "FCT_METRIC_DRIFT",
        "what_it_does":"Returns the minimum metric value in the drift fact table.",
        "passes_when": "Always returns a value — used for range monitoring.",
        "fails_when":  "Not a pass/fail check; used to track value bounds over time.",
        "why_it_matters": "Tracking the minimum helps catch negative values or unexpected floor breaches that indicate data corruption.",
    },
    {
        "type":        "System",
        "check":       "MAX",
        "table":       "FCT_METRIC_DRIFT",
        "what_it_does":"Returns the maximum metric value in the drift fact table.",
        "passes_when": "Always returns a value — used for range monitoring.",
        "fails_when":  "Not a pass/fail check; used to track value bounds over time.",
        "why_it_matters": "Tracking the maximum helps catch runaway values or ceiling breaches caused by misconfigured tracking or test data leaking into production.",
    },
    # ── Custom DMFs ──────────────────────────────────────────────────────────
    {
        "type":        "Custom",
        "check":       "CARDINALITY_METRIC_NAME",
        "table":       "RAW_DAILY_METRICS",
        "what_it_does":"Counts the number of distinct metric names in the table.",
        "passes_when": "Returns exactly 3 (revenue, conversion_rate, active_users).",
        "fails_when":  "Returns any other value — a metric was added, removed, or misspelled.",
        "why_it_matters": "If a new metric name appears (e.g. 'sessions' from a bad load), or an expected one disappears, the drift models will silently produce incomplete results.",
    },
    {
        "type":        "Custom",
        "check":       "REVENUE_NON_NEGATIVE",
        "table":       "RAW_DAILY_METRICS",
        "what_it_does":"Counts rows where revenue is less than zero.",
        "passes_when": "Returns 0 — all revenue values are ≥ 0.",
        "fails_when":  "Returns > 0 — one or more negative revenue rows found.",
        "why_it_matters": "Revenue can never be negative in this model. A negative value indicates a bad transformation, a refund being miscategorized, or corrupted source data.",
    },
    {
        "type":        "Custom",
        "check":       "CONVERSION_RATE_VALID",
        "table":       "RAW_DAILY_METRICS",
        "what_it_does":"Counts rows where conversion_rate is outside the range [0, 1].",
        "passes_when": "Returns 0 — all conversion rates are between 0 and 1.",
        "fails_when":  "Returns > 0 — one or more rates exceed 1.0 or are negative.",
        "why_it_matters": "A conversion rate above 1.0 is mathematically impossible. It signals a denominator error (e.g. sessions undercounted) or a tracking bug.",
    },
    {
        "type":        "Custom",
        "check":       "ACTIVE_USERS_POSITIVE",
        "table":       "RAW_DAILY_METRICS",
        "what_it_does":"Counts rows where active_users is zero or less.",
        "passes_when": "Returns 0 — all active_users values are > 0.",
        "fails_when":  "Returns > 0 — one or more rows have zero or negative active users.",
        "why_it_matters": "Zero active users on any given day is almost always a data collection failure, not a real business event. It would cause division-by-zero errors in downstream rate calculations.",
    },
    {
        "type":        "Custom",
        "check":       "VOLUME_ANOMALY",
        "table":       "RAW_DAILY_METRICS",
        "what_it_does":"Checks whether the latest day's row count is statistically anomalous (more than 2 standard deviations from the historical daily mean).",
        "passes_when": "Returns 0 — row count is within the expected range.",
        "fails_when":  "Returns 1 — row count is a statistical outlier.",
        "why_it_matters": "A sudden spike or drop in the number of rows per day suggests a load failure, a duplicate batch being ingested, or a source system problem — even if individual row values look fine.",
    },
    {
        "type":        "Custom",
        "check":       "MEAN_DRIFT_Z_SCORE",
        "table":       "FCT_METRIC_DRIFT",
        "what_it_does":"Computes the z-score of the mean metric value relative to its historical distribution.",
        "passes_when": "Absolute z-score ≤ 2 — mean is within expected range.",
        "fails_when":  "Absolute z-score > 2 — mean has shifted significantly (warn threshold).",
        "why_it_matters": "A persistent shift in the mean — even without a single-day spike — can indicate a structural change in the business or a systematic data issue. This check catches slow drift that single-row checks miss.",
    },
    {
        "type":        "Custom",
        "check":       "SEVERITY_VALID",
        "table":       "FCT_METRIC_DRIFT",
        "what_it_does":"Counts rows in fct_metric_drift where drift_severity is not one of the four expected values (critical, warning, watch, normal).",
        "passes_when": "Returns 0 — all severity classifications are valid.",
        "fails_when":  "Returns > 0 — unexpected severity values present.",
        "why_it_matters": "Invalid severity values mean the classification logic in the dbt model broke or a new value was introduced without updating downstream expectations. Any dashboard or alert routing that depends on severity would silently misclassify events.",
    },
    {
        "type":        "Custom",
        "check":       "ZSCORE_PLAUSIBLE",
        "table":       "FCT_METRIC_DRIFT",
        "what_it_does":"Counts rows where the absolute z-score exceeds 10 — a threshold considered statistically implausible for this dataset.",
        "passes_when": "Returns 0 — all z-scores are within a plausible range.",
        "fails_when":  "Returns > 0 — extreme z-scores detected, likely from corrupted input data.",
        "why_it_matters": "A z-score of ±10 is effectively impossible in normal business metrics. If one appears, it means the rolling average or standard deviation was calculated on bad data, producing a meaningless alert.",
    },
    {
        "type":        "Custom",
        "check":       "DATE_GAPS",
        "table":       "RAW_DAILY_METRICS",
        "what_it_does":"Counts the number of gaps greater than one day between consecutive metric dates.",
        "passes_when": "Returns 0 — dates are contiguous with no missing days.",
        "fails_when":  "Returns > 0 — one or more multi-day gaps found.",
        "why_it_matters": "Missing dates break the 14-day rolling window calculations. If data wasn't loaded for several days, the rolling average becomes unreliable and drift alerts may fire spuriously when data resumes.",
    },
    {
        "type":        "Custom",
        "check":       "Z_SCORE_READINESS",
        "table":       "FCT_METRIC_DRIFT",
        "what_it_does":"Counts rows where z_score is NULL, which happens when fewer than 7 days of history are available to calculate a meaningful standard deviation.",
        "passes_when": "Returns 0 — all rows have a valid z-score.",
        "fails_when":  "Returns > 0 — some rows lack enough history for z-score calculation (warn, not fail).",
        "why_it_matters": "Z-scores require sufficient historical data to be meaningful. Early in a dataset's life, or after a gap, z-scores are unreliable. This check flags those rows so they aren't treated as confident drift signals.",
    },
]


def render():
    st.markdown("## Check Glossary")
    st.markdown(
        "Plain-English reference for every DMF check in the metric-drift quality layer. "
        "Use this to understand what each check measures, when it fires, and why it matters."
    )

    df = pd.DataFrame(CHECKS)

    # ── Filter controls ───────────────────────────────────────────────────────
    col_type, col_table, col_search = st.columns([1, 1, 2])
    with col_type:
        type_filter = st.selectbox("Type", ["All", "System", "Custom"])
    with col_table:
        table_filter = st.selectbox("Table", ["All"] + sorted(df["table"].unique().tolist()))
    with col_search:
        search = st.text_input("Search", placeholder="Filter by check name or keyword...")

    filtered = df.copy()
    if type_filter != "All":
        filtered = filtered[filtered["type"] == type_filter]
    if table_filter != "All":
        filtered = filtered[filtered["table"] == table_filter]
    if search:
        mask = filtered.apply(lambda row: search.lower() in row.to_string().lower(), axis=1)
        filtered = filtered[mask]

    st.caption(f"{len(filtered)} of {len(df)} checks shown")
    st.markdown("---")

    # ── Check cards ───────────────────────────────────────────────────────────
    for _, row in filtered.iterrows():
        type_color = "#3182CE" if row["type"] == "System" else "#38A169"
        type_bg    = "#EBF4FF" if row["type"] == "System" else "#F0FFF4"

        st.html(f"""
        <div style="border:1px solid #E2E8F0;border-radius:8px;padding:18px 20px;
                    margin-bottom:12px;background:#FFFFFF">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap">
                <span style="font-size:1rem;font-weight:700;color:#1A1A1A;font-family:monospace">
                    {row['check']}
                </span>
                <span style="background:{type_bg};color:{type_color};border-radius:4px;
                             padding:2px 8px;font-size:0.72rem;font-weight:600;
                             text-transform:uppercase;letter-spacing:0.05em">
                    {row['type']}
                </span>
                <span style="background:#F5F5F0;color:#718096;border-radius:4px;
                             padding:2px 8px;font-size:0.72rem;font-weight:500">
                    {row['table']}
                </span>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:10px">
                <div>
                    <div style="font-size:0.7rem;color:#718096;font-weight:600;
                                text-transform:uppercase;letter-spacing:0.05em;margin-bottom:3px">
                        What it does
                    </div>
                    <div style="font-size:0.85rem;color:#2D3748;line-height:1.5">
                        {row['what_it_does']}
                    </div>
                </div>
                <div>
                    <div style="font-size:0.7rem;color:#718096;font-weight:600;
                                text-transform:uppercase;letter-spacing:0.05em;margin-bottom:3px">
                        Why it matters
                    </div>
                    <div style="font-size:0.85rem;color:#2D3748;line-height:1.5">
                        {row['why_it_matters']}
                    </div>
                </div>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
                <div style="background:#F0FFF4;border-radius:6px;padding:8px 12px">
                    <span style="font-size:0.7rem;color:#38A169;font-weight:600;
                                 text-transform:uppercase;letter-spacing:0.05em">✓ Passes when</span>
                    <div style="font-size:0.82rem;color:#276749;margin-top:3px">{row['passes_when']}</div>
                </div>
                <div style="background:#FFF5F5;border-radius:6px;padding:8px 12px">
                    <span style="font-size:0.7rem;color:#E53E3E;font-weight:600;
                                 text-transform:uppercase;letter-spacing:0.05em">✕ Fails when</span>
                    <div style="font-size:0.82rem;color:#9B2C2C;margin-top:3px">{row['fails_when']}</div>
                </div>
            </div>
        </div>""")
