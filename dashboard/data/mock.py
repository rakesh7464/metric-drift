"""
Mock data generator that mirrors the exact schema of the metric-drift Snowflake models.
Used when Snowflake is unavailable or in demo/offline mode.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

random.seed(42)
np.random.seed(42)

# ── Constants matching seed data ──────────────────────────────────────────────
METRICS    = ["revenue", "conversion_rate", "active_users"]
SEGMENTS   = ["all", "product"]
START_DATE = datetime(2026, 1, 1)
END_DATE   = datetime(2026, 3, 31)
DAYS       = (END_DATE - START_DATE).days + 1

# Severity weights for health score: fail=10, warn=3, pass=0
SEVERITY_WEIGHT = {"fail": 10, "warn": 3, "pass": 0}

DMF_CHECKS = [
    # (dmf_name, check_type, table)
    ("SNOWFLAKE.CORE.NULL_COUNT",                         "system",   "RAW_DAILY_METRICS"),
    ("SNOWFLAKE.CORE.DUPLICATE_COUNT",                    "system",   "RAW_DAILY_METRICS"),
    ("SNOWFLAKE.CORE.ROW_COUNT",                          "system",   "RAW_DAILY_METRICS"),
    ("SNOWFLAKE.CORE.FRESHNESS",                          "system",   "RAW_DAILY_METRICS"),
    ("SNOWFLAKE.CORE.MIN",                                "system",   "FCT_METRIC_DRIFT"),
    ("SNOWFLAKE.CORE.MAX",                                "system",   "FCT_METRIC_DRIFT"),
    ("METRIC_DRIFT.DMF.CARDINALITY_METRIC_NAME",          "custom",   "RAW_DAILY_METRICS"),
    ("METRIC_DRIFT.DMF.REVENUE_NON_NEGATIVE",             "custom",   "RAW_DAILY_METRICS"),
    ("METRIC_DRIFT.DMF.CONVERSION_RATE_VALID",            "custom",   "RAW_DAILY_METRICS"),
    ("METRIC_DRIFT.DMF.ACTIVE_USERS_POSITIVE",            "custom",   "RAW_DAILY_METRICS"),
    ("METRIC_DRIFT.DMF.VOLUME_ANOMALY",                   "custom",   "RAW_DAILY_METRICS"),
    ("METRIC_DRIFT.DMF.MEAN_DRIFT_Z_SCORE",               "custom",   "FCT_METRIC_DRIFT"),
    ("METRIC_DRIFT.DMF.SEVERITY_VALID",                   "custom",   "FCT_METRIC_DRIFT"),
    ("METRIC_DRIFT.DMF.ZSCORE_PLAUSIBLE",                 "custom",   "FCT_METRIC_DRIFT"),
    ("METRIC_DRIFT.DMF.DATE_GAPS",                        "custom",   "RAW_DAILY_METRICS"),
    ("METRIC_DRIFT.DMF.Z_SCORE_READINESS",                "custom",   "FCT_METRIC_DRIFT"),
]

# Known injected violations matching seed data
KNOWN_VIOLATIONS = {
    "METRIC_DRIFT.DMF.REVENUE_NON_NEGATIVE":    ("fail",  1.0,  "1 negative revenue row(s) detected"),
    "METRIC_DRIFT.DMF.CONVERSION_RATE_VALID":   ("fail",  1.0,  "1 conversion_rate value(s) outside [0, 1]"),
    "METRIC_DRIFT.DMF.ACTIVE_USERS_POSITIVE":   ("fail",  1.0,  "1 active_users row(s) with value <= 0"),
    "METRIC_DRIFT.DMF.DATE_GAPS":               ("fail",  5.0,  "5 date gap(s) > 1 day detected in RAW_DAILY_METRICS"),
}


def _make_dmf_snapshot(measured_at: datetime, introduce_violations: bool = True) -> list:
    rows = []
    for dmf_name, check_type, table in DMF_CHECKS:
        if introduce_violations and dmf_name in KNOWN_VIOLATIONS:
            status, value, note = KNOWN_VIOLATIONS[dmf_name]
        elif dmf_name == "SNOWFLAKE.CORE.NULL_COUNT":
            value, status, note = 0.0, "pass", "OK"
        elif dmf_name == "SNOWFLAKE.CORE.DUPLICATE_COUNT":
            value, status, note = 0.0, "pass", "OK"
        elif dmf_name == "SNOWFLAKE.CORE.ROW_COUNT":
            value, status, note = 542.0, "pass", "OK"
        elif dmf_name == "SNOWFLAKE.CORE.FRESHNESS":
            value, status, note = 0.0, "pass", "OK"
        elif dmf_name == "SNOWFLAKE.CORE.MIN":
            value, status, note = -250.0, "pass", "OK"
        elif dmf_name == "SNOWFLAKE.CORE.MAX":
            value, status, note = 99850.0, "pass", "OK"
        elif dmf_name == "METRIC_DRIFT.DMF.CARDINALITY_METRIC_NAME":
            value, status, note = 3.0, "pass", "OK"
        elif dmf_name == "METRIC_DRIFT.DMF.VOLUME_ANOMALY":
            value, status, note = 0.0, "pass", "OK"
        elif dmf_name == "METRIC_DRIFT.DMF.MEAN_DRIFT_Z_SCORE":
            value = round(np.random.uniform(-1.5, 1.5), 3)
            status, note = "pass", "OK"
        elif dmf_name == "METRIC_DRIFT.DMF.SEVERITY_VALID":
            value, status, note = 0.0, "pass", "OK"
        elif dmf_name == "METRIC_DRIFT.DMF.ZSCORE_PLAUSIBLE":
            value, status, note = 0.0, "pass", "OK"
        elif dmf_name == "METRIC_DRIFT.DMF.Z_SCORE_READINESS":
            value, status, note = 0.0, "pass", "OK"
        else:
            value, status, note = 0.0, "pass", "OK"

        rows.append({
            "measured_at":    measured_at,
            "db_name":        "METRIC_DRIFT",
            "schema_name":    "DEV_SEEDS" if table == "RAW_DAILY_METRICS" else "DEV_MARTS",
            "table_name":     table,
            "dmf_name":       dmf_name,
            "check_type":     check_type,
            "dmf_value":      value,
            "quality_status": status,
            "quality_note":   note,
        })
    return rows


def get_dmf_quality_log(days_history: int = 30) -> pd.DataFrame:
    """Returns historical DMF snapshots simulating daily runs over `days_history` days."""
    all_rows = []
    base = datetime(2026, 4, 18, 8, 0, 0)  # last confirmed live run

    for i in range(days_history - 1, -1, -1):
        ts = base - timedelta(days=i)
        # Violations only exist in the most recent ~5 days (simulates they were introduced then)
        has_violations = i <= 4
        all_rows.extend(_make_dmf_snapshot(ts, introduce_violations=has_violations))

    df = pd.DataFrame(all_rows)
    df["measured_at"] = pd.to_datetime(df["measured_at"])
    return df


def _make_drift_row(
    metric_date, metric_name, segment,
    metric_value, avg_14d, stddev, z_score
) -> dict:
    sev = (
        "critical" if abs(z_score) >= 3.0 else
        "warning"  if abs(z_score) >= 2.0 else
        "watch"    if abs(z_score) >= 1.5 else
        "normal"
    )
    direction = "above" if z_score > 0 else "below" if z_score < 0 else "on_target"
    pct_dev = round(((metric_value - avg_14d) / avg_14d) * 100, 2) if avg_14d else 0.0
    return {
        "metric_date":       metric_date,
        "metric_name":       metric_name,
        "segment":           segment,
        "metric_value":      round(metric_value, 2),
        "rolling_14d_avg":   round(avg_14d, 2),
        "rolling_14d_stddev":round(stddev, 2),
        "z_score":           round(z_score, 3),
        "drift_severity":    sev,
        "drift_direction":   direction,
        "pct_deviation":     pct_dev,
        "is_drift_alert":    sev != "normal",
        "is_weekend":        metric_date.weekday() >= 5,
    }


def get_fct_metric_drift() -> pd.DataFrame:
    """Generates ~540 rows of synthetic drift data matching seed schema."""
    rows = []
    base_params = {
        ("revenue",         "all"):     (50000, 8000),
        ("revenue",         "product"): (20000, 3500),
        ("conversion_rate", "all"):     (0.035, 0.005),
        ("conversion_rate", "product"): (0.042, 0.006),
        ("active_users",    "all"):     (12000, 1500),
        ("active_users",    "product"): (4500,  600),
    }

    for (metric, segment), (mu, sigma) in base_params.items():
        values = []
        for d in range(DAYS):
            date = START_DATE + timedelta(days=d)
            # inject anomalies
            if metric == "revenue" and segment == "all" and date == datetime(2026, 3, 5):
                v = -250.0
            elif metric == "conversion_rate" and segment == "product" and date == datetime(2026, 3, 1):
                v = 1.15
            elif metric == "active_users" and segment == "all" and date == datetime(2026, 3, 10):
                v = 0.0
            else:
                v = max(0.0, np.random.normal(mu, sigma))
            values.append((date, v))

        # skip date gap window
        gap_start = datetime(2026, 3, 15)
        gap_end   = datetime(2026, 3, 19)
        values = [(d, v) for d, v in values if not (gap_start < d < gap_end)]

        window = 14
        for i, (date, val) in enumerate(values):
            if i < window:
                continue
            window_vals = [v for _, v in values[i - window:i]]
            avg = np.mean(window_vals)
            std = np.std(window_vals)
            z   = (val - avg) / std if std > 0 else 0.0
            rows.append(_make_drift_row(date, metric, segment, val, avg, std, z))

    df = pd.DataFrame(rows)
    df["metric_date"] = pd.to_datetime(df["metric_date"])
    return df


def get_fct_drift_alerts() -> pd.DataFrame:
    drift = get_fct_metric_drift()
    alerts = drift[drift["is_drift_alert"]].copy()
    alerts = alerts.sort_values(["metric_name", "segment", "metric_date"])
    alerts["prev_day_value"] = alerts.groupby(["metric_name", "segment"])["metric_value"].shift(1)
    alerts["day_over_day_pct_change"] = (
        ((alerts["metric_value"] - alerts["prev_day_value"]) / alerts["prev_day_value"].abs()) * 100
    ).round(2)
    alerts["consecutive_alert_window"] = (
        alerts.groupby(["metric_name", "segment"]).cumcount() + 1
    )
    return alerts.reset_index(drop=True)


_AI_INSIGHTS = {
    "critical": [
        "Revenue dropped sharply below the 14-day average, likely driven by a data pipeline issue or a major drop in campaign performance. Verify upstream ingestion and check if a promotion ended abruptly.",
        "Active users fell to zero — this is almost certainly a data collection failure rather than an actual behavior change. Check the tracking pipeline for the affected date.",
        "Conversion rate exceeded 1.0, which is mathematically impossible. This is a data quality violation — likely a mis-tagged event or duplicate session count inflating conversions.",
    ],
    "warning": [
        "Revenue is running 20-30% below the rolling average. Weekend patterns may be a factor, but the magnitude warrants checking campaign budgets and any recent product changes.",
        "Active users are trending below average for the third consecutive day. Could indicate a seasonal dip or a degraded acquisition channel — worth cross-referencing with marketing spend.",
        "Conversion rate is elevated above the 14-day average. This could be a genuine win (targeted campaign) or a measurement artifact. Validate with session-level data.",
    ],
    "watch": [
        "Metric is drifting slightly above the expected range. No immediate action needed, but worth monitoring over the next 48 hours for continuation.",
        "Mild downward drift detected. Could be early signal of a trend or normal variance — flag for review if it persists into the next day.",
        "Slight deviation from rolling average. Weekend effect may explain the delta. Monitor but no escalation required at this severity level.",
    ],
}


def get_fct_drift_insights() -> pd.DataFrame:
    alerts = get_fct_drift_alerts()
    rng = np.random.default_rng(99)

    def pick_insight(sev):
        pool = _AI_INSIGHTS.get(sev, _AI_INSIGHTS["watch"])
        return pool[rng.integers(len(pool))]

    alerts["ai_insight"] = alerts["drift_severity"].apply(pick_insight)
    return alerts[[
        "metric_date", "metric_name", "segment", "metric_value",
        "rolling_14d_avg", "z_score", "drift_severity", "drift_direction",
        "pct_deviation", "ai_insight",
    ]]
