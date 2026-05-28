"""
Health score calculation for the DMF quality layer.

Scoring model (inspired by SLO/error-budget thinking):
  - Start at 100.
  - Each failing check deducts points based on check type and how many
    consecutive snapshots it has been failing.
  - Warnings deduct a smaller amount.
  - Score is clamped to [0, 100].

Severity weights:
  fail (critical-level DMF): -8 per check per snapshot period
  warn:                       -2 per check
  pass:                        0

Check-type multiplier:
  system DMF (null_count, row_count, etc.): ×1.5  (data integrity baseline)
  custom DMF (business rules, ranges):      ×1.0
"""

import pandas as pd

_BASE_DEDUCTION = {"fail": 8, "warn": 2, "pass": 0}
_TYPE_MULTIPLIER = {"system": 1.5, "custom": 1.0}


def compute_health_score(dmf_df: pd.DataFrame) -> dict:
    """
    Given the full DMF quality log, compute the current health score
    and supporting breakdown.

    Returns a dict with:
      score          : int 0-100
      grade          : str  (A/B/C/D/F)
      status_label   : str  (Healthy / At Risk / Degraded / Critical)
      total_checks   : int
      passing        : int
      warnings       : int
      failures       : int
      deduction      : float  (total points lost)
      check_breakdown: list of dicts per dmf_name
    """
    if dmf_df.empty:
        return _empty_score()

    # Use only the most recent snapshot per DMF+table combination
    latest = (
        dmf_df
        .sort_values("measured_at", ascending=False)
        .groupby(["dmf_name", "table_name"], as_index=False)
        .first()
    )

    total_deduction = 0.0
    breakdown = []

    for _, row in latest.iterrows():
        base = _BASE_DEDUCTION.get(row["quality_status"], 0)
        multiplier = _TYPE_MULTIPLIER.get(row.get("check_type", "custom"), 1.0)
        deduction = base * multiplier
        total_deduction += deduction
        breakdown.append({
            "dmf_name":       row["dmf_name"],
            "table_name":     row["table_name"],
            "check_type":     row.get("check_type", "custom"),
            "quality_status": row["quality_status"],
            "dmf_value":      row["dmf_value"],
            "quality_note":   row["quality_note"],
            "measured_at":    row["measured_at"],
            "deduction":      deduction,
        })

    score = max(0, round(100 - total_deduction))
    passing  = (latest["quality_status"] == "pass").sum()
    warnings = (latest["quality_status"] == "warn").sum()
    failures = (latest["quality_status"] == "fail").sum()

    return {
        "score":           score,
        "grade":           _grade(score),
        "status_label":    _label(score),
        "total_checks":    len(latest),
        "passing":         int(passing),
        "warnings":        int(warnings),
        "failures":        int(failures),
        "deduction":       total_deduction,
        "check_breakdown": sorted(breakdown, key=lambda x: (
            {"fail": 0, "warn": 1, "pass": 2}[x["quality_status"]], x["dmf_name"]
        )),
    }


def compute_score_history(dmf_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute health score for each daily snapshot to build a trend line.
    Returns a DataFrame with columns: date, score, failures, warnings.
    """
    if dmf_df.empty:
        return pd.DataFrame(columns=["date", "score", "failures", "warnings"])

    dmf_df = dmf_df.copy()
    dmf_df["date"] = dmf_df["measured_at"].dt.date

    records = []
    for date, group in dmf_df.groupby("date"):
        result = compute_health_score(group)
        records.append({
            "date":     pd.Timestamp(date),
            "score":    result["score"],
            "failures": result["failures"],
            "warnings": result["warnings"],
        })

    return pd.DataFrame(records).sort_values("date")


def _grade(score: int) -> str:
    if score >= 95: return "A"
    if score >= 85: return "B"
    if score >= 70: return "C"
    if score >= 50: return "D"
    return "F"


def _label(score: int) -> str:
    if score >= 95: return "Healthy"
    if score >= 85: return "Monitoring"
    if score >= 70: return "At Risk"
    if score >= 50: return "Degraded"
    return "Critical"


def _empty_score() -> dict:
    return {
        "score": 100, "grade": "A", "status_label": "No Data",
        "total_checks": 0, "passing": 0, "warnings": 0, "failures": 0,
        "deduction": 0.0, "check_breakdown": [],
    }
