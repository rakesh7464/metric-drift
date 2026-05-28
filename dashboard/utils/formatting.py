"""Shared formatting helpers for the dashboard."""

STATUS_COLORS = {
    "fail":     "#E53E3E",
    "warn":     "#D97706",
    "pass":     "#38A169",
    "critical": "#E53E3E",
    "warning":  "#D97706",
    "watch":    "#D97757",
    "normal":   "#38A169",
}

STATUS_ICONS = {
    "fail":     "x",
    "warn":     "!",
    "pass":     "check",
    "critical": "x",
    "warning":  "!",
    "watch":    "~",
    "normal":   "check",
}

GRADE_COLORS = {
    "A": "#38A169",
    "B": "#68D391",
    "C": "#D97706",
    "D": "#E53E3E",
    "F": "#742A2A",
}

SCORE_BG_COLORS = {
    "Healthy":    "#F0FFF4",
    "Monitoring": "#FFFBEB",
    "At Risk":    "#FFFBEB",
    "Degraded":   "#FFF5F5",
    "Critical":   "#FFF5F5",
    "No Data":    "#F5F5F0",
}


def status_badge_html(status: str) -> str:
    color = STATUS_COLORS.get(status, "#718096")
    bg = color + "20"
    label = status.upper()
    return (
        f'<span style="background:{bg};color:{color};border:1px solid {color};'
        f'border-radius:4px;padding:2px 8px;font-size:0.75rem;font-weight:600;'
        f'letter-spacing:0.05em">{label}</span>'
    )


def severity_color(severity: str) -> str:
    return STATUS_COLORS.get(severity, "#718096")


def fmt_pct(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.1f}%"


def fmt_zscore(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}σ"


def short_dmf_name(full_name: str) -> str:
    """METRIC_DRIFT.DMF.REVENUE_NON_NEGATIVE -> revenue_non_negative"""
    return full_name.split(".")[-1].lower()
