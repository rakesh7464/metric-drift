"""
Overview page — overall health score, KPI summary, and top issues.
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from utils.health_score import compute_health_score, compute_score_history
from utils.formatting import GRADE_COLORS, short_dmf_name, STATUS_COLORS, severity_color


def render(dmf_df: pd.DataFrame, drift_df: pd.DataFrame, alerts_df: pd.DataFrame, filters: dict = {}):
    st.markdown("## Overview")
    st.markdown(
        "Real-time health summary of the metric-drift data quality stack — "
        "DMF checks, drift alerts, and pipeline status at a glance."
    )

    # Apply date filter
    if filters.get("date_range"):
        start, end = filters["date_range"]
        if not dmf_df.empty:
            dmf_df = dmf_df[
                (dmf_df["measured_at"].dt.date >= start) &
                (dmf_df["measured_at"].dt.date <= end)
            ]
        if not alerts_df.empty:
            alerts_df = alerts_df[
                (alerts_df["metric_date"].dt.date >= start) &
                (alerts_df["metric_date"].dt.date <= end)
            ]

    health = compute_health_score(dmf_df)
    score_history = compute_score_history(dmf_df)

    # ── Top KPI row ───────────────────────────────────────────────────────────
    col_score, col_checks, col_alerts, col_drift = st.columns([2, 1, 1, 1])

    with col_score:
        _render_score_gauge(health)

    with col_checks:
        _kpi_tile("Total Checks", str(health["total_checks"]), "DMF monitors active")
        st.markdown(" ")
        _kpi_tile(
            "Passing",
            str(health["passing"]),
            f"{round(health['passing'] / max(health['total_checks'], 1) * 100)}% pass rate",
            color="#38A169",
        )

    with col_alerts:
        _kpi_tile(
            "Failures",
            str(health["failures"]),
            "Checks requiring action",
            color="#E53E3E" if health["failures"] > 0 else "#38A169",
        )
        st.markdown(" ")
        _kpi_tile(
            "Warnings",
            str(health["warnings"]),
            "Checks to monitor",
            color="#D97706" if health["warnings"] > 0 else "#718096",
        )

    with col_drift:
        total_drift = len(alerts_df) if not alerts_df.empty else 0
        critical_drift = (
            len(alerts_df[alerts_df["drift_severity"] == "critical"])
            if not alerts_df.empty else 0
        )
        _kpi_tile(
            "Drift Alerts",
            str(total_drift),
            f"{critical_drift} critical",
            color="#E53E3E" if critical_drift > 0 else "#D97757",
        )
        st.markdown(" ")
        active_metrics = alerts_df["metric_name"].nunique() if not alerts_df.empty else 0
        _kpi_tile(
            "Affected Metrics",
            str(active_metrics),
            "metrics with alerts",
            color="#D97706" if active_metrics > 0 else "#718096",
        )

    st.markdown("---")

    # ── Score trend + Issues ──────────────────────────────────────────────────
    left, right = st.columns([3, 2])

    with left:
        st.markdown("#### Health Score — 30-Day Trend")
        if not score_history.empty:
            _render_score_trend(score_history)
        else:
            st.info("No historical data available.")

    with right:
        st.markdown("#### Active Issues")
        failing = [c for c in health["check_breakdown"] if c["quality_status"] in ("fail", "warn")]
        if failing:
            for check in failing[:6]:
                _render_issue_card(check)
        else:
            st.success("All checks passing. No active issues.")

    st.markdown("---")

    # ── Drift severity distribution ───────────────────────────────────────────
    if not alerts_df.empty:
        st.markdown("#### Drift Alert Distribution")
        _render_drift_distribution(alerts_df)


# ── Components ────────────────────────────────────────────────────────────────

def _render_score_gauge(health: dict):
    score = health["score"]
    grade = health["grade"]
    label = health["status_label"]
    grade_color = GRADE_COLORS.get(grade, "#718096")

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number={"font": {"size": 48, "color": grade_color}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#CBD5E0"},
            "bar": {"color": grade_color, "thickness": 0.25},
            "bgcolor": "white",
            "borderwidth": 0,
            "steps": [
                {"range": [0,  50], "color": "#FFF5F5"},
                {"range": [50, 70], "color": "#FFFBEB"},
                {"range": [70, 85], "color": "#FFFBEB"},
                {"range": [85, 95], "color": "#F0FFF4"},
                {"range": [95, 100], "color": "#F0FFF4"},
            ],
            "threshold": {
                "line": {"color": grade_color, "width": 3},
                "thickness": 0.75,
                "value": score,
            },
        },
        title={
            "text": (
                f"<b>Health Score</b><br>"
                f"<span style='font-size:0.9em;color:{grade_color}'>"
                f"Grade {grade} — {label}</span>"
            ),
            "font": {"size": 16},
        },
    ))
    fig.update_layout(
        height=220,
        margin=dict(t=60, b=0, l=20, r=20),
        paper_bgcolor="white",
        font={"family": "sans-serif"},
    )
    st.plotly_chart(fig, use_container_width=True)


def _kpi_tile(label: str, value: str, subtitle: str = "", color: str = "#1A1A1A"):
    st.html(f"""
    <div style="background:#F5F5F0;border-radius:8px;padding:14px 16px;
                border-left:3px solid {color}">
        <div style="font-size:0.72rem;color:#718096;font-weight:500;
                    text-transform:uppercase;letter-spacing:0.06em">{label}</div>
        <div style="font-size:1.8rem;font-weight:700;color:{color};line-height:1.2">{value}</div>
        <div style="font-size:0.75rem;color:#718096;margin-top:2px">{subtitle}</div>
    </div>""")


def _render_score_trend(history: pd.DataFrame):
    fig = go.Figure()

    bands = [
        (95, 100, "#F0FFF4"),
        (85,  95, "#F0FFF4"),
        (70,  85, "#FFFBEB"),
        (50,  70, "#FFF5F5"),
        (0,   50, "#FFF5F5"),
    ]
    for low, high, color in bands:
        fig.add_hrect(y0=low, y1=high, fillcolor=color, opacity=0.4, line_width=0)

    fig.add_trace(go.Scatter(
        x=history["date"],
        y=history["score"],
        mode="lines+markers",
        name="Health Score",
        line=dict(color="#D97757", width=2.5),
        marker=dict(size=5, color="#D97757"),
        fill="tozeroy",
        fillcolor="rgba(217,119,87,0.08)",
        hovertemplate="<b>%{x|%b %d}</b><br>Score: %{y}<extra></extra>",
    ))
    fig.add_hline(y=95, line_dash="dot", line_color="#38A169", line_width=1,
                  annotation_text="Target (95)", annotation_position="right")

    fig.update_layout(
        height=220,
        margin=dict(t=10, b=10, l=0, r=60),
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(showgrid=False, tickformat="%b %d"),
        yaxis=dict(range=[0, 105], showgrid=True, gridcolor="#F0F0F0"),
        showlegend=False,
        font={"family": "sans-serif", "size": 12},
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_issue_card(check: dict):
    status = check["quality_status"]
    color = "#E53E3E" if status == "fail" else "#D97706"
    icon = "✕" if status == "fail" else "⚠"
    name = short_dmf_name(check["dmf_name"])
    note = check["quality_note"]
    table = check["table_name"]

    st.html(f"""
    <div style="border-left:3px solid {color};background:#FAFAFA;
                border-radius:0 6px 6px 0;padding:10px 14px;margin-bottom:8px">
        <div style="display:flex;justify-content:space-between;align-items:center">
            <span style="font-weight:600;font-size:0.85rem;color:#1A1A1A">{icon} {name}</span>
            <span style="font-size:0.7rem;color:#718096">{table}</span>
        </div>
        <div style="font-size:0.78rem;color:#4A5568;margin-top:3px">{note}</div>
    </div>""")


def _render_drift_distribution(alerts_df: pd.DataFrame):
    col1, col2 = st.columns(2)

    with col1:
        sev_counts = (
            alerts_df.groupby("drift_severity")
            .size()
            .reindex(["critical", "warning", "watch"], fill_value=0)
            .reset_index(name="count")
        )
        colors = [STATUS_COLORS.get(s, "#718096") for s in sev_counts["drift_severity"]]
        fig = go.Figure(go.Bar(
            x=sev_counts["drift_severity"],
            y=sev_counts["count"],
            marker_color=colors,
            text=sev_counts["count"],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Alerts: %{y}<extra></extra>",
        ))
        fig.update_layout(
            title="Alerts by Severity",
            height=240,
            margin=dict(t=40, b=20, l=0, r=0),
            paper_bgcolor="white",
            plot_bgcolor="white",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
            font={"family": "sans-serif", "size": 12},
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        metric_counts = (
            alerts_df.groupby("metric_name")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=True)
        )
        fig = go.Figure(go.Bar(
            x=metric_counts["count"],
            y=metric_counts["metric_name"],
            orientation="h",
            marker_color="#D97757",
            text=metric_counts["count"],
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>Alerts: %{x}<extra></extra>",
        ))
        fig.update_layout(
            title="Alerts by Metric",
            height=240,
            margin=dict(t=40, b=20, l=0, r=40),
            paper_bgcolor="white",
            plot_bgcolor="white",
            xaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
            yaxis=dict(showgrid=False),
            font={"family": "sans-serif", "size": 12},
        )
        st.plotly_chart(fig, use_container_width=True)
