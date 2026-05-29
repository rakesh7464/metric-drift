"""
Historical Trend page — quality score over time, check pass rates, drift frequency.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from utils.health_score import compute_score_history
from utils.formatting import STATUS_COLORS, short_dmf_name


def render(dmf_df: pd.DataFrame, drift_df: pd.DataFrame, filters: dict):
    st.markdown("## Historical Trend")
    st.markdown(
        "Track how data quality has evolved over time — "
        "health score trajectory, check pass rates, and drift frequency by metric."
    )

    if dmf_df.empty and drift_df.empty:
        st.warning("No historical data available.")
        return

    # ── Apply date filter ─────────────────────────────────────────────────────
    filtered_dmf = dmf_df.copy()
    filtered_drift = drift_df.copy()

    if filters.get("date_range"):
        start, end = filters["date_range"]
        if not filtered_dmf.empty:
            filtered_dmf = filtered_dmf[
                (filtered_dmf["measured_at"].dt.date >= start) &
                (filtered_dmf["measured_at"].dt.date <= end)
            ]
        if not filtered_drift.empty:
            filtered_drift = filtered_drift[
                (filtered_drift["metric_date"].dt.date >= start) &
                (filtered_drift["metric_date"].dt.date <= end)
            ]

    # ── Health score trend ────────────────────────────────────────────────────
    if not filtered_dmf.empty:
        st.markdown("#### Health Score Over Time")
        score_history = compute_score_history(filtered_dmf)
        if not score_history.empty:
            _render_score_area(score_history)
        else:
            st.info("Not enough snapshots to build a trend.")

        st.markdown("---")

        # ── Check pass rate by day ────────────────────────────────────────────
        st.markdown("#### Daily Check Pass Rate")
        _render_pass_rate_chart(filtered_dmf)

        st.markdown("---")

        # ── Failure breakdown by check ────────────────────────────────────────
        st.markdown("#### Check Failure Frequency")
        _render_failure_heatmap(filtered_dmf)

        st.markdown("---")

    # ── Drift frequency over time ─────────────────────────────────────────────
    if not filtered_drift.empty:
        st.markdown("#### Drift Alert Frequency by Metric")
        _render_drift_frequency(filtered_drift)

        st.markdown("---")

        # ── Z-score distribution ──────────────────────────────────────────────
        st.markdown("#### Z-Score Distribution")
        _render_zscore_distribution(filtered_drift)


def _render_score_area(history: pd.DataFrame):
    fig = go.Figure()

    # Grade band fills
    bands = [
        (95, 100, "rgba(240,255,244,0.6)", "Healthy"),
        (85,  95, "rgba(240,255,244,0.4)", "Monitoring"),
        (70,  85, "rgba(255,251,235,0.5)", "At Risk"),
        (50,  70, "rgba(255,245,245,0.5)", "Degraded"),
        (0,   50, "rgba(255,245,245,0.6)", "Critical"),
    ]
    for low, high, color, _ in bands:
        fig.add_hrect(y0=low, y1=high, fillcolor=color, opacity=1, line_width=0)

    # Failures / warnings as background bars (secondary axis feel)
    fig.add_trace(go.Bar(
        x=history["date"],
        y=history["failures"],
        name="Failures",
        marker_color="rgba(229,62,62,0.15)",
        yaxis="y2",
        hovertemplate="<b>%{x|%b %d}</b><br>Failures: %{y}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=history["date"],
        y=history["warnings"],
        name="Warnings",
        marker_color="rgba(217,119,87,0.15)",
        yaxis="y2",
        hovertemplate="<b>%{x|%b %d}</b><br>Warnings: %{y}<extra></extra>",
    ))

    # Score line
    fig.add_trace(go.Scatter(
        x=history["date"],
        y=history["score"],
        mode="lines+markers",
        name="Health Score",
        line=dict(color="#D97757", width=2.5),
        marker=dict(size=5, color="#D97757"),
        hovertemplate="<b>%{x|%b %d}</b><br>Score: %{y}<extra></extra>",
    ))

    fig.add_hline(y=95, line_dash="dot", line_color="#38A169", line_width=1,
                  annotation_text="Target", annotation_position="right")

    fig.update_layout(
        height=280,
        margin=dict(t=10, b=10, l=0, r=60),
        paper_bgcolor="white",
        plot_bgcolor="white",
        barmode="stack",
        xaxis=dict(showgrid=False, tickformat="%b %d"),
        yaxis=dict(range=[0, 105], showgrid=True, gridcolor="#F0F0F0", title="Score"),
        yaxis2=dict(overlaying="y", side="right", showgrid=False, title="Issue Count"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font={"family": "sans-serif", "size": 12},
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_pass_rate_chart(dmf_df: pd.DataFrame):
    dmf_df = dmf_df.copy()
    dmf_df["date"] = dmf_df["measured_at"].dt.date

    daily = (
        dmf_df.groupby(["date", "quality_status"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    for col in ["pass", "warn", "fail"]:
        if col not in daily.columns:
            daily[col] = 0

    daily["total"] = daily["pass"] + daily["warn"] + daily["fail"]
    daily["pass_rate"] = (daily["pass"] / daily["total"].replace(0, 1) * 100).round(1)
    daily["date"] = pd.to_datetime(daily["date"])

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=daily["date"], y=daily["fail"],
        name="Fail", marker_color="#E53E3E",
        hovertemplate="<b>%{x|%b %d}</b><br>Fail: %{y}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=daily["date"], y=daily["warn"],
        name="Warn", marker_color="#D97706",
        hovertemplate="<b>%{x|%b %d}</b><br>Warn: %{y}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=daily["date"], y=daily["pass"],
        name="Pass", marker_color="#68D391",
        hovertemplate="<b>%{x|%b %d}</b><br>Pass: %{y}<extra></extra>",
    ))

    fig.update_layout(
        barmode="stack",
        height=240,
        margin=dict(t=10, b=10, l=0, r=0),
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(showgrid=False, tickformat="%b %d"),
        yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Check Count"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font={"family": "sans-serif", "size": 12},
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_failure_heatmap(dmf_df: pd.DataFrame):
    failing = dmf_df[dmf_df["quality_status"].isin(["fail", "warn"])].copy()
    if failing.empty:
        st.success("No failures or warnings in this date range.")
        return

    failing["date"] = failing["measured_at"].dt.date
    failing["check_label"] = failing["dmf_name"].apply(short_dmf_name)

    pivot = (
        failing.groupby(["check_label", "date"])
        .size()
        .unstack(fill_value=0)
    )

    # Sort by total failures descending
    pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).index]

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[str(d) for d in pivot.columns],
        y=pivot.index.tolist(),
        colorscale=[[0, "#F0FFF4"], [0.5, "#FFFBEB"], [1, "#E53E3E"]],
        hovertemplate="<b>%{y}</b><br>%{x}<br>Issues: %{z}<extra></extra>",
        showscale=True,
        colorbar=dict(title="Issues", thickness=12),
    ))

    fig.update_layout(
        height=max(200, len(pivot) * 30 + 60),
        margin=dict(t=10, b=10, l=0, r=0),
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        yaxis=dict(tickfont=dict(size=11)),
        font={"family": "sans-serif", "size": 12},
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_drift_frequency(drift_df: pd.DataFrame):
    alerts = drift_df[drift_df["is_drift_alert"]].copy()
    if alerts.empty:
        st.info("No drift alerts in the selected range.")
        return

    alerts["week"] = alerts["metric_date"].dt.to_period("W").dt.start_time

    weekly = (
        alerts.groupby(["week", "metric_name", "drift_severity"])
        .size()
        .reset_index(name="count")
    )

    metrics = sorted(weekly["metric_name"].unique())
    cols = st.columns(len(metrics))

    for i, metric in enumerate(metrics):
        with cols[i]:
            mdata = weekly[weekly["metric_name"] == metric]
            pivot = (
                mdata.groupby(["week", "drift_severity"])["count"]
                .sum()
                .unstack(fill_value=0)
                .reset_index()
            )
            fig = go.Figure()
            for sev, color in [("critical", "#E53E3E"), ("warning", "#D97706"), ("watch", "#D97757")]:
                if sev in pivot.columns:
                    fig.add_trace(go.Bar(
                        x=pivot["week"], y=pivot[sev],
                        name=sev, marker_color=color,
                        hovertemplate=f"<b>%{{x|%b %d}}</b><br>{sev}: %{{y}}<extra></extra>",
                    ))
            fig.update_layout(
                title=metric,
                barmode="stack",
                height=200,
                margin=dict(t=40, b=10, l=0, r=0),
                paper_bgcolor="white",
                plot_bgcolor="white",
                xaxis=dict(showgrid=False, tickformat="%b %d", tickangle=-45),
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
                showlegend=False,
                font={"family": "sans-serif", "size": 11},
            )
            st.plotly_chart(fig, use_container_width=True)


def _render_zscore_distribution(drift_df: pd.DataFrame):
    col1, col2 = st.columns(2)

    with col1:
        fig = go.Figure()
        for metric in sorted(drift_df["metric_name"].unique()):
            data = drift_df[drift_df["metric_name"] == metric]["z_score"].dropna()
            fig.add_trace(go.Violin(
                y=data,
                name=metric,
                box_visible=True,
                meanline_visible=True,
                fillcolor="rgba(217,119,87,0.2)",
                line_color="#D97757",
            ))
        # Alert thresholds
        for val, label, color in [(1.5, "watch", "#D97757"), (2.0, "warning", "#D97706"), (3.0, "critical", "#E53E3E")]:
            fig.add_hline(y=val,  line_dash="dot", line_color=color, line_width=1, annotation_text=label, annotation_position="right")
            fig.add_hline(y=-val, line_dash="dot", line_color=color, line_width=1)

        fig.update_layout(
            title="Z-Score Distribution by Metric",
            height=280,
            margin=dict(t=40, b=10, l=0, r=60),
            paper_bgcolor="white",
            plot_bgcolor="white",
            yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="z-score"),
            showlegend=True,
            legend=dict(orientation="h", y=-0.2),
            font={"family": "sans-serif", "size": 12},
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        alerts = drift_df[drift_df["is_drift_alert"]]
        if not alerts.empty:
            sev_by_metric = (
                alerts.groupby(["metric_name", "drift_severity"])
                .size()
                .reset_index(name="count")
            )
            color_map = {"critical": "#E53E3E", "warning": "#D97706", "watch": "#D97757"}
            fig = go.Figure()
            for sev, color in color_map.items():
                sev_data = sev_by_metric[sev_by_metric["drift_severity"] == sev]
                fig.add_trace(go.Bar(
                    x=sev_data["metric_name"],
                    y=sev_data["count"],
                    name=sev,
                    marker_color=color,
                ))
            fig.update_layout(
                title="Total Alerts by Metric + Severity",
                barmode="group",
                height=280,
                margin=dict(t=40, b=10, l=0, r=0),
                paper_bgcolor="white",
                plot_bgcolor="white",
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
                legend=dict(orientation="h", y=-0.2),
                font={"family": "sans-serif", "size": 12},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No drift alerts to display.")
