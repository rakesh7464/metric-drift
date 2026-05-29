"""
Drift Alerts page — metric drift events, AI insights, and metric trend charts.
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from utils.formatting import STATUS_COLORS, fmt_pct, fmt_zscore


def render(
    drift_df: pd.DataFrame,
    alerts_df: pd.DataFrame,
    insights_df: pd.DataFrame,
    filters: dict,
):
    st.markdown("## Drift Alerts")
    st.markdown(
        "Z-score-based drift detection across all metrics and segments. "
        "Critical (|z| ≥ 3), warning (|z| ≥ 2), and watch (|z| ≥ 1.5) events, "
        "with AI-generated context from Snowflake Cortex."
    )

    if alerts_df.empty:
        st.info("No drift alerts in the selected range.")
        return

    # ── Apply filters ─────────────────────────────────────────────────────────
    filtered_alerts = alerts_df.copy()
    filtered_insights = insights_df.copy() if not insights_df.empty else pd.DataFrame()

    if filters.get("severity") and filters["severity"] != "All":
        sev = filters["severity"].lower()
        filtered_alerts = filtered_alerts[filtered_alerts["drift_severity"] == sev]
        if not filtered_insights.empty:
            filtered_insights = filtered_insights[filtered_insights["drift_severity"] == sev]

    if filters.get("date_range"):
        start, end = filters["date_range"]
        filtered_alerts = filtered_alerts[
            (filtered_alerts["metric_date"].dt.date >= start) &
            (filtered_alerts["metric_date"].dt.date <= end)
        ]
        if not filtered_insights.empty:
            filtered_insights = filtered_insights[
                (filtered_insights["metric_date"].dt.date >= start) &
                (filtered_insights["metric_date"].dt.date <= end)
            ]

    if filtered_alerts.empty:
        st.info("No alerts match the current filters.")
        return

    # ── Alert feed ────────────────────────────────────────────────────────────
    st.markdown("#### Alert Feed")

    insight_lookup = {}
    if not filtered_insights.empty and "ai_insight" in filtered_insights.columns:
        for _, row in filtered_insights.iterrows():
            key = (str(row["metric_date"].date()), row["metric_name"], row["segment"])
            insight_lookup[key] = row["ai_insight"]

    sorted_alerts = filtered_alerts.sort_values(
        ["drift_severity", "metric_date"],
        key=lambda s: s.map({"critical": 0, "warning": 1, "watch": 2}) if s.name == "drift_severity" else s,
        ascending=[True, False],
    )

    page_size = 10
    total = len(sorted_alerts)
    total_pages = max(1, (total + page_size - 1) // page_size)

    col_count, col_page = st.columns([3, 1])
    with col_count:
        st.caption(f"{total} alert{'s' if total != 1 else ''} found")
    with col_page:
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)

    page_alerts = sorted_alerts.iloc[(page - 1) * page_size: page * page_size]

    for _, row in page_alerts.iterrows():
        sev = row["drift_severity"]
        key = (str(row["metric_date"].date()), row["metric_name"], row["segment"])
        insight = insight_lookup.get(key, "")
        _render_alert_card(row, sev, insight)

    st.markdown("---")

    # ── Metric trend chart ────────────────────────────────────────────────────
    st.markdown("#### Metric Trend")
    if not drift_df.empty:
        _render_metric_selector(drift_df, alerts_df, filters)


def _render_alert_card(row: pd.Series, severity: str, insight: str):
    color = STATUS_COLORS.get(severity, "#718096")
    bg = "#FFF5F5" if severity == "critical" else "#FFFBEB" if severity == "warning" else "#FFFAF5"
    icon = {"critical": "✕", "warning": "⚠", "watch": "~"}.get(severity, "·")

    direction_arrow = (
        "↑" if row.get("drift_direction") == "above" else
        "↓" if row.get("drift_direction") == "below" else "→"
    )

    dod = row.get("day_over_day_pct_change")
    dod_str = fmt_pct(dod) if pd.notna(dod) else "n/a"
    dod_color = (
        "#E53E3E" if pd.notna(dod) and dod < 0 else
        "#38A169" if pd.notna(dod) and dod > 0 else "#718096"
    )

    metric_value = row["metric_value"]
    value_str = f"{metric_value:,.2f}" if isinstance(metric_value, float) else str(metric_value)

    insight_block = ""
    if insight:
        insight_block = f"""
        <div style="margin-top:10px;padding:10px 12px;background:rgba(255,255,255,0.7);
                    border-radius:6px;border-left:2px solid #CBD5E0">
            <div style="font-size:0.7rem;color:#718096;font-weight:600;
                        text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px">
                AI Insight (Cortex)
            </div>
            <div style="font-size:0.82rem;color:#2D3748;line-height:1.5">{insight}</div>
        </div>"""

    st.html(f"""
    <div style="background:{bg};border-left:4px solid {color};border-radius:0 8px 8px 0;
                padding:14px 16px;margin-bottom:10px">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px">
            <div>
                <span style="color:{color};font-weight:700;font-size:0.85rem">
                    {icon} {severity.upper()}
                </span>
                <span style="color:#718096;font-size:0.8rem;margin-left:8px">
                    {row['metric_date'].strftime('%b %d, %Y')} · {row['metric_name']} · {row['segment']}
                </span>
            </div>
            <div style="text-align:right">
                <span style="font-weight:700;font-size:1rem;color:#1A1A1A">{value_str}</span>
                <span style="color:{dod_color};font-size:0.8rem;margin-left:8px">{dod_str} DoD</span>
            </div>
        </div>
        <div style="display:flex;gap:20px;margin-top:8px;flex-wrap:wrap">
            <span style="font-size:0.78rem;color:#4A5568">
                <b>z-score:</b> {fmt_zscore(row['z_score'])}
            </span>
            <span style="font-size:0.78rem;color:#4A5568">
                <b>14d avg:</b> {row['rolling_14d_avg']:,.2f}
            </span>
            <span style="font-size:0.78rem;color:#4A5568">
                <b>deviation:</b> {fmt_pct(row['pct_deviation'])}
            </span>
            <span style="font-size:0.78rem;color:#4A5568">
                <b>direction:</b> {direction_arrow} {row.get('drift_direction', '—')}
            </span>
        </div>
        {insight_block}
    </div>""")


def _render_metric_selector(drift_df: pd.DataFrame, alerts_df: pd.DataFrame, filters: dict):
    col_metric, col_segment = st.columns(2)
    with col_metric:
        metrics = sorted(drift_df["metric_name"].unique())
        selected_metric = st.selectbox("Metric", options=metrics)
    with col_segment:
        segments = sorted(drift_df["segment"].unique())
        selected_segment = st.selectbox("Segment", options=segments)

    series = drift_df[
        (drift_df["metric_name"] == selected_metric) &
        (drift_df["segment"] == selected_segment)
    ].sort_values("metric_date")

    alert_series = alerts_df[
        (alerts_df["metric_name"] == selected_metric) &
        (alerts_df["segment"] == selected_segment)
    ]

    if filters.get("date_range"):
        start, end = filters["date_range"]
        series = series[
            (series["metric_date"].dt.date >= start) &
            (series["metric_date"].dt.date <= end)
        ]
        alert_series = alert_series[
            (alert_series["metric_date"].dt.date >= start) &
            (alert_series["metric_date"].dt.date <= end)
        ]

    if series.empty:
        st.info("No data for this metric/segment combination.")
        return

    _render_metric_chart(series, alert_series, selected_metric, selected_segment)


def _render_metric_chart(series, alert_series, metric, segment):
    fig = go.Figure()

    # ±1σ band
    fig.add_trace(go.Scatter(
        x=series["metric_date"],
        y=series["rolling_14d_avg"] + series["rolling_14d_stddev"],
        mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=series["metric_date"],
        y=series["rolling_14d_avg"] - series["rolling_14d_stddev"],
        mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor="rgba(203,213,224,0.3)",
        name="±1σ band", hoverinfo="skip",
    ))

    # Rolling average
    fig.add_trace(go.Scatter(
        x=series["metric_date"], y=series["rolling_14d_avg"],
        mode="lines", name="14d avg",
        line=dict(color="#CBD5E0", width=1.5, dash="dot"),
        hovertemplate="<b>%{x|%b %d}</b><br>14d avg: %{y:,.2f}<extra></extra>",
    ))

    # Actual values
    fig.add_trace(go.Scatter(
        x=series["metric_date"], y=series["metric_value"],
        mode="lines", name=metric,
        line=dict(color="#4A90D9", width=2),
        hovertemplate="<b>%{x|%b %d}</b><br>Value: %{y:,.2f}<extra></extra>",
    ))

    # Alert markers
    for sev, color in [("critical", "#E53E3E"), ("warning", "#D97706"), ("watch", "#D97757")]:
        sev_alerts = alert_series[alert_series["drift_severity"] == sev]
        if not sev_alerts.empty:
            merged = sev_alerts.merge(
                series[["metric_date", "metric_value"]], on="metric_date", how="left",
                suffixes=("_a", ""),
            )
            val_col = "metric_value" if "metric_value" in merged.columns else "metric_value_a"
            fig.add_trace(go.Scatter(
                x=merged["metric_date"], y=merged[val_col],
                mode="markers", name=sev,
                marker=dict(
                    size=10 if sev == "critical" else 8,
                    color=color, symbol="circle",
                    line=dict(color="white", width=1.5),
                ),
                hovertemplate=f"<b>%{{x|%b %d}}</b><br>{sev.upper()}<br>Value: %{{y:,.2f}}<extra></extra>",
            ))

    fig.update_layout(
        title=f"{metric} · {segment}",
        height=300,
        margin=dict(t=40, b=20, l=0, r=0),
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(showgrid=False, tickformat="%b %d"),
        yaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font={"family": "sans-serif", "size": 12},
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)
