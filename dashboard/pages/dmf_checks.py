"""
DMF Checks page — per-check drill-down with status, values, and trend.
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from utils.formatting import short_dmf_name, STATUS_COLORS


def render(dmf_df: pd.DataFrame, filters: dict):
    st.markdown("## DMF Check Details")
    st.markdown(
        "Drill into individual Data Metric Function results. "
        "Each check runs continuously against the seed and mart tables post-deployment."
    )

    if dmf_df.empty:
        st.warning("No DMF data available.")
        return

    # ── Apply filters ─────────────────────────────────────────────────────────
    filtered = dmf_df.copy()

    if filters.get("status") and filters["status"] != "All":
        filtered = filtered[filtered["quality_status"] == filters["status"].lower()]

    if filters.get("check_type") and filters["check_type"] != "All":
        filtered = filtered[filtered["check_type"] == filters["check_type"].lower()]

    if filters.get("date_range"):
        start, end = filters["date_range"]
        filtered = filtered[
            (filtered["measured_at"].dt.date >= start) &
            (filtered["measured_at"].dt.date <= end)
        ]

    if filtered.empty:
        st.info("No checks match the current filters.")
        return

    # ── Latest snapshot summary ───────────────────────────────────────────────
    st.markdown("#### Current Check Status")

    latest = (
        filtered
        .sort_values("measured_at", ascending=False)
        .groupby(["dmf_name", "table_name"], as_index=False)
        .first()
        .sort_values(
            "quality_status",
            key=lambda s: s.map({"fail": 0, "warn": 1, "pass": 2}),
        )
    )

    _render_check_table(latest)

    st.markdown("---")

    # ── Drill-down selector ───────────────────────────────────────────────────
    st.markdown("#### Check Drill-Down")
    check_options = sorted(filtered["dmf_name"].unique())
    selected_check = st.selectbox(
        "Select a check to inspect",
        options=check_options,
        format_func=short_dmf_name,
    )

    if selected_check:
        check_data = filtered[filtered["dmf_name"] == selected_check].sort_values("measured_at")
        _render_check_detail(check_data, selected_check)


def _render_check_table(df: pd.DataFrame):
    rows_html = ""
    for _, row in df.iterrows():
        status = row["quality_status"]
        color = STATUS_COLORS.get(status, "#718096")
        bg = "#FFF5F5" if status == "fail" else "#FFFBEB" if status == "warn" else "#FFFFFF"
        icon = "✕" if status == "fail" else "⚠" if status == "warn" else "✓"
        name = short_dmf_name(row["dmf_name"])

        check_type = row.get("check_type", "custom")
        if check_type == "system":
            type_badge = (
                '<span style="background:#EBF4FF;color:#3182CE;border-radius:3px;'
                'padding:1px 6px;font-size:0.7rem;font-weight:500">system</span>'
            )
        else:
            type_badge = (
                '<span style="background:#F0FFF4;color:#38A169;border-radius:3px;'
                'padding:1px 6px;font-size:0.7rem;font-weight:500">custom</span>'
            )

        value_str = f"{row['dmf_value']:.3f}" if row["dmf_value"] is not None else "—"
        ts = row["measured_at"].strftime("%b %d %H:%M") if pd.notna(row["measured_at"]) else "—"
        note = str(row["quality_note"])

        rows_html += f"""
        <tr style="background:{bg};border-bottom:1px solid #F0F0F0">
            <td style="padding:10px 12px;font-weight:700;color:{color};font-size:0.9rem">{icon}</td>
            <td style="padding:10px 12px">
                <div style="font-weight:600;font-size:0.85rem;color:#1A1A1A">{name}</div>
                <div style="font-size:0.7rem;color:#A0AEC0;margin-top:2px">{row['dmf_name']}</div>
            </td>
            <td style="padding:10px 12px">{type_badge}</td>
            <td style="padding:10px 12px;font-size:0.82rem;color:#4A5568">{row['table_name']}</td>
            <td style="padding:10px 12px;font-family:monospace;font-size:0.82rem;color:#2D3748">{value_str}</td>
            <td style="padding:10px 12px;font-size:0.82rem;color:#4A5568">{note}</td>
            <td style="padding:10px 12px;font-size:0.75rem;color:#A0AEC0;white-space:nowrap">{ts}</td>
        </tr>"""

    html = f"""
    <div style="overflow-x:auto;border-radius:8px;border:1px solid #E2E8F0;margin-bottom:8px">
      <table style="width:100%;border-collapse:collapse;font-family:sans-serif">
        <thead>
          <tr style="background:#F5F5F0;border-bottom:2px solid #E2E8F0">
            <th style="padding:10px 12px;text-align:left;font-size:0.72rem;color:#718096;
                       text-transform:uppercase;letter-spacing:0.06em;width:28px"></th>
            <th style="padding:10px 12px;text-align:left;font-size:0.72rem;color:#718096;
                       text-transform:uppercase;letter-spacing:0.06em">Check</th>
            <th style="padding:10px 12px;text-align:left;font-size:0.72rem;color:#718096;
                       text-transform:uppercase;letter-spacing:0.06em">Type</th>
            <th style="padding:10px 12px;text-align:left;font-size:0.72rem;color:#718096;
                       text-transform:uppercase;letter-spacing:0.06em">Table</th>
            <th style="padding:10px 12px;text-align:left;font-size:0.72rem;color:#718096;
                       text-transform:uppercase;letter-spacing:0.06em">Value</th>
            <th style="padding:10px 12px;text-align:left;font-size:0.72rem;color:#718096;
                       text-transform:uppercase;letter-spacing:0.06em">Note</th>
            <th style="padding:10px 12px;text-align:left;font-size:0.72rem;color:#718096;
                       text-transform:uppercase;letter-spacing:0.06em">Measured At</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>"""

    st.html(html)


def _render_check_detail(check_data: pd.DataFrame, dmf_name: str):
    latest_row = check_data.iloc[-1]
    status = latest_row["quality_status"]
    color = STATUS_COLORS.get(status, "#718096")
    name = short_dmf_name(dmf_name)
    check_type = latest_row.get("check_type", "custom")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.html(f"""
        <div style="background:#F5F5F0;border-radius:8px;padding:16px;
                    border-top:3px solid {color}">
            <div style="font-size:0.72rem;color:#718096;text-transform:uppercase;
                        letter-spacing:0.06em;font-weight:500">Current Status</div>
            <div style="font-size:1.4rem;font-weight:700;color:{color};margin-top:4px">
                {status.upper()}
            </div>
            <div style="font-size:0.78rem;color:#4A5568;margin-top:4px">
                {latest_row['quality_note']}
            </div>
        </div>""")

    with col2:
        st.html(f"""
        <div style="background:#F5F5F0;border-radius:8px;padding:16px">
            <div style="font-size:0.72rem;color:#718096;text-transform:uppercase;
                        letter-spacing:0.06em;font-weight:500">Latest Value</div>
            <div style="font-size:1.4rem;font-weight:700;color:#1A1A1A;margin-top:4px;
                        font-family:monospace">
                {latest_row['dmf_value']:.4f}
            </div>
            <div style="font-size:0.78rem;color:#718096;margin-top:4px">
                Measured {latest_row['measured_at'].strftime('%b %d, %Y %H:%M')}
            </div>
        </div>""")

    with col3:
        st.html(f"""
        <div style="background:#F5F5F0;border-radius:8px;padding:16px">
            <div style="font-size:0.72rem;color:#718096;text-transform:uppercase;
                        letter-spacing:0.06em;font-weight:500">Check Info</div>
            <div style="font-size:0.9rem;font-weight:600;color:#1A1A1A;margin-top:4px">
                {name}
            </div>
            <div style="font-size:0.78rem;color:#718096;margin-top:4px">
                {check_type.upper()} · {latest_row['table_name']}
            </div>
        </div>""")

    st.markdown("")

    if len(check_data) > 1:
        st.markdown(f"**Value over time — `{name}`**")
        _render_value_trend(check_data, dmf_name)
    else:
        st.info("Only one measurement available — trend chart requires multiple snapshots.")


def _render_value_trend(check_data: pd.DataFrame, dmf_name: str):
    threshold_map = {
        "null_count":             ("= 0 to pass", 0),
        "duplicate_count":        ("= 0 to pass", 0),
        "row_count":              ("> 0 to pass", 1),
        "conversion_rate_valid":  ("= 0 to pass", 0),
        "revenue_non_negative":   ("= 0 to pass", 0),
        "active_users_positive":  ("= 0 to pass", 0),
        "volume_anomaly":         ("= 0 to pass", 0),
        "date_gaps":              ("= 0 to pass", 0),
        "cardinality_metric_name":("= 3 to pass", 3),
        "severity_valid":         ("= 0 to pass", 0),
        "zscore_plausible":       ("= 0 to pass", 0),
        "z_score_readiness":      ("= 0 to pass", 0),
    }
    short = short_dmf_name(dmf_name)
    threshold_info = threshold_map.get(short)

    point_colors = [STATUS_COLORS.get(s, "#718096") for s in check_data["quality_status"]]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=check_data["measured_at"],
        y=check_data["dmf_value"],
        mode="lines+markers",
        line=dict(color="#D97757", width=2),
        marker=dict(size=8, color=point_colors, line=dict(color="white", width=1.5)),
        hovertemplate="<b>%{x|%b %d %H:%M}</b><br>Value: %{y:.4f}<extra></extra>",
    ))

    if threshold_info:
        label, val = threshold_info
        fig.add_hline(y=val, line_dash="dot", line_color="#718096", line_width=1,
                      annotation_text=label, annotation_position="right")

    fig.update_layout(
        height=220,
        margin=dict(t=10, b=10, l=0, r=80),
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(showgrid=False, tickformat="%b %d"),
        yaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
        showlegend=False,
        font={"family": "sans-serif", "size": 12},
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)
