"""
Metric Drift | Data Quality Monitor
Streamlit dashboard for the metric-drift DMF quality layer.
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta

st.set_page_config(
    page_title="Metric Drift | Data Quality Monitor",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    [data-testid="stSidebar"] {
        background: #F5F5F0;
        border-right: 1px solid #E2E8F0;
    }
    [data-testid="stSidebar"] .block-container { padding-top: 1.5rem; }
    /* Hide the default Streamlit sidebar top decoration (app name + deploy button) */
    [data-testid="stSidebarHeader"] { display: none; }
    [data-testid="collapsedControl"] { display: none; }
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
    hr { border-color: #E2E8F0; margin: 1.5rem 0; }
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        border-bottom: 2px solid #E2E8F0;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 6px 6px 0 0;
        padding: 8px 20px;
        font-weight: 500;
        color: #718096;
    }
    .stTabs [aria-selected="true"] {
        background: #FFFFFF;
        color: #D97757;
        border-bottom: 2px solid #D97757;
    }
    [data-testid="stSelectbox"] > div,
    [data-testid="stNumberInput"] > div { border-radius: 6px; }
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────

def _load_data(use_mock: bool):
    if use_mock:
        from data.mock import (
            get_dmf_quality_log,
            get_fct_metric_drift,
            get_fct_drift_alerts,
            get_fct_drift_insights,
        )
        return (
            get_dmf_quality_log(days_history=30),
            get_fct_metric_drift(),
            get_fct_drift_alerts(),
            get_fct_drift_insights(),
        )
    else:
        from data.snowflake_loader import (
            get_dmf_quality_log,
            get_fct_metric_drift,
            get_fct_drift_alerts,
            get_fct_drift_insights,
            SnowflakeConnectionError,
        )
        try:
            return (
                get_dmf_quality_log(),
                get_fct_metric_drift(),
                get_fct_drift_alerts(),
                get_fct_drift_insights(),
            )
        except SnowflakeConnectionError as e:
            st.error(f"Snowflake connection failed: {e}")
            st.info("Falling back to mock data.")
            from data.mock import (
                get_dmf_quality_log as mock_dmf,
                get_fct_metric_drift as mock_drift,
                get_fct_drift_alerts as mock_alerts,
                get_fct_drift_insights as mock_insights,
            )
            return (
                mock_dmf(days_history=30),
                mock_drift(),
                mock_alerts(),
                mock_insights(),
            )


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("**Navigate**")
    st.radio(
        "Navigate",
        options=["Overview", "DMF Checks", "Drift Alerts", "Historical Trend", "Check Glossary"],
        label_visibility="collapsed",
        key="sidebar_nav",
    )

    st.markdown("---")
    st.markdown("**Data Source**")
    use_mock = st.toggle(
        "Use mock data",
        value=True,
        help="Toggle off to connect to Snowflake via secrets.toml",
    )

    st.markdown("---")
    st.markdown("**Filters**")

    default_end   = date(2026, 4, 18)
    default_start = default_end - timedelta(days=29)
    date_range = st.date_input(
        "Date range",
        value=(default_start, default_end),
        help="Applies to all views",
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        date_start, date_end = date_range
    else:
        date_start, date_end = default_start, default_end

    severity_filter   = st.selectbox("Drift severity",  ["All", "Critical", "Warning", "Watch"])
    check_type_filter = st.selectbox("Check type",       ["All", "Custom", "System"])
    status_filter     = st.selectbox("Check status",     ["All", "Fail", "Warn", "Pass"])

    st.markdown("---")

    if not use_mock:
        if st.button("Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    st.html("""
        <div style="font-size:0.72rem;color:#A0AEC0;margin-top:1rem">
            metric-drift · <a href="https://github.com/rakesh7464/metric-drift"
            style="color:#A0AEC0">github</a>
        </div>""")


# ── Load data ─────────────────────────────────────────────────────────────────

filters = {
    "date_range":  (date_start, date_end),
    "severity":    severity_filter,
    "check_type":  check_type_filter,
    "status":      status_filter,
}

with st.spinner("Loading data..."):
    dmf_df, drift_df, alerts_df, insights_df = _load_data(use_mock)

# Data source banner
if use_mock:
    st.html(
        '<div style="padding:10px 16px;border-radius:8px;margin-bottom:1rem;font-size:0.85rem;'
        'font-weight:500;background:#FFFBEB;color:#92400E;border:1px solid #F6C05C">'
        '⚡ Mock mode — displaying synthetic data that mirrors the metric-drift schema. '
        'Toggle "Use mock data" off to connect to Snowflake.</div>'
    )
else:
    st.html(
        '<div style="padding:10px 16px;border-radius:8px;margin-bottom:1rem;font-size:0.85rem;'
        'font-weight:500;background:#F0FFF4;color:#276749;border:1px solid #9AE6B4">'
        '✓ Connected to Snowflake — live data from metric-drift DEV environment.</div>'
    )


# ── Tabs ──────────────────────────────────────────────────────────────────────

from views.overview         import render as render_overview
from views.dmf_checks       import render as render_dmf_checks
from views.drift_alerts     import render as render_drift_alerts
from views.historical_trend import render as render_historical
from views.check_glossary   import render as render_glossary

tab_overview, tab_checks, tab_drift, tab_history, tab_glossary = st.tabs([
    "Overview",
    "DMF Checks",
    "Drift Alerts",
    "Historical Trend",
    "Check Glossary",
])

with tab_overview:
    render_overview(dmf_df, drift_df, alerts_df, filters)

with tab_checks:
    render_dmf_checks(dmf_df, filters)

with tab_drift:
    render_drift_alerts(drift_df, alerts_df, insights_df, filters)

with tab_history:
    render_historical(dmf_df, drift_df, filters)

with tab_glossary:
    render_glossary()
