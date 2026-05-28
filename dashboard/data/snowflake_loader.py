"""
Snowflake data loader for the metric-drift dashboard.
Reads credentials from st.secrets["snowflake"] (secrets.toml or Streamlit Cloud secrets).
Falls back gracefully — callers should handle SnowflakeConnectionError.
"""

import pandas as pd
import streamlit as st


class SnowflakeConnectionError(Exception):
    pass


def _get_connection():
    try:
        import snowflake.connector
        creds = st.secrets["snowflake"]
        return snowflake.connector.connect(
            account=creds["account"],
            user=creds["user"],
            password=creds["password"],
            warehouse=creds.get("warehouse", "COMPUTE_WH"),
            database=creds.get("database", "METRIC_DRIFT"),
            schema=creds.get("schema", "DEV_MARTS"),
            role=creds.get("role", "ACCOUNTADMIN"),
        )
    except Exception as e:
        raise SnowflakeConnectionError(str(e))


@st.cache_data(ttl=300, show_spinner=False)
def get_dmf_quality_log() -> pd.DataFrame:
    conn = _get_connection()
    query = """
        SELECT
            measured_at,
            db_name,
            schema_name,
            table_name,
            dmf_name,
            CASE
                WHEN dmf_name ILIKE '%null_count%'
                  OR dmf_name ILIKE '%duplicate_count%'
                  OR dmf_name ILIKE '%row_count%'
                  OR dmf_name ILIKE '%freshness%'
                  OR dmf_name ILIKE 'SNOWFLAKE.CORE.MIN%'
                  OR dmf_name ILIKE 'SNOWFLAKE.CORE.MAX%'
                THEN 'system'
                ELSE 'custom'
            END AS check_type,
            dmf_value,
            quality_status,
            quality_note
        FROM METRIC_DRIFT.DEV_MARTS.FCT_DMF_QUALITY_LOG
        ORDER BY measured_at DESC
    """
    try:
        df = pd.read_sql(query, conn)
        df.columns = [c.lower() for c in df.columns]
        df["measured_at"] = pd.to_datetime(df["measured_at"])
        return df
    finally:
        conn.close()


@st.cache_data(ttl=300, show_spinner=False)
def get_fct_metric_drift() -> pd.DataFrame:
    conn = _get_connection()
    query = """
        SELECT
            metric_date, metric_name, segment, metric_value,
            rolling_14d_avg, rolling_14d_stddev, z_score,
            drift_severity, drift_direction, pct_deviation,
            is_drift_alert, is_weekend
        FROM METRIC_DRIFT.DEV_MARTS.FCT_METRIC_DRIFT
        ORDER BY metric_date
    """
    try:
        df = pd.read_sql(query, conn)
        df.columns = [c.lower() for c in df.columns]
        df["metric_date"] = pd.to_datetime(df["metric_date"])
        return df
    finally:
        conn.close()


@st.cache_data(ttl=300, show_spinner=False)
def get_fct_drift_alerts() -> pd.DataFrame:
    conn = _get_connection()
    query = """
        SELECT
            metric_date, metric_name, segment, metric_value,
            rolling_14d_avg, z_score, drift_severity,
            drift_direction, pct_deviation, is_weekend,
            prev_day_value, consecutive_alert_window,
            day_over_day_pct_change
        FROM METRIC_DRIFT.DEV_MARTS.FCT_DRIFT_ALERTS
        ORDER BY metric_date DESC
    """
    try:
        df = pd.read_sql(query, conn)
        df.columns = [c.lower() for c in df.columns]
        df["metric_date"] = pd.to_datetime(df["metric_date"])
        return df
    finally:
        conn.close()


@st.cache_data(ttl=300, show_spinner=False)
def get_fct_drift_insights() -> pd.DataFrame:
    conn = _get_connection()
    query = """
        SELECT
            metric_date, metric_name, segment, metric_value,
            rolling_14d_avg, z_score, drift_severity,
            drift_direction, pct_deviation, ai_insight
        FROM METRIC_DRIFT.DEV_MARTS.FCT_DRIFT_INSIGHTS
        ORDER BY metric_date DESC
    """
    try:
        df = pd.read_sql(query, conn)
        df.columns = [c.lower() for c in df.columns]
        df["metric_date"] = pd.to_datetime(df["metric_date"])
        return df
    finally:
        conn.close()
