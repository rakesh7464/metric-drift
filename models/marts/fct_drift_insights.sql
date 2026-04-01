with alerts as (
    select * from {{ ref('fct_drift_alerts') }}
),

prompted as (
    select
        *,
        'You are an analytics engineer explaining metric anomalies to a business stakeholder. '
        || 'Be concise (2-3 sentences). '
        || 'Metric: ' || metric_name
        || '. Date: ' || metric_date::varchar
        || '. Value: ' || metric_value::varchar
        || '. 14-day average: ' || round(rolling_14d_avg, 2)::varchar
        || '. Z-score: ' || round(z_score, 2)::varchar
        || '. Direction: ' || drift_direction
        || '. Severity: ' || drift_severity
        || '. Deviation from average: ' || pct_deviation::varchar || '%'
        || '. Weekend: ' || iff(is_weekend, 'yes', 'no')
        || '. Day-over-day change: ' || coalesce(day_over_day_pct_change::varchar || '%', 'n/a')
        || '. Explain what happened and suggest a possible cause.'
        as prompt
    from alerts
)

select
    metric_date,
    metric_name,
    segment,
    metric_value,
    rolling_14d_avg,
    z_score,
    drift_severity,
    drift_direction,
    pct_deviation,
    snowflake.cortex.complete('snowflake-arctic', prompt)::varchar as ai_insight
from prompted
