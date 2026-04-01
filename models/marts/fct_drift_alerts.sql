with drift_events as (
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
        is_weekend
    from {{ ref('fct_metric_drift') }}
    where is_drift_alert
),

with_context as (
    select
        *,
        lag(metric_value) over (
            partition by metric_name, segment
            order by metric_date
        ) as prev_day_value,
        sum(1) over (
            partition by metric_name, segment
            order by metric_date
            rows between 2 preceding and current row
        ) as consecutive_alert_window
    from drift_events
)

select
    *,
    round(
        ((metric_value - prev_day_value) / nullif(prev_day_value, 0)) * 100,
        2
    ) as day_over_day_pct_change
from with_context
