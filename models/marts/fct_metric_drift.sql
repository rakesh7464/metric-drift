with stats as (
    select * from {{ ref('int_metric_rolling_stats') }}
    where z_score is not null
),

drift_classified as (
    select
        metric_date,
        metric_name,
        segment,
        metric_value,
        rolling_14d_avg,
        rolling_14d_stddev,
        z_score,
        is_weekend,

        case
            when abs(z_score) >= 3.0 then 'critical'
            when abs(z_score) >= 2.0 then 'warning'
            when abs(z_score) >= 1.5 then 'watch'
            else 'normal'
        end as drift_severity,

        case
            when z_score > 0 then 'above'
            when z_score < 0 then 'below'
            else 'on_target'
        end as drift_direction,

        round(
            ((metric_value - rolling_14d_avg) / nullif(rolling_14d_avg, 0)) * 100,
            2
        ) as pct_deviation

    from stats
)

select
    *,
    drift_severity != 'normal' as is_drift_alert
from drift_classified
