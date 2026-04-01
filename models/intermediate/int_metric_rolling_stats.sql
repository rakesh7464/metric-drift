with daily as (
    select * from {{ ref('stg_daily_metrics') }}
),

rolling as (
    select
        metric_date,
        metric_name,
        segment,
        metric_value,
        is_weekend,

        avg(metric_value) over (
            partition by metric_name, segment
            order by metric_date
            rows between 13 preceding and current row
        ) as rolling_14d_avg,

        stddev(metric_value) over (
            partition by metric_name, segment
            order by metric_date
            rows between 13 preceding and current row
        ) as rolling_14d_stddev,

        count(*) over (
            partition by metric_name, segment
            order by metric_date
            rows between 13 preceding and current row
        ) as rolling_14d_count

    from daily
)

select
    *,
    case
        when rolling_14d_stddev > 0 and rolling_14d_count >= 7
        then (metric_value - rolling_14d_avg) / rolling_14d_stddev
        else null
    end as z_score
from rolling
