with source as (
    select * from {{ ref('raw_daily_metrics') }}
)

select
    metric_date::date as metric_date,
    metric_name,
    metric_value::float as metric_value,
    segment,
    dayofweek(metric_date::date) as day_of_week,
    case
        when dayofweek(metric_date::date) in (0, 6) then true
        else false
    end as is_weekend
from source
