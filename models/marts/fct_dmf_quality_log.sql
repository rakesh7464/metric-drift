-- Reads Snowflake DMF measurement history for the seed table and fct_metric_drift.
-- Uses TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(...)) with named params.
-- No ref() or source() — reads Snowflake system function directly; no dbt lineage.
-- Builds as an empty table if DMFs have not yet been assigned (expected on first run).
--
-- Schema names are derived from dbt's target schema (e.g., DEV → DEV_SEEDS, DEV_MARTS).

{% set seeds_schema  = target.schema ~ '_SEEDS'  %}
{% set marts_schema  = target.schema ~ '_MARTS'  %}
{% set seed_table    = 'METRIC_DRIFT.' ~ seeds_schema ~ '.RAW_DAILY_METRICS' %}
{% set mart_table    = 'METRIC_DRIFT.' ~ marts_schema ~ '.FCT_METRIC_DRIFT'  %}

with seed_results as (
    select *
    from table(
        snowflake.local.data_quality_monitoring_results(
            ref_entity_name   => '{{ seed_table }}',
            ref_entity_domain => 'table'
        )
    )
),

mart_results as (
    select *
    from table(
        snowflake.local.data_quality_monitoring_results(
            ref_entity_name   => '{{ mart_table }}',
            ref_entity_domain => 'table'
        )
    )
),

dmf_results as (
    select * from seed_results
    union all
    select * from mart_results
),

renamed as (
    select
        measurement_time::timestamp_ntz                                    as measured_at,
        table_database                                                     as db_name,
        table_schema                                                       as schema_name,
        table_name,
        metric_database || '.' || metric_schema || '.' || metric_name     as dmf_name,
        value::float                                                       as dmf_value
    from dmf_results
),

classified as (
    select
        measured_at,
        db_name,
        schema_name,
        table_name,
        dmf_name,
        dmf_value,

        case
            -- System DMF failures
            when dmf_name ilike '%null_count%'             and dmf_value > 0   then 'fail'
            when dmf_name ilike '%duplicate_count%'        and dmf_value > 0   then 'fail'
            when dmf_name ilike '%row_count%'              and dmf_value = 0   then 'fail'
            -- Value range failures (returns count of violating rows)
            when dmf_name ilike '%conversion_rate_valid%'  and dmf_value > 0   then 'fail'
            when dmf_name ilike '%revenue_non_negative%'   and dmf_value > 0   then 'fail'
            when dmf_name ilike '%active_users_positive%'  and dmf_value > 0   then 'fail'
            -- Business rule failures
            when dmf_name ilike '%severity_valid%'         and dmf_value > 0   then 'fail'
            when dmf_name ilike '%zscore_plausible%'       and dmf_value > 0   then 'fail'
            -- Volume anomaly (returns 0 = normal, 1 = anomalous)
            when dmf_name ilike '%volume_anomaly%'         and dmf_value = 1   then 'fail'
            -- Partition completeness (returns count of date gaps)
            when dmf_name ilike '%date_gaps%'              and dmf_value > 0   then 'fail'
            -- Cardinality (expected: exactly 3 distinct metric names)
            when dmf_name ilike '%cardinality%'            and dmf_value != 3  then 'fail'
            -- Warnings (expected during data ramp-up or minor drift)
            when dmf_name ilike '%z_score_readiness%'      and dmf_value > 0   then 'warn'
            when dmf_name ilike '%mean_drift%'
                 and dmf_value is not null
                 and abs(dmf_value) > 2                                         then 'warn'
            else 'pass'
        end as quality_status,

        case
            when dmf_name ilike '%null_count%' and dmf_value > 0
                then dmf_value::varchar || ' NULL(s) detected in ' || table_name
            when dmf_name ilike '%duplicate_count%' and dmf_value > 0
                then dmf_value::varchar || ' duplicate row(s) in ' || table_name
            when dmf_name ilike '%row_count%' and dmf_value = 0
                then table_name || ' has 0 rows — table appears empty'
            when dmf_name ilike '%conversion_rate_valid%' and dmf_value > 0
                then dmf_value::varchar || ' conversion_rate value(s) outside [0, 1]'
            when dmf_name ilike '%revenue_non_negative%' and dmf_value > 0
                then dmf_value::varchar || ' negative revenue row(s) detected'
            when dmf_name ilike '%active_users_positive%' and dmf_value > 0
                then dmf_value::varchar || ' active_users row(s) with value <= 0'
            when dmf_name ilike '%cardinality%' and dmf_value != 3
                then 'Expected 3 distinct metric names, found ' || dmf_value::varchar
            when dmf_name ilike '%volume_anomaly%' and dmf_value = 1
                then 'Latest date row count is statistically anomalous (>2σ from mean)'
            when dmf_name ilike '%mean_drift%'
                 and dmf_value is not null and abs(dmf_value) > 2
                then 'Mean drift z-score = ' || dmf_value::varchar || ' (|z| > 2)'
            when dmf_name ilike '%date_gaps%' and dmf_value > 0
                then dmf_value::varchar || ' date gap(s) > 1 day detected in ' || table_name
            when dmf_name ilike '%z_score_readiness%' and dmf_value > 0
                then dmf_value::varchar || ' row(s) have NULL z_score (< 7 days history)'
            else 'OK'
        end as quality_note

    from renamed
)

select * from classified
order by
    case quality_status when 'fail' then 1 when 'warn' then 2 else 3 end,
    measured_at desc
