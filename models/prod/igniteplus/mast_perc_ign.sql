{{ config(
  materialized='table',
  tags=["igniteplus", "prod"]
) }}

with completion_perc as (
    select 
        *,
        case
            when
                planned_duration_days is null
                or actual_duration_days is null
                or actual_duration_days = 0
                then null
            else least(
                (planned_duration_days::numeric / actual_duration_days::numeric) * 100,
                100
            )
        end as completion_perc
    from {{ ref('master_data_ign_25_26') }}
),

monitor_data as (
    select 
        s_no,
        project_id,
        monitored_year,
        intervention_year,
        quarter,
        school_address,
        district,
        date_of_visit,
        visited_by,
        folder_link
    from {{ ref('monitor_data_ignite') }}
),

one_monitor_row as (
    select
        monitor_data.*,
        row_number() over (
            partition by project_id
            order by date_of_visit desc nulls last, s_no desc nulls last
        ) as monitor_row_number
    from monitor_data
)

select 
    cp.*,
    md.intervention_year,
    md.monitored_year,
    md.date_of_visit,
    md.visited_by,
    md.folder_link
from completion_perc as cp
left join one_monitor_row as md
    on cp.project_id = md.project_id
    and md.monitor_row_number = 1
    -- on cp.financial_year = md.intervention_year
    -- and cp.school_name = md.school_address
    -- and cp.district = md.district