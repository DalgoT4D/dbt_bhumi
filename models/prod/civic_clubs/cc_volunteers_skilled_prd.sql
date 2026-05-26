{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

with unpivoted as (
    select 'October'  as month, volunteer_name, oct_q3  as hours from {{ ref('cc_volunteers_skilled_stg') }}
    union all
    select 'November' as month, volunteer_name, nov_q3  as hours from {{ ref('cc_volunteers_skilled_stg') }}
    union all
    select 'December' as month, volunteer_name, dec_q3  as hours from {{ ref('cc_volunteers_skilled_stg') }}
    union all
    select 'January'  as month, volunteer_name, jan_q4  as hours from {{ ref('cc_volunteers_skilled_stg') }}
    union all
    select 'February' as month, volunteer_name, feb_q4  as hours from {{ ref('cc_volunteers_skilled_stg') }}
    union all
    select 'March'    as month, volunteer_name, march_q4 as hours from {{ ref('cc_volunteers_skilled_stg') }}
)

select
    month,
    '2025-2026' as academic_year,
    null as city,
    null as state,
    count(distinct volunteer_name) as total_volunteer_count,
    count(distinct case when hours > 0 then volunteer_name end) as active_volunteer_count,
    sum(hours) as total_volunteering_hours
from unpivoted
group by month
