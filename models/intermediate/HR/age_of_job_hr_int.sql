{{ config(
  materialized='table',
  tags=["hr", "int"]
) }}

with source as (
    select * from {{ ref('age_of_job_stg') }}
),

-- hiring SLA by category: manager level 100 days, below manager level 80 days.
-- "below manager" is checked first because it also contains "manager"; matching on the start of the
-- value covers both "Manager" and "Manager Level"
sla as (
    select
        *,
        case
            when lower(category) like 'below manager%' then 80
            when lower(category) like 'manager%' then 100
        end as sla_days
    from source
)

select
    *,
    -- NULL when the category or age of job is missing
    case
        when age_of_job <= sla_days then 'Achieved'
        when age_of_job > sla_days then 'Missed'
    end as sla_status
from sla
