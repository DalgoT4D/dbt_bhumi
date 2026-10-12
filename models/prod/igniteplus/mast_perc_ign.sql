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
            else round(
                least(
                    (planned_duration_days::numeric / actual_duration_days::numeric) * 100,
                    100
                ),
                2
            )
        end as completion_perc
    from {{ ref('mast_x_miles') }}
)

select 
    *
from completion_perc
-- on cp.financial_year = md.intervention_year
-- and cp.school_name = md.school_address
-- and cp.district = md.district
