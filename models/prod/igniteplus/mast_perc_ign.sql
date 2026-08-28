{{ config(
  materialized='table',
  tags=["igniteplus", "prod"]
) }}

with completion_perc as (
    select 
        *,
    case
      when planned_duration_days is null
        or actual_duration_days is null
        or actual_duration_days = 0
        then null
      else least(
        (planned_duration_days::numeric / actual_duration_days::numeric) * 100,
        100
      )
    end as completion_perc
    from {{ ref('master_data_ign_25_26') }}
)

select *
from completion_perc