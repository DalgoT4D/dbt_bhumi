{{ config(
  materialized='table',
  tags=["cv", "prod"]
) }}


with source as (
    select * from {{ ref('cv_corporate_catalyse_int') }}
),

partner_agg as (
    select
        corporate_partner_id,
        fy,
        quarter,
        month,
        count(distinct event_id) as event_count
    from source
    group by corporate_partner_id, fy, quarter, month
),

cal_partner as (
    select
        corporate_partner_id,
        fy,
        quarter,
        month,
        event_count,
        sum(case when event_count >= 1 then 1 else 0 end) as active_partner_count,
        sum(case when event_count > 1 then 1 else 0 end) as recurring_partner_count,
        sum(case when event_count = 1 then 1 else 0 end) as new_partner_count
    from partner_agg
    group by corporate_partner_id, fy, quarter, month, event_count
)

select *
from cal_partner
where
    corporate_partner_id is not null
    and coalesce(trim(corporate_partner_id::text), '') <> ''
    and coalesce(trim(corporate_partner_id::text), '') <> '{}'
    and fy is not null
    and quarter is not null
    and month is not null
order by fy, quarter, month
