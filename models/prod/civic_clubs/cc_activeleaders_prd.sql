{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

select
    city,
    district,
    case
        when lower(btrim(continuing)) = 'yes' then 'Active'
        when lower(btrim(continuing)) = 'no' then 'Inactive'
        when continuing is null or btrim(continuing) = '' then null
        else continuing
    end as status,
    count(leader_name) as leader_count
from {{ ref('cc_activeleaders_stg') }}
group by city, district, continuing
