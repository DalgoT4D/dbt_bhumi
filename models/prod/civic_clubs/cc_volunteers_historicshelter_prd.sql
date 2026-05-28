{{ config(
    materialized='table',
    tags=["civic_clubs", "prod"]
) }}

select
    city,
    year,
    project_name,
    award,
    count(volunteer_name) as num_awards
from {{ ref('cc_volunteers_historicshelter_stg') }}
group by city, year, project_name, award
