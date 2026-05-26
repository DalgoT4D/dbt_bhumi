{{ config(
  materialized='table',
  tags=["civic_clubs", "staging"]
) }}

select
    "Acedamic_Year" as academic_year,
    "City" as city,
    "State" as state,
    "College_clubs" as college_clubs,
    cast("Leaders" as int) as leaders,
    cast("Volunteer" as int) as volunteers,
    cast("Num_of_Events" as int) as num_of_events,
    cast("Volunteering_Hours" as numeric) as volunteering_hours

from {{ source('civic_clubs', 'City_Wise_Active_Club_Data') }}
where "City" is not null and btrim("City") != ''
