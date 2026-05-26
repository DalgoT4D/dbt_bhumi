{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

with union_sources as (
select 
"Year" as year,
"Month" as month,
"Club_Name" as club_name,
"City" as city,
cast("Leaders" as int) as leaders,
cast("Edu_Engaged" as int) as edu_engaged,
cast("Orientation" as int) as orientation,
cast("Civic_Engaged" as int) as civic_engaged,
cast("Classes_Taken" as int) as classes_taken,
cast("Shelter_Mapped" as int) as shelters_mapped,
cast("Total_Edu_Vols" as int) as total_edu_vols,
cast("Civic_Activities" as int) as civic_activities,  
cast("Total_Civic_Vols" as int) as total_civic_vols,
'East'as region,
'2025-26' as academic_year
FROM {{ source('civic_clubs', 'east') }}

UNION ALL

select
"Year" as year,
"Month" as month,
"Club_Name" as club_name,
"City" as city,
cast("Leaders" as int) as leaders,
cast("Edu_Engaged" as int) as edu_engaged,
cast("Orientation" as int) as orientation,
cast("Civic_Engaged" as int) as civic_engaged,
cast("Classes_Taken" as int) as classes_taken,
cast("Shelter_Mapped" as int) as shelters_mapped,
cast("Total_Edu_Vols" as int) as total_edu_vols,
cast("Civic_Activities" as int) as civic_activities,  
cast("Total_Civic_Vols" as int) as total_civic_vols,
'North-West' as region,
'2025-26' as academic_year
FROM {{ source('civic_clubs', 'north_west') }}

UNION ALL

select
"Year" as year,
"Month" as month,
"Club_Name" as club_name,
"City" as city,
cast("Leaders" as int) as leaders,
cast("Edu_Engaged" as int) as edu_engaged,
cast("Orientation" as int) as orientation,
cast("Civic_Engaged" as int) as civic_engaged,
cast("Classes_Taken" as int) as classes_taken,
cast("Shelter_Mapped" as int) as shelters_mapped,
cast("Total_Edu_Vols" as int) as total_edu_vols,
cast("Civic_Activities" as int) as civic_activities,  
cast("Total_Civic_Vols" as int) as total_civic_vols,
'South'as region,
'2025-26' as academic_year
FROM {{ source('civic_clubs', 'south') }}
)

select DISTINCT * from union_sources
where club_name is not null and btrim(club_name) != ''
