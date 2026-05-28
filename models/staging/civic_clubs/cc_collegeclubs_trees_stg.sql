{{ config(
  materialized='table',
  tags=["civic_clubs", "staging"]
) }}


select
    "Quarter" as quarter,
    "Year" as academic_year,
    cast(replace("Plant_Survived__", '%', '') as numeric) / 100.0 as plant_survival_rate,
    cast("Total_Trees_Planted" as int) as trees_planted
from {{ source('civic_clubs', 'Tree_Plantation') }}
