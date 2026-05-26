{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

select
    quarter,
    '2025-26' as academic_year,
    trees_planted,
    plant_survival_rate as trees_survival_rate,
    round(trees_planted * plant_survival_rate)::int as trees_survived
from {{ ref('cc_collegeclubs_trees_stg') }}
