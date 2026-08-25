{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

with competency_mappings as (
    select 
        NULLIF(BTRIM(id::TEXT),'') as cm_id,
        NULLIF(BTRIM(goal::TEXT),'')::INTEGER as goal,
        NULLIF(BTRIM(start_date::TEXT),'')::DATE as cm_start_date,
        case
            when NULLIF(TRIM(start_date::TEXT), '') is NULL then NULL
            when EXTRACT(month from TRIM(start_date::TEXT)::DATE) <= 3
                then
                    (EXTRACT(year from TRIM(start_date::TEXT)::DATE)::INTEGER - 1)::TEXT
                    || ' - ' || EXTRACT(year from TRIM(start_date::TEXT)::DATE)::INTEGER::TEXT
            else
                EXTRACT(year from TRIM(start_date::TEXT)::DATE)::INTEGER::TEXT
                || ' - ' || (EXTRACT(year from TRIM(start_date::TEXT)::DATE)::INTEGER + 1)::TEXT
        end as academic_year,
        case
            when NULLIF(TRIM(start_date::TEXT), '') is NULL then NULL
            when EXTRACT(month from TRIM(start_date::TEXT)::DATE) <= 3
                then
                    EXTRACT(year from TRIM(start_date::TEXT)::DATE)::INTEGER - 1
            else
                EXTRACT(year from TRIM(start_date::TEXT)::DATE)::INTEGER
        end as year_map,
        case
            when NULLIF(TRIM(start_date::TEXT), '') is NULL then NULL
            else TO_CHAR(TRIM(start_date::TEXT)::DATE, 'Mon')
        end as month,
        NULLIF(BTRIM(end_date::TEXT),'')::DATE as cm_end_date,
        NULLIF(BTRIM(cohort_year::TEXT),'') as cm_cohort,
        NULLIF(BTRIM(competency_id::TEXT),'') as competency_id
    from {{ source('fellowship_school_app_25_26', 'competency_mappings_25_26') }}
)


select 
    cm_id,
    goal,
    cm_start_date,
    academic_year,
    year_map::TEXT as year_map,
    month,
    cm_end_date,
    cm_cohort,
    competency_id,
    case
        when LOWER(LEFT(month, 3)) in ('apr', 'may', 'jun') then 'Q1'
        when LOWER(LEFT(month, 3)) in ('jul', 'aug', 'sep') then 'Q2'
        when LOWER(LEFT(month, 3)) in ('oct', 'nov', 'dec') then 'Q3'
        when LOWER(LEFT(month, 3)) in ('jan', 'feb', 'mar') then 'Q4'
    end as quarter
from competency_mappings
where
    cm_id is not NULL
    and competency_id is not NULL
