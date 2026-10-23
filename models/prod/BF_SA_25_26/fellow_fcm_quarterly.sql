{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with fellow_fcm_avg as (
    select 
        academic_year,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        cohort,
        funding_year,
        donor_name,
        school_name,
        school_state,
        school_type,
        city,
        grade,
        grade_section,
        round(avg(fcm_avg)) as fcm_avg
    from {{ ref('fellow_fcm_int') }}
    group by
        academic_year,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        cohort,
        funding_year,
        donor_name,
        school_name,
        school_state,
        school_type,
        city,
        grade,
        grade_section
        
),

fcm_avg as (
    select 
        academic_year,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        cohort,
        funding_year,
        donor_name,
        school_name,
        school_state,
        school_type,
        city,
        grade,
        grade_section,
        'FCM' as parameters,

        case
            when fcm_avg is null then 'Black'
            when fcm_avg <= 49 then 'Red'
            when fcm_avg >= 50 and fcm_avg <= 74 then 'Amber'
            when fcm_avg >= 75 then 'Green'
        end as brag
    from fellow_fcm_avg
)

select * from fcm_avg
