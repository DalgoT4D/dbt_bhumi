with fcm_ratings as (
    select 
        NULLIF(BTRIM(id::TEXT),'') as id_fcm_rating,
        -- NULLIF(BTRIM(description::TEXT),'') as description,
        NULLIF(BTRIM(fcm_update_id::TEXT),'') as fcm_update_id,
        NULLIF(BTRIM(competency_mapping_id::TEXT),'') as competency_mapping_id,
        NULLIF(BTRIM(rating::TEXT),'')::INTEGER as rating

    from {{ source('fellowship_school_app_25_26', 'fcm_ratings_25_26') }}
    where
        NULLIF(BTRIM(id::TEXT),'') is not NULL
),

fcm_updates as (
    select 
        NULLIF(BTRIM(id::TEXT),'') as id_fcm_update,
        NULLIF(BTRIM(city::TEXT),'') as city,
        NULLIF(BTRIM(pm_id::TEXT),'') as pm_id,
        NULLIF(BTRIM(cohort::TEXT),'') as cohort,
        NULLIF(BTRIM(school::TEXT),'') as school,
        NULLIF(BTRIM(pm_name::TEXT),'') as pm_name,
        NULLIF(BTRIM(fellow_id::TEXT),'') as fellow_id,
        NULLIF(BTRIM(fellow_name::TEXT),'') as fellow_name,
        REGEXP_REPLACE(BTRIM(grade_section::TEXT), '[^0-9]', '', 'g') as grade,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(BTRIM(grade_section::TEXT), ''), '-', '', 'g'), '([0-9])([a-zA-Z])', '\1 \2', 'g')) as grade_section,
        NULLIF(BTRIM(reporting_date::TEXT),'')::DATE as reporting_date,
        TO_CHAR(reporting_date, 'Mon YYYY') as month_year,
        NULLIF(BTRIM(total_students::TEXT),'')::INTEGER as total_students,
        NULLIF(BTRIM(reporting_period::TEXT),'') as reporting_period

    from {{ source('fellowship_school_app_25_26', 'fcm_updates_25_26') }}
    where
        NULLIF(BTRIM(id::TEXT),'') is not NULL
),

competency_mappings as (
    select 
        NULLIF(BTRIM(id::TEXT),'') as id_competency_mapping,
        NULLIF(BTRIM(goal::TEXT),'')::INTEGER as goal,
        NULLIF(BTRIM(end_date::TEXT),'')::DATE as cm_end_date,
        NULLIF(BTRIM(is_active::TEXT),'')::BOOLEAN as is_active,
        NULLIF(BTRIM(start_date::TEXT),'')::DATE as cm_start_date,
        NULLIF(BTRIM(cohort_year::TEXT),'') as cm_cohort,
        NULLIF(BTRIM(competency_id::TEXT),'') as competency_id
    from {{ source('fellowship_school_app_25_26', 'competency_mappings_25_26') }}
    where
        NULLIF(BTRIM(id::TEXT),'') is not NULL
        and NULLIF(BTRIM(is_active::TEXT),'') = 'true'
),

competencies as (
    select 
        NULLIF(BTRIM(id::TEXT),'') as id_competency,
        NULLIF(BTRIM(name::TEXT),'') as name_competency,
        NULLIF(BTRIM(is_active::TEXT),'')::BOOLEAN as is_active

    from {{ source('fellowship_school_app_25_26', 'competencies_25_26') }}
    where
        NULLIF(BTRIM(id::TEXT),'') is not NULL
        and NULLIF(BTRIM(is_active::TEXT),'') = 'true'
)

select distinct
    fcm_r.id_fcm_rating,
    fcm_r.rating,
    fcm_u.fellow_id,  
    fcm_u.fellow_name,
    fcm_u.cohort,
    fcm_u.pm_id,
    fcm_u.pm_name,
    fcm_u.school,
    fcm_u.grade,
    fcm_u.grade_section,
    fcm_u.city,
    fcm_u.reporting_date,
    fcm_u.month_year,
    fcm_u.reporting_period,
    fcm_u.total_students,
    cm.goal,
    cm.cm_start_date,
    cm.cm_end_date,
    cm.cm_cohort,
    c.name_competency

from fcm_ratings as fcm_r
left join fcm_updates as fcm_u
    on fcm_r.fcm_update_id = fcm_u.id_fcm_update
left join competency_mappings as cm
    on fcm_r.competency_mapping_id = cm.id_competency_mapping
left join competencies as c
    on cm.competency_id = c.id_competency
