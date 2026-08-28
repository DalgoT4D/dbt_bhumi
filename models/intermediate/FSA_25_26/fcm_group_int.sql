{{ config(
  materialized='table',
  tags=["fsa_25_26", "int"]
) }}

select distinct
    fu.fcm_update_id,
    fu.pm_id,
    fu.cohort,
    fu.school,
    fu.fellow_id,
    fu.grade,
    fu.grade_section,
    fu.reporting_period,

    fr.fcm_rating_id,
    fr.rating,
    case
        when cm.goal is not NULL and cm.goal != 0
            then round((fr.rating::numeric / nullif(cm.goal::numeric, 0)) * 100, 2) 
    end as fcm_avg,

    cm.cm_id as competency_mapping_id,
    cm.goal,
    cm.academic_year,
    cm.year_map,
    cm.month,
    cm.quarter,

    c.id_competency as competency_id,
    c.name_competency,
    c.is_active

from {{ ref('fcm_updates') }} as fu

left join {{ ref('fcm_ratings') }} as fr
    on fu.fcm_update_id = fr.fcm_update_id

left join {{ ref('competency_map') }} as cm
    on fr.competency_mapping_id = cm.cm_id

left join {{ ref('competencies') }} as c
    on cm.competency_id = c.id_competency
