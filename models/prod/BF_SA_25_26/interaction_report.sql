{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

select
    fellow_name,
    cohort,
    pm,
    school,
    donor_name,
    month,
    quarter,
    odc_count,
    checkin_count,
    fcm_update_count,
    school_leader_checkin_count,
    teacher_circle_count,
    community_visit_count,
    teaching_hours_sum,
    helo_circles_count,
    ptms_count,
    case
        when coalesce(odc_count, 0) + coalesce(checkin_count, 0) = 0
            then 'Likely unreported'
        else 'Reported'
    end as pm_unreported,
    case
        when
            coalesce(school_leader_checkin_count, 0)
            + coalesce(teacher_circle_count, 0)
            + coalesce(community_visit_count, 0)
            + coalesce(teaching_hours_sum, 0)
            + coalesce(helo_circles_count, 0)
            + coalesce(ptms_count, 0) = 0
            then 'Likely unreported'
        else 'Reported'
    end as fellow_unreported
from {{ ref('interaction_int') }} 
