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
      when odc_count + checkin_count = 0 then 'Likely unreported'
      else 'Reported'
    end as pm_unreported,
    case
      when school_leader_checkin_count + teacher_circle_count + community_visit_count
        + teaching_hours_sum + helo_circles_count + ptms_count = 0
        then 'Likely unreported'
      else 'Reported'
    end as fellow_unreported
from {{ ref('interaction_int') }} 