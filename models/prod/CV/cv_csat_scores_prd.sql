with source as (
    select * from {{ ref('cv_csat_form_responses_stg') }}
)

select
    submission_id,
    submission_date,
    overall_event_experience_score as csat_score,
    case
        when overall_event_experience_score >= 8 then '>=8'
        when overall_event_experience_score >= 5 then '5-8'
        when overall_event_experience_score >= 3 then '3-5'
        else '<3'
    end as csat_category

from source
order by submission_date
