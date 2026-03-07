with source as (
    select * from {{ ref('cv_nps_form_responses_stg') }}
)

select
    submission_id,
    submission_date,
    nps_score

from source
order by submission_date
