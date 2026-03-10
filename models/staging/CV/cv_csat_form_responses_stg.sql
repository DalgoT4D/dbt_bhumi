with clean_dates as (
    select
        *,
        regexp_replace(event_date::text, '[^0-9./\-]', '', 'g') as event_date_clean
    from {{ source('CV_Gsheet_Raw_Data', 'Form_responses_new') }}
),

csat_responses as (
    select
        -- identifiers
        nullif(btrim(submission_id::text), '') as submission_id,

        -- respondent info
        nullif(btrim(name::text), '') as name,
        nullif(btrim(company_name::text), '') as company_name,
        nullif(btrim(bhumi_spoc_name::text), '') as bhumi_spoc_name,

        -- event info
        nullif(btrim(event_name::text), '') as event_name,
        {{ validate_date('event_date_clean') }} as event_date,

        -- location
        coalesce(btrim(city::text), '') as city,
        coalesce(btrim(please_specify_your_city::text), '') as city_specified,

        -- date
        submission_date::date as submission_date,

        -- ratings (int)
        case when btrim(overall_event_experience::text) ~ '^\d+$' then (overall_event_experience::text)::integer end as overall_event_experience_score,
        case when btrim(how_useful_did_you_find_the_orientation_context_setting_and_deb::text) ~ '^\d+$' then (how_useful_did_you_find_the_orientation_context_setting_and_deb::text)::integer end as orientation_usefulness_score,

        -- open-ended feedback
        coalesce(btrim(what_activity_would_you_like_to_volunteer_for_next_::text), '') as next_volunteering_activity,
        coalesce(btrim(any_other_feedback_suggestions_orientation_event_closure_::text), '') as additional_feedback

    from clean_dates
)

select * from csat_responses
