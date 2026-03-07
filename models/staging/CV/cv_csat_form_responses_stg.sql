with source as (
    select * from {{ source('CV_Gsheet_Raw_Data', 'Form_responses_new') }}
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
        event_date::date as event_date,

        -- location
        coalesce(btrim(city::text), '') as city,
        coalesce(btrim(please_specify_your_city::text), '') as city_specified,

        -- date
        submission_date::date as submission_date,

        -- ratings (int)
        overall_event_experience as overall_event_experience_score,
        how_useful_did_you_find_the_orientation_context_setting_and_deb as orientation_usefulness_score,

        -- open-ended feedback
        coalesce(btrim(what_activity_would_you_like_to_volunteer_for_next_::text), '') as next_volunteering_activity,
        coalesce(btrim(any_other_feedback_suggestions_orientation_event_closure_::text), '') as additional_feedback

    from source
)

select * from csat_responses
