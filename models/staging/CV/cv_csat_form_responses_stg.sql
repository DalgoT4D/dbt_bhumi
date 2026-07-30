{{ config(
  materialized='table',
  tags=["cv", "staging"]
) }}

with clean_dates as (
    select
        *,
        nullif(trim("Event_Event_Start_Date"::text), '') as event_date_clean
    from {{ source('zc_bvms_data', 'CSAT_Form_Report') }}
),

csat_responses as (
    select
        -- identifiers
        nullif(btrim("ID"::text), '') as submission_id,

        -- respondent info
        nullif(btrim("Name"::text), '') as name,

        -- event info
        nullif(btrim("Event_Event_Name"::text), '') as event_name,
        {{ validate_date('event_date_clean') }} as event_date, -- noqa: LT02
        "Event_Coordinator_Name"::text as event_coordinator_name,

        -- date
        "Added_Time"::date as submission_date,

        -- ratings (int)
        substring(trim("Overall_Event_Experience"::text) from '([0-9]+)')::int as overall_event_experience_score,
        substring(trim("How_useful_did_you_find_the_orientation_context_setting_and_deb"::text) from '([0-9]+)')::int as orientation_usefulness_score,

        -- open-ended feedback
        coalesce(btrim("What_type_of_volunteer_activity_would_you_like_to_participate_i"::text), '') as next_volunteering_activity,
        coalesce(btrim("Please_share_any_feedback_or_suggestions_regarding_the_orientat"::text), '') as additional_feedback

    from clean_dates
)

select * from csat_responses
