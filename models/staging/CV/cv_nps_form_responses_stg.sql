with source as (
    select * from {{ source('CV_Gsheet_Raw_Data', 'Form_responses') }}
),

nps_responses as (
    select
        -- identifiers
        nullif(btrim(submission_id::text), '') as submission_id,

        -- respondent info
        nullif(btrim(name::text), '') as name,
        nullif(btrim(email::text), '') as email,
        nullif(btrim(corporate_name::text), '') as corporate_name,

        -- date
        submission_date::date as submission_date,

        -- NPS / rating (int)
        based_on_your_experience_in_the_last_three_months_how_likely_ar as nps_score,

        -- likelihood to continue (varchar)
        coalesce(btrim(how_likely_are_you_to_continue_working_with_bhumi_in_the_next_6::text), '') as likelihood_to_continue,

        -- dimension ratings (varchar)
        coalesce(btrim(a_communication_account_management::text), '') as rating_communication_account_management,
        coalesce(btrim(b_project_implementation_coordination::text), '') as rating_project_implementation_coordination,
        coalesce(btrim(c_employee_experience_engagement::text), '') as rating_employee_experience_engagement,
        coalesce(btrim(d_impact_reporting_documentation::text), '') as rating_impact_reporting_documentation,

        -- open-ended feedback
        coalesce(btrim(what_is_the_one_key_thing_bhumi_could_improve_to_strengthen_thi::text), '') as key_improvement_area,
        coalesce(btrim(please_use_this_space_to_share_why_you_selected_the_above_optio::text), '') as likelihood_reason,
        coalesce(btrim(any_additional_comments_or_suggestions_::text), '') as additional_comments

    from source
)

select * from nps_responses
