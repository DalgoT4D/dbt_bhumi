with centre_identification as (
    select
        -- identifiers
        nullif(btrim("ID"::text), '') as id,
        coalesce(initcap(btrim("Center_Name"::text)), '') as center_name,
        coalesce(initcap(btrim("Center_Type"::text)), '') as center_type,
        coalesce(initcap(btrim("Center_Status"::text)), '') as center_status,
        coalesce(initcap(btrim("Request_Type"::text)), '') as request_type,

        -- location information
        coalesce(("City"::jsonb)->>'zc_display_value', ("City"::jsonb)->>'City', '') as city,
        coalesce(("State"::jsonb)->>'zc_display_value', ("State"::jsonb)->>'State', '') as state,
        coalesce(initcap(btrim("Center_Address"::text)), '') as center_address,
        nullif(btrim("Pin_code"::text), '') as pin_code,

        -- organizational information
        coalesce(("Chapter"::jsonb)->>'zc_display_value', ("Chapter"::jsonb)->>'Chapter', '') as chapter,
        coalesce(("Program"::jsonb)->>'zc_display_value', ("Program"::jsonb)->>'Program', '') as program,
        coalesce(("Project"::jsonb)->>'zc_display_value', ("Project"::jsonb)->>'Project', '') as project,
        coalesce(("Stage_New"::jsonb)->>'zc_display_value', ("Stage_New"::jsonb)->>'Stage', '') as stage_new,

        -- finalized programs
        case
            when "Finalized_Programs_with_Bhumi"::text like '[%' then "Finalized_Programs_with_Bhumi"::text
            else null
        end as finalized_programs_list,
        case
            when "Finalized_Programs_with_Bhumi"::text like '[%' then jsonb_array_length("Finalized_Programs_with_Bhumi"::jsonb)
        end as finalized_programs_count,

        -- point of contact information
        coalesce(initcap(btrim("POC_Name"::text)), '') as poc_name,
        coalesce(initcap(btrim("POC_Designation"::text)), '') as poc_designation,

        -- center contact information
        nullif(btrim("Center_Email_ID"::text), '') as center_email_id,

        -- head of organization information
        coalesce(initcap(btrim("Head_of_the_organisation"::text)), '') as head_of_organisation,

        -- donor information
        coalesce(initcap(btrim("Add_Donor_Name_of_the_donor"::text)), '') as donor_name,

        -- children count by grade
        case when btrim("No_of_Children_s_in_PRE_KG"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_PRE_KG"::text)::integer end as children_pre_kg,
        case when btrim("No_of_Children_s_in_LKG"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_LKG"::text)::integer end as children_lkg,
        case when btrim("No_of_Childrens_in_UKG"::text) ~ '^[0-9]+$' then ("No_of_Childrens_in_UKG"::text)::integer end as children_ukg,
        case when btrim("No_of_Children_s_in_class_1"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_1"::text)::integer end as children_class_1,
        case when btrim("No_of_Children_s_in_class_2"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_2"::text)::integer end as children_class_2,
        case when btrim("No_of_Children_s_in_class_3"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_3"::text)::integer end as children_class_3,
        case when btrim("No_of_Children_s_in_class_4"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_4"::text)::integer end as children_class_4,
        case when btrim("No_of_Children_s_in_class_5"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_5"::text)::integer end as children_class_5,
        case when btrim("No_of_Children_s_in_class_6"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_6"::text)::integer end as children_class_6,
        case when btrim("No_of_Children_s_in_class_7"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_7"::text)::integer end as children_class_7,
        case when btrim("No_of_Children_s_in_class_8"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_8"::text)::integer end as children_class_8,
        case when btrim("No_of_Children_s_in_class_9"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_9"::text)::integer end as children_class_9,
        case when btrim("No_of_Children_s_in_class_10"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_10"::text)::integer end as children_class_10,
        case when btrim("No_of_Children_s_in_class_11"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_11"::text)::integer end as children_class_11,
        case when btrim("No_of_Children_s_in_class_12"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_class_12"::text)::integer end as children_class_12,
        case when btrim("No_of_Children_s_in_ITI"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_ITI"::text)::integer end as children_iti,
        case when btrim("No_of_Children_s_in_College"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_College"::text)::integer end as children_college,
        case when btrim("No_of_Children_s_in_Non_Formal_Education"::text) ~ '^[0-9]+$' then ("No_of_Children_s_in_Non_Formal_Education"::text)::integer end as children_non_formal_education,

        -- children count by gender and overall
        case when btrim("No_of_Male_Children"::text) ~ '^[0-9]+$' then ("No_of_Male_Children"::text)::integer end as children_male,
        case when btrim("No_of_Female_Children"::text) ~ '^[0-9]+$' then ("No_of_Female_Children"::text)::integer end as children_female,
        case when btrim("Overall_Children_count"::text) ~ '^[0-9]+$' then ("Overall_Children_count"::text)::integer end as overall_children_count,
        case when btrim("Total_Count"::text) ~ '^[0-9]+$' then ("Total_Count"::text)::integer end as total_count,

        -- resources and infrastructure
        case when btrim("No_of_working_computers"::text) ~ '^[0-9]+$' then ("No_of_working_computers"::text)::integer end as working_computers,
        case when btrim("No_of_rooms_provided_for_Bhumi"::text) ~ '^[0-9]+$' then ("No_of_rooms_provided_for_Bhumi"::text)::integer end as rooms_provided_for_bhumi,
        case when btrim("Quantity_of_Blackboard_Whiteboard"::text) ~ '^[0-9]+$' then ("Quantity_of_Blackboard_Whiteboard"::text)::integer end as quantity_blackboard_whiteboard,
        coalesce(btrim("Other_available_resources"::text), '') as other_available_resources,
        coalesce(btrim("Are_the_rooms_provided_have_sufficient_space_furniture_and_infr"::text), '') as rooms_sufficient_space,

        -- estimated amount
        case when btrim("Estimated_Amount_in_Rs"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Estimated_Amount_in_Rs"::text)::numeric end as estimated_amount_rs,

        -- interest and engagement
        coalesce(initcap(btrim("Are_they_interested_in_Bhumi"::text)), '') as interested_in_bhumi,
        coalesce(btrim("If_No_Maybe_Reasons"::text), '') as no_maybe_reasons,
        coalesce(btrim("Overall_attitude_of_the_centre_authority"::text), '') as overall_attitude,

        -- schedule and preferences
        coalesce(btrim("Center_Mgmt_referred_day_for_Bhumi_classes"::text), '') as referred_day_for_classes,
        coalesce(btrim("Center_Mgmt_referred_time_for_Bhumi_classes"::text), '') as referred_time_for_classes,
        coalesce(btrim("Preferred_Accessible_Mode"::text), '') as preferred_accessible_mode,
        coalesce(btrim("Weekly_prayer_time_if_any"::text), '') as weekly_prayer_time,
        coalesce(btrim("Any_other_weekend_classes"::text), '') as other_weekend_classes,

        -- additional context and information
        coalesce(btrim("What_type_of_school_most_children_attend"::text), '') as school_type_children_attend,
        coalesce(btrim("What_do_children_usually_do_after_class_12th"::text), '') as children_after_class_12,
        coalesce(btrim("Class_till_which_the_Children_are_retained_at_Home_Centre"::text), '') as children_retained_till_class,
        coalesce(btrim("What_is_the_age_group_of_children_at_the_center_for_eg_age_8_16"::text), '') as age_group_of_children,

        -- comments and feedback
        coalesce(btrim("Comments"::text), '') as comments,
        coalesce(btrim("Additional_Info_Feedback"::text), '') as additional_info_feedback,

        -- timestamp
        case
            when btrim("Modified_Time"::text) = '' then null
            when "Modified_Time"::text ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then ("Modified_Time"::text)::timestamp
            when "Modified_Time"::text ~ '^\d{2}-[A-Za-z]{3}-\d{4}( \d{2}:\d{2}:\d{2})?$' then to_timestamp("Modified_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')
        end as modified_time

    from {{ source('zc_bvms_data', 'STP_Centre_Identification_New_Report') }}
)

select distinct
    -- identifiers
    id,
    center_name,
    center_type,
    center_status,
    request_type,

    -- location
    city,
    state,
    center_address,
    pin_code,

    -- organizational
    chapter,
    program,
    project,
    stage_new,
    finalized_programs_list,
    finalized_programs_count,

    -- contacts
    poc_name,
    poc_designation,
    center_email_id,
    head_of_organisation,
    donor_name,

    -- children counts by grade
    children_pre_kg,
    children_lkg,
    children_ukg,
    children_class_1,
    children_class_2,
    children_class_3,
    children_class_4,
    children_class_5,
    children_class_6,
    children_class_7,
    children_class_8,
    children_class_9,
    children_class_10,
    children_class_11,
    children_class_12,
    children_iti,
    children_college,
    children_non_formal_education,

    -- children counts by gender
    children_male,
    children_female,
    overall_children_count,
    total_count,

    -- resources
    working_computers,
    rooms_provided_for_bhumi,
    quantity_blackboard_whiteboard,
    other_available_resources,
    rooms_sufficient_space,
    estimated_amount_rs,

    -- interest and engagement
    interested_in_bhumi,
    no_maybe_reasons,
    overall_attitude,

    -- schedule
    referred_day_for_classes,
    referred_time_for_classes,
    preferred_accessible_mode,
    weekly_prayer_time,
    other_weekend_classes,

    -- context
    school_type_children_attend,
    children_after_class_12,
    children_retained_till_class,
    age_group_of_children,

    -- feedback
    comments,
    additional_info_feedback,

    -- timestamp
    modified_time

from centre_identification
