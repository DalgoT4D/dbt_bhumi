{{ config(
  materialized='table',
  tags=["igniteplus", "staging"]
) }}

with master as (

    select
        coalesce(btrim("Project_ID"), '') as project_id,
        case when btrim("Sl_No") ~ '^\d+$' then btrim("Sl_No")::integer end as sl_no,

        case
            when btrim("Financial_Year") ~ '^\d{2}-\d{2}$'
                then
                    '20' || substring(btrim("Financial_Year") from '^(\d{2})')
                    || '-' || '20' || substring(btrim("Financial_Year") from '-(\d{2})$')
            when btrim("Financial_Year") ~ '^\d{4}-\d{4}$'
                then btrim("Financial_Year")
        end as financial_year,

        coalesce(initcap(btrim("Quarter")), '') as quarter,
        coalesce(initcap(btrim("Name_of_school___Location")), '') as school_name,
        coalesce(initcap(btrim("Type_of_School")), '') as school_type,
        coalesce(initcap(btrim("District")), '') as district,
        coalesce(initcap(btrim("School_Classification")), '') as school_classification,
        case when btrim("School_count") ~ '^\d+$' then btrim("School_count")::integer end as school_count,
        case when btrim("Student_count") ~ '^\d+$' then btrim("Student_count")::integer end as student_count,
        coalesce(initcap(btrim("STEM_STP")), '') as stem_stp,
        coalesce(initcap(btrim("Fellowship")), '') as fellowship,
        case when btrim("Eco_Champ") ~ '^\d+$' then btrim("Eco_Champ")::integer end as eco_champ,
        coalesce(btrim("URL_Link"), '') as link,
        coalesce(initcap(btrim("Name_of_CSR_Partner")), '') as csr_partner,
        coalesce(initcap(btrim("Project_Status")), '') as project_status,

        case
            when regexp_replace(split_part(btrim("Project_execution_Budget__MOU_"), E'\\t', 1), '₹|,|\s', '', 'g') ~ '^\d+$'
                then (
                    regexp_replace(
                        split_part(btrim("Project_execution_Budget__MOU_"), E'\\t', 1),
                        '₹|,|\s',
                        '',
                        'g'
                    )::integer
                )
        end as project_execution_budget,

        case
            when regexp_replace(split_part(btrim("Total_project_budget__With_Branding__PM___Admin_cost___MOU_"), E'\\t', 1), '₹|,|\s', '', 'g') ~ '^\d+$'
                then (
                    regexp_replace(
                        split_part(btrim("Total_project_budget__With_Branding__PM___Admin_cost___MOU_"), E'\\t', 1),
                        '₹|,|\s',
                        '',
                        'g'
                    )::integer
                )
        end as total_project_budget,

        -- DATE CLEANING
        case
            when btrim("Planned_Start_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                then to_date(
                    regexp_replace(btrim("Planned_Start_Date"), '\s*-\s*', '-', 'g'),
                    'DD-Mon-YYYY'
                )
        end as planned_start_date,

        case
            when btrim("Planned_End_date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                then to_date(
                    regexp_replace(btrim("Planned_End_date"), '\s*-\s*', '-', 'g'),
                    'DD-Mon-YYYY'
                )
        end as planned_end_date,

        case
            when btrim("Actual_Start_date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                then to_date(
                    regexp_replace(btrim("Actual_Start_date"), '\s*-\s*', '-', 'g'),
                    'DD-Mon-YYYY'
                )
        end as actual_start_date,

        case
            when btrim("Actual_End_date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                then to_date(
                    regexp_replace(btrim("Actual_End_date"), '\s*-\s*', '-', 'g'),
                    'DD-Mon-YYYY'
                )
        end as actual_end_date,

        case when btrim("Actual_Duration__days_") ~ '^\d+$' then btrim("Actual_Duration__days_")::integer end as actual_duration_days,
        case when btrim("Planned_Duration__days_") ~ '^\d+$' then btrim("Planned_Duration__days_")::integer end as planned_duration_days,
        case when btrim("Date_of_Confirmation_from_CR_team") ~ '^\d+$' then btrim("Date_of_Confirmation_from_CR_team")::integer end as cr_confirmation_days,
        case when btrim("Date_of_funds_received") ~ '^\d+$' then btrim("Date_of_funds_received")::integer end as funds_received_days,
        case when btrim("No_of_Washroom_Constructred__Project_details_") ~ '^\d+$' then btrim("No_of_Washroom_Constructred__Project_details_")::integer end as washroom_constructed,
        case when btrim("No_of_Washroom_Renovated___Project_details_") ~ '^\d+$' then btrim("No_of_Washroom_Renovated___Project_details_")::integer end as washroom_renovated,
        case when btrim("No_of_Classroom_Constructred___Project_details_") ~ '^\d+$' then btrim("No_of_Classroom_Constructred___Project_details_")::integer end as classroom_constructed,
        case when btrim("No_of_Classroom_Renovated___Project_details_") ~ '^\d+$' then btrim("No_of_Classroom_Renovated___Project_details_")::integer end as classroom_renovated,
        case when btrim("No_of_furnitures_provided___Project_details_") ~ '^\d+$' then btrim("No_of_furnitures_provided___Project_details_")::integer end as furniture_count,
        case when btrim("No_of_RO_Plant_Installed___Project_details_") ~ '^\d+$' then btrim("No_of_RO_Plant_Installed___Project_details_")::integer end as ro_plants,
        case when btrim("No_of_existing_RO_Plant_AMC_extened___Project_details_") ~ '^\d+$' then btrim("No_of_existing_RO_Plant_AMC_extened___Project_details_")::integer end as ro_plant_amc_extended,
        case when btrim("No_of_Solar_panel_work_Installed___Project_details_") ~ '^\d+$' then btrim("No_of_Solar_panel_work_Installed___Project_details_")::integer end as solar_panels,
        case when btrim("Others___Project_details_") ~ '^\d+$' then btrim("Others___Project_details_")::integer end as other_project_counts,
        -- TEXT REMAINING
        coalesce(initcap(btrim("Remarks")), '') as remarks,
        coalesce(initcap(btrim("Post_completion_monitored_report_Link")), '') as post_completion_monitored_report_link

    from {{ source('iginteplus_25_26', 'Ignite__Master_Data_25_26') }}

),

budget_scale as (

    select
        master.*,
        case
            when master.project_execution_budget <= 1000000 then 'Small'
            when master.project_execution_budget <= 5000000 then 'Medium'
            when master.project_execution_budget > 5000000 then 'Large'
        end as project_scale
    from master

)

select 
    project_id,
    sl_no,
    financial_year,
    quarter,
    district,
    school_type,
    school_classification,
    school_name,
    csr_partner,
    project_status,
    stem_stp,
    fellowship,
    eco_champ,
    project_execution_budget,
    total_project_budget,
    project_scale,
    -- planned_start_date,
    -- actual_start_date,
    -- planned_end_date,
    -- actual_end_date,
    actual_duration_days,
    planned_duration_days,
    school_count,
    student_count,
    cr_confirmation_days,
    funds_received_days,
    washroom_constructed,
    washroom_renovated,
    classroom_constructed,
    classroom_renovated,
    furniture_count,
    ro_plants,
    ro_plant_amc_extended,
    solar_panels,
    other_project_counts,
    link,
    remarks,
    post_completion_monitored_report_link 

from budget_scale
where school_name <> ''
