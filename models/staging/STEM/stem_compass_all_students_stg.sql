with students as (
    select
        -- identifiers
        nullif(btrim("ID"::text), '') as id,
        coalesce(initcap(btrim("Name"::text)), '') as student_name,
        case when btrim("Class"::text) ~ '^\d+$' then ("Class"::text)::integer end as class_number,

        -- donor/state/district jsons
        coalesce(("Donor"::jsonb)->>'zc_display_value', ("Donor"::jsonb)->>'Name_of_the_donor', '') as donor,
        coalesce(("State"::jsonb)->>'zc_display_value', ("State"::jsonb)->>'State', '') as state,
        coalesce(initcap(btrim("Gender"::text)), '') as gender,
        coalesce(initcap(btrim("Status"::text)), '') as status,
        coalesce(("District"::jsonb)->>'zc_display_value', ("District"::jsonb)->>'District', '') as district,

        -- timestamp
        case
            when btrim("Added_Time"::text) = '' then null
            when "Added_Time"::text ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then ("Added_Time"::text)::timestamp
            when "Added_Time"::text ~ '^\d{2}-[A-Za-z]{3}-\d{4}( \d{2}:\d{2}:\d{2})?$' then to_timestamp("Added_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')
        end as added_time,

        -- school
        coalesce(("School_Name"::jsonb)->>'zc_display_value', ("School_Name"::jsonb)->>'Centre_Name', '') as school_name,

        -- project and trainer
        case
            when "Project_Name"::text like '[%' then coalesce(("Project_Name"::jsonb->0)->>'zc_display_value', ("Project_Name"::jsonb->0)->>'Project_Name', '')
            else coalesce(initcap(btrim("Project_Name"::text)), '')
        end as project_name_primary,
        case
            when "Project_Name"::text like '[%' then "Project_Name"::text
        end as project_name_list,
        case
            when "Project_Name"::text like '[%' then jsonb_array_length("Project_Name"::jsonb)
        end as project_name_count,
        coalesce(("Trainer_Name"::jsonb)->>'zc_display_value', ("Trainer_Name"::jsonb)->>'Name', '') as trainer_name,
        nullif(btrim(("Trainer_Name"::jsonb)->>'ID'), '') as trainer_id,

        -- income
        case when btrim("Father_Income"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Father_Income"::text)::numeric end as father_income,
        case when btrim("Mother_Income"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Mother_Income"::text)::numeric end as mother_income,

        -- management and academic info
        coalesce(("Zonal_Manager"::jsonb)->>'zc_display_value', ("Zonal_Manager"::jsonb)->>'Name', '') as zonal_manager,
        coalesce(initcap(btrim("Academic_Year2"::text)), '') as academic_year,
        coalesce(initcap(btrim("Class_Division"::text)), '') as class_division,
        nullif(btrim("School_Roll_No"::text), '') as school_roll_no,
        coalesce(("Reporting_Manager"::jsonb)->>'zc_display_value', ("Reporting_Manager"::jsonb)->>'Name', '') as reporting_manager_name,
        nullif(btrim(("Reporting_Manager"::jsonb)->>'ID'), '') as reporting_manager_id,
        nullif(btrim(("Reporting_Manager"::jsonb)->>'Email'), '') as reporting_manager_email
        
    from {{ source('zc_bvms_data', 'Compass_All_Students') }}
)

select distinct
    id,
    student_name,
    class_number,
    class_division,
    school_roll_no,
    school_name,
    state,
    district,
    donor,
    project_name_primary,
    project_name_list,
    project_name_count,
    trainer_name,
    trainer_id,
    reporting_manager_name,
    reporting_manager_id,
    reporting_manager_email,
    zonal_manager,
    academic_year,
    status,
    gender,
    added_time,
    father_income,
    mother_income
from students
