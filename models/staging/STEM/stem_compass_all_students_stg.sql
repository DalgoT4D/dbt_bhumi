with students as (
    select
        -- identifiers
        nullif(Btrim("ID"::text), '') as id,
        Coalesce(Initcap(Btrim("Name"::text)), '') as student_name,
        case when Btrim("Class"::text) ~ '^\d+$' then ("Class"::text)::integer end as class_number,

        -- donor/state/district jsons
        Coalesce(("Donor"::jsonb)->>'zc_display_value', ("Donor"::jsonb)->>'Name_of_the_donor', '') as donor,
        Coalesce(("State"::jsonb)->>'zc_display_value', ("State"::jsonb)->>'State', '') as state,
        Coalesce(Initcap(Btrim("Gender"::text)), '') as gender,
        Coalesce(Initcap(Btrim("Status"::text)), '') as status,
        Coalesce(("District"::jsonb)->>'zc_display_value', ("District"::jsonb)->>'District', '') as district,

        -- timestamp
        case
            when Btrim("Added_Time"::text) = '' then null
            when "Added_Time"::text ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then ("Added_Time"::text)::timestamp
            when "Added_Time"::text ~ '^\d{2}-[A-Za-z]{3}-\d{4}( \d{2}:\d{2}:\d{2})?$' then to_timestamp("Added_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')
        end as added_time,

        -- school
        Coalesce(("School_Name"::jsonb)->>'zc_display_value', ("School_Name"::jsonb)->>'Centre_Name', '') as school_name,

        -- project and trainer
        case
            when "Project_Name"::text like '[%' then Coalesce(("Project_Name"::jsonb->0)->>'zc_display_value', ("Project_Name"::jsonb->0)->>'Project_Name', '')
            else Coalesce(Initcap(Btrim("Project_Name"::text)), '')
        end as project_name_primary,
        case
            when "Project_Name"::text like '[%' then "Project_Name"::text
            else null
        end as project_name_list,
        case
            when "Project_Name"::text like '[%' then jsonb_array_length("Project_Name"::jsonb)
        end as project_name_count,
        Coalesce(("Trainer_Name"::jsonb)->>'zc_display_value', ("Trainer_Name"::jsonb)->>'Name', '') as trainer_name,
        nullif(Btrim(("Trainer_Name"::jsonb)->>'ID'), '') as trainer_id,

        -- income
        case when Btrim("Father_Income"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Father_Income"::text)::numeric end as father_income,
        case when Btrim("Mother_Income"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Mother_Income"::text)::numeric end as mother_income,

        -- management and academic info
        Coalesce(("Zonal_Manager"::jsonb)->>'zc_display_value', ("Zonal_Manager"::jsonb)->>'Name', '') as zonal_manager,
        Coalesce(Initcap(Btrim("Academic_Year2"::text)), '') as academic_year,
        Coalesce(Initcap(Btrim("Class_Division"::text)), '') as class_division,
        nullif(Btrim("School_Roll_No"::text), '') as school_roll_no,
        Coalesce(("Reporting_Manager"::jsonb)->>'zc_display_value', ("Reporting_Manager"::jsonb)->>'Name', '') as reporting_manager_name,
        nullif(Btrim(("Reporting_Manager"::jsonb)->>'ID'), '') as reporting_manager_id,
        nullif(Btrim(("Reporting_Manager"::jsonb)->>'Email'), '') as reporting_manager_email
        
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
