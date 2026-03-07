with attendances as (
    select
        -- basic identifiers
        case when Btrim("ID"::text) <> '' then Btrim("ID"::text) end as id,
        case when Btrim("Class"::text) ~ '^\d+$' then ("Class"::text)::integer end as class_number,
        Coalesce(Initcap(Btrim("Class"::text)), '') as class_name,

        -- donor / trainer as json objects with display values
        Coalesce(("Donor"::jsonb)->>'zc_display_value', ("Donor"::jsonb)->>'Name', '') as donor,
        Coalesce(("Trainer"::jsonb)->>'zc_display_value', ("Trainer"::jsonb)->>'Name', '') as trainer_name,
        nullif(Btrim(("Trainer"::jsonb)->>'ID'), '') as trainer_id,

        -- timestamps and dates
        case
            when Btrim("Added_Time"::text) = '' then null
            when "Added_Time"::text ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then ("Added_Time"::text)::timestamp
            when "Added_Time"::text ~ '^\d{2}-[A-Za-z]{3}-\d{4}( \d{2}:\d{2}:\d{2})?$' then to_timestamp("Added_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')
        end as added_time,
        Coalesce(Initcap(Btrim("Class_Type"::text)), '') as class_type,

        Coalesce(("School_Name"::jsonb)->>'zc_display_value', ("School_Name"::jsonb)->>'Centre_Name', '') as school_name,
        Coalesce(("Project_Name"::jsonb)->>'zc_display_value', ("Project_Name"::jsonb)->>'Project_Name', '') as project_name,

        case
            when Btrim("Session_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then ("Session_Date"::text)::date
            when Btrim("Session_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date("Session_Date"::text, 'MM/DD/YYYY')
            when Btrim("Session_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date("Session_Date"::text, 'DD-Mon-YYYY')
        end as session_date,
        Coalesce(Initcap(Btrim("Session_Type"::text)), '') as session_type,

        Coalesce(("Academic_Year"::jsonb)->>'zc_display_value', ("Academic_Year"::jsonb)->>'Year_Name', '') as academic_year,
        case when Btrim("Total_Student"::text) ~ '^\d+$' then ("Total_Student"::text)::integer end as total_students,
        Coalesce(Initcap(Btrim("Class_Division"::text)), '') as class_division,
        case when Btrim("Absent_Students"::text) ~ '^\d+$' then ("Absent_Students"::text)::integer end as absent_students,

        -- volunteer info; supports boolean flags and free text
        case
            when Lower(Btrim("Volunteer_Engaged"::text)) in ('yes', 'y', 'true', '1') then true
            when Lower(Btrim("Volunteer_Engaged"::text)) in ('no', 'n', 'false', '0') then false
        end as volunteer_engaged_flag,
        case
            when "Volunteer_Engaged2"::text like '[%' then "Volunteer_Engaged2"::text
            else Coalesce(Btrim("Volunteer_Engaged2"::text), '')
        end as volunteer_engaged_secondary,

        -- attendance lists (stored as raw strings, plus counts where parsable)
        case
            when "Absent_Students_List"::text like '[%' then "Absent_Students_List"::text
            else Coalesce(Btrim("Absent_Students_List"::text), '')
        end as absent_students_list,
        case
            when "Present_Students_List"::text like '[%' then "Present_Students_List"::text
            else Coalesce(Btrim("Present_Students_List"::text), '')
        end as present_students_list,
        case when Btrim("Present_Students_List1"::text) ~ '^\d+$' then ("Present_Students_List1"::text)::integer end as present_students_count,
        case
            when "Absent_Students_List"::text like '[%' then jsonb_array_length("Absent_Students_List"::jsonb)
        end as absent_students_count,

        -- combined class flag and volunteering hours
        case
            when Lower(Btrim("Was_it_a_combined_class"::text)) in ('yes', 'y', 'true', '1') then true
            when Lower(Btrim("Was_it_a_combined_class"::text)) in ('no', 'n', 'false', '0') then false
        end as was_combined_class,
        case
            when Lower(Btrim("Volunteering_hrs_Engaged"::text)) in ('', 'na') then null
            when Btrim("Volunteering_hrs_Engaged"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Volunteering_hrs_Engaged"::text)::numeric
            when Btrim("Volunteering_hrs_Engaged"::text) ~ '^[0-9]+(\.[0-9]+)?h$' then Replace(Btrim("Volunteering_hrs_Engaged"::text), 'h', '')::numeric
        end as volunteering_hours_engaged
    from {{ source('zc_bvms_data', 'All_Session_Attendances') }}
)

select distinct
    id,
    class_number,
    class_name,
    donor,
    trainer_name,
    trainer_id,
    added_time,
    class_type,
    school_name,
    project_name,
    session_date,
    session_type,
    academic_year,
    total_students,
    class_division,
    absent_students,
    absent_students_count,
    volunteer_engaged_flag,
    volunteer_engaged_secondary,
    absent_students_list,
    present_students_list,
    present_students_count,
    was_combined_class,
    volunteering_hours_engaged
from attendances
