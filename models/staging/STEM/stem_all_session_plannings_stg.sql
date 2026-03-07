with plannings as (
    select
        -- identifiers
        case when Btrim("ID"::text) <> '' then Btrim("ID"::text) end as id,
        case when Btrim("Class"::text) ~ '^\d+$' then ("Class"::text)::integer end as class_number,
        Coalesce(Initcap(Btrim("Class"::text)), '') as class_name,

        -- donor / school / project / trainer parsed from json objects
        Coalesce(("Donor"::jsonb)->>'zc_display_value', ("Donor"::jsonb)->>'Name_of_the_donor', '') as donor,
        Coalesce(("School_Name"::jsonb)->>'zc_display_value', ("School_Name"::jsonb)->>'Centre_Name', '') as school_name,
        Coalesce(("Project_Name"::jsonb)->>'zc_display_value', ("Project_Name"::jsonb)->>'Project_Name', '') as project_name,
        Coalesce(("Trainer_Name"::jsonb)->>'zc_display_value', ("Trainer_Name"::jsonb)->>'Name', '') as trainer_name,
        nullif(Btrim(("Trainer_Name"::jsonb)->>'ID'), '') as trainer_id,

        -- academic year and month info
        Coalesce(Initcap(Btrim("Academic_Year"::text)), '') as academic_year,
        Coalesce(Initcap(Btrim("Session_Month"::text)), '') as session_month,
        case
            when Btrim("Session_Month"::text) ~ '^[A-Za-z]+$'
                then to_char(to_date('01 ' || Initcap(Btrim("Session_Month"::text)) || ' 2000', 'DD Month YYYY'), 'MM')::integer
        end as session_month_number,

        -- class details and plans
        Coalesce(Initcap(Btrim("Class_Division"::text)), '') as class_division,
        case when Btrim("Session_Planned"::text) ~ '^\d+$' then ("Session_Planned"::text)::integer end as session_planned,
        case when Btrim("Experiment_Planned"::text) ~ '^\d+$' then ("Experiment_Planned"::text)::integer end as experiment_planned,

        -- timestamps
        case
            when Btrim("Trainer_Name_Added_Time"::text) = '' then null
            when "Trainer_Name_Added_Time"::text ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then ("Trainer_Name_Added_Time"::text)::timestamp
            when "Trainer_Name_Added_Time"::text ~ '^\d{2}-[A-Za-z]{3}-\d{4}( \d{2}:\d{2}:\d{2})?$' then to_timestamp("Trainer_Name_Added_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')
        end as trainer_name_added_time
    from {{ source('zc_bvms_data', 'All_Session_Plannings') }}
)

select distinct
    id,
    class_number,
    class_name,
    donor,
    school_name,
    project_name,
    trainer_name,
    trainer_id,
    academic_year,
    session_month,
    session_month_number,
    class_division,
    session_planned,
    experiment_planned,
    trainer_name_added_time
from plannings
