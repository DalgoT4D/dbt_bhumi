with trainers as (
    select
        -- identifiers
        nullif(btrim("ID"::text), '') as id,
        coalesce(initcap(btrim("Name"::text)), '') as trainer_name,
        lower(nullif(btrim("Email"::text), '')) as trainer_email,

        -- location / organization info from json
        coalesce(("State"::jsonb)->>'zc_display_value', ("State"::jsonb)->>'State', '') as state,
        coalesce(("Chapter"::jsonb)->>'zc_display_value', ("Chapter"::jsonb)->>'Chapter_Name', '') as chapter,
        coalesce(("Program"::jsonb)->>'zc_display_value', ("Program"::jsonb)->>'Program_Name', '') as program,
        coalesce(("District"::jsonb)->>'zc_display_value', ("District"::jsonb)->>'District', '') as district,

        -- school array raw + count
        case
            when "School"::text like '[%' then "School"::text
            else coalesce(btrim("School"::text), '')
        end as school_list,
        case
            when "School"::text like '[%' then jsonb_array_length("School"::jsonb)
        end as school_count,

        -- status and phone
        coalesce(initcap(btrim("Status"::text)), '') as status,
        case
            when btrim("Phone_Number"::text) ~ '^\+?\d+$' then replace(btrim("Phone_Number"::text), '+', '')
            else nullif(btrim("Phone_Number"::text), '')
        end as phone_number,

        -- reporting manager parsed
        coalesce(("Reporting_Manager"::jsonb)->>'zc_display_value', ("Reporting_Manager"::jsonb)->>'Name', '') as reporting_manager_name,
        nullif(btrim(("Reporting_Manager"::jsonb)->>'Email'), '') as reporting_manager_email,
        nullif(btrim(("Reporting_Manager"::jsonb)->>'ID'), '') as reporting_manager_id,

        -- timestamps
        case
            when btrim("Added_Time"::text) = '' then null
            when "Added_Time"::text ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then ("Added_Time"::text)::timestamp
            when "Added_Time"::text ~ '^\d{2}-[A-Za-z]{3}-\d{4}( \d{2}:\d{2}:\d{2})?$' then to_timestamp("Added_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')
        end as added_time
    from {{ source('zc_bvms_data', 'Trainer_Report') }}
)

select distinct
    id,
    trainer_name,
    trainer_email,
    state,
    chapter,
    program,
    district,
    school_list,
    school_count,
    status,
    phone_number,
    reporting_manager_name,
    reporting_manager_email,
    reporting_manager_id,
    added_time
from trainers
where trainer_name is not null
