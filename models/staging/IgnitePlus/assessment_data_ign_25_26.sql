with cleaned as (
    select
        case
            when
                btrim("Date_of_NA") <> ''
                and (
                    btrim("Date_of_NA") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Date_of_NA") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Date_of_NA") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Date_of_NA")
                        else regexp_replace(btrim("Date_of_NA"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Date_of_NA") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as date_of_na,
        nullif(trim("Name_of_School"), '') as name_of_school,
        nullif(trim("School_Type"), '') as school_type,
        nullif(trim("Location"), '') as location,
        nullif(trim("District"), '') as district,
        nullif(trim("State"), '') as state,
        nullif(trim("Scope_of_work"), '') as scope_of_work,
        nullif(trim("NA_Link"), '') as na_link,
        nullif(trim("Folder_Link"), '') as folder_link,
        nullif(trim("NA_Done_by"), '') as na_done_by,
        case
            when trim("Estimated_amount") = '' then null
            else regexp_replace(trim("Estimated_amount"), '[^0-9\.]+', '', 'g')::numeric
        end as estimated_amount
    from {{ source('iginteplus_25_26', 'Ignite__Need_Assessment_Data_25_26') }}
)

select 
    date_of_na,
    name_of_school,
    school_type,
    location,
    district,
    state,
    scope_of_work,
    na_link,
    folder_link,
    na_done_by,
    estimated_amount
from cleaned
where name_of_school is not null
