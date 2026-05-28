with cleaned as (
    select
        case
            when btrim("S_NO") ~ '^\d+$'
                then "S_NO"::integer
        end as s_no,
        nullif(btrim("NA_Link"), '') as na_link,
        nullif(initcap(btrim("District")), '') as district,
        nullif(initcap(btrim("Location")), '') as location,
        nullif(btrim("Remarks_"), '') as remarks,
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
        nullif(initcap(btrim("NA_Done_by")), '') as na_done_by,
        nullif(btrim("Folder_Link"), '') as folder_link,
        nullif(initcap(btrim("School_Type")), '') as school_type,
        nullif(initcap(btrim("Name_of_School")), '') as name_of_school
    from {{ source('iginteplus_25_26', 'Ignite__Need_Assessment_Data_25_26') }}
)

select 
    s_no,
    na_link,
    district,
    location,
    remarks,
    date_of_na,
    na_done_by,
    folder_link,
    school_type,
    name_of_school
from cleaned
where name_of_school is not null
