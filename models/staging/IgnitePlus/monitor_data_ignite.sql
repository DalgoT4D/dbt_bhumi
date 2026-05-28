with cleaned as (
    select
        case
            when btrim("S_NO") ~ '^\d+$'
                then "S_NO"::integer
        end as s_no,
        case
            when btrim("Year") ~ '^\d{4}$'
                then "Year"::integer
        end as year,
        nullif(initcap(btrim("Visited_By")), '') as visited_by,
        nullif(btrim("Folder_Link"), '') as folder_link,
        case
            when
                btrim("Date_of_Visit") <> ''
                and (
                    btrim("Date_of_Visit") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Date_of_Visit") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{2,4}$'
                    or btrim("Date_of_Visit") ~ '^\d{1,2}\s+[A-Za-z]{3}\s+\d{2,4}$'
                )
                then to_date(
                    case
                        when btrim("Date_of_Visit") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Date_of_Visit")
                        else regexp_replace(btrim("Date_of_Visit"), '\s+', '-', 'g')
                    end,
                    case
                        when btrim("Date_of_Visit") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        when btrim("Date_of_Visit") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$' then 'DD-Mon-YYYY'
                        else 'DD-Mon-YY'
                    end
                )
        end as date_of_visit,
        nullif(initcap(btrim("Name_of_District__State")), '') as district_state,
        nullif(initcap(btrim("Name_of_School___Address")), '') as school_address
    from {{ source('iginteplus_25_26', 'Monitored_Projects') }}
)

select 
    s_no,
    year,
    visited_by,
    folder_link,
    date_of_visit,
    district_state,
    school_address
from cleaned
where school_address is not null
