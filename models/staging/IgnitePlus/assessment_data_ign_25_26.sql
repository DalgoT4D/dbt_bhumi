{{ config(
  materialized='table',
  tags=["igniteplus", "staging"]
) }}

with cleaned as (
    select
        coalesce(btrim("S_NO"), '') as s_no,
        coalesce(btrim("Financial_Year"), '') as financial_year,
        coalesce(initcap(btrim("Quarter")), '') as quarter,
        case
            when btrim("Date_of_NA") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                then to_date(
                    regexp_replace(btrim("Date_of_NA"), '\s*-\s*', '-', 'g'),
                    'DD-Mon-YYYY'
                )
        end as date_of_need_assessment,
        coalesce(initcap(btrim("Name_of_School")), '') as name_of_school,
        coalesce(initcap(btrim("School_Type")), '') as school_type,
        coalesce(initcap(btrim("District")), '') as district,
        coalesce(btrim("Map_Location"), '') as location,
        coalesce(btrim("NA_Link"), '') as na_link,
        coalesce(initcap(btrim("NA_Done_by")), '') as need_assessment_done_by,
        coalesce(btrim("Folder_URL_Link"), '') as folder_link,
        coalesce(initcap(btrim("Remarks_")), '') as remarks
    from {{ source('iginteplus_25_26', 'Ignite__Need_Assessment_Data_25_26') }}
)

select 
    s_no,
    financial_year,
    quarter,
    date_of_need_assessment,
    name_of_school,
    school_type,
    na_link,
    district,
    location,
    folder_link,
    need_assessment_done_by,
    remarks
from cleaned
where name_of_school is not null
