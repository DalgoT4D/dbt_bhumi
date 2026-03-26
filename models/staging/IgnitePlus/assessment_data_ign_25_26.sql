with cleaned as (
    select
        nullif(trim("Number_of_School_Need_assessment_done"), '')::int as number_of_school,
        nullif(trim("Name_of_School"), '') as name_of_school,
        nullif(trim("Location"), '') as location,
        nullif(trim("District"), '') as district,
        nullif(trim("State"), '') as state,
        nullif(trim("Scope_of_work"), '') as scope_of_work,
        nullif(trim("NA_Link"), '') as na_link,
        nullif(trim("Folder_Link"), '') as folder_link,
        case
            when trim("Estimated_amount") = '' then null
            else regexp_replace(trim("Estimated_amount"), '[^0-9\.]+', '', 'g')::numeric
        end as estimated_amount
    from {{ source('iginteplus_25_26', 'Ignite__Need_Assessment_Data_25_26') }}
)

select 
    number_of_school,
    name_of_school,
    location,
    district,
    state,
    scope_of_work,
    na_link,
    folder_link,
    estimated_amount
from cleaned
where number_of_school is not null
