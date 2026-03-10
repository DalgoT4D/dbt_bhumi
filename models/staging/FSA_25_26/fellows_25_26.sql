with fellows as (
    select 
        NULLIF(BTRIM(id::TEXT),'') as id,
        NULLIF(BTRIM(pm_id::TEXT), '') as pm_id,
        NULLIF(BTRIM(cohort_year::TEXT),'') as cohort_year,
        NULLIF(BTRIM(employee_id::TEXT), '') as employee_id,
        NULLIF(INITCAP(BTRIM(year_1_donor)),'') as year_1_donor,
        NULLIF(INITCAP(BTRIM(year_2_donor)),'') as year_2_donor,
        NULLIF(INITCAP(LOWER(NULLIF(BTRIM(placement_city),''))),'') as placement_city,
        case
            when NULLIF(BTRIM(date_of_joining::TEXT),'') is null then null
            when BTRIM(date_of_joining::TEXT) ~ '^\\d{4}-\\d{2}-\\d{2}$' then BTRIM(date_of_joining::TEXT)::DATE
            when BTRIM(date_of_joining::TEXT) ~ '^\\d{2}/\\d{2}/\\d{4}$' then TO_DATE(BTRIM(date_of_joining::TEXT),'DD/MM/YYYY')
            when BTRIM(date_of_joining::TEXT) ~ '^\\d{2}-\\d{2}-\\d{4}$' then TO_DATE(BTRIM(date_of_joining::TEXT),'DD-MM-YYYY')
        end as date_of_joining,
        case
            when NULLIF(BTRIM(date_of_leaving::TEXT),'') is null then null
            when BTRIM(date_of_leaving::TEXT) ~ '^\\d{4}-\\d{2}-\\d{2}$' then BTRIM(date_of_leaving::TEXT)::DATE
            when BTRIM(date_of_leaving::TEXT) ~ '^\\d{2}/\\d{2}/\\d{4}$' then TO_DATE(BTRIM(date_of_leaving::TEXT),'DD/MM/YYYY')
            when BTRIM(date_of_leaving::TEXT) ~ '^\\d{2}-\\d{2}-\\d{4}$' then TO_DATE(BTRIM(date_of_leaving::TEXT),'DD-MM-YYYY')
        end as date_of_leaving,
        case
            when NULLIF(BTRIM(date_of_birth::TEXT),'') is null then null
            when BTRIM(date_of_birth::TEXT) ~ '^\\d{4}-\\d{2}-\\d{2}$' then BTRIM(date_of_birth::TEXT)::DATE
            when BTRIM(date_of_birth::TEXT) ~ '^\\d{2}/\\d{2}/\\d{4}$' then TO_DATE(BTRIM(date_of_birth::TEXT),'DD/MM/YYYY')
            when BTRIM(date_of_birth::TEXT) ~ '^\\d{2}-\\d{2}-\\d{4}$' then TO_DATE(BTRIM(date_of_birth::TEXT),'DD-MM-YYYY')
        end as date_of_birth
    from {{ source('fellowship_school_app_25_26', 'fellows_raw_data_25_26') }}
),

profiles as (
    select
        NULLIF(BTRIM(id::TEXT),'') as id,
        NULLIF(BTRIM(first_name || ' ' || last_name),'') as full_name,
        NULLIF(BTRIM(is_active::TEXT),'') as is_active
    from {{ source('fellowship_school_app_25_26', 'profiles_25_26') }}
)

select distinct
    f.id as fellow_id,
    f.pm_id,
    f.cohort_year,
    f.employee_id as fellow_employee_id,
    f.year_1_donor,
    f.year_2_donor,
    f.placement_city as fellow_placement_city,
    f.date_of_joining as fellow_doj,
    f.date_of_leaving as fellow_dol,
    f.date_of_birth as fellow_dob,
    p.full_name as fellow_full_name
from fellows as f
inner join profiles as p
    on f.id = p.id
where
    f.id is not null
    and p.full_name is not null
    and p.is_active = 'true'      
    and f.employee_id != 'FAKE001' 
