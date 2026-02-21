with fellow_school as (
    select
        NULLIF(BTRIM(id::TEXT),'') as id,
        NULLIF(BTRIM(fellow_id::TEXT),'') as fellow_id,
        NULLIF(BTRIM(grade::TEXT),'')::INTEGER as grade,
        NULLIF(BTRIM(period::TEXT),'') as period,
        NULLIF(BTRIM(is_active::TEXT),'') as is_active,
        NULLIF(BTRIM(school_id::TEXT),'') as school_id,
        NULLIF(BTRIM(no_of_students::TEXT),'')::INTEGER as no_of_students
    from {{ source('fellowship_school_app_25_26', 'fellow_school_grade_25_26') }}
    where
        NULLIF(BTRIM(id::TEXT),'') is not null
        and NULLIF(BTRIM(is_active::TEXT),'') = 'true'
),

schools as (
    select
        NULLIF(BTRIM(id::TEXT),'') as school_id,
        NULLIF(INITCAP(BTRIM(name::TEXT)),'') as school_name,
        NULLIF(INITCAP(BTRIM(state::TEXT)),'') as school_state,
        NULLIF(INITCAP(BTRIM(address::TEXT)),'') as school_address,
        NULLIF(INITCAP(BTRIM(district::TEXT)),'') as school_district,
        NULLIF(INITCAP(BTRIM(is_active::TEXT)),'') as is_active,
        NULLIF(BTRIM(udise_code::TEXT),'') as udise_code,
        NULLIF(INITCAP(BTRIM(school_type::TEXT)),'') as school_type
    from {{ source('fellowship_school_app_25_26', 'schools_raw_data_25_26') }}
    where
        NULLIF(BTRIM(id::TEXT),'') is not null
        and NULLIF(BTRIM(is_active::TEXT),'') = 'true'
)

select distinct
    fs.id,
    fs.fellow_id,
    fs.grade,
    NULLIF(SUBSTRING(fs.period, 2, POSITION(',' in fs.period) - 2)::DATE, null) as period_from,
    case
        when SUBSTRING(fs.period, POSITION(',' in fs.period) + 1, POSITION(')' in fs.period) - POSITION(',' in fs.period) - 1) = '' then null
        else NULLIF(SUBSTRING(fs.period, POSITION(',' in fs.period) + 1, POSITION(')' in fs.period) - POSITION(',' in fs.period) - 1)::DATE, null)
    end as period_to,
    fs.no_of_students,
    s.school_id,
    s.school_name, 
    s.school_state, 
    s.school_address,
    s.school_district,
    s.udise_code,
    s.school_type
from fellow_school as fs
inner join schools as s
    on fs.school_id = s.school_id
