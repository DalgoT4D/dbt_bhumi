with fellow_school as (
    select * from (
        select 
            NULLIF(BTRIM(fellow_id::TEXT),'') as fellow_id,
            NULLIF(BTRIM(grade::TEXT),'') as grade,
            NULLIF(BTRIM(is_active::TEXT),'') as is_active,
            NULLIF(BTRIM(school_id::TEXT),'') as school_id,
            NULLIF(BTRIM(no_of_students::TEXT),'')::INTEGER as no_of_students,
            ROW_NUMBER() over (
                partition by NULLIF(BTRIM(fellow_id::TEXT), '')
                order by created_at desc
            ) as rn
        from {{ source('fellowship_school_app_25_26', 'fellow_school_grade_25_26') }}
        where
            NULLIF(BTRIM(id::TEXT),'') is not null
            and NULLIF(BTRIM(is_active::TEXT),'') = 'true'
    ) as sub
    where rn = 1
),

fellows_data as (
    select
        fellow_id,
        fellow_full_name,
        cohort_year,
        fellow_employee_id,
        year_1_donor,
        year_2_donor,
        fellow_placement_city,
        fellow_doj,
        fellow_dol,
        fellow_dob,
        pm_id
    from {{ ref('fellows_25_26') }}
),

pms_data as (
    select
        pm_id,
        pm_full_name,
        pms_location
    from {{ ref('pms_25_26') }}
),

schools as (
    select
        school_id,
        school_name,
        school_state,
        school_address,
        school_district,
        is_active,
        udise_code,
        school_type
    from {{ ref('school_data_25_26') }}
)

select distinct
    fs.fellow_id,
    fs.grade,
    fs.no_of_students,
    s.school_id,
    s.school_name, 
    s.school_state, 
    s.school_address,
    s.school_district,
    s.udise_code,
    s.school_type,
    f.fellow_full_name,
    f.cohort_year,
    f.fellow_employee_id,
    f.year_1_donor,
    f.year_2_donor,
    f.fellow_placement_city,
    f.fellow_doj,
    f.fellow_dol,
    f.fellow_dob,
    p.pm_id,
    p.pm_full_name,
    p.pms_location
from fellow_school as fs
left join schools as s
    on fs.school_id = s.school_id
left join fellows_data as f
    on fs.fellow_id = f.fellow_id
left join pms_data as p
    on f.pm_id = p.pm_id
where
    fs.fellow_id is not null
    and s.school_id is not null
