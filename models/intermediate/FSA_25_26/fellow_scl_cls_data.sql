{{ config(
  materialized='table',
  tags=["fsa_25_26", "int"]
) }}

with fellow_donor as (
    select
        fs.*,
        dm.funding_year,
        dm.donor_id,
        d.donor_name
    from {{ ref('fellows') }} as fs
    left join {{ ref('donor_map') }} as dm
        on fs.fellow_id = dm.fellow_id
    left join {{ ref('donor_fsa') }} as d
        on dm.donor_id = d.donor_id
)

select distinct

    f.funding_year || ' - ' || (f.funding_year::integer + 1)::text as academic_year,

    f.fellow_id,
    p.full_name as fellow_name,
    f.pm_id,
    p1.full_name as pm_name,
    f.cohort_year as cohort,
    fs.funding_year,

    f.donor_id,
    f.donor_name,

    fs.grade,
    fs.section,
    fs.grade_section,
    fs.no_of_students,

    s.school_id,
    s.school_name,
    s.school_state,
    s.city,
    -- s.is_active,
    -- s.udise_code,
    s.school_type,
    s.male_students_in_school,
    s.total_students_in_school,
    s.female_students_in_school,
    s.total_support_staff_in_school,
    s.total_teaching_staff_in_school

from fellow_donor as f

left join {{ ref('profiles_raw_data') }} as p
    on f.fellow_id = p.user_id

left join {{ ref('profiles_raw_data') }} as p1
    on f.pm_id = p1.user_id

left join {{ ref('fellow_school_fsa') }} as fs
    on
        f.fellow_id = fs.fellow_id
        and f.funding_year = fs.funding_year

left join {{ ref('school_data_int') }} as s
    on
        fs.school_id = s.school_id
        and f.funding_year = s.funding_year
