-- Student Fellowship Program Comprehensive
-- Joins fellow_school_25_26 with fellow_25_26 and pms_25_26

select distinct
    -- fellow_school_25_26 columns
    fs.id,
    fs.fellow_id,
    fs.school_id,
    fs.grade,
    fs.period_from,
    fs.period_to,
    fs.no_of_students,
    fs.school_name,
    fs.school_state,
    fs.school_address,
    fs.school_district,
    fs.udise_code,
    fs.school_type,
    -- fellow_25_26 columns
    f.pm_id,
    f.fellow_full_name,
    f.cohort_year,
    f.fellow_employee_id,
    f.year_1_donor,
    f.year_2_donor,
    f.fellow_placement_city,
    f.fellow_doj,
    f.fellow_dol,
    f.fellow_dob,
    -- pms_25_26 columns
    pm.pm_full_name,
    pm.pms_location
from {{ ref('fellow_school_25_26') }} as fs
left join {{ ref('fellows_25_26') }} as f
    on fs.fellow_id = f.fellow_id
left join {{ ref('pms_25_26') }} as pm
    on f.pm_id = pm.pm_id
where
    fs.fellow_id is not null
    and f.pm_id is not null
    and fs.school_id is not null
