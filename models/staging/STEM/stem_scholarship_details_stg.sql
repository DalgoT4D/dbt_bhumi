with scholarship_details as (
    select
        -- identifiers
        Coalesce(Initcap(Btrim(donor::text)), '') as donor,
        Coalesce(Initcap(Btrim(district::text)), '') as district,
        Coalesce(Initcap(Btrim(school_name::text)), '') as school_name,
        Coalesce(Initcap(Btrim(student_name::text)), '') as student_name,
        Coalesce(Initcap(Btrim(trainer_name::text)), '') as trainer_name,
        Coalesce(Btrim(name_of_parent_father_mother_guardian_::text), '') as parent_guardian_name,
        Coalesce(Btrim(any_other_remarks::text), '') as any_other_remarks,
        Coalesce(Btrim(grade_college_for_which_scholarship_awarded::text), '') as grade_college_scholarship_awarded,

        -- numeric conversions
        case when Btrim(current_grade::text) ~ '^\d+$' then (current_grade::text)::integer end as current_grade,
        case when Btrim(acedamic_marks_::text) ~ '^[0-9]+(\.[0-9]+)?$' then (acedamic_marks_::text)::numeric end as academic_marks,
        case when Btrim(scholarship_amount::text) ~ '^[0-9]+(\.[0-9]+)?$' then (scholarship_amount::text)::numeric end as scholarship_amount
    from {{ source('Stem_gsheet_data', 'Scholarship_Details') }}
)

select distinct
    donor,
    district,
    school_name,
    student_name,
    trainer_name,
    current_grade,
    academic_marks,
    any_other_remarks,
    scholarship_amount,
    parent_guardian_name,
    grade_college_scholarship_awarded
from scholarship_details
where student_name != ''
