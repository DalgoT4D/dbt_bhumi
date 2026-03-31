select
    donor,
    district,
    school_name,
    student_name,
    trainer_name,
    current_grade,
    academic_marks,
    scholarship_amount,
    grade_college_scholarship_awarded
from {{ ref('stem_scholarship_details_stg') }}
order by
    donor,
    district,
    school_name,
    student_name
