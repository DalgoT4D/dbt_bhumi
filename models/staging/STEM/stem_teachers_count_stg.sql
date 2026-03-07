with teachers_count as (
    select
        case when btrim(s_no::text) ~ '^[0-9]+$' then s_no::integer end as s_no,
        coalesce(initcap(btrim(state::text)), '') as state,
        coalesce(initcap(btrim(district::text)), '') as district,
        coalesce(initcap(btrim(name_of_school::text)), '') as school_name,
        coalesce(btrim(remarks::text), '') as remarks,

        -- assessment counts
        case when btrim(assessment_completed_q_2::text) ~ '^[0-9]+$' then assessment_completed_q_2::integer end as assessment_completed_q2,
        case when btrim(assessment_completed_q_3::text) ~ '^[0-9]+$' then assessment_completed_q_3::integer end as assessment_completed_q3,
        case when btrim(assessment_completed_q_4::text) ~ '^[0-9]+$' then assessment_completed_q_4::integer end as assessment_completed_q4,

        -- teacher counts
        case when btrim(teachers_count_including_hm::text) ~ '^[0-9]+$' then teachers_count_including_hm::integer end as teachers_count_including_hm,
        case when btrim(no_of_targeted_teacher_count::text) ~ '^[0-9]+$' then no_of_targeted_teacher_count::integer end as targeted_teacher_count

    from {{ source('Stem_gsheet_data', 'Teachers_Count') }}
)

select distinct
    s_no,
    state,
    district,
    school_name,
    teachers_count_including_hm,
    targeted_teacher_count,
    assessment_completed_q2,
    assessment_completed_q3,
    assessment_completed_q4,
    remarks
from teachers_count
