with all_student_id as (
    (
        select distinct  student_id_base as student_id
        from {{ ref('baseline_2425_stg') }}
        where student_id_base is not null
    )
    
    union

    (
        select distinct  student_id_mid as student_id
        from {{ ref('midline_2425_stg') }}
        where student_id_mid is not null
    )

    union

    (
        select distinct  student_id_end as student_id
        from {{ ref('endline_2425_stg') }}
        where student_id_end is not null
    )
)

select 
    s.student_id,
    -- Baseline columns
    not coalesce(b.student_id_base is null, false) as baseline_attendence,
    b.city_base,
    b.student_name_base,
    b.classroom_id_base,
    b.pm_name_base,
    b.school_name_base,
    b.fellow_name_base,
    b.cohort_base,
    b.grade_taught_base,
    -- Midline columns
    not coalesce(m.student_id_mid is null, false) as midline_attendence,
    m.city_mid,
    m.student_name_mid,
    m.classroom_id_mid,
    m.pm_name_mid,
    m.school_name_mid,
    m.fellow_name_mid,
    m.cohort_mid,
    m.grade_taught_mid,
    -- Endline columns
    not coalesce(e.student_id_end is null, false) as endline_attendence,
    e.city_end,
    e.student_name_end,
    e.classroom_id_end,
    e.pm_name_end,
    e.school_name_end,
    e.fellow_name_end,
    e.cohort_end,
    e.grade_taught_end
from all_student_id as s
left join {{ ref('baseline_2425_stg') }} as b 
    on s.student_id = b.student_id_base
left join {{ ref('midline_2425_stg') }} as m 
    on s.student_id = m.student_id_mid
left join {{ ref('endline_2425_stg') }} as e 
    on s.student_id = e.student_id_end
