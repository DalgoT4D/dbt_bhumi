with all_student_id as (
    select distinct 
        student_id_base as student_id
    from {{ref('baseline_25_26_stg')}}
    where student_id_base is not null
    
    -- Uncomment the following unions if needed
    -- union

    -- select distinct 
    --     student_id_mid as student_id
    -- from {{ref('midline_2425_stg')}}
    -- where student_id_mid is not null

    -- union

    -- select distinct 
    --     student_id_end as student_id
    -- from {{ref('endline_2425_stg')}}
    -- where student_id_end is not null
)

select 
    s.student_id,
    -- Baseline columns
    case when b.student_id_base is null then False else True end as baseline_attendence,
    b.city_base,
    b.student_name_base,
    b.classroom_id_base,
    b.pm_name_base,
    b.school_name_base,
    b.fellow_name_base,
    b.cohort_base,
    b.student_grade_base
    -- Midline columns
    -- case when m.student_id_mid is null then False else True end as midline_attendence,
    -- m.city_mid,
    -- m.student_name_mid,
    -- m.classroom_id_mid,
    -- m.PM_name_mid,
    -- m.school_name_mid,
    -- m.fellow_name_mid,
    -- m.cohort_mid,
    -- m.grade_taught_mid,
    -- -- Endline columns
    -- case when e.student_id_end is null then False else True end as endline_attendence,
    -- e.city_end,
    -- e.student_name_end,
    -- e.classroom_id_end,
    -- e.PM_name_end,
    -- e.school_name_end,
    -- e.fellow_name_end,
    -- e.cohort_end,
    -- e.grade_taught_end
from all_student_id s
left join {{ref('baseline_25_26_stg')}} b 
    on s.student_id = b.student_id_base
-- left join {{ref('midline_2425_stg')}} m 
--     on s.student_id = m.student_id_mid
-- left join {{ref('endline_2425_stg')}} e 
--     on s.student_id = e.student_id_end
