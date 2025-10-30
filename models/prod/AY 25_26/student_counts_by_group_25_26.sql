with baseline as (
    select
        d.city_base as city,
        d.school_name_base as school,
        d.fellow_name_base as fellow,
        d.student_grade_base as grade,
        count(distinct d.student_id) as baseline_count
    from {{ ref('base_mid_end_comb_students_25_26_dim') }} d
    where d.baseline_attendence = True
    group by d.city_base, d.school_name_base, d.fellow_name_base, d.student_grade_base
),

-- midline as (
--     select
--         d.city_mid as city,
--         d.school_name_mid as school,
--         d.fellow_name_mid as fellow,
--         d.grade_taught_mid as grade,
--         count(distinct d.student_id) as midline_count
--     from {{ ref('base_mid_end_comb_students_2425_dim') }} d
--     where d.midline_attendence = True
--     group by d.city_mid, d.school_name_mid, d.fellow_name_mid, d.grade_taught_mid
-- ),

-- endline as (
--     select
--         d.city_end as city,
--         d.school_name_end as school,
--         d.fellow_name_end as fellow,
--         d.grade_taught_end as grade,
--         count(distinct d.student_id) as endline_count
--     from {{ ref('base_mid_end_comb_students_2425_dim') }} d
--     where d.endline_attendence = True
--     group by d.city_end, d.school_name_end, d.fellow_name_end, d.grade_taught_end
-- ),

all_combinations as (
    select distinct city, school, fellow, grade from baseline
    -- union
    -- select distinct city, school, fellow, grade from midline
    -- union
    -- select distinct city, school, fellow, grade from endline
)

select
    ac.city,
    ac.school,
    ac.fellow,
    ac.grade,
    coalesce(b.baseline_count, 0) as baseline_count
    -- coalesce(m.midline_count, 0) as midline_count,
    -- coalesce(e.endline_count, 0) as endline_count
from all_combinations ac
left join baseline b
    on ac.city = b.city
    and ac.school = b.school
    and ac.fellow = b.fellow
    and ac.grade = b.grade
-- left join midline m
--     on ac.city = m.city
--     and ac.school = m.school
--     and ac.fellow = m.fellow
--     and ac.grade = m.grade
-- left join endline e
--     on ac.city = e.city
--     and ac.school = e.school
--     and ac.fellow = e.fellow
--     and ac.grade = e.grade
order by ac.city, ac.school, ac.fellow, ac.grade


