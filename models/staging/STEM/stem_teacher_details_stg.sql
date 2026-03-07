with teachers as (
    select
        case when btrim(sno::text) ~ '^[0-9]+$' then sno::integer end as sno,
        coalesce(initcap(btrim(state::text)), '') as state,
        coalesce(initcap(btrim(teacher::text)), '') as teacher,
        coalesce(initcap(btrim(district::text)), '') as district,
        coalesce(btrim(handling_class::text), '') as handling_class,
        coalesce(btrim(handling_subjects::text), '') as handling_subjects,
        coalesce(initcap(btrim(name_of_the_school::text)), '') as school_name
    from {{ source('Stem_gsheet_data', 'Teacher_Details') }}
)

select distinct
    sno,
    state,
    district,
    school_name,
    teacher,
    handling_class,
    handling_subjects
from teachers
