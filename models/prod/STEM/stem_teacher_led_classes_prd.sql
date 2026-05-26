{{ config(
  materialized='table',
  tags=["stem", "prod"]
) }}

with classes_raw as (
    select
        extract(year from date)::integer as year,
        extract(month from date)::integer as month,
        case
            when extract(month from date)::integer in (4, 5, 6) then 'Q1'
            when extract(month from date)::integer in (7, 8, 9) then 'Q2'
            when extract(month from date)::integer in (10, 11, 12) then 'Q3'
            when extract(month from date)::integer in (1, 2, 3) then 'Q4'
        end as academic_quarter,
        case
            when extract(month from date) >= 4
                then extract(year from date)::integer::text || '-' || (extract(year from date)::integer + 1)::text
            else (extract(year from date)::integer - 1)::text || '-' || extract(year from date)::integer::text
        end as academic_year,
        donor,
        district,
        school_name,
        trainer_name,
        teacher_name
    from {{ ref('stem_teacher_led_classes_stg') }}
)

select
    academic_year,
    academic_quarter,
    donor,
    district,
    school_name,
    trainer_name,
    count(distinct teacher_name) as no_of_teachers,
    count(*) as no_of_classes
from classes_raw
group by
    academic_year,
    academic_quarter,
    donor,
    district,
    school_name,
    trainer_name
order by
    academic_year,
    academic_quarter,
    donor,
    district,
    school_name,
    trainer_name
