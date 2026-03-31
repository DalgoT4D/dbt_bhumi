select
    extract(year from date)::integer as year,
    extract(quarter from date)::integer as quarter,
    donor,
    district,
    school_name,
    trainer_name,
    count(*) as no_of_classes
from {{ ref('stem_teacher_led_classes_stg') }}
group by
    extract(year from date),
    extract(quarter from date),
    donor,
    district,
    school_name,
    trainer_name
order by
    year,
    quarter,
    donor,
    district,
    school_name,
    trainer_name
