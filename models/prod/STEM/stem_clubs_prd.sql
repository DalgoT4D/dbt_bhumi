select
    activity_year,
    activity_quarter,
    donor,
    district,
    school_name,
    manager,
    trainer,
    count(*) as total_activities,
    sum(no_of_children_attended) as total_children_attended
from {{ ref('stem_clubs_int') }}
group by
    activity_year,
    activity_quarter,
    donor,
    district,
    school_name,
    manager,
    trainer
order by
    activity_year,
    activity_quarter,
    donor,
    district,
    school_name
