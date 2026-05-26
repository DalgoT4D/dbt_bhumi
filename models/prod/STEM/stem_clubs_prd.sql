with clubs_raw as (
    select
        donor,
        district,
        school_name,
        manager,
        trainer,
        no_of_children_attended,
        case
            when extract(month from event_meeting_date)::integer in (4, 5, 6) then 'Q1'
            when extract(month from event_meeting_date)::integer in (7, 8, 9) then 'Q2'
            when extract(month from event_meeting_date)::integer in (10, 11, 12) then 'Q3'
            when extract(month from event_meeting_date)::integer in (1, 2, 3) then 'Q4'
        end as academic_quarter,
        case
            when extract(month from event_meeting_date) >= 4
                then extract(year from event_meeting_date)::integer::text || '-' || (extract(year from event_meeting_date)::integer + 1)::text
            else (extract(year from event_meeting_date)::integer - 1)::text || '-' || extract(year from event_meeting_date)::integer::text
        end as academic_year
    from {{ ref('stem_clubs_int') }}
    where event_meeting_date is not null
)

select
    academic_year,
    academic_quarter,
    donor,
    district,
    school_name,
    manager,
    trainer,
    count(*) as total_activities,
    sum(no_of_children_attended) as total_children_attended
from clubs_raw
group by
    academic_year,
    academic_quarter,
    donor,
    district,
    school_name,
    manager,
    trainer
order by
    academic_year,
    academic_quarter,
    donor,
    district,
    school_name
