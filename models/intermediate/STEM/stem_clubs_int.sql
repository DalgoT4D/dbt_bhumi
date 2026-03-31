with school_clubs as (
    select
        *,
        lower(regexp_replace(school, '[^a-zA-Z0-9]', '', 'g')) as join_key
    from {{ ref('stem_school_clubs_stg') }}
),

club_activities as (
    select
        *,
        lower(regexp_replace(school_name, '[^a-zA-Z0-9]', '', 'g')) as join_key
    from {{ ref('stem_club_activities_stg') }}
)

select
    -- shared identifiers (coalesce across both sources)
    coalesce(sc.donor, ca.donor) as donor,
    coalesce(sc.district, ca.district) as district,
    coalesce(sc.school, ca.school_name) as school_name,
    sc.manager,
    coalesce(sc.trainer, ca.trainer) as trainer,

    -- from stem_school_clubs_stg
    sc.s_no,
    sc.date_orientation_trainer,
    sc.date_orientation_school,
    sc.stem_club_start_date,

    -- club start year
    extract(year from sc.stem_club_start_date)::integer as club_start_year,

    -- activity year and quarter (from event_meeting_date, already a date type)
    extract(year from ca.event_meeting_date)::integer as activity_year,
    'Q' || extract(quarter from ca.event_meeting_date)::integer::text as activity_quarter,

    -- from stem_club_activities_stg
    ca.event_meeting_date,
    ca.topic_covered,
    ca.no_of_children_attended
from school_clubs as sc
full outer join club_activities as ca
    on sc.join_key = ca.join_key
