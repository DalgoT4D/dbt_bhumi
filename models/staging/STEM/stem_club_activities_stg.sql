with akshay as (
    select
        donor,
        trainer,
        district,
        school_name,
        topic_covered,
        event_meeting_date,
        no_of_children_attended_ as no_of_children_attended
    from {{ source('Stem_gsheet_data', 'Akshay_club_act') }}
),

babu as (
    select
        donor,
        trainer,
        district,
        school_name,
        topic_covered,
        event_meeting_date,
        no_of_children_attended
    from {{ source('Stem_gsheet_data', 'Babu_club_act') }}
),

shanthi as (
    select
        donor,
        trainer,
        district,
        school_name,
        topic_covered,
        event_meeting_date,
        no_of_children_attended
    from {{ source('Stem_gsheet_data', 'Shanthi_club_act') }}
),

shivani as (
    select
        donor,
        trainer,
        district,
        school_name,
        topic_covered,
        event_meeting_date,
        no_of_children_attended
    from {{ source('Stem_gsheet_data', 'Shivani_club_act') }}
),

sn as (
    select
        donor,
        trainer,
        district,
        school_name,
        topic_covered,
        event_meeting_date,
        no_of_children_attended
    from {{ source('Stem_gsheet_data', 'SN_club_act') }}
),

sneha as (
    select
        donor,
        trainer,
        district,
        school_name,
        topic_covered,
        event_meeting_date,
        no_of_children_attended
    from {{ source('Stem_gsheet_data', 'Sneha_club_act') }}
),

uma as (
    select
        donor,
        trainer,
        district,
        school_name,
        topic_covered,
        event_meeting_date,
        no_of_children_attended
    from {{ source('Stem_gsheet_data', 'Uma_club_act') }}
),

unified as (
    select * from akshay
    union all
    select * from babu
    union all
    select * from shanthi
    union all
    select * from shivani
    union all
    select * from sn
    union all
    select * from sneha
    union all
    select * from uma
),

cleaned as (
    select
        -- date
        regexp_replace(event_meeting_date::text, '[^0-9./\-]', '', 'g') as date_clean,

        -- identifiers
        Coalesce(Initcap(Btrim(donor::text)), '') as donor,
        Coalesce(Initcap(Btrim(trainer::text)), '') as trainer,
        Coalesce(Initcap(Btrim(district::text)), '') as district,
        Coalesce(Initcap(Btrim(school_name::text)), '') as school_name,
        Coalesce(Btrim(topic_covered::text), '') as topic_covered,
        case
            when Btrim(no_of_children_attended::text) ~ '^\d+$'
                then (no_of_children_attended::text)::integer
        end as no_of_children_attended
    from unified
)

select distinct
    {{ validate_date('date_clean') }} as event_meeting_date,
    donor,
    trainer,
    district,
    school_name,
    topic_covered,
    no_of_children_attended
from cleaned
where school_name != ''
