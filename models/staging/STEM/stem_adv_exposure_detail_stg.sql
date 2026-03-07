-- strips non-date characters (invisible chars, carriage returns, tabs, non-breaking spaces)
-- keeping only digits and separators (. / -) before passing into validate_date
with clean_dates as (
    select
        *,
        regexp_replace(date_of_the_stem_exposure::text, '[^0-9./\-]', '', 'g') as date_exposure_clean
    from {{ source('Stem_gsheet_data', 'STEM_Adv_Dtl') }}
),

adv_exposure as (
    select
        coalesce(initcap(btrim(donor::text)), '') as donor,
        coalesce(btrim(month::text), '') as month,
        coalesce(initcap(btrim(district::text)), '') as district,
        coalesce(initcap(btrim(school_name::text)), '') as school_name,
        coalesce(btrim(topic_covered::text), '') as topic_covered,
        case when btrim(no_of_volunteers_::text) ~ '^[0-9]+$' then no_of_volunteers_::integer end as no_of_volunteers,
        case when btrim(no_of_children_attended::text) ~ '^[0-9]+$' then no_of_children_attended::integer end as no_of_children_attended,
        {{ validate_date('date_exposure_clean') }} as date_of_exposure,
        coalesce(btrim(institution_that_conducted_the_exposure_event::text), '') as institution_name

    from clean_dates
)

select distinct
    donor,
    month,
    district,
    school_name,
    topic_covered,
    no_of_volunteers,
    no_of_children_attended,
    date_of_exposure,
    institution_name
from adv_exposure
