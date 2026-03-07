-- strips non-date characters (invisible chars, carriage returns, tabs, non-breaking spaces)
-- keeping only digits and separators (. / -) before passing into validate_date
with clean_dates as (
    select
        *,
        regexp_replace("date_of_orientation_to_trainer"::text, '[^0-9./\-]', '', 'g') as date_trainer_clean,
        regexp_replace("date_of_orientation_to_school_"::text, '[^0-9./\-]', '', 'g') as date_school_clean,
        regexp_replace("stem_club_start_date"::text, '[^0-9./\-]', '', 'g') as stem_club_date_clean
    from {{ source('Stem_gsheet_data', 'Consol_Y2_SCHOOLS') }}
),

schools as (
    select
        -- identifiers
        case when btrim("s_no"::text) ~ '^[0-9]+$' then ("s_no"::text)::integer end as s_no,
        coalesce(initcap(btrim("donor"::text)), '') as donor,
        coalesce(initcap(btrim("district"::text)), '') as district,
        coalesce(initcap(btrim("school"::text)), '') as school,
        coalesce(initcap(btrim("manager_"::text)), '') as manager,
        coalesce(initcap(btrim("trainer"::text)), '') as trainer,

        -- dates (passed through cleaned columns)
        {{ validate_date('date_trainer_clean') }} as date_orientation_trainer,
        {{ validate_date('date_school_clean') }} as date_orientation_school,
        {{ validate_date('stem_club_date_clean') }} as stem_club_start_date

    from clean_dates
)

select distinct
    s_no,
    donor,
    district,
    school,
    manager,
    trainer,
    date_orientation_trainer,
    date_orientation_school,
    stem_club_start_date
from schools
