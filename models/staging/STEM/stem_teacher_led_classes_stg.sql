{{ config(
  materialized='table',
  tags=["stem", "staging"]
) }}

with akshay as (
    select
        date,
        donor,
        grade,
        section,
        subject,
        district,
        school_name,
        teacher_name,
        trainer_name,
        tool_kit_used,
        topic_covered
    from {{ source('Stem_gsheet_data', 'Akshay_stem_class') }}
),

rajendrababu as (
    select
        date,
        donor,
        grade,
        section,
        subject,
        district,
        school_name,
        teacher_name,
        trainer_name,
        tool_kit_used,
        topic_covered
    from {{ source('Stem_gsheet_data', 'Rajendrababu_stem_class') }}
),

shanthi as (
    select
        date,
        donor,
        grade,
        section,
        subject,
        district,
        school_name,
        teacher_name,
        trainer_name,
        tool_kit_used,
        topic_covered
    from {{ source('Stem_gsheet_data', 'Shanthi_stem_class') }}
),

shivangi as (
    select
        date,
        donor,
        grade,
        section,
        subject,
        district,
        school_name,
        teacher_name,
        trainer_name,
        tool_kit_used,
        topic_covered
    from {{ source('Stem_gsheet_data', 'Shivangi_stem_class') }}
),

shivani as (
    select
        date,
        donor,
        grade,
        section,
        subject,
        district,
        school_name,
        teacher_name,
        trainer_name,
        tool_kit_used,
        topic_covered
    from {{ source('Stem_gsheet_data', 'Shivani_stem_class') }}
),

sneha as (
    select
        date,
        donor,
        grade,
        section,
        subject,
        district,
        school_name,
        teacher_name,
        trainer_name,
        tool_kit_used,
        topic_covered
    from {{ source('Stem_gsheet_data', 'SNEHA_stem_class') }}
),

uma as (
    select
        date,
        donor,
        grade,
        section,
        subject,
        district,
        school_name,
        teacher_name,
        trainer_name,
        tool_kit_used,
        topic_covered
    from {{ source('Stem_gsheet_data', 'UMA_stem_class') }}
),

unified as (
    select * from akshay
    union all
    select * from rajendrababu
    union all
    select * from shanthi
    union all
    select * from shivangi
    union all
    select * from shivani
    union all
    select * from sneha
    union all
    select * from uma
),

cleaned as (
    select
        -- date
        regexp_replace(date::text, '[^0-9./\-]', '', 'g') as date_clean,

        -- identifiers
        coalesce(initcap(btrim(donor::text)), '') as donor,
        coalesce(btrim(grade::text), '') as grade,
        coalesce(btrim(section::text), '') as section,
        coalesce(btrim(subject::text), '') as subject,
        coalesce(initcap(btrim(district::text)), '') as district,
        coalesce(initcap(btrim(school_name::text)), '') as school_name,
        coalesce(initcap(btrim(teacher_name::text)), '') as teacher_name,
        coalesce(initcap(btrim(trainer_name::text)), '') as trainer_name,
        coalesce(btrim(tool_kit_used::text), '') as tool_kit_used,
        coalesce(btrim(topic_covered::text), '') as topic_covered
    from unified
)

select distinct
    {{ validate_date('date_clean') }} as date, -- noqa: LT02
    donor,
    grade,
    section,
    subject,
    district,
    school_name,
    teacher_name,
    trainer_name,
    tool_kit_used,
    topic_covered
from cleaned
where school_name != ''
