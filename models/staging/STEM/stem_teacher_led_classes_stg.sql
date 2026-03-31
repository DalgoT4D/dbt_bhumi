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
        Coalesce(Initcap(Btrim(donor::text)), '') as donor,
        Coalesce(Btrim(grade::text), '') as grade,
        Coalesce(Btrim(section::text), '') as section,
        Coalesce(Btrim(subject::text), '') as subject,
        Coalesce(Initcap(Btrim(district::text)), '') as district,
        Coalesce(Initcap(Btrim(school_name::text)), '') as school_name,
        Coalesce(Initcap(Btrim(teacher_name::text)), '') as teacher_name,
        Coalesce(Initcap(Btrim(trainer_name::text)), '') as trainer_name,
        Coalesce(Btrim(tool_kit_used::text), '') as tool_kit_used,
        Coalesce(Btrim(topic_covered::text), '') as topic_covered
    from unified
)

select distinct
    {{ validate_date('date_clean') }} as date,
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
