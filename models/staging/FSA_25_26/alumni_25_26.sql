with alumni_raw as (
    select
        NULLIF(BTRIM("Name"::TEXT), '') as name,

        NULLIF(BTRIM("Cohort"::TEXT), '') as cohort,

        NULLIF(BTRIM("Hometown"::TEXT), '') as hometown,

        NULLIF(BTRIM("Contact__"::TEXT), '') as contact,

        NULLIF(BTRIM("School_in_BF"::TEXT), '') as school_in_bf,

        NULLIF(BTRIM("Residing_city"::TEXT), '') as residing_city,

        NULLIF(BTRIM("Bhumi_Email_ID"::TEXT), '') as bhumi_email_id

    from {{ source('fellowship_school_app_25_26', 'alumni_25_26') }}

)

select 
    name,
    cohort,
    hometown,
    contact,
    school_in_bf,
    residing_city,
    bhumi_email_id    
from alumni_raw
where
    name is not null
    and bhumi_email_id is not null
