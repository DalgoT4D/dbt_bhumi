with donor_mapping_cleaned as (
    select
        NULLIF(BTRIM(id::TEXT), '')         as id,
        NULLIF(BTRIM(donor_id::TEXT), '')   as donor_id,
        NULLIF(BTRIM(fellow_id::TEXT), '')  as fellow_id,
        NULLIF(BTRIM(notes::TEXT), '')      as notes,
        NULLIF(BTRIM(funding_year::TEXT), '')      as funding_year
    from {{ source('fellowship_school_app_25_26', 'donor_mapping') }}
    where
        NULLIF(BTRIM(id::TEXT), '')        is not null
        and NULLIF(BTRIM(donor_id::TEXT), '')  is not null
        and NULLIF(BTRIM(fellow_id::TEXT), '') is not null
),

donors_cleaned as (
    select
        NULLIF(BTRIM(id::TEXT), '')                     as id,
        NULLIF(INITCAP(BTRIM(name::TEXT)), '')          as donor_name
    from {{ source('fellowship_school_app_25_26', 'donors') }}
    where NULLIF(BTRIM(id::TEXT), '') is not null
)

select
    dm.id,
    dm.fellow_id,
    dm.donor_id,
    d.donor_name,
    dm.funding_year,
    dm.notes
from donor_mapping_cleaned  as dm
left join donors_cleaned    as d
    on dm.donor_id = d.id
