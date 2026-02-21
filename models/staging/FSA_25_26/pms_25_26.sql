with profiles as (
    select
        NULLIF(BTRIM(id::TEXT),'') as id,
        NULLIF(BTRIM(first_name || ' ' || last_name),'') as full_name,
        NULLIF(BTRIM(is_active::TEXT),'') as is_active
    from {{ source('fellowship_school_app_25_26', 'profiles_25_26') }}
),

pms as (
    select 
        NULLIF(BTRIM(id::TEXT),'') as id,
        NULLIF(BTRIM(location::TEXT),'') as location
    from {{ source('fellowship_school_app_25_26', 'pms_raw_data_25_26') }}
)

select distinct
    pm.id as pm_id,
    p.full_name as pm_full_name,
    pm.location as pms_location
from pms as pm
inner join profiles as p
    on pm.id = p.id
where
    pm.id is not null
    and p.full_name is not null
    and p.is_active = 'true'
