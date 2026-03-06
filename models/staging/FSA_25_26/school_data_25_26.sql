select 
    NULLIF(BTRIM(id::TEXT),'') as school_id,
    NULLIF(BTRIM(name::TEXT),'') as school_name,
    NULLIF(BTRIM(state::TEXT),'') as school_state,
    NULLIF(BTRIM(address::TEXT),'') as school_address,
    NULLIF(BTRIM(district::TEXT),'') as school_district,
    NULLIF(BTRIM(is_active::TEXT),'') as is_active,
    NULLIF(BTRIM(udise_code::TEXT),'') as udise_code,
    NULLIF(BTRIM(school_type::TEXT),'') as school_type
from {{ source('fellowship_school_app_25_26', 'schools_raw_data_25_26') }}
where
    id is not null
    and is_active = 'TRUE'
    and udise_code != '123456'
