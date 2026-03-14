select
    id,
    center_name as school_name,
    donor_name as donor,
    state,
    city,
    chapter,
    program,
    project,
    center_type,
    center_status,
    overall_children_count,
    total_count,
    modified_time::date as date_modified
from {{ ref('stem_stp_centre_identification_stg') }}
where
    center_type = 'School'
    or center_type is null
    -- NOTE: blank center_type values are nulls from the source and are  retained. - to be checked
    or center_type = ''
