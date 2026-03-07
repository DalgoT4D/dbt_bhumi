select
    id,
    donor_name,
    state,
    city,
    chapter,
    program,
    project,
    center_type,
    center_status,
    modified_time::date as date_modified
from {{ ref('stem_stp_centre_identification_stg') }}
