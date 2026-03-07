select
    id as trainer_id,
    trainer_name,
    school_count,
    reporting_manager_name as manager_name,
    reporting_manager_id as manager_id,
    date(added_time) as added_date
from {{ ref('stem_trainer_report_stg') }}
where lower(status) = 'active'
