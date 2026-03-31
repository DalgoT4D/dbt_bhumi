select
    added_date as time_period,
    donor,
    trainer_location as location,
    trainer_name as trainer,
    manager_name as manager,
    trainer_status as status,
    consolidated_rating_2025_26 as trainer_competency_level
from {{ ref('stem_trainer_comsolidated_tcm_int') }}
order by
    donor,
    trainer_location,
    manager_name,
    trainer_name
