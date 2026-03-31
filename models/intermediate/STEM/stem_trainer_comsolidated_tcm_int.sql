select
    coalesce(t.trainer_name, tcm.trainer_name) as trainer_name,
    t.id as trainer_id,
    t.school_count,
    Btrim(split_part(coalesce(t.reporting_manager_name, tcm.manager), ' - ', 1)) as manager_name,
    t.reporting_manager_id as manager_id,
    date(t.added_time) as added_date,
    t.status as trainer_status,
    tcm.donor,
    tcm.trainer_location,
    tcm.q_2,
    tcm.q_3,
    tcm.q_4,
    tcm.baseline,
    tcm.improvement,
    tcm.consolidated_rating_2025_26
from {{ ref('stem_trainer_report_stg') }} as t
full outer join {{ ref('stem_consolidated_tcm_stg') }} as tcm
    on regexp_replace(lower(t.trainer_name), '[^a-z0-9]', '', 'g')
    = regexp_replace(lower(tcm.trainer_name), '[^a-z0-9]', '', 'g')
