select
    added_date as time_period,
    case
        when extract(month from added_date) >= 4
            then extract(year from added_date)::integer::text || '-' || (extract(year from added_date)::integer + 1)::text
        else (extract(year from added_date)::integer - 1)::text || '-' || extract(year from added_date)::integer::text
    end as academic_year,
    case
        when extract(month from added_date) in (4, 5, 6) then 'Q1'
        when extract(month from added_date) in (7, 8, 9) then 'Q2'
        when extract(month from added_date) in (10, 11, 12) then 'Q3'
        when extract(month from added_date) in (1, 2, 3) then 'Q4'
    end as academic_quarter,
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
