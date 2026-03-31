with source as (
    select
        donor,
        level,
        district,
        school_name,
        total_mark,
        scoring_period,
        substring(scoring_period from 'Q([0-9])')::integer as quarter_num,
        substring(scoring_period from '[0-9]{4}')::integer as period_year
    from {{ ref('stem_teacher_champion_scoring_stg') }}
),

with_academic_year as (
    select
        donor,
        level,
        district,
        school_name,
        total_mark,
        scoring_period,
        quarter_num,
        'Q' || quarter_num::text as quarter,
        case
            when scoring_period != ''
                then
                    case
                        when quarter_num = 1
                            then (period_year - 1)::text || '-' || period_year::text
                        else period_year::text || '-' || (period_year + 1)::text
                    end
        end as academic_year,
        substring(level from '[0-9]+')::integer as level_num
    from source
)

select
    academic_year,
    quarter,
    donor,
    district,
    school_name,
    count(*) as total_teachers,
    count(case when level_num >= 3 then 1 end) as teachers_level_3_plus,
    round(avg(total_mark), 2) as avg_total_mark
from with_academic_year
group by
    academic_year,
    quarter,
    donor,
    district,
    school_name
order by
    academic_year,
    quarter,
    donor,
    district,
    school_name
