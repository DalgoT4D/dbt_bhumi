with source as (
    select
        donor,
        district,
        manager,
        trainer,
        stem_club_start_date,
        case
            when stem_club_start_date is not null
                then
                    (case
                        when extract(month from stem_club_start_date) >= 4
                            then extract(year from stem_club_start_date)
                        else extract(year from stem_club_start_date) - 1
                    end)::integer::text
                    || '-'
                    || (case
                        when extract(month from stem_club_start_date) >= 4
                            then extract(year from stem_club_start_date) + 1
                        else extract(year from stem_club_start_date)
                    end)::integer::text
        end as academic_year
    from {{ ref('stem_school_clubs_stg') }}
)

select
    donor,
    district,
    manager,
    trainer,
    academic_year,
    count(*) as school_count
from source
group by
    donor,
    district,
    manager,
    trainer,
    academic_year
order by
    academic_year,
    donor,
    district,
    manager,
    trainer
