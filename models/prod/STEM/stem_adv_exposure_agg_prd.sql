with source as (
    select
        donor,
        district,
        school_name,
        institution_name,
        no_of_volunteers,
        no_of_children_attended,
        date_of_exposure,
        case
            when date_of_exposure is not null
                then
                    (case
                        when extract(month from date_of_exposure) >= 4
                            then extract(year from date_of_exposure)
                        else extract(year from date_of_exposure) - 1
                    end)::integer::text
                    || '-'
                    || (case
                        when extract(month from date_of_exposure) >= 4
                            then extract(year from date_of_exposure) + 1
                        else extract(year from date_of_exposure)
                    end)::integer::text
        end as academic_year
    from {{ ref('stem_adv_exposure_detail_stg') }}
)

select
    donor,
    district,
    school_name,
    institution_name,
    academic_year,
    sum(no_of_volunteers) as total_volunteers,
    sum(no_of_children_attended) as total_children_attended
from source
group by
    donor,
    district,
    school_name,
    institution_name,
    academic_year
order by
    academic_year,
    donor,
    district,
    school_name,
    institution_name
