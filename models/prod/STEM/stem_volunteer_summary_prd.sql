select
    volunteer_type,
    location,
    corporate_name,
    category,
    cluster_mapped,
    manager_mapped,
    volunteering_year,
    volunteering_month,
    sum(total_volunteering_hours) as total_volunteering_hours,
    sum(no_of_volunteers) as total_volunteers
from {{ ref('stem_volunteer_count_stg') }}
group by
    volunteer_type,
    location,
    corporate_name,
    category,
    cluster_mapped,
    manager_mapped,
    volunteering_year,
    volunteering_month
order by
    volunteer_type,
    location,
    volunteering_year,
    volunteering_month
