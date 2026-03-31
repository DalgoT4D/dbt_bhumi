-- pre-clean date strings for intern records before passing to validate_date
with clean_interns as (
    select
        *,
        regexp_replace(start_date::text, '[^0-9./\-]', '', 'g') as start_date_clean,
        regexp_replace(end_date::text, '[^0-9./\-]', '', 'g') as end_date_clean
    from {{ source('Stem_gsheet_data', 'Interns_Volunteer_Tracker') }}
),

corporate as (
    select
        'corporate' as volunteer_type,
        Coalesce(Initcap(Btrim(location::text)), '') as location,
        Coalesce(Initcap(Btrim(corporate_name::text)), '') as corporate_name,
        case when Btrim(no_of_volunteers::text) ~ '^\d+$' then (no_of_volunteers::text)::integer end as no_of_volunteers,
        Coalesce(Btrim(volunteering_month::text), '') as volunteering_month,
        case when Btrim(hours_per_volunteer::text) ~ '^[0-9]+(\.[0-9]+)?$' then (hours_per_volunteer::text)::numeric end as hours_per_volunteer,
        case when Btrim(total_volunteering_hours_d_x_e_::text) ~ '^[0-9]+(\.[0-9]+)?$' then (total_volunteering_hours_d_x_e_::text)::numeric end as total_volunteering_hours,
        Coalesce(Btrim(brief_description_about_the_activity::text), '') as activity_description,
        case
            when Btrim(volunteer_year::text) ~ '\d{4}'
                then regexp_replace(Btrim(volunteer_year::text), '.*(\d{4}).*', '\1')
        end as volunteering_year,
        null::varchar as category,
        null::varchar as name_of_volunteer,
        null::varchar as institution_name,
        null::varchar as education_background,
        null::varchar as email_id,
        null::varchar as phone_number,
        null::varchar as cluster_mapped,
        null::varchar as manager_mapped,
        null::varchar as certificate_issued,
        null::date as start_date,
        null::date as end_date
    from {{ source('Stem_gsheet_data', 'Corporate_Volunteering_Data') }}
),

stem_fest as (
    select
        'stem_fest' as volunteer_type,
        Coalesce(Initcap(Btrim(stem_fest_location::text)), '') as location,
        null::varchar as corporate_name,
        case when Btrim(no_of_volunteers::text) ~ '^\d+$' then (no_of_volunteers::text)::integer end as no_of_volunteers,
        Coalesce(Btrim(volunteering_month::text), '') as volunteering_month,
        case when Btrim(hours_per_volunteer::text) ~ '^[0-9]+(\.[0-9]+)?$' then (hours_per_volunteer::text)::numeric end as hours_per_volunteer,
        case when Btrim(total_volunteering_hours_cx_d_::text) ~ '^[0-9]+(\.[0-9]+)?$' then (total_volunteering_hours_cx_d_::text)::numeric end as total_volunteering_hours,
        null::varchar as activity_description,
        case
            when Btrim(volunteering_year::text) ~ '\d{4}'
                then regexp_replace(Btrim(volunteering_year::text), '.*(\d{4}).*', '\1')
        end as volunteering_year,
        null::varchar as category,
        null::varchar as name_of_volunteer,
        null::varchar as institution_name,
        null::varchar as education_background,
        null::varchar as email_id,
        null::varchar as phone_number,
        null::varchar as cluster_mapped,
        null::varchar as manager_mapped,
        null::varchar as certificate_issued,
        null::date as start_date,
        null::date as end_date
    from {{ source('Stem_gsheet_data', 'STEM_FEST_X_Volunteers') }}
),

interns as (
    select
        'intern' as volunteer_type,
        Coalesce(Initcap(Btrim(cluster_mapped::text)), '') as location,
        null::varchar as corporate_name,
        null::integer as no_of_volunteers,
        Coalesce(Btrim(intern_volunteer_month::text), '') as volunteering_month,
        null::numeric as hours_per_volunteer,
        case when Btrim(total_intern_hours::text) ~ '^[0-9]+(\.[0-9]+)?$' then (total_intern_hours::text)::numeric end as total_volunteering_hours,
        Coalesce(Btrim(activities_engaged_in::text), '') as activity_description,
        case
            when Btrim(volunteer_year::text) ~ '\d{4}'
                then regexp_replace(Btrim(volunteer_year::text), '.*(\d{4}).*', '\1')
        end as volunteering_year,
        Coalesce(Btrim(category::text), '') as category,
        Coalesce(Initcap(Btrim(name_of_the_intern_volunteer::text)), '') as name_of_volunteer,
        Coalesce(Initcap(Btrim(name_institution_name_::text)), '') as institution_name,
        Coalesce(Btrim(education_background_course_enrlolled_in::text), '') as education_background,
        Coalesce(Btrim(email_id_::text), '') as email_id,
        Coalesce(Btrim(phone_number::text), '') as phone_number,
        Coalesce(Btrim(cluster_mapped::text), '') as cluster_mapped,
        Coalesce(Btrim(manager_mapped::text), '') as manager_mapped,
        Coalesce(Btrim(certificate_issued::text), '') as certificate_issued,
        {{ validate_date('start_date_clean') }} as start_date,
        {{ validate_date('end_date_clean') }} as end_date
    from clean_interns
),

unified as (
    select * from corporate
    union all
    select * from stem_fest
    union all
    select * from interns
)

select distinct
    volunteer_type,
    location,
    corporate_name,
    no_of_volunteers,
    volunteering_month,
    volunteering_year,
    hours_per_volunteer,
    total_volunteering_hours,
    activity_description,
    category,
    name_of_volunteer,
    institution_name,
    education_background,
    email_id,
    phone_number,
    cluster_mapped,
    manager_mapped,
    certificate_issued,
    start_date,
    end_date
from unified
