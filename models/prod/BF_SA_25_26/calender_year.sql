{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with academic_years as (
    select distinct academic_year
    from {{ ref("fellow_scl_cls_data") }}
),

months as (
    select generate_series(1, 12) as month_number
)

select
    academic_year,
    to_char(
        date '2000-01-01' + ((month_number - 1) * interval '1 month'),
        'Mon'
    ) as month,
    case
        when month_number between 4 and 6 then 'Q1'
        when month_number between 7 and 9 then 'Q2'
        when month_number between 10 and 12 then 'Q3'
        when month_number between 1 and 3 then 'Q4'
    end as quarter
from academic_years
cross join months
order by academic_year, month_number
