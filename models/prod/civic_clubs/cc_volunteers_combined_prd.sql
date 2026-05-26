{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

select
    'EcoChamps' as program_name,
    month,
    academic_year,
    city,
    state,
    active_volunteer_count,
    total_volunteer_hours
from {{ ref('cc_volunteers_ecochamp_prd') }}

union all

select
    'Civic' as program_name,
    month,
    academic_year,
    city,
    state,
    active_volunteer_count
    
from {{ ref('cc_volunteers_eventregn_prd') }}

union all

select
    'SkillEd' as program_name,
    month,
    academic_year,
    city,
    state,
    active_volunteer_count,
    total_volunteering_hours as total_volunteer_hours
from {{ ref('cc_volunteers_skilled_prd') }}

union all

select
    'College Clubs' as program_name,
    month,
    academic_year,
    city,
    state,
    total_volunteers as active_volunteer_count,
    total_volunteering_hours as total_volunteer_hours
from {{ ref('cc_volunteers_collegeclubs_citywise_prd') }}

union all

select
    'Campaigns' as program_name,
    month,
    academic_year,
    city,
    state,
    num_volunteers_engaged as active_volunteer_count,
    num_volunteering_hours as total_volunteer_hours
from {{ ref('cc_volunteers_collegeclubs_citywise_prd') }}

