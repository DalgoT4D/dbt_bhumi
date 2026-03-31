{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

select 
"College__Club_Name_" as college_club_name,
"POC" as poc,
"City" as city,
case
  when "State" in ('TN', 'Tamil Nadu')       then 'Tamil Nadu'
  when "State" in ('AP')                      then 'Andhra Pradesh'
  when "State" in ('UP')                      then 'Uttar Pradesh'
  when "State" in ('NA')                      then 'Not Available'
  when "State" in ('HP')                      then 'Himachal Pradesh'
  when "State" in ('MP')                      then 'Madhya Pradesh'
  when "State" in ('Maharashtra')             then 'Maharashtra'
  when "State" in ('Delhi')                   then 'Delhi'
  when "State" in ('Telangana')               then 'Telangana'
  when "State" in ('Karnataka')               then 'Karnataka'
  when "State" in ('Kerala')                  then 'Kerala'
  else "State"
end as state,
"Status" as status,
"Launch_Month" as launch_month,
"Inactive_Month" as inactive_month,
"Contact" as contact,
'2025-26' as academic_year

FROM {{ source('civic_clubs', 'Total_College_Partners') }}