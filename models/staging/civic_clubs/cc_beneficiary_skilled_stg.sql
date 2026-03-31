{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

Select
"ID" as beneficiary_id,
cast("Age" as int) as age,
"Batch" ->> 'Batch_Name' as batch_name,
"Institute_Name" ->> 'Name' as institute_name,
"Name" as name,
"Gender" as gender,
cast("CGPA_Percentage" as float) as cgpa_percentage,
"Specialization" ->> 'Specialization' as specialization,
"Year_of_Passing" as year_of_passing,
"Mode_Of_Training" as mode_of_training,
"Beneficiary_Status" as beneficiary_status,
"History_of_arrears" as history_of_arrears

from {{ source('zc_skilled_data', 'School_Beneficiary_Details_Report') }}