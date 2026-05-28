{{ config(
  materialized='table',
  tags=["civic_clubs", "staging"]
) }}

Select
    "ID" As beneficiary_id,
    cast("Age" As int) As age,
    "Batch" ->> 'Batch_Name' As batch_name,
    "Institute_Name" ->> 'Name' As institute_name,
    "Name" As name,
    "Gender" As gender,
    cast("CGPA_Percentage" As float) As cgpa_percentage,
    "Specialization" ->> 'Specialization' As specialization,
    "Year_of_Passing" As year_of_passing,
    "Mode_Of_Training" As mode_of_training,
    "Beneficiary_Status" As beneficiary_status,
    "History_of_arrears" As history_of_arrears

From {{ source('zc_skilled_data', 'School_Beneficiary_Details_Report') }}
