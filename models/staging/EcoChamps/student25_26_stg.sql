select
  nullif(trim("School"), '') as "School",                                 -- string
  nullif(trim("School_ID"), '') as "School_ID",                          -- string
  nullif(trim("Grade"), '') as "Grade",                                  -- string
  cast(nullif(trim("Student_Count"), '') as integer) as "Student_Count", -- integer
  nullif(trim("Center_Coordiantor_(1)"), '') as "Center_Coordiantor_1",    -- string
  nullif(trim("Center_Coordianator_(2)"), '') as "Center_Coordianator_2",    -- string
  cast(nullif(trim("Baseline"), '') as numeric) as "Baseline",           -- numeric/float
  nullif(trim("Kitchen_Garden"), '') as "Kitchen_Garden",                -- string
  nullif(trim("Waste_Management"), '') as "Waste_Management",            -- string
  nullif(trim("Water_Conservation"), '') as "Water_Conservation",        -- string
  nullif(trim("Climate"), '') as "Climate",                              -- string
  nullif(trim("Lifestyle_Choices"), '') as "Lifestyle_Choices",          -- string
  cast(nullif(trim("Endline"), '') as numeric) as "Endline",             -- numeric/float
  nullif(trim("Mo"), '') as "Mo",                                        -- string
  nullif(trim("Session_Completion_Status"), '') as "Session_Completion_Status", -- string
  cast(nullif(replace(trim("Classroom_Attendance"), '%', ''), '') as integer) as "Classroom_Attendance", -- integer
  cast(nullif(trim("Progress_Bar_Q2"), '') as numeric) as "Progress_Bar_Q2", -- numeric/float
  cast(nullif(trim("Total_Students_Count"), '') as integer) as "Total_Students_Count", -- integer
  nullif(trim("Donor_Mapped"), '') as "Donor_Mapped"                     -- string
from {{ source('ecochamps25_26', 'Data_Tracker') }}
