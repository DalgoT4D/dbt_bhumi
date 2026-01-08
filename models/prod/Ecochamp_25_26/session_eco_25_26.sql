-- Convert session columns into rows
-- Each student will have 7 rows (one per session type)

with base_data as (
    select * from {{ ref('combine_eco_25_26') }}
)

select
    "Roll No",
    "Student Name",
    "School ID",
    "School",
    "Grade",
    "Chapter",
    "Donor Mapped",
    "Student Status",
	
    session,
    session_attendance,
    session_date,
    quarter

from base_data
cross join
    lateral (
        values
        ('Baseline', base_data.baseline_attendance, base_data.baseline_date, base_data.q_baseline),
        ('Endline', base_data.endline_attendance, base_data.endline_date, base_data.q_endline),
        ('Kitchen Garden', base_data.kitchen_garden_attendance, base_data.kitchen_garden_date, base_data.q_kitchen_garden),
        ('Waste Management', base_data.waste_management_attendance, base_data.waste_management_date, base_data.q_waste_management),
        ('Water Conservation', base_data.water_conservation_attendance, base_data.water_conservation_date, base_data.q_water_conservation),
        ('Climate', base_data.climate_attendance, base_data.climate_date, base_data.q_climate),
        ('Life Style & Choices', base_data.lifestyle_choices_attendance, base_data.lifestyle_choices_date, base_data.q_lifestyle_choices)
    ) as unpivot(session, session_attendance, session_date, quarter)

order by "School", "Roll No", session_date, session
