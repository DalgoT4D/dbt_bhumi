-- Combine fields from eco_base_end_25_26 and eco_modules_25_26
-- Produces one row per student/roll combining baseline/endline and module attendance data
with base as (
	select * from {{ ref('eco_base_end_25_26') }}
),
modules as (
	select * from {{ ref('eco_modules_25_26') }}
),

-- 1) Matches by Roll No (both non-null and equal)
matched_by_roll as (
	select
		coalesce(b."Roll No", m."Roll No")                                        as "Roll No",
		coalesce(b."Student Name", m."Student Name")                              as "Student Name",
		coalesce(b."School ID", m."School ID")                                    as "School ID",
		coalesce(b."School", m."School")                                          as "School",
		coalesce(b."Grade", m."Grade")                                            as "Grade",
		coalesce(b."Chapter", m."Chapter")                                        as "Chapter",
		coalesce(b."Center Coordiantor (1)", m."Center Coordiantor (1)")           as "Center Coordiantor (1)",
		coalesce(b."Center Coordianator (2)", m."Center Coordianator (2)")         as "Center Coordianator (2)",
		coalesce(b."Student Status", m."Student Status")                          as "Student Status",
		coalesce(b."Donor Mapped", m."Donor Mapped")                              as "Donor Mapped",
		coalesce(b."Modlues Completed", m."Modlues Completed")                    as "Modlues Completed",

		-- baseline / endline fields (prefer from base)
		b.baseline_score,
		b.baseline_attendance,
		b.baseline_date,
		b.endline_score,
		b.endline_attendance,
		b.endline_date,
		b."Assessment Attendance %",

		-- module-level fields (prefer from modules)
		m."Attendance %",
		m.kitchen_garden_attendance,
		m.kitchen_garden_date,
		m.waste_management_attendance,
		m.waste_management_date,
		m.water_conservation_attendance,
		m.water_conservation_date,
		m.climate_attendance,
		m.climate_date,
		m.lifestyle_choices_attendance,
		m.lifestyle_choices_date,
		m."Modules Attendance %"
	from base b
	join modules m
		on b."Roll No" = m."Roll No"
	where b."Roll No" is not null and m."Roll No" is not null
),

-- 2) Matches by School ID + Student Name when at least one Roll No is NULL
matched_by_school_name as (
	select
		coalesce(b."Roll No", m."Roll No")                                        as "Roll No",
		coalesce(b."Student Name", m."Student Name")                              as "Student Name",
		coalesce(b."School ID", m."School ID")                                    as "School ID",
		coalesce(b."School", m."School")                                          as "School",
		coalesce(b."Grade", m."Grade")                                            as "Grade",
		coalesce(b."Chapter", m."Chapter")                                        as "Chapter",
		coalesce(b."Center Coordiantor (1)", m."Center Coordiantor (1)")           as "Center Coordiantor (1)",
		coalesce(b."Center Coordianator (2)", m."Center Coordianator (2)")         as "Center Coordianator (2)",
		coalesce(b."Student Status", m."Student Status")                          as "Student Status",
		coalesce(b."Donor Mapped", m."Donor Mapped")                              as "Donor Mapped",
		coalesce(b."Modlues Completed", m."Modlues Completed")                    as "Modlues Completed",

		-- baseline / endline fields (prefer from base)
		b.baseline_score,
		b.baseline_attendance,
		b.baseline_date,
		b.endline_score,
		b.endline_attendance,
		b.endline_date,
		b."Assessment Attendance %",

		-- module-level fields (prefer from modules)
		m."Attendance %",
		m.kitchen_garden_attendance,
		m.kitchen_garden_date,
		m.waste_management_attendance,
		m.waste_management_date,
		m.water_conservation_attendance,
		m.water_conservation_date,
		m.climate_attendance,
		m.climate_date,
		m.lifestyle_choices_attendance,
		m.lifestyle_choices_date,
		m."Modules Attendance %"
	from base b
	join modules m
	  on b."School ID" = m."School ID"
	 and b."Student Name" = m."Student Name"
	where (b."Roll No" is null or m."Roll No" is null)
),

-- 3) base-only rows (no matching module by Roll No, nor by SchoolID+Name when eligible)
base_only as (
	select
		b."Roll No",
		b."Student Name",
		b."School ID",
		b."School",
		b."Grade",
		b."Chapter",
		b."Center Coordiantor (1)",
		b."Center Coordianator (2)",
		b."Student Status",
		b."Donor Mapped",
		b."Modlues Completed",

		-- baseline / endline fields
		b.baseline_score,
		b.baseline_attendance,
		b.baseline_date,
		b.endline_score,
		b.endline_attendance,
		b.endline_date,
		b."Assessment Attendance %",

		-- module placeholders
		null::numeric as "Attendance %",
		null::text as kitchen_garden_attendance,
		null::date as kitchen_garden_date,
		null::text as waste_management_attendance,
		null::date as waste_management_date,
		null::text as water_conservation_attendance,
		null::date as water_conservation_date,
		null::text as climate_attendance,
		null::date as climate_date,
		null::text as lifestyle_choices_attendance,
		null::date as lifestyle_choices_date,
		null::numeric as "Modules Attendance %"
	from base b
	where not exists (
		select 1 from modules m
		where m."Roll No" is not null and b."Roll No" is not null and b."Roll No" = m."Roll No"
	)
	and not exists (
		select 1 from modules m
		where (b."Roll No" is null or m."Roll No" is null)
		  and b."School ID" = m."School ID"
		  and b."Student Name" = m."Student Name"
	)
),

-- 4) modules-only rows (no matching base by Roll No, nor by SchoolID+Name when eligible)
modules_only as (
	select
		m."Roll No",
		m."Student Name",
		m."School ID",
		m."School",
		m."Grade",
		m."Chapter",
		m."Center Coordiantor (1)",
		m."Center Coordianator (2)",
		m."Student Status",
		m."Donor Mapped",
		m."Modlues Completed",

		-- base/endline placeholders
		null::numeric as baseline_score,
		null::text as baseline_attendance,
		null::date as baseline_date,
		null::numeric as endline_score,
		null::text as endline_attendance,
		null::date as endline_date,
		null::numeric as "Assessment Attendance %",

		-- module fields (populate from modules)
		m."Attendance %",
		m.kitchen_garden_attendance,
		m.kitchen_garden_date,
		m.waste_management_attendance,
		m.waste_management_date,
		m.water_conservation_attendance,
		m.water_conservation_date,
		m.climate_attendance,
		m.climate_date,
		m.lifestyle_choices_attendance,
		m.lifestyle_choices_date,
		m."Modules Attendance %"
	from modules m
	where not exists (
		select 1 from base b
		where b."Roll No" is not null and m."Roll No" is not null and b."Roll No" = m."Roll No"
	)
	and not exists (
		select 1 from base b
		where (b."Roll No" is null or m."Roll No" is null)
		  and b."School ID" = m."School ID"
		  and b."Student Name" = m."Student Name"
	)
),

-- final union: matched by roll, matched by school+name (fallback), plus only-ones
combined_data as (
	select * from matched_by_roll
	union all
	select * from matched_by_school_name
	union all
	select * from base_only
	union all
	select * from modules_only
)

select
	*,
	
	-- Calculate quarters for each assessment/module based on date values
	case
		when baseline_date between date '2025-04-01' and date '2025-06-30' then 'Q1'
		when baseline_date between date '2025-07-01' and date '2025-09-30' then 'Q2'
		when baseline_date between date '2025-10-01' and date '2025-12-31' then 'Q3'
		when baseline_date between date '2026-01-01' and date '2026-03-31' then 'Q4'
		else null
	end as q_baseline,
	
	case
		when endline_date between date '2025-04-01' and date '2025-06-30' then 'Q1'
		when endline_date between date '2025-07-01' and date '2025-09-30' then 'Q2'
		when endline_date between date '2025-10-01' and date '2025-12-31' then 'Q3'
		when endline_date between date '2026-01-01' and date '2026-03-31' then 'Q4'
		else null
	end as q_endline,
	
	case
		when kitchen_garden_date between date '2025-04-01' and date '2025-06-30' then 'Q1'
		when kitchen_garden_date between date '2025-07-01' and date '2025-09-30' then 'Q2'
		when kitchen_garden_date between date '2025-10-01' and date '2025-12-31' then 'Q3'
		when kitchen_garden_date between date '2026-01-01' and date '2026-03-31' then 'Q4'
		else null
	end as q_kitchen_garden,
	
	case
		when waste_management_date between date '2025-04-01' and date '2025-06-30' then 'Q1'
		when waste_management_date between date '2025-07-01' and date '2025-09-30' then 'Q2'
		when waste_management_date between date '2025-10-01' and date '2025-12-31' then 'Q3'
		when waste_management_date between date '2026-01-01' and date '2026-03-31' then 'Q4'
		else null
	end as q_waste_management,
	
	case
		when water_conservation_date between date '2025-04-01' and date '2025-06-30' then 'Q1'
		when water_conservation_date between date '2025-07-01' and date '2025-09-30' then 'Q2'
		when water_conservation_date between date '2025-10-01' and date '2025-12-31' then 'Q3'
		when water_conservation_date between date '2026-01-01' and date '2026-03-31' then 'Q4'
		else null
	end as q_water_conservation,
	
	case
		when climate_date between date '2025-04-01' and date '2025-06-30' then 'Q1'
		when climate_date between date '2025-07-01' and date '2025-09-30' then 'Q2'
		when climate_date between date '2025-10-01' and date '2025-12-31' then 'Q3'
		when climate_date between date '2026-01-01' and date '2026-03-31' then 'Q4'
		else null
	end as q_climate,
	
	case
		when lifestyle_choices_date between date '2025-04-01' and date '2025-06-30' then 'Q1'
		when lifestyle_choices_date between date '2025-07-01' and date '2025-09-30' then 'Q2'
		when lifestyle_choices_date between date '2025-10-01' and date '2025-12-31' then 'Q3'
		when lifestyle_choices_date between date '2026-01-01' and date '2026-03-31' then 'Q4'
		else null
	end as q_lifestyle_choices

from combined_data
