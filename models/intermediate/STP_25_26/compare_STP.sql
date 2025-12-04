SELECT
  sa.academic_year,
  sa.school_name,
  sa.class_type,
  sa.trainer_name,
  sa.class,
  sa.class_division,
  sa.intervention_name,
  sa.session_type,
  to_char(sa.session_date, 'FMMonth') AS session_month,
  count(*) AS session_count,
  min(sa.session_date) AS first_session_date,
  max(sa.session_date) AS last_session_date
FROM {{ ref('session_attendance_STP') }} AS sa
WHERE sa.session_date IS NOT NULL
GROUP BY
  sa.academic_year,
  sa.school_name,
  sa.class_type,
  sa.trainer_name,
  sa.class,
  sa.class_division,
  sa.intervention_name,
  sa.session_type,
  to_char(sa.session_date, 'FMMonth')
ORDER BY
  sa.school_name,
  sa.class,
  sa.class_division,
  session_month