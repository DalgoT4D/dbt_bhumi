-- Combine session planned and session conducted data
-- Join only on school, class, class division.
-- Return planned columns (prefixed with planned_) and conducted columns (prefixed with conducted_).

SELECT
  -- canonical join keys
  COALESCE(NULLIF(trim(sp."School Name"::text), ''), NULLIF(trim(sc.school_name::text), '')) AS school_name,
  COALESCE(NULLIF(trim(sp.class::text), ''), NULLIF(trim(sc.class::text), '')) AS class,
  COALESCE(NULLIF(trim(sp."Class Division"::text), ''), NULLIF(trim(sc.class_division::text), '')) AS class_division,

  -- Planned (from session_planned_STP)
  sp."Trainer Name"                           AS planned_trainer_name,
  sp."Intervention name"                      AS planned_intervention_name,
  sp."Session Planned"                        AS planned_session_count,
  sp."Experiment Planned"                     AS planned_experiment,
  sp."donor"                                  AS planned_donor,

  -- Conducted (from session_conducted_STP)
  sc.class_type                                AS conducted_class_type,
  sc.trainer_name                              AS conducted_trainer_name,
  sc.intervention_name                         AS conducted_intervention_name,
  sc.session_type                              AS conducted_session_type,
  sc.session_month                             AS conducted_session_month,
  sc.session_count                             AS conducted_session_count

FROM {{ ref('session_planned_STP') }} AS sp
FULL OUTER JOIN {{ ref('session_conducted_STP') }} AS sc
  ON LOWER(TRIM(COALESCE(sp."School Name"::text, ''))) = LOWER(TRIM(COALESCE(sc.school_name::text, '')))
  AND LOWER(TRIM(COALESCE(sp.class::text, ''))) = LOWER(TRIM(COALESCE(sc.class::text, '')))
  AND LOWER(TRIM(COALESCE(sp."Class Division"::text, ''))) = LOWER(TRIM(COALESCE(sc.class_division::text, '')))


