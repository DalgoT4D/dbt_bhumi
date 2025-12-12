SELECT
  COALESCE(pc.school_name_norm, cc.school_name_norm) AS "School Name",

  -- Planned fields (prefixed with 'planned')
  NULLIF(STRING_AGG(DISTINCT pc.planned_trainer_name, ', '), '')           AS "planned Trainer name",
  NULLIF(STRING_AGG(DISTINCT pc.planned_intervention_name, ', '), '')      AS "planned Intervention name",
  SUM(pc.session_planned)                                                  AS "Session Planned",
  NULLIF(STRING_AGG(DISTINCT experiment_planned, ', '), '')                AS "Experiment Planned",
  NULLIF(STRING_AGG(DISTINCT pc.planned_donor, ', '), '')                  AS "planned donor",

  -- Conducted fields (prefixed with 'conducted')
  NULLIF(STRING_AGG(DISTINCT cc.conducted_intervention_name, ', '), '')    AS "conducted Intervention name",
  NULLIF(STRING_AGG(DISTINCT cc.session_type, ', '), '')                   AS "conducted session_type",
  NULLIF(STRING_AGG(DISTINCT cc.session_month, ', '), '')                  AS "conducted session_month",
  SUM(cc.session_count)                                                    AS "conducted session count",
  NULLIF(STRING_AGG(DISTINCT cc.trainer_name, ', '), '')                   AS "conducted Trainer name"

FROM
  (
    -- Normalize planned table keys and expose planned columns
    SELECT
      trim(lower("School Name")) AS school_name_norm,
      "Trainer Name"             AS planned_trainer_name,
      "Intervention name"        AS planned_intervention_name,
      -- assume numeric; if not numeric, cast / change aggregation
      COALESCE("Session Planned", 0)::numeric    AS session_planned,
      trim(lower("Experiment Planned")) AS experiment_planned,
      donor                        AS planned_donor
    FROM {{ ref('session_planned_STP') }}
  ) pc
FULL OUTER JOIN
  (
    -- Normalize conducted table keys and expose conducted columns
    SELECT
      trim(lower(school_name))    AS school_name_norm,
      trainer_name                AS trainer_name,
      intervention_name           AS conducted_intervention_name,
      session_type,
      session_month,
      COALESCE(session_count, 0)::numeric AS session_count
    FROM {{ ref('session_conducted_STP') }}
  ) cc
ON pc.school_name_norm = cc.school_name_norm

GROUP BY
  COALESCE(pc.school_name_norm, cc.school_name_norm)
ORDER BY 1,2,3
