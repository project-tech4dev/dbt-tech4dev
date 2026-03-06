{{ config(
    materialized='table',
    tags=['nps', 'survey']
    ) }}

WITH base AS (
  SELECT
    *,
    CASE
      WHEN EXTRACT(MONTH FROM form_filled_date) >= 4
        THEN EXTRACT(YEAR FROM form_filled_date)
      ELSE EXTRACT(YEAR FROM form_filled_date) - 1
    END AS fy_start_year
  FROM {{ source('erp_next', 'tabNPS_Survey') }} stg_nps_survey
  where stg_nps_survey.name !=20
)

SELECT
  base.project,
  projects.project_name,
  projects.owner,
  base.fy_start_year,

  -- earliest entry in that FY
  MIN(base.form_filled_date) AS form_filled_date,

  -- averages
  AVG(base.recommend_program) AS recommend_program,
  AVG(base.satisfaction_value_add) AS satisfaction_value_add,
  AVG(base.program_outreach) AS program_outreach,
  AVG(base.internal_capacity) AS internal_capacity,
  AVG(base.confident_leverage) AS confident_leverage,
  AVG(base.improved_efficiency) AS improved_efficiency,
  AVG(base.technology_innovation) AS technology_innovation

FROM base
JOIN {{ref('int_projects')}} projects
  ON projects.name = base.project
GROUP BY base.project, projects.project_name, projects.owner,fy_start_year