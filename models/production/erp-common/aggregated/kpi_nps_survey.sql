{{ config(
    materialized='table',
    tags=['nps', 'survey']
    ) }}

WITH calendar AS (
    SELECT * FROM {{ ref('calendar') }}
),

-- NPS scores calculation (recommend_program: 1-5 scale)
-- NPS = (% of promoters - % of detractors) * 100
-- Promoters: score 5, Neutrals: score 3-4, Detractors: score 1-2
nps_scores AS (
    SELECT
        cal.start_date,
        cal.end_date,
        cal.period_type,
        COUNT(*) AS total_responses,
        COUNT(CASE WHEN nps.recommend_program = 5 THEN 1 END) AS promoters,
        COUNT(CASE WHEN nps.recommend_program BETWEEN 3 AND 4 THEN 1 END) AS neutrals,
        COUNT(CASE WHEN nps.recommend_program <= 2 THEN 1 END) AS detractors,
        ROUND(
            ((COUNT(CASE WHEN nps.recommend_program = 5 THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100) -
            (COUNT(CASE WHEN nps.recommend_program <= 2 THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100))::NUMERIC,
            2
        ) AS nps_score
    FROM calendar cal
    JOIN {{ source('erp_next', 'tabNPS_Survey') }} nps
      ON nps.form_filled_date::date BETWEEN cal.start_date AND cal.end_date
    WHERE nps.recommend_program IS NOT NULL
      AND nps.recommend_program BETWEEN 1 AND 5
    GROUP BY cal.start_date, cal.end_date, cal.period_type
),

-- Average scores for all other columns (1-5 scale)
average_scores AS (
    SELECT
        cal.start_date,
        cal.end_date,
        cal.period_type,
        COUNT(*) AS total_responses,
        ROUND(AVG(nps.program_outreach::NUMERIC), 2) AS avg_program_outreach,
        ROUND(AVG(nps.internal_capacity::NUMERIC), 2) AS avg_internal_capacity,
        ROUND(AVG(nps.confident_leverage::NUMERIC), 2) AS avg_confident_leverage,
        ROUND(AVG(nps.improved_efficiency::NUMERIC), 2) AS avg_improved_efficiency,
        ROUND(AVG(nps.technology_innovation::NUMERIC), 2) AS avg_technology_innovation,
        ROUND(AVG(nps.satisfaction_value_add::NUMERIC), 2) AS avg_satisfaction_value_add
    FROM calendar cal
    JOIN {{ source('erp_next', 'tabNPS_Survey') }} nps
      ON nps.form_filled_date::date BETWEEN cal.start_date AND cal.end_date
    WHERE nps.program_outreach IS NOT NULL
      OR nps.internal_capacity IS NOT NULL
      OR nps.confident_leverage IS NOT NULL
      OR nps.improved_efficiency IS NOT NULL
      OR nps.technology_innovation IS NOT NULL
      OR nps.satisfaction_value_add IS NOT NULL
    GROUP BY cal.start_date, cal.end_date, cal.period_type
)

-- Final result
SELECT
    cal.start_date,
    cal.end_date,
    cal.period_type,
    cal.financial_label,
    cal.sort_order,
    COALESCE(ns.total_responses, 0) AS nps_total_responses,
    COALESCE(ns.promoters, 0) AS nps_promoters,
    COALESCE(ns.neutrals, 0) AS nps_neutrals,
    COALESCE(ns.detractors, 0) AS nps_detractors,
    COALESCE(ns.nps_score, 0) AS nps_score,
    COALESCE(avg_s.total_responses, 0) AS survey_total_responses,
    COALESCE(avg_s.avg_program_outreach, 0) AS avg_program_outreach,
    COALESCE(avg_s.avg_internal_capacity, 0) AS avg_internal_capacity,
    COALESCE(avg_s.avg_confident_leverage, 0) AS avg_confident_leverage,
    COALESCE(avg_s.avg_improved_efficiency, 0) AS avg_improved_efficiency,
    COALESCE(avg_s.avg_technology_innovation, 0) AS avg_technology_innovation,
    COALESCE(avg_s.avg_satisfaction_value_add, 0) AS avg_satisfaction_value_add

FROM calendar cal
LEFT JOIN nps_scores ns
  ON ns.start_date = cal.start_date AND ns.period_type = cal.period_type
LEFT JOIN average_scores avg_s
  ON avg_s.start_date = cal.start_date AND avg_s.period_type = cal.period_type

ORDER BY cal.start_date
