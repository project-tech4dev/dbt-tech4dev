{{ config(
    materialized='table',
    tags=['projects']
    ) }}

WITH calendar AS (
    SELECT * FROM {{ ref('calendar') }}
),

-- Projects started per period and product_type
projects_started AS (
    SELECT
        cal.start_date,
        cal.end_date,
        cal.period_type,
        p.product_type,
        COUNT(*) AS projects_started
    FROM calendar cal
    JOIN {{ ref('projects') }} p
      ON p.expected_start_date BETWEEN cal.start_date AND cal.end_date
    GROUP BY cal.start_date, cal.end_date, cal.period_type, p.product_type
),

-- Projects ongoing per period and product_type
projects_ongoing AS (
    SELECT
        cal.start_date,
        cal.end_date,
        cal.period_type,
        p.product_type,
        COUNT(*) AS projects_ongoing
    FROM calendar cal
    JOIN {{ ref('projects') }} p
      ON p.project_duration && daterange(cal.start_date, cal.end_date, '[]')
    GROUP BY cal.start_date, cal.end_date, cal.period_type,p.product_type
),

-- Projects ended per period and product_type
projects_ended AS (
    SELECT
        cal.start_date,
        cal.end_date,
        cal.period_type,
        p.product_type,
        COUNT(*) AS projects_ended
    FROM calendar cal
    JOIN {{ ref('projects') }} p
      ON p.expected_end_date BETWEEN cal.start_date AND cal.end_date
    GROUP BY cal.start_date, cal.end_date, cal.period_type,p.product_type
)

-- Final union of metrics
SELECT
    cal.start_date,
    cal.end_date,
    cal.period_type,
    cal.financial_label,
    cal.sort_order,
    COALESCE(ps.product_type, po.product_type, pe.product_type) AS product_type,
    COALESCE(ps.projects_started, 0) AS projects_started,
    COALESCE(po.projects_ongoing, 0) AS projects_ongoing,
    COALESCE(pe.projects_ended, 0) AS projects_ended

FROM calendar cal
LEFT JOIN projects_started ps
  ON ps.start_date = cal.start_date AND ps.period_type = cal.period_type
LEFT JOIN projects_ongoing po
  ON po.start_date = cal.start_date AND po.period_type = cal.period_type AND po.product_type = ps.product_type
LEFT JOIN projects_ended pe
  ON pe.start_date = cal.start_date AND pe.period_type = cal.period_type AND pe.product_type = ps.product_type

ORDER BY cal.start_date, product_type
