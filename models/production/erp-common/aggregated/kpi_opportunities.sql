{{ config(
    materialized='table',
    tags=['opportunities']
    ) }}

WITH calendar AS (
    SELECT * FROM {{ ref('calendar') }}
),

-- Opportunities created per period and product_type
opportunities_created AS (
    SELECT
        cal.start_date,
        cal.end_date,
        cal.period_type,
        ptt.product_type,
        COUNT(*) AS opportunities_created
    FROM calendar cal
    JOIN {{ source('erp_next', 'tabOpportunity') }} o
      ON o.creation::date BETWEEN cal.start_date AND cal.end_date
    LEFT JOIN {{ source('erp_next', 'tabProduct_Type_Table') }} ptt
      ON ptt.parent = o.name
    GROUP BY cal.start_date, cal.end_date, cal.period_type, ptt.product_type
),

-- Opportunities converted per period and product_type
opportunities_converted AS (
    SELECT
        cal.start_date,
        cal.end_date,
        cal.period_type,
        ptt.product_type,
        COUNT(*) AS opportunities_converted
    FROM calendar cal
    JOIN {{ source('erp_next', 'tabOpportunity') }} o
      ON COALESCE(o.custom_conversion_date, o.modified)::date BETWEEN cal.start_date AND cal.end_date
      AND o.status = 'Converted'
    LEFT JOIN {{ source('erp_next', 'tabProduct_Type_Table') }} ptt
      ON ptt.parent = o.name
    GROUP BY cal.start_date, cal.end_date, cal.period_type, ptt.product_type
),

-- Get all unique combinations of calendar periods and product types
period_product_combinations AS (
    SELECT DISTINCT
        cal.start_date,
        cal.end_date,
        cal.period_type,
        cal.financial_label,
        cal.sort_order,
        COALESCE(pt.product_type, 'Unknown') AS product_type
    FROM calendar cal
    CROSS JOIN (
        SELECT DISTINCT product_type FROM opportunities_created
        UNION
        SELECT DISTINCT product_type FROM opportunities_converted
        UNION
        SELECT 'Unknown' AS product_type
    ) pt
)

-- Final result
SELECT
    ppc.start_date,
    ppc.end_date,
    ppc.period_type,
    ppc.financial_label,
    ppc.sort_order,
    ppc.product_type,
    COALESCE(oc.opportunities_created, 0) AS opportunities_created,
    COALESCE(ocv.opportunities_converted, 0) AS opportunities_converted

FROM period_product_combinations ppc
LEFT JOIN opportunities_created oc
  ON oc.start_date = ppc.start_date 
  AND oc.period_type = ppc.period_type 
  AND COALESCE(oc.product_type, 'Unknown') = ppc.product_type
LEFT JOIN opportunities_converted ocv
  ON ocv.start_date = ppc.start_date 
  AND ocv.period_type = ppc.period_type 
  AND COALESCE(ocv.product_type, 'Unknown') = ppc.product_type

ORDER BY ppc.start_date, ppc.product_type
