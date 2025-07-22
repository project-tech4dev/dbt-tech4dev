{{ config(
    materialized='table',
    tags=['projects']
    ) }}

{% set start_date = '2022-04-01' %}
{% set end_date = '2030-03-31' %}

-- RECURSIVE CTE TO GENERATE QUARTERS (financial year from April)
WITH RECURSIVE quarters AS (
    SELECT 
        DATE '{{ start_date }}' AS start_date,
        (DATE '{{ start_date }}' + INTERVAL '3 months' - INTERVAL '1 day')::date AS end_date
    UNION ALL
    SELECT 
        (start_date + INTERVAL '3 months')::date,
        (start_date + INTERVAL '6 months' - INTERVAL '1 day')::date
    FROM quarters
    WHERE (start_date + INTERVAL '3 months')::date <= DATE '{{ end_date }}'
),

-- ADD FINANCIAL YEAR AND LABEL
quarter_calendar AS (
    SELECT
        start_date,
        end_date,
        'quarter' AS period_type,
        
        -- Determine financial year
        CASE 
            WHEN EXTRACT(MONTH FROM start_date) >= 4 
                THEN 'FY' || RIGHT((EXTRACT(YEAR FROM start_date))::text, 2)
            ELSE 'FY' || RIGHT((EXTRACT(YEAR FROM start_date)-1)::text, 2)
        END AS financial_year_label,

        -- Determine quarter in financial year
        CASE 
            WHEN EXTRACT(MONTH FROM start_date) IN (4,5,6) THEN 'Q1'
            WHEN EXTRACT(MONTH FROM start_date) IN (7,8,9) THEN 'Q2'
            WHEN EXTRACT(MONTH FROM start_date) IN (10,11,12) THEN 'Q3'
            ELSE 'Q4'
        END AS financial_quarter,

        -- Combined label
        CASE 
            WHEN EXTRACT(MONTH FROM start_date) IN (4,5,6) THEN 'Q1'
            WHEN EXTRACT(MONTH FROM start_date) IN (7,8,9) THEN 'Q2'
            WHEN EXTRACT(MONTH FROM start_date) IN (10,11,12) THEN 'Q3'
            ELSE 'Q4'
        END 
        || ' ' || 
        CASE 
            WHEN EXTRACT(MONTH FROM start_date) >= 4 
                THEN 'FY' || RIGHT((EXTRACT(YEAR FROM start_date))::text, 2)
            ELSE 'FY' || RIGHT((EXTRACT(YEAR FROM start_date)-1)::text, 2)
        END AS financial_label
    FROM quarters
),

-- FINANCIAL YEAR ROLLUP
financial_years AS (
    SELECT
        MIN(start_date) AS start_date,
        MAX(end_date) AS end_date,
        'year' AS period_type,
        financial_year_label AS financial_label
    FROM quarter_calendar
    GROUP BY financial_year_label
)

-- UNION QUARTERS AND YEARS
SELECT 
    start_date,
    end_date,
    period_type,
    financial_label,
    ROW_NUMBER() OVER (ORDER BY start_date) AS sort_order
FROM quarter_calendar

UNION ALL

SELECT 
    start_date,
    end_date,
    period_type,
    financial_label,
    ROW_NUMBER() OVER (ORDER BY start_date) AS sort_order
FROM financial_years

ORDER BY start_date
