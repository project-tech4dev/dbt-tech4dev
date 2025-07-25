{{ config(
    materialized='table',
    tags=['projects']
    ) }}

WITH calendar AS (
    SELECT * FROM {{ ref('calendar') }}
),

unpivoted_actuals AS (
    SELECT
        financial_label,
        product_type,
        item,
        amount
    FROM {{ ref('int_fcxo_budget_actuals') }}
),

pivoted_actuals AS (
    SELECT
        financial_label,
        product_type,

        MAX(CASE WHEN item = 'Actual Revenue' THEN amount END) AS actual_revenue,
        MAX(CASE WHEN item = 'Target Revenue' THEN amount END) AS target_revenue,
        MAX(CASE WHEN item = 'Actual Expenses' THEN amount END) AS actual_expenses

    FROM unpivoted_actuals
    GROUP BY financial_label, product_type
)

SELECT
    c.start_date,
    c.end_date,
    c.financial_label,
    c.period_type,
    p.product_type,
    c.sort_order,
    p.actual_revenue,
    p.target_revenue,
    p.actual_expenses,
    p.actual_revenue/NULLIF(p.actual_expenses, 0) AS sustainability,
    p.actual_revenue/NULLIF(p.target_revenue,0) as target_revenue_met
FROM pivoted_actuals p
JOIN calendar c
    ON p.financial_label = c.financial_label
