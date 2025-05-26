{{ config(
    tags = ['digests']
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'erp_next',
            'tabMonthly_Digest'
        ) }}
        -- raw table
)
SELECT
    project,
    project_name,
    product_type,
    MONTH,
    YEAR,
    hours_spend,
    hours_committed,
    highlights,
    challenges,
    unplanned_work,
    plan_next_month,
    lessons_learned,
    last_edited_time,
    creation_time,
    MAKE_DATE(
        YEAR::INTEGER,
        CASE MONTH
            WHEN 'Jan' THEN 1
            WHEN 'Feb' THEN 2
            WHEN 'Mar' THEN 3
            WHEN 'Apr' THEN 4
            WHEN 'May' THEN 5
            WHEN 'Jun' THEN 6
            WHEN 'Jul' THEN 7
            WHEN 'Aug' THEN 8
            WHEN 'Sep' THEN 9
            WHEN 'Oct' THEN 10
            WHEN 'Nov' THEN 11
            WHEN 'Dec' THEN 12
        END,
        1
    ) AS report_date
FROM
    source
WHERE
    COALESCE(
        docstatus,
        0
    ) = 1 -- keep only “submitted” rows
    AND YEAR > 2020
