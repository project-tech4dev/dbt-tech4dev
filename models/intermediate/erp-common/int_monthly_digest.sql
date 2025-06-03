{{ config(
    tags = ['digests']
) }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('erp_next', 'tabMonthly_Digest') }}
        -- raw table
),
projects AS (
    SELECT
        name,
        project_name
    FROM
        {{ source('erp_next', 'tabProject') }}
)
SELECT
    s.project,
    p.project_name,
    s.product_type,
    s.MONTH,
    s.YEAR,
    s.hours_spend,
    s.hours_committed,
    s.highlights,
    s.challenges,
    s.unplanned_work,
    s.plan_next_month,
    s.lessons_learned,
    s.last_edited_time,
    s.creation_time,
    MAKE_DATE(
        s.YEAR::INTEGER,
        CASE s.MONTH
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
    source s
    LEFT JOIN projects p ON s.project = p.name
WHERE
    /* removing submitted row logic as doctype has been fixed
    COALESCE(s.docstatus, 0) = 1 -- keep only “submitted” rows
    AND */
    s.YEAR > 2020