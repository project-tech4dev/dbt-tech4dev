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
    creation_time
FROM
    source
WHERE
    COALESCE(
        docstatus,
        0
    ) = 1 -- keep only “submitted” rows
    AND YEAR > 2020
