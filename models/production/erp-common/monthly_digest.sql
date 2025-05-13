{{ config(
    tags = ['digests']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('int_monthly_digest') }}
)
SELECT
    project,
    project_name,
    product_type,
    YEAR,
    MONTH,
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
    base
