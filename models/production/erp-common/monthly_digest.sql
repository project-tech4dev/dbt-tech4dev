{{ config(
    tags = ['projects']
) }}

SELECT
        *
    FROM
        {{ ref('int_monthly_digest') }}
