{{ config(
    tags = ['digests']
) }}

SELECT
        *
    FROM
        {{ ref('int_monthly_digest') }}
