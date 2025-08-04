{{ config(
    tags = ['projects']
) }}

SELECT
        *, CASE WHEN report_date = MAX(report_date) OVER (PARTITION BY project) THEN 'Yes' ELSE 'No' END AS is_most_recent
    FROM
        {{ ref('int_monthly_digest') }}
