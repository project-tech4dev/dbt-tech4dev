{{ config(
  materialized='table',
  tags=["consulting", "dalgo"]
) }}

SELECT
    support_issue_id,
    TO_DATE(reporting_day, 'DD/MM/YYYY')::DATE AS reporting_day,
    TO_DATE(resolution_day, 'DD/MM/YYYY')::DATE AS resolution_day,
    raised_by,
    assigned_to
    ngo,
    status,
    description,
    hours_spent::numeric
FROM {{ source('staging_dalgo', 'support_issues_tracker') }}
