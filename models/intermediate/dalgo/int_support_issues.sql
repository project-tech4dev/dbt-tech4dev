{{ config(
  materialized='table',
  tags=["consulting", "dalgo"]
) }}

SELECT
    support_issue_id,
    reporting_day,
    resolution_day,
    raised_by,
    assigned_to
    ngo,
    status,
    description,
    hours_spent
FROM {{ source('staging_dalgo', 'support_issues_tracker') }}
