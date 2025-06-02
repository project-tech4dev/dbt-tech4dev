{{ config(
  materialized='table',
  tags=["consulting", "dalgo"]
) }}

SELECT
    start_date,
    end_date,
    consultant,
    ngo,
    project_program,
    category,
    dashboard,
    description,
    hours_spent,
    billing_status,
    num_charts_made,
    attendees,
    json_blob
FROM {{ source('staging_dalgo', 'consulting_time_tracker') }}
