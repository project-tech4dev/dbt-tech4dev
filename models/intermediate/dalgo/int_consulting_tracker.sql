{{ config(
  materialized='table',
  tags=["consulting", "dalgo"]
) }}

SELECT
    TO_DATE(start_date, 'DD/MM/YYYY')::DATE AS start_date,
    TO_DATE(end_date, 'DD/MM/YYYY')::DATE AS end_date,
    consultant,
    ngo,
    project_program,
    category,
    dashboard,
    description,
    hours_spent::numeric,
    billing_status,
    num_charts_made::integer,
    attendees,
    json_blob
FROM {{ source('staging_dalgo', 'consulting_time_tracker') }}
