{{ config(
  materialized='table',
  tags=["consulting", "dalgo"]
) }}


SELECT 
* 
FROM {{ ref('int_support_issues') }}