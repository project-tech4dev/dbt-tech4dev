{{ config(
  materialized='table',
  tags=["consulting", "dalgo", "dalgo_consulting"]
) }}


SELECT 
* 
FROM {{ ref('int_consulting_tracker') }}