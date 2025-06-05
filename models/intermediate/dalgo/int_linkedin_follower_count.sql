{{ config(
  materialized='table',
  tags=["linkedin", "dalgo"]
) }}

SELECT
  "firstDegreeSize" AS total_followers
FROM {{ source('staging_dalgo', 'total_follower_count') }}