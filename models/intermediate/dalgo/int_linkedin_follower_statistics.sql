{{ config(
  materialized='table',
  tags=["linkedin", "dalgo"]
) }}

SELECT
  "organizationalEntity" as organizational_entity,
  jsonb_array_elements("followerCountsByFunction"::jsonb) AS function_stats,
  jsonb_array_elements("followerCountsByIndustry"::jsonb) AS industry_stats,
  jsonb_array_elements("followerCountsBySeniority"::jsonb) AS seniority_stats,
  jsonb_array_elements("followerCountsByAssociationType"::jsonb) AS association_type_stats,
  jsonb_array_elements("followerCountsByStaffCountRange"::jsonb) AS staff_count_stats
FROM {{ source('staging_dalgo', 'follower_statistics') }}
