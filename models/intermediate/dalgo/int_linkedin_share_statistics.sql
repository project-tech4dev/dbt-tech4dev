{{ config(
  materialized='table',
  tags=["linkedin", "dalgo"]
) }}

SELECT
  "organizationalEntity" as organizational_entity,
  "totalShareStatistics"::jsonb ->> 'likeCount' AS like_count,
  "totalShareStatistics"::jsonb ->> 'clickCount' AS click_count,
  "totalShareStatistics"::jsonb ->> 'engagement' AS engagement,
  "totalShareStatistics"::jsonb ->> 'shareCount' AS share_count,
  "totalShareStatistics"::jsonb ->> 'commentCount' AS comment_count,
  "totalShareStatistics"::jsonb ->> 'impressionCount' AS impression_count,
  "totalShareStatistics"::jsonb ->> 'shareMentionsCount' AS share_mentions_count,
  "totalShareStatistics"::jsonb ->> 'commentMentionsCount' AS comment_mentions_count,
  "totalShareStatistics"::jsonb ->> 'uniqueImpressionsCount' AS unique_impressions_count
FROM {{ source('staging_dalgo', 'share_statistics') }}