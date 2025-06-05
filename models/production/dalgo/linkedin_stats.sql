{{ config(
  materialized='table',
  tags=['linkedin', 'dalgo']
) }}

WITH
orgs AS (
  SELECT * FROM {{ ref('int_linkedin_org_lookup') }}
),
shares AS (
  SELECT
    organizational_entity AS urn,
    CAST(like_count AS INTEGER) AS like_count,
    CAST(click_count AS INTEGER) AS click_count,
    CAST(engagement AS FLOAT) AS engagement,
    CAST(share_count AS INTEGER) AS share_count,
    CAST(comment_count AS INTEGER) AS comment_count,
    CAST(impression_count AS INTEGER) AS impression_count,
    CAST(share_mentions_count AS INTEGER) AS share_mentions_count,
    CAST(comment_mentions_count AS INTEGER) AS comment_mentions_count,
    CAST(unique_impressions_count AS INTEGER) AS unique_impressions_count
  FROM {{ ref('int_linkedin_share_statistics') }}
),
follower_count AS (
  SELECT total_followers FROM {{ ref('int_linkedin_follower_count') }}
  LIMIT 1
)

SELECT
  o.id,
  o.org_name,
  o.logo_url,
  o.website_url,
  o.localized_name,
  o.vanity_name,
  o.organization_type,
  o.primary_organization_type,
  o.staff_count_range,
  o.description,

  s.like_count,
  s.click_count,
  s.engagement,
  s.share_count,
  s.comment_count,
  s.impression_count,
  s.unique_impressions_count,
  s.share_mentions_count,
  s.comment_mentions_count,

  f.total_followers

FROM orgs o
LEFT JOIN shares s ON o.urn = s.urn
CROSS JOIN follower_count f