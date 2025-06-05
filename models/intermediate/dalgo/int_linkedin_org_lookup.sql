{{ config(
  materialized='table',
  tags=["linkedin", "dalgo"]
) }}

SELECT
  id,
  "_URN" AS urn,
  name::jsonb -> 'localized' ->> 'en_US' AS org_name,
  "logoV2"::jsonb ->> 'original' AS logo_url,
  website::jsonb -> 'localized' ->> 'en_US' AS website_url,
  description::jsonb -> 'localized' ->> 'en_US' AS description,
  "localizedName" as localized_name,
  "staffCountRange" as staff_count_range,
  "vanityName" as vanity_name,
  "organizationType" as organization_type,
  "primaryOrganizationType" as primary_organization_type
FROM {{ source('staging_dalgo', 'organization_lookup') }}