{{ config(
  materialized='table',
  tags=["campaigns", "zoho"]
) }}

SELECT
    contactemailaddress,
    contactid,
    cast(sent_time as bigint) as sent_time_ms,
    to_timestamp(cast(sent_time as bigint)/1000) at time zone 'Asia/Kolkata' as sent_time,
    count(*) over (partition by contactid) as contact_campaigns
FROM {{ source('staging_zoho', 'campaign_recipients') }}