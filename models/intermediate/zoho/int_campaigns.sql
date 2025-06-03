{{ config(
  materialized='table',
  tags=["campaigns", "zoho"]
) }}

SELECT
    campaign_key,
    campaign_name,
    subject,
    from_email,
    campaign_status,
    cast(sent_time as bigint) as sent_time_ms,
    to_timestamp(cast(sent_time as bigint)/1000) at time zone 'Asia/Kolkata' as sent_time,
    CASE
        when lower(from_email) like '%dalgo%' then 'Dalgo'
        when lower(from_email) like '%glific%' then 'Glific'
        when lower(from_email) like '%fcxo%' then 'FCXO'
        when lower(from_email) like '%t4d%'  then 'T4D'
        else 'Other'
    END as org
FROM {{ source('staging_zoho', 'recent_campaigns') }}
WHERE campaign_status = 'Sent'