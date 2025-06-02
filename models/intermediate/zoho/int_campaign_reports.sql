{{ config(
  materialized='table',
  tags=["campaigns", "zoho"]
) }}

SELECT
    campaign_id,
    cast(emails_sent_count as integer) as emails_sent,
    cast(opens_count as integer) as opens,
    cast(delivered_count as integer) as delivered,
    cast(unique_clicks_count as integer) as clicks,
    cast(open_percent as float) as open_rate,
    cast(delivered_percent as float) as delivery_rate,
    cast(unique_clicked_percent as float) as clickthrough_rate
FROM {{ source('staging_zoho', 'campaign_reports') }}