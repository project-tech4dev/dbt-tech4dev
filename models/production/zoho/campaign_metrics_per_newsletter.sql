{{ config(
  materialized='table',
  tags=["campaigns", "zoho"]
) }}

with campaigns as (
    select * from {{ ref('int_campaigns') }}
),
reports as (
    select * from {{ ref('int_campaign_reports') }}
)

select
    c.campaign_name,
    c.subject,
    c.org,
    c.sent_time,
    r.emails_sent,
    r.delivered,
    r.opens,
    r.clicks,
    r.open_rate,
    r.delivery_rate,
    r.clickthrough_rate
from campaigns c
left join reports r on c.campaign_key = r.campaign_id