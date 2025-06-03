{{ config(
  materialized='table',
  tags=["campaigns", "zoho"]
) }}

with campaigns as (
    select campaign_key, org from {{ ref('int_campaigns') }}
),
recipients as (
    select contactid, sent_time from {{ ref('int_campaign_recipients') }}
),
combined as (
    select
        c.org,
        r.contactid,
        min(r.sent_time) as first_sent,
        max(r.sent_time) as last_sent,
        count(*) as num_campaigns
    from recipients r
    join campaigns c on r.sent_time in (
        select sent_time from {{ ref('int_campaigns') }}
    )
    group by c.org, r.contactid
)

select
    org,
    count(distinct contactid) as total_contacts_reached,
    sum(num_campaigns) as total_messages_sent
from combined
group by org