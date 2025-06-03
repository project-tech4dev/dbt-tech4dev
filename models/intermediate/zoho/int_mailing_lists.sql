{{ config(
  materialized='table',
  tags=["campaigns", "zoho"]
) }}

SELECT
    sno,
    date,
    zuid as zoho_id,
    owner,
    issmart as is_smart,
    listdgs as list_dgs,
    listkey as list_key,
    sentcnt as sent_count,
    editable,
    listname as list_name,
    deletable,
    is_public,
    listunino as list_unique_id_no,
    lockstatus as lock_status,
    servicetype as service_type,

    -- Raw timestamp fields
    created_time,
    created_time_gmt,
    updated_time_gmt,

    -- Converted to timestamp
    TO_TIMESTAMP(cast(created_time as bigint) / 1000) as created_time_ts,
    TO_TIMESTAMP(cast(created_time_gmt as bigint) / 1000) as created_time_gmt_ts,
    TO_TIMESTAMP(cast(updated_time_gmt as bigint) / 1000) as updated_time_gmt_ts,

    noofcontacts as no_of_contacts,
    noofunsubcnt as no_of_subscribers_cnt,
    noofbouncecnt as no_of_bounce_cnt,
    list_created_date,
    list_created_time,
    list_campaigns_count

FROM {{ source('staging_zoho', 'mailing_lists') }}