{{ config(
    tags=['opportunities']
) }}

select
    o.name,
    o.opportunity_type,
    o.status,
    o.creation::date as creation_date,
    o.creation as creation_timestamp,
    ptt.product_type as product_type

from {{ source('erp_next','tabOpportunity') }} o
left join {{ source('erp_next','tabProduct_Type_Table') }} ptt
    on ptt.parent = o.name

where o.creation is not null
