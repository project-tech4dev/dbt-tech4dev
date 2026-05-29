{{ config(
    materialized='table',
    tags=['bizdev']
) }}

with source as (
    select * from {{ source('bizdev_automation', 'NGO_Research') }}
),

final as (
    select
        "NGO_Name"              as ngo_name,
        "HQ_City"               as hq_city,
        "Overview"              as overview,
        "Programs"              as programs,
        nullif("Budget__", '')::numeric as budget,
        "Org_Website"           as org_website,
        "Profile_URL"           as profile_url,

        "Leader_1_Name"         as leader_1_name,
        "Leader_1_Role"         as leader_1_role,
        "Leader_1_LinkedIn"     as leader_1_linkedin,
        "Leader_2_Name"         as leader_2_name,
        "Leader_2_Role"         as leader_2_role,
        "Leader_2_LinkedIn"     as leader_2_linkedin,
        "Leader_3_Name"         as leader_3_name,
        "Leader_3_Role"         as leader_3_role,
        "Leader_3_LinkedIn"     as leader_3_linkedin

    from source
)

select * from final
