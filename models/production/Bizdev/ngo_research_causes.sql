{{ config(
    materialized='table',
    tags=['bizdev']
) }}

with source as (
    select * from {{ source('bizdev_automation', 'NGO_Research') }}
),

unpivoted as (
    select "NGO_Name" as ngo_name, 'Child & Youth Development' as cause from source where "_Cause__Child___Youth_Development" = 'TRUE'
    union all
    select "NGO_Name", 'Disaster Management'   from source where "_Cause__Disaster_Management"   = 'TRUE'
    union all
    select "NGO_Name", 'Education'             from source where "_Cause__Education"             = 'TRUE'
    union all
    select "NGO_Name", 'Energy & Environment'  from source where "_Cause__Energy___Environment"  = 'TRUE'
    union all
    select "NGO_Name", 'Food & Nutrition'      from source where "_Cause__Food___Nutrition"      = 'TRUE'
    union all
    select "NGO_Name", 'Gender'                from source where "_Cause__Gender"                = 'TRUE'
    union all
    select "NGO_Name", 'Health'                from source where "_Cause__Health"               = 'TRUE'
    union all
    select "NGO_Name", 'Human Rights'          from source where "_Cause__Human_Rights"          = 'TRUE'
    union all
    select "NGO_Name", 'Livelihoods'           from source where "_Cause__Livelihoods"           = 'TRUE'
    union all
    select "NGO_Name", 'Skill Development'     from source where "_Cause__Skill_Development"     = 'TRUE'
)

select
    ngo_name,
    cause
from unpivoted
