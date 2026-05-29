{{ config(
    materialized='table',
    tags=['bizdev']
) }}

with source as (
    select * from {{ source('bizdev_automation', 'NGO_Research') }}
),

unpivoted as (
    select "NGO_Name" as ngo_name, 'Andaman & Nicobar Islands' as state from source where "_State__Andaman___Nicobar_Islands" = 'TRUE'
    union all
    select "NGO_Name", 'Andhra Pradesh'        from source where "_State__Andhra_Pradesh"       = 'TRUE'
    union all
    select "NGO_Name", 'Arunachal Pradesh'     from source where "_State__Arunachal_Pradesh"    = 'TRUE'
    union all
    select "NGO_Name", 'Assam'                 from source where "_State__Assam"                = 'TRUE'
    union all
    select "NGO_Name", 'Bihar'                 from source where "_State__Bihar"                = 'TRUE'
    union all
    select "NGO_Name", 'Chandigarh'            from source where "_State__Chandigarh"           = 'TRUE'
    union all
    select "NGO_Name", 'Chhattisgarh'          from source where "_State__Chhattisgarh"         = 'TRUE'
    union all
    select "NGO_Name", 'Dadra & Nagar Haveli'  from source where "_State__Dadra___Nagar_Haveli" = 'TRUE'
    union all
    select "NGO_Name", 'Daman & Diu'           from source where "_State__Daman___Diu"          = 'TRUE'
    union all
    select "NGO_Name", 'Delhi'                 from source where "_State__Delhi"                = 'TRUE'
    union all
    select "NGO_Name", 'Goa'                   from source where "_State__Goa"                  = 'TRUE'
    union all
    select "NGO_Name", 'Gujarat'               from source where "_State__Gujarat"              = 'TRUE'
    union all
    select "NGO_Name", 'Haryana'               from source where "_State__Haryana"              = 'TRUE'
    union all
    select "NGO_Name", 'Himachal Pradesh'      from source where "_State__Himachal_Pradesh"     = 'TRUE'
    union all
    select "NGO_Name", 'Jammu & Kashmir'       from source where "_State__Jammu___Kashmir"      = 'TRUE'
    union all
    select "NGO_Name", 'Jharkhand'             from source where "_State__Jharkhand"            = 'TRUE'
    union all
    select "NGO_Name", 'Karnataka'             from source where "_State__Karnataka"            = 'TRUE'
    union all
    select "NGO_Name", 'Kerala'                from source where "_State__Kerala"               = 'TRUE'
    union all
    select "NGO_Name", 'Lakshadweep'           from source where "_State__Lakshadweep"          = 'TRUE'
    union all
    select "NGO_Name", 'Madhya Pradesh'        from source where "_State__Madhya_Pradesh"       = 'TRUE'
    union all
    select "NGO_Name", 'Maharashtra'           from source where "_State__Maharashtra"          = 'TRUE'
    union all
    select "NGO_Name", 'Manipur'               from source where "_State__Manipur"              = 'TRUE'
    union all
    select "NGO_Name", 'Meghalaya'             from source where "_State__Meghalaya"            = 'TRUE'
    union all
    select "NGO_Name", 'Mizoram'               from source where "_State__Mizoram"              = 'TRUE'
    union all
    select "NGO_Name", 'Nagaland'              from source where "_State__Nagaland"             = 'TRUE'
    union all
    select "NGO_Name", 'Odisha'                from source where "_State__Odisha"               = 'TRUE'
    union all
    select "NGO_Name", 'Puducherry'            from source where "_State__Puducherry"           = 'TRUE'
    union all
    select "NGO_Name", 'Punjab'                from source where "_State__Punjab"               = 'TRUE'
    union all
    select "NGO_Name", 'Rajasthan'             from source where "_State__Rajasthan"            = 'TRUE'
    union all
    select "NGO_Name", 'Sikkim'                from source where "_State__Sikkim"               = 'TRUE'
    union all
    select "NGO_Name", 'Tamil Nadu'            from source where "_State__Tamil_Nadu"           = 'TRUE'
    union all
    select "NGO_Name", 'Telangana'             from source where "_State__Telangana"            = 'TRUE'
    union all
    select "NGO_Name", 'Tripura'               from source where "_State__Tripura"              = 'TRUE'
    union all
    select "NGO_Name", 'Uttar Pradesh'         from source where "_State__Uttar_Pradesh"        = 'TRUE'
    union all
    select "NGO_Name", 'Uttarakhand'           from source where "_State__Uttarakhand"          = 'TRUE'
    union all
    select "NGO_Name", 'West Bengal'           from source where "_State__West_Bengal"          = 'TRUE'
)

select
    ngo_name,
    state
from unpivoted
