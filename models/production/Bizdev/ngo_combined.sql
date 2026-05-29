{{ config(
    materialized='table',
    tags=['bizdev']
) }}

with source as (
    select * from {{ source('bizdev_automation', 'Combined') }}
),

final as (
    select
        "FY_Year"              as fy_year,
        "NGO_Name"             as ngo_name,
        "HQ_Location"          as hq_location,
        "Profile_URL"          as profile_url,
        "Total_Revenue__FY___"::numeric as total_revenue,

        case
            when "Total_Revenue__FY___"::numeric > 200000000   then '>20Cr'
            when "Total_Revenue__FY___"::numeric >= 100000000  then '10-20Cr'
            when "Total_Revenue__FY___"::numeric >= 70000000   then '7-10Cr'
            when "Total_Revenue__FY___"::numeric >= 40000000   then '4-7Cr'
            when "Total_Revenue__FY___"::numeric >= 10000000   then '1-4Cr'
            when "Total_Revenue__FY___"::numeric > 0   then'<1Cr'
            else 'N/A'
        end as revenue_category

    from source
)

select * from final
