{{ config(
    tags=['projects']
) }}

select * from {{ ref('int_resource_allocation') }}