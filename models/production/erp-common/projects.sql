{{ config(
    tags=['projects1']
) }}

select * from {{ ref('int_projects') }}