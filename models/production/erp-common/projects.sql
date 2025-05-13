{{ config(
    tags=['projects']
) }}

select * from {{ ref('int_projects') }}