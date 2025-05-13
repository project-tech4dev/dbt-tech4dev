{{ config(
    tags=['projects']
) }}

select * from {{ ref('stg_projects') }}