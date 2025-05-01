{{ config(
    tags=['projects']
) }}

select
    name,
    project_name
from {{ source('erp_next','tabProject') }}