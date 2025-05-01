{{ config(
    tags=['tasks']
) }}

select
    name,
    subject,
    status,
    project,
    parent_task
from {{ source('erp_next','tabTask') }}