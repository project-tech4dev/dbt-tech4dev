{{ config(
    tags=['projects']
) }}

select
    name,
    lft as predecessor,
    rgt as successor,
    subject,
    status,
    project,
    parent_task
from {{ source('erp_next','tabTask') }}