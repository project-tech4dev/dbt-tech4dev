{{ config(
    tags=['projects']
) }}

select
    name,
    project_name as project,
    status,
    expected_start_date,
    expected_end_date
from {{ source('erp_next','tabProject') }}