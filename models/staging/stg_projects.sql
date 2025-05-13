{{ config(
    tags=['projects']
) }}

select
    name,
    project_name,
    status,
    expected_start_date,
    expected_end_date
from {{ source('erp_next','tabProject') }}