{{ config(
    tags=['projects']
) }}

select
    name,
    project_name,
    status,
    to_char(expected_start_date, 'DD-MM-YYYY') as expected_start_date,
    to_char(expected_end_date, 'DD-MM-YYYY') as expected_end_date

from {{ source('erp_next','tabProject') }}
