{{ config(
    tags=['projects']
) }}

select *,
       daterange(expected_start_date, expected_end_date, '[]') as project_duration
 from {{ ref('int_projects') }}