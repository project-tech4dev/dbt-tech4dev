{{ config(
    tags=['projects']
) }}

select *,
       daterange(to_date(expected_start_date, 'DD-MM-YYYY'), to_date(expected_end_date, 'DD-MM-YYYY'), '[]') as project_duration
 from {{ ref('int_projects') }}