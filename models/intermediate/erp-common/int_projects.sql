{{ config(
    tags=['projects']
) }}

select
    p.name,
    p.project_name,
    p.custom_product as product_type,
    p.status,
    e.employee_name as owner,
    p.expected_start_date,
    p.expected_end_date,
    to_char(p.expected_start_date, 'DD-MM-YYYY') as expected_start_date_text,
    to_char(p.expected_end_date, 'DD-MM-YYYY') as expected_end_date_text,
    string_agg(f.focus_areas, ', ') as focus_areas

from {{ source('erp_next','tabProject') }} p
left join {{ source('erp_next','tabEmployee') }} e
    on p.custom_project_owner = e.name
left join {{ source('erp_next','tabFocus_Area_Table') }} f
    on f.parent = p.name

group by
    p.name,
    p.project_name,
    p.custom_product,
    p.status,
    e.employee_name,
    p.expected_start_date,
    p.expected_end_date