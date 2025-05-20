{{ config(
    tags=['projects']
) }}

with source as (
    select
        idx,
        name,
        hours,
        month,
        product,
        project,
        project_name,
        activity,
        owner,
        {{ validate_date('modified') }} as modified_date,
        parent,
        docstatus,
        modified_by
    from {{ source('erp_next', 'resource_allocation') }}
-- skip all cancelled items
    where docstatus != 2 

)

select 
  s.*, 
  r.total_capacity, 
  r.month_start_date,
  r.employee_name 
from source as s
left join {{ ref('int_resource_time_allocation') }} as r
  on s.parent = r.allocation_id