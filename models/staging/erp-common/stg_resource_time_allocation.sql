with source as (
    select
        idx,
        name,
        employee,
        {{ validate_date('modified') }} as modified_date,
        _comments,
        _liked_by,
        docstatus,
        _user_tags,
        department,
        modified_by,
        total_hours::numeric as total_hours,
        amended_from,
        balance_hours::numeric as balance_hours,
        employee_name,
        total_capacity::numeric as total_capacity,
        {{ validate_date('month_start_date') }} as month_start_date
    from {{ source('erp_next', 'tabResource_Time_Allocation') }}
)

select 
    idx,
    name as allocation_id,
    employee,
    employee_name,
    department,
    total_hours,
    balance_hours,
    total_capacity,
    month_start_date,
    modified_date,
    -- Extract month and year from month_start_date for grouping purposes
    EXTRACT(MONTH FROM month_start_date) as month_number,
    EXTRACT(YEAR FROM month_start_date) as year_number,
    -- First day of the next month for date range queries
    (month_start_date + INTERVAL '1 MONTH')::DATE as month_end_date
from source
where month_start_date is not null  -- Ensure we have valid dates