with monthly_allocation as (
    select
        month_start_date,
        month_end_date,
        department,
        employee_name,
        total_hours as utilized_hours, -- total_hours is the utilized hours
        balance_hours,
        total_capacity,
        -- Calculate utilization percentage based on total capacity
        round((total_hours / nullif(total_capacity, 0)) * 100, 2) as utilization_percentage
    from {{ ref('stg_resource_time_allocation') }}
),

department_summary as (
    select
        month_start_date,
        department,
        count(distinct employee_name) as employee_count,
        sum(utilized_hours) as total_utilized_hours,
        sum(balance_hours) as total_balance_hours,
        sum(total_capacity) as total_capacity,
        round(avg(utilization_percentage), 2) as avg_utilization_percentage
    from monthly_allocation
    group by month_start_date, department
)

select
    ma.*,
    ds.employee_count as dept_employee_count,
    ds.total_utilized_hours as dept_utilized_hours,
    ds.total_balance_hours as dept_balance_hours,
    ds.total_capacity as dept_total_capacity,
    ds.avg_utilization_percentage as dept_avg_utilization
from monthly_allocation ma
left join department_summary ds 
    on ma.month_start_date = ds.month_start_date 
    and ma.department = ds.department
order by 
    ma.month_start_date desc,
    ma.department,
    ma.employee_name