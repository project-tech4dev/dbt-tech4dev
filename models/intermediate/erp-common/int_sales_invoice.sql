{{ config(
    tags = ['projects']
) }}

select name,customer,status, due_date, outstanding_amount from {{ source('erp_next', 'sales_invoice') }}
