{{ config(
    tags = ['projects']
) }}

select product_type, name,customer,status, due_date, base_grand_total as outstanding_amount from {{ source('erp_next', 'sales_invoice') }}
