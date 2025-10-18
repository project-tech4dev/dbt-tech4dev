{{ config(
    tags = ['projects']
) }}

select *, datediff(now(),due_date) as invoice_ageing from {{ref('int_sales_invoice')}}