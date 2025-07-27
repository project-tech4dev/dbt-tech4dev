{{ config(
    tags = ['projects']
) }}

select * from {{ref('int_sales_invoice')}}