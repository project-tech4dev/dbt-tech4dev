{{ config(
    tags = ['projects']
) }}

select *, (now() - due_date)::int as invoice_ageing from {{ref('int_sales_invoice')}}