{{ config(
    tags = ['projects']
) }}

select *, EXTRACT(DAY FROM (now() - due_date))::int as invoice_ageing from {{ref('int_sales_invoice')}}
