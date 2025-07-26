{{ config(
    tags = ['financials']
) }}

with unpivoted as (

    {{ dbt_utils.unpivot(
        relation=source('erp_next', 'budget_actuals'),
        cast_to='text',
        exclude=['item', 'product_type','_airbyte_raw_id','_airbyte_extracted_at','_airbyte_meta' ],
        field_name='raw_financial_label',
        value_name='raw_amount'
    ) }}

),

final as (

    select
        item,
        product_type,
        -- Transform 'q1_fy22' to 'Q1 FY22'
        upper(replace(raw_financial_label, '_', ' ')) as financial_label,
        cast(replace(raw_amount, ',', '') as numeric) as amount
    from unpivoted

)

select * from final
