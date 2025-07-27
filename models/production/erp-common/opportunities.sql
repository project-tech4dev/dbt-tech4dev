{{ config(
    tags=['opportunities']
) }}

select * 
from {{ ref('int_opportunities') }}
