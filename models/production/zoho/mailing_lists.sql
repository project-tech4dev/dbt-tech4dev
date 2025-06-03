{{ config(
  materialized='table',
  tags=["campaigns", "zoho"]
) }}

SELECT 
* 
FROM {{ ref('int_mailing_lists') }}