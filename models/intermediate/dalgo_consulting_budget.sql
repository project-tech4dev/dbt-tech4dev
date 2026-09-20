--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
SELECT
CAST("Client" AS character varying) AS "Client",
CAST("Status" AS character varying) AS "Status",
CAST("Quarter" AS character varying) AS "Quarter",
CAST("Comments" AS character varying) AS "Comments",
CAST("Month_Year" AS date) AS "Month_Year",
CAST("Revenue_Earnt" AS numeric) AS "Revenue_Earnt",
CAST("Consulting_Type" AS character varying) AS "Consulting_Type",
CAST("Revenue_Estimate__Revised_" AS numeric) AS "Revenue_Estimate__Revised_",
CAST("_airbyte_raw_id" AS character varying) AS "_airbyte_raw_id",
CAST("_airbyte_extracted_at" AS timestamp with time zone) AS "_airbyte_extracted_at",
CAST("_airbyte_meta" AS jsonb) AS "_airbyte_meta"
FROM {{source('staging', 'Sheet2')}}
)
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1