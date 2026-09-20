--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
SELECT
CAST("Date" AS character varying) AS "Date",
CAST("Name" AS character varying) AS "Name",
CAST("Time" AS character varying) AS "Time",
CAST("Amount" AS numeric) AS "Amount",
CAST("Minutes" AS numeric) AS "Minutes",
CAST("Sr__No_" AS character varying) AS "Sr__No_",
CAST("Time_taken" AS character varying) AS "Time_taken",
CAST("Payment_Time" AS character varying) AS "Payment_Time",
CAST("Payment_Status" AS character varying) AS "Payment_Status",
CAST("Read_Date___Time" AS character varying) AS "Read_Date___Time",
CAST("_airbyte_raw_id" AS character varying) AS "_airbyte_raw_id",
CAST("_airbyte_extracted_at" AS timestamp with time zone) AS "_airbyte_extracted_at",
CAST("_airbyte_meta" AS jsonb) AS "_airbyte_meta"
FROM {{source('staging', 'trust_activity_raw')}}
)
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1