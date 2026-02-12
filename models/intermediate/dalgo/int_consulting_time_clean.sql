--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate', tags=["consulting", "dalgo"]) }}
WITH cte2 as (
SELECT
CAST("ngo" AS text) AS "ngo",
CAST("category" AS text) AS "category",
CAST("end_date" AS date) AS "end_date",
CAST("attendees" AS text) AS "attendees",
CAST("dashboard" AS text) AS "dashboard",
CAST("json_blob" AS jsonb) AS "json_blob",
CAST("consultant" AS text) AS "consultant",
CAST("start_date" AS date) AS "start_date",
CAST("description" AS text) AS "description",
CAST("hours_spent" AS numeric) AS "hours_spent",
CAST("billing_status" AS text) AS "billing_status",
CAST("logged_on_erp_" AS text) AS "logged_on_erp_",
CAST("num_charts_made" AS integer) AS "num_charts_made",
CAST("project_program" AS text) AS "project_program",
CAST("_airbyte_raw_id" AS character varying) AS "_airbyte_raw_id",
CAST("_airbyte_extracted_at" AS timestamp with time zone) AS "_airbyte_extracted_at",
CAST("_airbyte_meta" AS jsonb) AS "_airbyte_meta"
FROM {{ref('int_consulting_time_raw')}}
) , cte1 as (
SELECT "ngo", "category", "end_date", "attendees", "dashboard", "json_blob", "consultant", "start_date", "description", "hours_spent", "billing_status", "logged_on_erp_", "num_charts_made", "project_program"
FROM cte2
)
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1