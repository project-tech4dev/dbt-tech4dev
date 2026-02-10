--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
{{ dbt_utils.union_relations(relations=[source('staging_dalgo', 'consulting_time_tracker_anusha'),source('staging_dalgo', 'consulting_time_tracker_pratiksha'),source('staging_dalgo', 'consulting_time_tracker_ritabrata'),source('staging_dalgo', 'consulting_time_tracker_siddhant')] , include=['_airbyte_meta','end_date','logged_on_erp_','consultant','billing_status','attendees','description','num_charts_made','dashboard','project_program','start_date','category','_airbyte_raw_id','json_blob','hours_spent','ngo','_airbyte_extracted_at'] , source_column_name=None)}})
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1