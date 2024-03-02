create or replace view STRIPE_DATE_DIMENSION(
	REPORTING_OPEN_DATE,
	REPORTING_YEAR,
	REPORTING_MONTH,
	REPORTING_CLOSE_DATE,
	MONTH_ID
) as (
    

SELECT DISTINCT
    DATE AS REPORTING_OPEN_DATE,
    YEAR AS REPORTING_YEAR,
    MONTH AS REPORTING_MONTH,
    LASTDAYOFMONTH AS REPORTING_CLOSE_DATE,
    To_char(REPORTING_OPEN_DATE, 'YYYYMM') AS MONTH_ID
FROM purchasing.transformed_prod.DATE_DIMENSION
WHERE
    YEAR >= 2024
    AND DAYOFMONTH = 1
    AND MONTH_ID <= (
        To_char(Getdate(), 'YYYYMM')
    )
ORDER BY
    MONTH_ID NULLS FIRST
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_date_dimension", "node_alias": "stripe_date_dimension", "node_package_name": "stripe", "node_original_file_path": "models/stripe_date_dimension.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_date_dimension", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": [], "materialized": "view"} */;