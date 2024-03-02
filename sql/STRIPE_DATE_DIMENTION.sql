create or replace view STRIPE_DATE_DIMENTION(
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
    REPORTING_YEAR * 100 + REPORTING_MONTH AS MONTH_ID
FROM purchasing.transformed_prod.DATE_DIMENSION
WHERE
    YEAR >= 2024
    AND DAYOFMONTH = 1
    AND MONTH_ID <= (
        Year(Getdate()) * 100 + Month(Getdate())
    )
ORDER BY
    MONTH_ID NULLS FIRST
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "839d7eb8-258d-4be9-8cc4-f85fb5af0112", "node_name": "stripe_date_dimention", "node_alias": "stripe_date_dimention", "node_package_name": "stripe", "node_original_file_path": "models/stripe_date_dimention.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_date_dimention", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": [], "materialized": "view"} */;