create or replace view STRIPE_CUSTOMERS_METADATA_PIVOTED(
	MERCHANT_ID,
	STRIPE_CUSTOMER_ID,
	BATCH_TIMESTAMP,
	LEGAL_ENTITY,
	CRM_CUSTOMER_ID,
	CUSTOMER_SEGMENT,
	MYOB_CUSTOMER_ID
) as (
     

SELECT *
FROM (
    SELECT
        MERCHANT_ID,
        CUSTOMER_ID,
        BATCH_TIMESTAMP,
        LEGAL_ENTITY,
        VALUE,
        KEY
    FROM BILLING.PUBLISHED_PROD.STRIPE_CUSTOMERS_METADATA
) PIVOT(Max(VALUE) FOR KEY IN ('CRMID', 'Customer Segment', 'Customer ID'))
    AS P (
        MERCHANT_ID,
        STRIPE_CUSTOMER_ID,
        BATCH_TIMESTAMP,
        LEGAL_ENTITY,
        CRM_CUSTOMER_ID,
        CUSTOMER_SEGMENT,
        MYOB_CUSTOMER_ID
    )
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_customers_metadata_pivoted", "node_alias": "stripe_customers_metadata_pivoted", "node_package_name": "stripe", "node_original_file_path": "models/stripe_customers_metadata_pivoted.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_customers_metadata_pivoted", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": [], "materialized": "view"} */;