create or replace view STRIPE_LAST_PAYMENT_PER_CUSTOMER(
	ADDRESS_STATE,
	CREATED,
	BUSINESS_VAT_ID,
	TAX_INFO_TYPE,
	ADDRESS_LINE1,
	DISCOUNT_CUSTOMER_ID,
	ADDRESS_LINE2,
	SHIPPING_PHONE,
	MERCHANT_ID,
	DISCOUNT_COUPON_ID,
	SHIPPING_ADDRESS_CITY,
	DEFAULT_SOURCE_ID,
	SHIPPING_ADDRESS_STATE,
	INVOICE_SETTINGS_DEFAULT_PAYMENT_METHOD_ID,
	TAX_INFO_TAX_ID,
	NAME,
	SHIPPING_ADDRESS_POSTAL_CODE,
	ADDRESS_COUNTRY,
	EMAIL,
	PHONE,
	DESCRIPTION,
	DELINQUENT,
	DISCOUNT_END,
	ACCOUNT_BALANCE,
	SHIPPING_ADDRESS_LINE1,
	ID,
	ADDRESS_POSTAL_CODE,
	DISCOUNT_START,
	ADDRESS_CITY,
	BATCH_TIMESTAMP,
	DISCOUNT_SUBSCRIPTION,
	CURRENCY,
	DELETED,
	SHIPPING_ADDRESS_LINE2,
	SHIPPING_NAME,
	SHIPPING_ADDRESS_COUNTRY,
	LEGAL_ENTITY,
	CUSTOMER_SEGMENT,
	CRM_ID,
	STRIPE_CUSTOMER_ID,
	STRIPE_TO_HUBSPOT_KEY,
	LATEST_PAYMENT_DATE
) as (
     


WITH last_payment_by_customer AS (
    SELECT
        customer_id,
        Max(created)::date AS latest_payment_date
    FROM BILLING.PUBLISHED_PROD.STRIPE_PAYMENT_INTENTS 
    WHERE status = 'succeeded'
    GROUP BY 1
),

customer_metadata AS (
    SELECT
        customer_id,
        value,
        key
    FROM BILLING.PUBLISHED_PROD.STRIPE_CUSTOMERS_METADATA 
    WHERE key = 'CRMID'
    QUALIFY Row_number() OVER(PARTITION BY customer_id ORDER BY 1) = 1
)

SELECT
    cust.*,
    meta.value AS stripe_to_hubspot_key,
    last_payment.latest_payment_date
FROM  BILLING.PUBLISHED_PROD.STRIPE_CUSTOMERS  AS cust
LEFT JOIN last_payment_by_customer AS last_payment
    ON cust.id = last_payment.customer_id
LEFT JOIN customer_metadata AS meta
    ON cust.id = meta.customer_id
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_last_payment_per_customer", "node_alias": "stripe_last_payment_per_customer", "node_package_name": "stripe", "node_original_file_path": "models/stripe_last_payment_per_customer.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_last_payment_per_customer", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": [], "materialized": "view"} */;