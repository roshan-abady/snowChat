create or replace view STRIPE_CREDITNOTES_REFUNDS(
	MERCHANT_ID,
	CURRENCY,
	CUSTOMER_ID,
	CUSTOMER_NAME,
	MYOB_CUSTOMER_ID,
	CUSTOMER_CRM_ID,
	LEGAL_ENTITY,
	DELINQUENT_FLAG,
	CUSTOMER_SEGMENT,
	CUSTOMERS_CREATED_LOCAL,
	CUSTOMER_CURRENCY,
	HUBSPOT_CUSTOMER_GST_STATUS,
	EFFECTIVE_AT,
	ID,
	INVOICE_ID,
	INVOICE_NUMBER,
	MEMO,
	NUMBER,
	REASON,
	REFUND_ID,
	HAS_REFUND_FLAG,
	STATUS,
	TYPE,
	VOIDED_AT,
	CREATEDAUSDATE,
	DESCRIPTION,
	QUANTITY,
	FAILURE_REASON,
	REFUND_REASON,
	REFUND_STATUS,
	INVOICE_BILLING_REASON,
	INVOICE_COLLECTION_METHOD,
	INVOICE_STATUS,
	CN_AMOUNT,
	TAX_AMOUNT,
	NET_CN_AMOUNT,
	CREATEDAUSDATETIME,
	DISCOUNT_AMOUNT,
	UNIT_AMOUNT,
	REFUND_AMOUNT
) as (
    

/*
    Purpose:     Creates a  view of stripe credit notes with refund data
    Date:        202402
    Description: Creates a  view of stripe credit notes with refund data
  */


SELECT
    a.merchant_id,
    a.currency,
    a.customer_id,
    customer.customer_name,
    customer.myob_customer_id,
    customer.customer_crm_id,
    customer.legal_entity,
    customer.delinquent_flag,
    customer.customer_segment,
    customer.customers_created_local,
    customer.currency AS customer_currency,
    customer.hubspot_customer_gst_status,
    a.effective_at,
    a.id,
    a.invoice_id,
    d.number AS invoice_number,
    a.memo,
    a.number,
    a.reason,
    a.refund_id,
    CASE WHEN a.refund_id IS null THEN 0 ELSE 1 END AS has_refund_flag,
    a.status,
    a.type,
    a.voided_at,
    Convert_timezone('UTC','Australia/Melbourne',a.created)::date AS createdausdate,
    b.description,
    b.quantity,
    c.failure_reason,
    c.reason AS refund_reason,
    c.status AS refund_status,
    d.billing_reason AS invoice_billing_reason,
    d.collection_method AS invoice_collection_method,
    d.status AS invoice_status,
    Div0(a.amount, 100) AS cn_amount,
    Div0(e.amount, 100) AS tax_amount,
    Div0(a.amount, 100) - Div0(e.amount, 100) AS net_cn_amount,
    Convert_timezone('UTC','Australia/Melbourne',a.created) AS createdausdatetime,
    Div0(b.discount_amount, 100) AS discount_amount,
    Div0(b.unit_amount, 100) AS unit_amount,
    Div0(c.amount, 100) AS refund_amount
FROM BILLING.PUBLISHED_PROD.STRIPE_CREDIT_NOTES AS a
LEFT JOIN BILLING.PUBLISHED_PROD.STRIPE_CREDIT_NOTE_LINE_ITEMS AS b ON a.id = b.credit_note_id
LEFT JOIN BILLING.PUBLISHED_PROD.STRIPE_REFUNDS AS c ON a.refund_id = c.id
LEFT JOIN BILLING.PUBLISHED_PROD.STRIPE_INVOICES AS d ON a.invoice_id = d.id
LEFT JOIN
    BILLING.PUBLISHED_PROD.STRIPE_CREDIT_NOTE_LINE_ITEM_TAX_AMOUNTS AS e
    ON b.id = e.credit_note_line_item_id
LEFT JOIN  OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_customers_with_metadata AS customer ON a.customer_id = customer.id
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_creditnotes_refunds", "node_alias": "stripe_creditnotes_refunds", "node_package_name": "stripe", "node_original_file_path": "models/stripe_creditnotes_refunds.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_creditnotes_refunds", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": ["stripe_customers_with_metadata"], "materialized": "view"} */;