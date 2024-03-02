create or replace view STRIPE_SUBSCRIPTION_TO_INVOICE(
	SUBSCRIPTION_ID,
	SUBSCRIPTION_ITEM_ID,
	SUBSCRIPTION_QUANTITY,
	SUBSCRIPTION_PLAN_ID,
	PLAN_INTERVAL,
	PLAN_PRODUCT_ID,
	PRICE_UNIT_AMOUNT,
	PRICE_RECURRING_INTERVAL_COUNT,
	INVOICE_NUMBER,
	INVOICE_ITEM_PERIOD_START_LOCAL,
	INVOICE_ITEM_PERIOD_END_LOCAL,
	INVOICE_CURRENCY,
	PAYROLL_SUBS,
	INVOICE_ITEM_GROSS_AMOUNT_EX_GST,
	INVOICE_LINE_ITEM_QUANTITY,
	MATCH_AMOUNT
) as (
     

WITH invoice_by_period AS (
    SELECT
        invoice_number,
        subscription_item_id,
        invoice_item_period_start_local,
        invoice_item_period_end_local,
        CASE WHEN invoice_item_description LIKE'%Employee%' THEN 'Y' ELSE 'N' END AS payroll_subs,
        invoice_currency,
        Sum(invoice_item_gross_amount_ex_gst) AS invoice_item_gross_amount_ex_gst,
        Sum(invoice_line_item_quantity) AS invoice_line_item_quantity
    FROM OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_invoices_gst
    GROUP BY
        invoice_number,
        subscription_item_id,
        invoice_item_period_start_local,
        invoice_item_period_end_local,
        payroll_subs,
        invoice_currency
)

SELECT
    sub_item.subscription_id,
    sub_item.subscription_item_id,
    sub_item.subscription_quantity,
    sub_item.subscription_plan_id,
    sub_item.plan_interval,
    sub_item.plan_product_id,
    Coalesce(sub_item.price_unit_amount,0) AS price_unit_amount,
    sub_item.price_recurring_interval_count,
    inv_item.invoice_number,
    inv_item.invoice_item_period_start_local,
    inv_item.invoice_item_period_end_local,
    inv_item.invoice_currency,
    inv_item.payroll_subs,
    inv_item.invoice_item_gross_amount_ex_gst,
    inv_item.invoice_line_item_quantity,
    CASE
        WHEN
            sub_item.subscription_quantity * price_unit_amount = inv_item.invoice_item_gross_amount_ex_gst * inv_item.invoice_line_item_quantity
            THEN 1
        ELSE 0
    END AS match_amount
-- from billing.PUBLISHED_PROD.stripe_subscription_items AS sub_item
FROM OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_subscriptions AS sub_item
LEFT JOIN invoice_by_period AS inv_item
    ON sub_item.subscription_item_id=inv_item.subscription_item_id
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_subscription_to_invoice", "node_alias": "stripe_subscription_to_invoice", "node_package_name": "stripe", "node_original_file_path": "models/stripe_subscription_to_invoice.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_subscription_to_invoice", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": ["stripe_invoices_gst", "stripe_subscriptions"], "materialized": "view"} */;