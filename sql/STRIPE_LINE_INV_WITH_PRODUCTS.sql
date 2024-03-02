create or replace view STRIPE_LINE_INV_WITH_PRODUCTS(
	INVOICE_ID,
	LINE_ID,
	QTY,
	PRODUCT_NAME,
	DESCRIPTION,
	EACH_LINE_ITEM,
	LINE_ITEMS_AMOUNT
) as (
     

SELECT DISTINCT
    inv_line_items.invoice_id,
    inv_line_items.id AS line_id,
    inv_line_items.quantity AS qty,
    prod.name AS product_name,
    inv_line_items.description,
    Round(inv_line_items.amount/100,2) AS each_line_item,
    each_line_item * qty AS line_items_amount
--FROM BILLING.PUBLISHED_PROD.STRIPE_INVOICE_LINE_ITEMS inv_line_items
FROM BILLING.PUBLISHED_PROD.STRIPE_INVOICE_LINE_ITEMS AS inv_line_items
--JOIN PURCHASING.PUBLISHED_PROD.STRIPE_PRICES price
INNER JOIN  PURCHASING.PUBLISHED_PROD.STRIPE_PRICES AS price
    ON inv_line_items.price_id = price.id
--JOIN PURCHASING.PUBLISHED_PROD.STRIPE_PRODUCTS prod
INNER JOIN  PURCHASING.PUBLISHED_PROD.STRIPE_PRODUCTS AS prod
    ON price.product_id = prod.id
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_line_inv_with_products", "node_alias": "stripe_line_inv_with_products", "node_package_name": "stripe", "node_original_file_path": "models/stripe_line_inv_with_products.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_line_inv_with_products", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": [], "materialized": "view"} */;