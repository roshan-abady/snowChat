create or replace view STRIPE_SUBSCRIPTIONS(
	SUBSCRIPTION_ID,
	SUBSCRIPTION_ITEM_ID,
	CUSTOMER_NAME,
	MYOB_CUSTOMER_ID,
	CUSTOMER_CRM_ID,
	LEGAL_ENTITY,
	DELINQUENT_FLAG,
	CUSTOMER_SEGMENT,
	ACCOUNT_BALANCE,
	CUSTOMER_CREATED,
	CURRENCY,
	DEFAULT_SOURCE_ID,
	DELETED,
	DELINQUENT,
	CUSTOMER_ID,
	INVOICE_SETTINGS_DEFAULT_PAYMENT_METHOD_ID,
	ADDRESS_COUNTRY,
	HUBSPOT_COMPANY_ARCHIE_CLIENT_ID,
	PRODUCT_NAME,
	PRODUCT_TYPE,
	UNIT_LABEL,
	SUBSCRIPTION_BILLING,
	BILLING_CYCLE_ANCHOR,
	CANCELLATION_REASON,
	CANCELLATION_REASON_TEXT,
	SUBSCRIPTIONS_CREATED,
	CURRENT_PERIOD_END,
	CURRENT_PERIOD_START,
	SUBSCRIPTION_CUSTOMER_ID,
	DISCOUNT_COUPON_ID,
	DISCOUNT_CUSTOMER_ID,
	DISCOUNT_END,
	DISCOUNT_START,
	DISCOUNT_SUBSCRIPTION,
	SUBSCRIPTION_PLAN_ID,
	SUBSCRIPTION_PRICE_ID,
	SUBSCRIPTION_QUANTITY,
	SUBSCRIPTION_START_TIME,
	SUBSCRIPTION_START_DATE,
	SUBSCRIPTION_FIRST_INVOICE_DATE,
	SUBSCRIPTION_START_DATE_TIME_LOCAL,
	ENDED_AT,
	ENDED_AT_DATE_LOCAL,
	CANCELED_AT,
	CANCELED_AT_DATE_LOCAL,
	SUBSCRIPTION_STATUS,
	TAX_PERCENT,
	TRIAL_END,
	TRIAL_START,
	SUBSCRIPTION_ITEMS_CREATED,
	SUBSCRIPTION_ITEMS_CREATED_DATE_LOCAL,
	PLAN_AMOUNT,
	PLAN_CREATED,
	PLAN_INTERVAL,
	PLAN_INTERVAL_COUNT,
	PLAN_NICKNAME,
	PRICE_CREATED,
	PRICE_ID,
	PRICE_PRODUCT_ID,
	PRICE_RECURRING_INTERVAL,
	PRICE_RECURRING_INTERVAL_COUNT,
	PRICE_UNIT_AMOUNT,
	SUBSCRIPTION_ITEM_QUANTITY,
	PRICE_PRODUCT_NAME,
	PRICE_PRODUCT_POSTING_CLASS,
	PRICE_PRODUCT_POSTING_TYPE,
	PRICE_PRODUCT_CODE,
	PRICE_ITEM_CLASS,
	PRICE_PROTECTION_PERIODS,
	PRODUCT_MIN_QUANTITY,
	PRODUCT_MAX_QUANTITY,
	PRICE_PRODUCT_EXTERNAL_REFERENCE,
	PRICE_COMPANY_SIZE_BUCKET,
	PRICE_MYOB_ADVANCED_EDITION,
	PRICE_INVENTORY_ID,
	PRICE_EFFECTIVE_YEAR,
	PLAN_CURRENCY,
	PLAN_ID,
	INTERVAL_COUNT,
	PLAN_PRODUCT_ID,
	TRIAL_PERIOD_DAYS,
	PRICE_NICKNAME,
	RECURRING_INTERVAL,
	RECURRING_INTERVAL_COUNT,
	RECURRING_TRIAL_PERIOD_DAYS,
	UNIT_AMOUNT_DECIMAL,
	SUBSCRIPTION_ITEM_AMOUNT,
	HUBSPOT_STRIPE_SUB_ID_MATCHED_FLAG,
	HUBSPOT_COMPANY_ID,
	HUBSPOT_COMPANY_MYOB_CUSTOMER_ID,
	HUBSPOT_COMPANY_NAME,
	HUBSPOT_CUSTOMER_STRIPE_SUBSCRIPTION_ID,
	HUBSPOT_COMPANY_GST_STATUS,
	HUBSPOT_COMPANY_LIFECYCLESTAGE,
	HUBSPOT_STRIPE_SUB_ID_COMPANY_MATCH_FLAG,
	HUBSPOT_COMPANY_RECENT_DEAL_CLOSE_DATE,
	HUBSPOT_STRIPE_CRM_ID_COMPANY_MATCH_FLAG,
	HUBSPOT_STRIPE_MYOB_CUSTOMER_ID_COMPANY_MATCH_FLAG
) as (
    

/* identifying first invoice date of subscription using stripe_invoices_gst invoice_date_local */
WITH get_first_invoice_date AS (
    SELECT
        subscription_id,
        Min(invoice_date_local) AS subscription_first_invoice_date
    FROM OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_invoices_gst
    GROUP BY subscription_id
)

SELECT -- noqa: ST06
    subscriptions.id AS subscription_id ,
    subscription_items.id AS subscription_item_id,
    customers.customer_name,
    customers.myob_customer_id,
    customers.customer_crm_id,
    customers.legal_entity,
    customers.delinquent AS delinquent_flag,
    customers.customer_segment,
    customers.account_balance,
    customers.customer_created,
    customers.currency,
    customers.default_source_id,
    customers.deleted,
    customers.delinquent,
    customers.id AS customer_id,
    customers.invoice_settings_default_payment_method_id,
    customers.address_country,
    null AS hubspot_company_archie_client_id,
    products.name AS product_name,
    products.type AS product_type,
    products.unit_label,
    subscriptions.billing AS subscription_billing,
    subscriptions.billing_cycle_anchor ,
    --coalesce(hubspot_company.property_archie_client_id , hubspot_company1.property_archie_client_id ) as hubspot_company_archie_client_id,
    subscriptions.cancellation_reason,
    subscriptions.cancellation_reason_text,
    subscriptions.created AS subscriptions_created,
    subscriptions.current_period_end,
    subscriptions.current_period_start,
    subscriptions.customer_id AS subscription_customer_id,
    subscriptions.discount_coupon_id,
    subscriptions.discount_customer_id,
    subscriptions.discount_end,
    subscriptions.discount_start,
    subscriptions.discount_subscription,
    subscriptions.plan_id AS subscription_plan_id,
    subscriptions.price_id AS subscription_price_id,
    subscriptions.quantity AS subscription_quantity,
    subscriptions.start_time AS subscription_start_time,
    subscriptions.start_date AS subscription_start_date,
    get_first_invoice_date.subscription_first_invoice_date,
    CASE WHEN
            subscriptions.legal_entity = 'AU'
            THEN Convert_timezone('UTC','Australia/Melbourne', subscriptions.start_date::datetime)
        WHEN
            subscriptions.legal_entity = 'NZ'
            THEN Convert_timezone('UTC','Pacific/Auckland',  subscriptions.start_date::datetime)
        ELSE subscriptions.start_date::datetime
    END AS  subscription_start_date_time_local,
    subscriptions.ended_at,
    CASE WHEN
            subscriptions.legal_entity = 'AU'
            THEN Convert_timezone('UTC','Australia/Melbourne',  subscriptions.ended_at::datetime)
        WHEN
            subscriptions.legal_entity = 'NZ'
            THEN Convert_timezone('UTC','Pacific/Auckland',   subscriptions.ended_at::datetime)
        ELSE  subscriptions.ended_at::datetime
    END AS  ended_at_date_local,
    subscriptions.canceled_at,
    CASE WHEN
            subscriptions.legal_entity = 'AU'
            THEN Convert_timezone('UTC','Australia/Melbourne',  subscriptions.canceled_at::datetime)
        WHEN
            subscriptions.legal_entity = 'NZ'
            THEN Convert_timezone('UTC','Pacific/Auckland',   subscriptions.canceled_at::datetime)
        ELSE  subscriptions.canceled_at::datetime
    END AS  canceled_at_date_local,
    subscriptions.status AS subscription_status,
    subscriptions.tax_percent,
    subscriptions.trial_end,
    subscriptions.trial_start,
    subscription_items.created AS subscription_items_created ,
    CASE WHEN
            subscriptions.legal_entity = 'AU'
            THEN Convert_timezone('UTC','Australia/Melbourne',  subscription_items.created::datetime)
        WHEN
            subscriptions.legal_entity = 'NZ'
            THEN Convert_timezone('UTC','Pacific/Auckland',   subscription_items.created::datetime)
        ELSE  subscription_items.created:datetime
    END AS subscription_items_created_date_local,
    subscription_items.plan_amount,
    subscription_items.plan_created,
    subscription_items.plan_interval,
    subscription_items.plan_interval_count,
    subscription_items.plan_nickname,
    subscription_items.price_created,
    subscription_items.price_id,
    subscription_items.price_product_id,
    subscription_items.price_recurring_interval,
    subscription_items.price_recurring_interval_count,
    subscription_items.price_unit_amount,
    subscription_items.quantity AS subscription_item_quantity,
    --  subscription_items.subscription, duplicate of subscriptions.id
    stripe_prices_products.product_name AS price_product_name,
    stripe_prices_products.posting_class AS price_product_posting_class,
    stripe_prices_products.posting_type AS price_product_posting_type,
    stripe_prices_products.product_code AS price_product_code,
    stripe_prices_products.item_class AS price_item_class,
    stripe_prices_products.price_protection_periods,
    stripe_prices_products.min_quantity AS product_min_quantity,
    stripe_prices_products.max_quantity AS product_max_quantity,
    stripe_prices_products.external_reference AS price_product_external_reference,
    stripe_prices_products.price_company_size_bucket,
    stripe_prices_products.price_myob_advanced_edition,
    stripe_prices_products.price_inventory_id ,
    stripe_prices_products.price_effective_year,
    plans.currency AS plan_currency,
    plans.id AS plan_id,
    plans.interval_count,
    --  plans.nickname,  duplicate of plans_nickname
    plans.product_id AS plan_product_id,
    plans.trial_period_days,
    prices.nickname AS price_nickname,
    prices.recurring_interval,
    prices.recurring_interval_count,
    prices.recurring_trial_period_days,
    prices.unit_amount_decimal,
    -- add subscription item amount which is PRICE_UNIT_AMOUNT * QUANTITY
    subscription_items.price_unit_amount*subscription_items.quantity AS subscription_item_amount,
    CASE WHEN hubspot_company2.property_stripe_subscription_id IS NOT null THEN 1 ELSE 0 END
        AS hubspot_stripe_sub_id_matched_flag,
    Coalesce(hubspot_company.objectid::varchar, hubspot_company1.objectid::varchar) AS hubspot_company_id,
    Coalesce(
        hubspot_company.property_myob_customer_id::varchar,  hubspot_company1.property_myob_customer_id::varchar
    ) AS hubspot_company_myob_customer_id,
    Coalesce(hubspot_company.property_name , hubspot_company1.property_name ) AS hubspot_company_name,
    hubspot_company2.property_stripe_subscription_id::varchar AS hubspot_customer_stripe_subscription_id,
    Coalesce(hubspot_company.property_gst_status, hubspot_company1.property_gst_status)
        AS hubspot_company_gst_status,
    Coalesce(hubspot_company.property_lifecyclestage , hubspot_company1.property_lifecyclestage)
        AS hubspot_company_lifecyclestage,
    CASE WHEN hubspot_company2.property_stripe_subscription_id IS NOT null THEN 1 ELSE 0 END
        AS hubspot_stripe_sub_id_company_match_flag,
    Coalesce(hubspot_company.property_recent_deal_close_date , hubspot_company1.property_recent_deal_close_date)
        AS hubspot_company_recent_deal_close_date,
    CASE WHEN hubspot_company.objectid IS NOT null THEN 1 ELSE 0 END AS hubspot_stripe_crm_id_company_match_flag,
    CASE WHEN hubspot_company1.property_myob_customer_id IS NOT null THEN 1 ELSE 0 END
        AS hubspot_stripe_myob_customer_id_company_match_flag
FROM BILLING.PUBLISHED_PROD.STRIPE_SUBSCRIPTION_ITEMS  AS subscription_items
LEFT JOIN BILLING.PUBLISHED_PROD.STRIPE_SUBSCRIPTIONS  AS subscriptions
    ON subscription_items.subscription = subscriptions.id
LEFT JOIN
    OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_prices_products AS stripe_prices_products
    ON subscription_items.price_id = stripe_prices_products.price_id
LEFT JOIN PURCHASING.PUBLISHED_PROD.STRIPE_PLANS  AS plans
    ON subscription_items.plan_id = plans.id
LEFT JOIN PURCHASING.PUBLISHED_PROD.STRIPE_PRODUCTS  AS products
    ON subscription_items.plan_product_id = products.id
LEFT JOIN PURCHASING.PUBLISHED_PROD.STRIPE_PRICES  AS prices
    ON subscription_items.price_id = prices.id
LEFT JOIN OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_customers_with_metadata  AS customers
    ON subscriptions.customer_id = customers.id
LEFT JOIN CRM.RAW.HUBSPOT_OBJECTSCOMPANIES  AS hubspot_company
    ON hubspot_company.objectid::varchar  = customers.customer_crm_id ---matched to company using hubspot CRM Id
LEFT JOIN CRM.RAW.HUBSPOT_OBJECTSCOMPANIES AS hubspot_company1
    ON hubspot_company1.property_myob_customer_id::varchar  = customers.myob_customer_id ---matched to company using hubspot myob customer id
LEFT JOIN CRM.RAW.HUBSPOT_OBJECTSCOMPANIES AS hubspot_company2
    ON hubspot_company2.property_stripe_subscription_id::varchar  = subscriptions.id ::varchar ---matched to company using hubspot company subscription id
LEFT JOIN get_first_invoice_date
    ON subscriptions.id = get_first_invoice_date.subscription_id
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_subscriptions", "node_alias": "stripe_subscriptions", "node_package_name": "stripe", "node_original_file_path": "models/stripe_subscriptions.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_subscriptions", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": ["stripe_invoices_gst", "stripe_prices_products", "stripe_customers_with_metadata"], "materialized": "view"} */;