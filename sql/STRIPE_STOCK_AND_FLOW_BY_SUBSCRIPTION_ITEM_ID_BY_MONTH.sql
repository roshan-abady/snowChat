create or replace view STRIPE_STOCK_AND_FLOW_BY_SUBSCRIPTION_ITEM_ID_BY_MONTH(
	MONTH_ID,
	REPORTING_OPEN_DATE,
	REPORTING_CLOSE_DATE,
	REPORTING_YEAR,
	REPORTING_MONTH,
	UNIQUE_SUBSCRIPTION_ITEM_ID,
	SUBSCRIPTION_ID,
	SUBSCRIPTION_ITEM_ID,
	MYOB_CUSTOMER_ID,
	CUSTOMER_CRM_ID,
	CUSTOMER_NAME,
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
	ADDRESS_COUNTRY,
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
	HUBSPOT_CUSTOMER_STRIPE_SUBSCRIPTION_ID,
	HUBSPOT_STRIPE_SUB_ID_MATCHED_FLAG,
	HUBSPOT_STRIPE_SUB_ID_COMPANY_MATCH_FLAG,
	HUBSPOT_COMPANY_ID,
	HUBSPOT_COMPANY_MYOB_CUSTOMER_ID,
	HUBSPOT_COMPANY_NAME,
	HUBSPOT_COMPANY_GST_STATUS,
	HUBSPOT_COMPANY_LIFECYCLESTAGE,
	HUBSPOT_COMPANY_RECENT_DEAL_CLOSE_DATE,
	HUBSPOT_STRIPE_CRM_ID_COMPANY_MATCH_FLAG,
	HUBSPOT_STRIPE_MYOB_CUSTOMER_ID_COMPANY_MATCH_FLAG,
	MRR,
	NUM_OPENING,
	NUM_CLOSING,
	INITIAL_NUM_ADDED,
	INITIAL_NUM_DELETED,
	NUM_ADDED,
	NUM_DELETED,
	QUANTITY_OPENING,
	QUANTITY_CLOSING,
	QUANTITY_ADDED,
	QUANTITY_DELETED,
	QUANTITY_NET_CHANGE_OVER_TIME,
	OPENING_ARR,
	CLOSING_ARR,
	ADDED_ARR,
	DELETED_ARR,
	NET_CHANGE_ARR
) as (
    WITH calculated_flags AS(
    SELECT DISTINCT
        dd.month_id,
        dd.reporting_open_date,
        dd.reporting_close_date,
        dd.reporting_year,
        dd.reporting_month,
        -- Created a unique month and subscription item id
        dd.month_id || '_' || subs.subscription_item_id AS unique_subscription_item_id,
        subs.subscription_id,
        subs.subscription_item_id,
        subs.myob_customer_id,
        subs.customer_crm_id,
        subs.customer_name,
        subs.legal_entity,
        subs.delinquent_flag,
        subs.customer_segment,
        subs.account_balance,
        subs.customer_created,
        subs.currency,
        subs.default_source_id,
        subs.deleted,
        subs.delinquent,
        subs.customer_id,
        subs.invoice_settings_default_payment_method_id,
        subs.hubspot_company_archie_client_id,
        subs.product_name,
        subs.product_type,
        subs.unit_label,
        subs.subscription_billing,
        subs.billing_cycle_anchor,
        subs.cancellation_reason,
        subs.cancellation_reason_text,
        subs.subscriptions_created,
        subs.current_period_end,
        subs.current_period_start,
        subs.subscription_customer_id,
        subs.discount_coupon_id,
        subs.discount_customer_id,
        subs.discount_end,
        subs.discount_start,
        subs.discount_subscription,
        subs.subscription_plan_id,
        subs.subscription_price_id,
        subs.subscription_quantity,
        subs.subscription_start_time,
        subs.subscription_start_date,
        subs.subscription_first_invoice_date,
        subs.subscription_start_date_time_local,
        subs.ended_at,
        subs.ended_at_date_local,
        subs.canceled_at,
        subs.canceled_at_date_local,
        subs.subscription_status,
        subs.tax_percent,
        subs.trial_end,
        subs.trial_start,
        subs.subscription_items_created,
        subs.subscription_items_created_date_local,
        subs.address_country,
        subs.plan_amount,
        subs.plan_created,
        subs.plan_interval,
        subs.plan_interval_count,
        subs.plan_nickname,
        subs.price_created,
        subs.price_id,
        subs.price_product_id,
        subs.price_recurring_interval,
        subs.price_recurring_interval_count,
        subs.price_unit_amount,
        subs.subscription_item_quantity,
        subs.price_product_name,
        subs.price_product_posting_class,
        subs.price_product_posting_type,
        subs.price_product_code,
        subs.price_item_class,
        subs.price_protection_periods,
        subs.product_min_quantity,
        subs.product_max_quantity,
        subs.price_product_external_reference,
        subs.price_company_size_bucket,
        subs.price_myob_advanced_edition,
        subs.price_inventory_id,
        subs.price_effective_year,
        subs.plan_currency,
        subs.plan_id,
        subs.interval_count,
        subs.plan_product_id,
        subs.trial_period_days,
        subs.price_nickname,
        subs.recurring_interval,
        subs.recurring_interval_count,
        subs.recurring_trial_period_days,
        subs.unit_amount_decimal,
        subs.hubspot_customer_stripe_subscription_id,
        subs.hubspot_stripe_sub_id_matched_flag,
        subs.hubspot_stripe_sub_id_company_match_flag,
        subs.hubspot_company_id,
        subs.hubspot_company_myob_customer_id,
        subs.hubspot_company_name,
        subs.hubspot_company_gst_status,
        subs.hubspot_company_lifecyclestage,
        subs.hubspot_company_recent_deal_close_date,
        subs.hubspot_stripe_crm_id_company_match_flag,
        subs.hubspot_stripe_myob_customer_id_company_match_flag,
        subs.subscription_item_amount AS mrr,
        -- Calculate num_opening based on subscriptions active at the beginning of the month
        CASE
            WHEN subs.subscription_start_date < dd.reporting_open_date
                AND (subs.canceled_at IS null OR subs.canceled_at > dd.reporting_open_date)
                AND subs.subscription_first_invoice_date < dd.reporting_open_date THEN 1
            ELSE 0
        END AS num_opening,
        -- Calculate num_closing based on subscriptions active at the end of the month
        CASE
            WHEN subs.subscription_start_date <= dd.reporting_close_date
                AND (subs.canceled_at IS null OR subs.canceled_at >= dd.reporting_close_date)
                AND subs.subscription_first_invoice_date <= dd.reporting_close_date THEN 1
            ELSE 0
        END AS num_closing,
        -- Calculate num_added for new subscriptions within the month
        CASE
            WHEN subs.subscription_first_invoice_date BETWEEN dd.reporting_open_date AND dd.reporting_close_date
                /* AND (num_closing = 1 OR num_deleted = 1 ) */
                THEN 1
            ELSE 0
        END AS initial_num_added,
        -- Calculate num_deleted for subscriptions canceled within the month
        CASE
            WHEN subs.canceled_at BETWEEN dd.reporting_open_date AND dd.reporting_close_date
                /* AND (num_opening = 1 OR num_added = 1 ) */
                THEN 1
            ELSE 0
        END AS initial_num_deleted
    FROM OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_date_dimension AS dd
    CROSS JOIN OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_subscriptions AS subs
    WHERE subs.subscription_first_invoice_date <= dd.reporting_close_date
        AND subs.subscription_item_id IS NOT null
    QUALIFY
        Row_number()
            OVER (
                PARTITION BY dd.month_id, subs.subscription_item_id ORDER BY subs.subscription_first_invoice_date DESC
            )
        = 1
),

quantity_nums AS (
    SELECT
        calculated_flags.price_unit_amount,
        calculated_flags.unique_subscription_item_id,
        calculated_flags.mrr,
        (calculated_flags.mrr * 12) AS arr,
        -- Refining num_added based on num_closing or initial_num_deleted
        CASE
            WHEN calculated_flags.initial_num_added = 1
                AND (calculated_flags.num_closing = 1 OR calculated_flags.initial_num_deleted = 1) THEN 1
            ELSE 0
        END AS num_added,
        -- Refining num_deleted based on num_opening or initial_num_added
        CASE
            WHEN calculated_flags.initial_num_deleted = 1
                AND (calculated_flags.num_opening = 1 OR calculated_flags.initial_num_added = 1) THEN 1
            ELSE 0
        END AS num_deleted,
        calculated_flags.num_opening,
        calculated_flags.num_closing,
        Iff(calculated_flags.num_opening = 1, calculated_flags.subscription_item_quantity, 0) AS quantity_opening,
        Iff(calculated_flags.num_closing = 1, calculated_flags.subscription_item_quantity, 0) AS quantity_closing,
        Iff(num_added = 1, calculated_flags.subscription_item_quantity, 0) AS quantity_added,
        Iff(num_deleted = 1, calculated_flags.subscription_item_quantity, 0) AS quantity_deleted,
        Sum(quantity_added - quantity_deleted) OVER (
            PARTITION BY calculated_flags.unique_subscription_item_id
            ORDER BY calculated_flags.reporting_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS quantity_net_change_over_time
    FROM calculated_flags
),

arr_calculations AS (
    SELECT
        unique_subscription_item_id,
        Iff(quantity_opening = 0, 0, quantity_opening*price_unit_amount) AS opening_arr,
        Iff(quantity_closing = 0, 0, quantity_closing*price_unit_amount) AS closing_arr,
        Iff(quantity_added = 0, 0, quantity_added*price_unit_amount) AS added_arr,
        Iff(quantity_deleted = 0, 0, quantity_deleted*price_unit_amount) AS deleted_arr,
        Iff(quantity_net_change_over_time = 0, 0, quantity_net_change_over_time*price_unit_amount) AS net_change_arr
    FROM quantity_nums
)

SELECT DISTINCT
    calculated_flags.*,
    quantity_nums.num_added,
    quantity_nums.num_deleted,
    quantity_nums.quantity_opening,
    quantity_nums.quantity_closing,
    quantity_nums.quantity_added,
    quantity_nums.quantity_deleted,
    quantity_nums.quantity_net_change_over_time,
    arr_calculations.opening_arr,
    arr_calculations.closing_arr,
    arr_calculations.added_arr,
    arr_calculations.deleted_arr,
    arr_calculations.net_change_arr
FROM
    calculated_flags
INNER JOIN
    quantity_nums ON calculated_flags.unique_subscription_item_id = quantity_nums.unique_subscription_item_id
INNER JOIN
    arr_calculations ON calculated_flags.unique_subscription_item_id = arr_calculations.unique_subscription_item_id
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_stock_and_flow_by_subscription_item_id_by_month", "node_alias": "stripe_stock_and_flow_by_subscription_item_id_by_month", "node_package_name": "stripe", "node_original_file_path": "models/stripe_stock_and_flow_by_subscription_item_id_by_month.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_stock_and_flow_by_subscription_item_id_by_month", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": ["stripe_date_dimension", "stripe_subscriptions"], "materialized": "view"} */;