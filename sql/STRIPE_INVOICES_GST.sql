create or replace view STRIPE_INVOICES_GST(
	INVOICE_NUMBER,
	INVOICE_ID,
	INVOICE_ITEM_ID,
	LINE_ITEM_NUM,
	INVOICE_ATTRIBUTES,
	MERCHANT_ID,
	INVOICE_AMOUNT_DUE,
	INVOICES_AMOUNT_PAID,
	INVOICES_AMOUNT_REMAINING,
	INVOICE_TAX,
	INVOICE_TAX_PERCENT,
	INVOICE_SUBTOTAL,
	INVOICES_TOTAL,
	CREDIT_NOTES_AMT,
	HAS_CREDIT_NOTE_FLAG,
	INVOICE_TOTAL_AMOUNT_INC_GST,
	INVOICE_AMOUNT,
	BILLING_REASON,
	INVOICES_CHARGE_ID,
	COLLECTION_METHOD,
	INVOICE_CURRENCY,
	CUSTOMER_ID,
	MYOB_CUSTOMER_ID,
	CUSTOMER_CRM_ID,
	LEGAL_ENTITY,
	DELINQUENT_FLAG,
	CUSTOMER_NAME,
	CUSTOMER_SEGMENT,
	INVOICE_DATE,
	INVOICE_DATE_LOCAL,
	DISCOUNT_COUPON_ID,
	DISCOUNT_CUSTOMER_ID,
	DISCOUNT_SUBSCRIPTION_ID,
	ENDING_BALANCE,
	PAID,
	INVOICE_STATUS,
	STATUS_TRANSITIONS_MARKED_UNCOLLECTIBLE_AT,
	STATUS_TRANSITIONS_MARKED_UNCOLLECTIBLE_AT_LOCAL,
	STATUS_TRANSITIONS_PAID_AT,
	STATUS_TRANSITIONS_PAID_AT_LOCAL,
	STATUS_TRANSITIONS_FINALIZED_AT,
	STATUS_TRANSITIONS_FINALIZED_AT_LOCAL,
	STATUS_TRANSITIONS_VOIDED_AT,
	STATUS_TRANSITIONS_VOIDED_AT_LOCAL,
	INVOICE_ITEM_ATTRIBUTES,
	INVOICE_DESCRIPTION,
	INVOICE_ITEM_DESCRIPTION,
	INVOICE_ITEM_GROSS_AMOUNT_EX_GST,
	INVOICE_DISCOUNTABLE,
	PLAN_ID,
	PRICE_ID,
	PRORATION,
	INVOICE_LINE_ITEM_QUANTITY,
	SOURCE_ID,
	SOURCE_TYPE,
	SUBSCRIPTION_ID,
	SUBSCRIPTION_ITEM_ID,
	INVOICE_ITEM_DISCOUNTABLE,
	INVOICE_ITEM_PERIOD_START,
	INVOICE_ITEM_PERIOD_START_LOCAL,
	INVOICE_ITEM_PERIOD_END,
	INVOICE_ITEM_PERIOD_END_LOCAL,
	INVOICE_ITEM_PLAN_ID,
	INVOICE_ITEM_PRICE_ID,
	PRICE_NICKNAME,
	PRICE_RECURRING_INTERVAL,
	PRICE_RECURRING_INTERVAL_COUNT,
	PRICE_UNIT_AMOUNT_DECIMAL,
	PRODUCT_NAME,
	POSTING_CLASS,
	POSTING_TYPE,
	PRODUCT_CODE,
	SALES_ACCOUNT,
	ITEM_CLASS,
	PRICE_PROTECTION_PERIODS,
	PRODUCT_MIN_QUANTITY,
	PRODUCT_MAX_QUANTITY,
	PRODUCT_EXTERNAL_REFERENCE,
	PRICE_COMPANY_SIZE_BUCKET,
	PRICE_MYOB_ADVANCED_EDITION,
	PRICE_INVENTORY_ID,
	PRICE_EFFECTIVE_YEAR,
	INVOICE_ITEM_PRORATION,
	INVOICE_ITEM_SOURCE_ID,
	INVOICE_ITEM_SOURCE_TYPE,
	INVOICE_ITEM_SUBSCRIPTION_ID,
	CHARGE_ATTRIBUTES,
	BALANCE_TRANSACTION_ID,
	CALCULATED_STATEMENT_DESCRIPTOR,
	CAPTURED,
	CAPTURED_AT,
	CARD_ID,
	CARD_NETWORK,
	CHARGES_CUSTOMER_ID,
	DESCRIPTION,
	FAILURE_CODE,
	FAILURE_MESSAGE,
	CHARGES_ID,
	OUTCOME_NETWORK_ADVICE_CODE,
	OUTCOME_NETWORK_DECLINE_CODE,
	OUTCOME_NETWORK_STATUS,
	OUTCOME_SELLER_MESSAGE,
	OUTCOME_TYPE,
	CHARGES_PAID,
	PAYMENT_INTENT,
	PAYMENT_METHOD_ID,
	PAYMENT_METHOD_TYPE,
	REFUNDED,
	CHARGES_STATUS,
	CUSTOMER_ATTRIBUTES,
	CUSTOMER_DELINQUENT_FLAG,
	BUSINESS_VAT_ID,
	TAX_INFO_TYPE,
	ADDRESS_COUNTRY,
	TAX_INFO_TAX_ID,
	HUBSPOT_CUSTOMER_OBJECT_ID,
	HUBSPOT_CUSTOMER_GST_STATUS,
	DISCOUNT_START,
	DISCOUNT_END,
	INVOICE_DUE_DATE,
	INVOICE_START_DATE,
	INVOICE_END_DATE,
	INVOICE_PERIOD_START_DATE,
	INVOICE_PERIOD_END_DATE,
	INVOICE_PERIOD_START_DATE_LOCAL,
	INVOICE_PERIOD_END_DATE_LOCAL,
	DISCOUNT_AMOUNT,
	DISCOUNT_PERCENT_OFF,
	PERIOD_START,
	PERIOD_END,
	CHARGES_AMOUNT,
	CHARGES_AMOUNT_REFUNDED,
	EXPORT_VS_DOMESTIC,
	TAX_ERROR_STATUS,
	TAX_ERROR_STATUS_REASON,
	HAS_COUPON_FLAG,
	COUPON_NAME,
	COUPON_ID
) as (
    

/*
    Purpose:     Creates a  view of stripe invoices with gst amounts
    Date:        202206
    Description:  Creates a view of stripe invoices  with gst amounts
  */

WITH credit_notes AS (
    SELECT
        cn.invoice_id,
        Round(Sum(cn.amount)/100,2) AS credit_notes_amt
    FROM billing.published_prod.stripe_credit_notes AS cn
    GROUP BY cn.invoice_id
)



SELECT
    invoices.number AS invoice_number,
    invoices.id AS invoice_id,
    invoice_line_items.id AS invoice_item_id,
    Row_number() OVER (PARTITION BY invoices.id  ORDER BY invoice_line_items.id) AS line_item_num,
    ' ##### INVOICE ATTRIBUTES: ###### ' AS invoice_attributes,
    invoices.merchant_id,
    Round(CASE WHEN line_item_num = 1
            THEN
                (CASE WHEN invoices.amount_due > 0
                        THEN invoices.amount_due/100
                    ELSE invoices.amount_due
                END)
    END ,2)
        AS invoice_amount_due,
    Round( CASE WHEN line_item_num = 1
            THEN CASE WHEN invoices.amount_paid > 0
                        THEN invoices.amount_paid/100
                    ELSE invoices.amount_paid
                END
    END ,2)
        AS invoices_amount_paid,
    Round( CASE WHEN line_item_num = 1
            THEN (CASE WHEN invoices.amount_remaining > 0
                    THEN invoices.amount_remaining/100
                ELSE  invoices.amount_remaining
            END)
    END , 2)
        AS invoices_amount_remaining,
    Round(CASE WHEN line_item_num = 1
            THEN CASE WHEN invoices.tax > 0
                        THEN invoices.tax/100
                    ELSE invoices.tax
                END
    END ,2)
        AS invoice_tax,
    CASE WHEN line_item_num = 1
            THEN (
                CASE WHEN invoices.tax_percent = 0
                        THEN 0
                    WHEN invoices.tax = 0 THEN 0
                    ELSE invoices.tax_percent/100
                END
            )
    END AS invoice_tax_percent,
    Round( CASE WHEN line_item_num = 1
            THEN (
                CASE WHEN invoices.subtotal > 0
                        THEN invoices.subtotal / 100
                    ELSE invoices.subtotal
                END
            )
    END,
    2) AS invoice_subtotal,
    Round(
        CASE WHEN line_item_num = 1
                THEN (
                    CASE WHEN invoices.total > 0
                            THEN invoices.total / 100
                        ELSE invoices.total
                    END
                )
        END,2
    ) AS invoices_total,
    Round(
        CASE WHEN line_item_num = 1
                THEN (
                    CASE WHEN cn.credit_notes_amt != 0
                            THEN cn.credit_notes_amt
                        ELSE 0
                    END
                )
        END,2
    ) AS credit_notes_amt,
    CASE WHEN credit_notes_amt != 0 AND credit_notes_amt IS NOT null THEN 1 ELSE 0 END AS has_credit_note_flag,
    Round(invoices_total,2) AS invoice_total_amount_inc_gst,
    Round(invoices_total,2) AS invoice_amount,
    invoices.billing_reason,
    invoices.charge_id AS invoices_charge_id,
    invoices.collection_method,
    invoices.currency AS invoice_currency,
    invoices.customer_id,
    customers.myob_customer_id,
    customers.customer_crm_id,
    customers.legal_entity,
    customers.delinquent AS delinquent_flag,
    customers.customer_name,
    customers.customer_segment,
    invoices.date::datetime AS invoice_date,
    CASE WHEN
            invoices.currency = 'aud'
            THEN Convert_timezone('UTC','Australia/Melbourne',  invoices.date::datetime)::date
        WHEN invoices.currency = 'nzd' THEN Convert_timezone('UTC','Pacific/Auckland',  invoices.date::datetime)::date
        ELSE invoices.date::datetime
    END AS invoice_date_local,
    invoices.discount_coupon_id,
    invoices.discount_customer_id,
    invoices.discount_subscription AS discount_subscription_id,
    invoices.ending_balance,
    invoices.paid,
    invoices.status AS invoice_status,
    invoices.status_transitions_marked_uncollectible_at::datetime AS status_transitions_marked_uncollectible_at,
    CASE WHEN
            invoices.currency = 'aud' AND invoices.status_transitions_marked_uncollectible_at IS NOT null
            THEN Convert_timezone(
                    'UTC','Australia/Melbourne', invoices.status_transitions_marked_uncollectible_at::datetime
                )::datetime
        WHEN
            invoices.currency = 'nzd' AND invoices.status_transitions_marked_uncollectible_at IS NOT null
            THEN Convert_timezone(
                    'UTC','Pacific/Auckland', invoices.status_transitions_marked_uncollectible_at::datetime
                )::datetime
        ELSE  invoices.status_transitions_marked_uncollectible_at::datetime
    END AS status_transitions_marked_uncollectible_at_local,
    invoices.status_transitions_paid_at::datetime AS status_transitions_paid_at,
    CASE WHEN
            invoices.currency = 'aud'
            THEN Convert_timezone('UTC','Australia/Melbourne', invoices.status_transitions_paid_at::datetime)::datetime
        WHEN
            invoices.currency = 'nzd'
            THEN Convert_timezone('UTC','Pacific/Auckland', invoices.status_transitions_paid_at::datetime)::datetime
        ELSE   invoices.status_transitions_paid_at::datetime
    END AS  status_transitions_paid_at_local,
    invoices.status_transitions_finalized_at::datetime AS status_transitions_finalized_at,
    CASE WHEN
            invoices.currency = 'aud'
            THEN Convert_timezone(
                    'UTC','Australia/Melbourne',  invoices.status_transitions_finalized_at::datetime
                )::datetime
        WHEN
            invoices.currency = 'nzd'
            THEN Convert_timezone(
                    'UTC','Pacific/Auckland',  invoices.status_transitions_finalized_at::datetime
                )::datetime
        ELSE     invoices.status_transitions_finalized_at::datetime
    END AS   status_transitions_finalized_at_local,
    invoices.status_transitions_voided_at::datetime AS status_transitions_voided_at,
    CASE WHEN
            invoices.currency = 'aud'
            THEN Convert_timezone(
                    'UTC','Australia/Melbourne',  invoices.status_transitions_voided_at::datetime
                )::datetime
        WHEN
            invoices.currency = 'nzd'
            THEN Convert_timezone('UTC','Pacific/Auckland',   invoices.status_transitions_voided_at::datetime)::datetime
        ELSE  invoices.status_transitions_voided_at::datetime
    END AS  status_transitions_voided_at_local,
    ' ##### INVOICE ITEM ATTRIBUTES: ###### ' AS invoice_item_attributes,
    null AS invoice_description,
    invoice_line_items.description AS invoice_item_description,
    Round(CASE WHEN invoice_line_items.amount > 0
            THEN invoice_line_items.amount / 100
        ELSE invoice_line_items.amount
    END,2) AS invoice_item_gross_amount_ex_gst,
    invoice_line_items.discountable AS invoice_discountable,
    invoice_line_items.plan_id,
    invoice_line_items.price_id,
    invoice_line_items.proration,
    invoice_line_items.quantity AS invoice_line_item_quantity,
    invoice_line_items.source_id,
    invoice_line_items.source_type,
    invoice_line_items.subscription AS subscription_id,
    invoice_line_items.subscription_item_id,
    invoice_line_items.discountable AS invoice_item_discountable,
    invoice_line_items.period_start::datetime AS invoice_item_period_start,
    CASE WHEN
            invoices.currency = 'aud'
            THEN Convert_timezone('UTC','Australia/Melbourne',  invoice_line_items.period_start::datetime)::date
        WHEN
            invoices.currency = 'nzd'
            THEN Convert_timezone('UTC','Pacific/Auckland',  invoice_line_items.period_start::datetime)::date
        ELSE invoice_line_items.period_start::datetime
    END AS invoice_item_period_start_local,
    invoice_line_items.period_end::datetime AS invoice_item_period_end,
    CASE WHEN
            invoices.currency = 'aud'
            THEN Convert_timezone('UTC','Australia/Melbourne', invoice_line_items.period_end::datetime)::date
        WHEN
            invoices.currency = 'nzd'
            THEN Convert_timezone('UTC','Pacific/Auckland',  invoice_line_items.period_end::datetime)::date
        ELSE invoice_line_items.period_end::datetime
    END AS invoice_item_period_end_local,
    invoice_line_items.plan_id AS invoice_item_plan_id,
    invoice_line_items.price_id AS invoice_item_price_id,
    stripe_prices_products.price_nickname,
    stripe_prices_products.price_recurring_interval,
    stripe_prices_products.price_recurring_interval_count,
    stripe_prices_products.price_unit_amount_decimal,
    stripe_prices_products.product_name ,
    stripe_prices_products.posting_class ,
    stripe_prices_products.posting_type ,
    stripe_prices_products.product_code ,
    stripe_prices_products.sales_account,
    stripe_prices_products.item_class,
    stripe_prices_products.price_protection_periods,
    stripe_prices_products.min_quantity AS product_min_quantity,
    stripe_prices_products.max_quantity AS product_max_quantity,
    stripe_prices_products.external_reference AS product_external_reference,
    stripe_prices_products.price_company_size_bucket,
    stripe_prices_products.price_myob_advanced_edition,
    stripe_prices_products.price_inventory_id,
    stripe_prices_products.price_effective_year,
    invoice_line_items.proration AS invoice_item_proration,
    invoice_line_items.source_id AS invoice_item_source_id,
    invoice_line_items.source_type AS invoice_item_source_type,
    invoice_line_items.subscription AS invoice_item_subscription_id,
    ' ##### CHARGE ATTRIBUTES: ###### ' AS charge_attributes,
    charges.balance_transaction_id,
    charges.calculated_statement_descriptor,
    charges.captured,
    charges.captured_at,
    charges.card_id,
    charges.card_network,
    charges.customer_id AS charges_customer_id,
    charges.description,
    charges.failure_code,
    charges.failure_message,
    charges.id AS charges_id,
    charges.outcome_network_advice_code,
    charges.outcome_network_decline_code,
    charges.outcome_network_status,
    charges.outcome_seller_message,
    charges.outcome_type,
    charges.paid AS charges_paid,
    charges.payment_intent,
    charges.payment_method_id,
    charges.payment_method_type,
    charges.refunded,
    charges.status AS charges_status,
    '#### CUSTOMERS ATTRIBUTES ##### '  AS customer_attributes,
    customers.delinquent AS customer_delinquent_flag,
    customers.business_vat_id,
    customers.tax_info_type,
    customers.address_country,
    customers.tax_info_tax_id,
    customers.hubspot_customer_object_id,
    customers.hubspot_customer_gst_status,
    invoices.discount_start::datetime AS discount_start,
    invoices.discount_end::datetime AS discount_end,
    invoices.due_date::datetime  AS invoice_due_date,
    invoices.period_start::datetime  AS invoice_start_date,
    invoices.period_end::datetime AS invoice_end_date,
    invoices.period_start::datetime AS invoice_period_start_date,
    invoices.period_end::datetime  AS invoice_period_end_date,
    CASE WHEN
            invoices.currency = 'aud'
            THEN Convert_timezone('UTC','Australia/Melbourne', invoices.period_start::datetime)::date
        WHEN
            invoices.currency = 'nzd'
            THEN Convert_timezone('UTC','Pacific/Auckland',  invoices.period_start::datetime)::date
        ELSE invoices.period_start::datetime
    END AS  invoice_period_start_date_local,
    CASE WHEN
            invoices.currency = 'aud'
            THEN Convert_timezone('UTC','Australia/Melbourne', invoices.period_end::datetime)::date
        WHEN
            invoices.currency = 'nzd'
            THEN Convert_timezone('UTC','Pacific/Auckland',  invoices.period_end::datetime)::date
        ELSE invoices.period_end::datetime
    END   AS invoice_period_end_date_local,
    Round(
        CASE WHEN
                invoices.subtotal IS NOT null AND invoices.subtotal != 0
                THEN (invoices.subtotal/100) * (coupons.percent_off / 100)
            ELSE 0
        END,2
    ) AS discount_amount,
    CASE WHEN coupons.percent_off IS NOT null AND coupons.percent_off != 0 THEN coupons.percent_off /100 ELSE 0 END
        AS discount_percent_off,
    invoice_line_items.period_start::datetime  AS period_start,
    invoice_line_items.period_end::datetime  AS period_end,
    CASE WHEN charges.amount > 0 THEN charges.amount / 100 ELSE charges.amount  END AS charges_amount,
    CASE WHEN charges.amount_refunded > 0 THEN charges.amount_refunded / 100  ELSE charges.amount_refunded  END
        AS  charges_amount_refunded,
    CASE WHEN customers.address_country = customers.legal_entity THEN 'Domestic'
        WHEN customers.address_country IS null THEN 'Other'
        ELSE 'Export'
    END AS export_vs_domestic,
    CASE WHEN customers.hubspot_customer_gst_status IS null THEN 'NOT OK'
        WHEN customers.hubspot_customer_gst_status = 'GST Exempt' AND invoices.tax_percent != 0 THEN 'NOT OK'
        WHEN customers.hubspot_customer_gst_status = 'GST registered' AND invoices.tax_percent = 0 THEN 'NOT OK'
        WHEN customers.address_country = 'AU' AND invoices.tax_percent = 10 THEN 'OK'
        WHEN customers.address_country = 'AU' AND invoices.tax_percent != 10 THEN 'NOT OK'
        WHEN customers.address_country IS null AND invoices.tax_percent != 0 THEN 'NOT OK'
        ELSE 'OK'
    END AS tax_error_status,
    CASE WHEN customers.hubspot_customer_gst_status IS null THEN 'null HUBSPOT GST STATUS'
        WHEN
            customers.hubspot_customer_gst_status = 'GST Exempt' AND invoices.tax_percent != 0
            THEN 'GST EXCEMPT WITH GST'
        WHEN
            customers.hubspot_customer_gst_status = 'GST registered' AND invoices.tax_percent = 0
            THEN 'GST REGISTERED MISSING GST'
        WHEN customers.address_country = 'AU' AND invoices.tax_percent = 10 THEN 'TAX AS EXPECTED'
        WHEN customers.address_country = 'AU' AND invoices.tax_percent != 10 THEN 'AU NOT 10%'
        WHEN
            customers.address_country IS null AND invoices.tax_percent != 0
            THEN 'CUSTOMER COUNTRY IS null AND TAX APPLIED'
        ELSE 'TAX AS EXPECTED'
    END AS tax_error_status_reason,
    CASE WHEN coupons.id IS NOT null THEN 1 ELSE 0 END AS has_coupon_flag,
    Coalesce(coupons.name, 'No Coupon') AS coupon_name,
    Coalesce(coupons.id, 'No Coupon') AS coupon_id
FROM BILLING.PUBLISHED_PROD.STRIPE_INVOICE_LINE_ITEMS AS invoice_line_items
LEFT JOIN BILLING.PUBLISHED_PROD.STRIPE_INVOICES AS invoices
    ON invoice_line_items.invoice_id = invoices.id
LEFT JOIN OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_prices_products AS stripe_prices_products
    ON invoice_line_items.price_id = stripe_prices_products.price_id
LEFT JOIN BILLING.PUBLISHED_PROD.STRIPE_CHARGES  AS charges ON invoices.charge_id = charges.id
LEFT JOIN OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_customers_with_metadata AS customers ON invoices.customer_id = customers.id
LEFT JOIN PURCHASING.PUBLISHED_PROD.STRIPE_COUPONS  AS coupons
    ON invoices.discount_coupon_id = coupons.id
LEFT JOIN credit_notes AS cn ON  invoices.id = cn.invoice_id
WHERE 1=1
QUALIFY Row_number() OVER (PARTITION BY invoice_line_items.id ORDER BY invoice_line_items.id)  = 1
ORDER BY invoices.number , invoice_line_items.id
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_invoices_gst", "node_alias": "stripe_invoices_gst", "node_package_name": "stripe", "node_original_file_path": "models/stripe_invoices_gst.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_invoices_gst", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": ["stripe_prices_products", "stripe_customers_with_metadata"], "materialized": "view"} */;