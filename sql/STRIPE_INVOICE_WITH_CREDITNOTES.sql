create or replace view STRIPE_INVOICE_WITH_CREDITNOTES(
	ID,
	CREDIT_NOTES_AMT,
	PAID,
	STATUS,
	INVOICE_DUE_DATE,
	INVOICE_DUE_DATE_LOCAL,
	INVOICE_DATE,
	INVOICE_DATE_LOCAL,
	CURRENT_DATE_LOCAL,
	CUSTOMER_ID,
	INV_NUMBER,
	INVOICE_AMT,
	INVOICE_DUE_AMT,
	INVOICE_REMAINING_AMT,
	INV_AMT_PAID,
	UNPAID_INVOICE,
	PAID_INVOICE,
	INVOICE_AGE_IN_DAYS,
	INVOICE_AGE_30_DAYS,
	INVOICE_AGE_31_TO_60_DAYS,
	INVOICE_AGE_61_TO_90_DAYS,
	INVOICE_AGE_90_DAYS_PLUS,
	UNPAID_INVOICE_AGE_30_DAYS,
	UNPAID_INVOICE_AGE_31_TO_60_DAYS,
	UNPAID_INVOICE_AGE_61_TO_90_DAYS,
	UNPAID_INVOICE_AGE_90_DAYS_PLUS,
	DAYS_AFTER_DUE,
	ARREARS_DAY,
	ARREARS_AMT
) as (
     


WITH credit_notes AS (
    SELECT
        cn.invoice_id,
        Round(Sum(cn.amount)/100,2) AS credit_notes_amt
    FROM billing.published_prod.stripe_credit_notes AS cn
    GROUP BY cn.invoice_id
)

SELECT
    inv.id,
    cn.credit_notes_amt,
    inv.paid,
    inv.status,
    inv.due_date::datetime AS invoice_due_date,
    CASE WHEN inv.currency = 'aud' THEN Convert_timezone('Australia/Melbourne', inv.due_date::datetime)::date  
        WHEN inv.currency = 'nzd' THEN Convert_timezone('Pacific/Auckland', inv.due_date::datetime)::date  
        ELSE inv.due_date::datetime END
        AS invoice_due_date_local,
    inv.date::datetime AS invoice_date,
    CASE WHEN inv.currency = 'aud' THEN Convert_timezone('Australia/Melbourne', inv.date::datetime)::date  
        WHEN inv.currency = 'nzd' THEN Convert_timezone('Pacific/Auckland', inv.date::datetime)::date  
        ELSE inv.date::datetime
    END AS invoice_date_local,
    CASE WHEN inv.currency = 'aud' THEN Convert_timezone('Australia/Melbourne',  Current_timestamp::datetime)::date  
        WHEN inv.currency = 'nzd' THEN Convert_timezone('Pacific/Auckland',  Current_timestamp::datetime)::date  
        ELSE  Current_timestamp::datetime
    END AS current_date_local,
    inv.customer_id,
    inv.number AS inv_number,
    Round(inv.total/100,2) AS invoice_amt,
    Round(inv.amount_due/100,2) AS invoice_due_amt,
    Round(inv.amount_remaining/100,2) AS invoice_remaining_amt,
    Round(inv.amount_paid/100,2) AS inv_amt_paid,
    Iff(NOT inv.paid, invoice_remaining_amt, 0) AS unpaid_invoice,
    Iff(inv.paid, invoice_amt, 0) AS paid_invoice,
    Datediff(DAY, invoice_date, Current_date ) AS invoice_age_in_days,
    Iff(invoice_age_in_days BETWEEN 0 AND 30, invoice_amt, 0) AS invoice_age_30_days,
    Iff(invoice_age_in_days BETWEEN 31 AND 60, invoice_amt, 0) AS invoice_age_31_to_60_days,
    Iff(invoice_age_in_days BETWEEN 61 AND 90, invoice_amt, 0) AS invoice_age_61_to_90_days,
    Iff(invoice_age_in_days >= 91, invoice_amt, 0) AS invoice_age_90_days_plus,
    Iff(invoice_age_in_days BETWEEN 0 AND 30, unpaid_invoice, 0) AS unpaid_invoice_age_30_days,
    Iff(invoice_age_in_days BETWEEN 31 AND 60, unpaid_invoice, 0) AS unpaid_invoice_age_31_to_60_days,
    Iff(invoice_age_in_days BETWEEN 61 AND 90, unpaid_invoice, 0) AS unpaid_invoice_age_61_to_90_days,
    Iff(invoice_age_in_days >= 91, unpaid_invoice, 0) AS unpaid_invoice_age_90_days_plus,
    Datediff('day',invoice_due_date,current_date_local) AS days_after_due,
    CASE
        WHEN inv.status IN ('open','uncollectible') AND days_after_due > 1
            THEN days_after_due
        ELSE 0
    END  AS arrears_day,
    Iff(arrears_day > 0, unpaid_invoice, 0) AS arrears_amt
FROM BILLING.PUBLISHED_PROD.STRIPE_INVOICES AS  inv
LEFT JOIN credit_notes AS cn ON inv.id = cn.invoice_id
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_invoice_with_creditnotes", "node_alias": "stripe_invoice_with_creditnotes", "node_package_name": "stripe", "node_original_file_path": "models/stripe_invoice_with_creditnotes.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_invoice_with_creditnotes", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": [], "materialized": "view"} */;