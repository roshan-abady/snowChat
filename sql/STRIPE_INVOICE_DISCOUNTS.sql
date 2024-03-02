create or replace view STRIPE_INVOICE_DISCOUNTS(
	INVOICEMONTH,
	INVOICEID,
	INVOICE_DATE,
	INVOICE_NUMBER,
	CUSTOMER_ID,
	MYOB_CUSTOMER_ID,
	AMOUNTPAID,
	AMOUNTREMAINING,
	AMOUNTDUE,
	TOTALAMOUNT,
	SUBTOTALAMOUNT,
	CURRENCY,
	DISCOUNTCUSTOMERID,
	DISCOUNTSUBSCRIPTION,
	DISCOUNT_START,
	DISCOUNT_END,
	HASCOUPON,
	REASON,
	COUPONID,
	COUPONAMOUNTOFF,
	COUPONPERCENTOFF
) as (
     

/*
    Purpose:     Creates a  view of stripe invoices with discounts
    Date:        202206
    Description:  Creates a view of stripe invoices with discounts. discounts are associated with invoice coupons
  */


WITH MonthlyInvoices AS (
-- Aggregated invoices by month
    SELECT
        Invoices.Date::date AS Invoice_Date,
        Invoices.Id AS InvoiceID,
        Invoices.Number AS Invoice_Number,
        Invoices.Customer_Id,
        B.Value AS Myob_Customer_Id,
        Invoices.DISCOUNT_COUPON_ID AS CouponID,
        Invoices.Discount_Start,
        Invoices.Discount_End,
        Invoices.Currency,
        Invoices.Discount_Customer_Id AS DiscountCustomerID,
        Invoices.Discount_Subscription AS DiscountSubscription,
        Date_trunc('month', Invoices.Date::date) AS InvoiceMonth,
        Round(Sum(Zeroifnull(Invoices.Total) / 100), 2) AS TotalAmount,
        Round(Sum(Zeroifnull(Invoices.Amount_Paid) / 100), 2) AS AmountPaid,
        Round(Sum(Zeroifnull(Invoices.Amount_Remaining) / 100), 2) AS AmountRemaining,
        Round(Sum(Zeroifnull(Invoices.Amount_Due) / 100), 2) AS AmountDue,
        Round(Sum(Zeroifnull(Invoices.Application_Fee) / 100), 2) AS ApplicationFee,
        Round(Sum(Zeroifnull(Invoices.Subtotal) / 100), 2) AS SubtotalAmount
    FROM
    --BILLING.PUBLISHED_PROD.STRIPE_INVOICES AS invoices
        BILLING.PUBLISHED_PROD.STRIPE_INVOICES    AS Invoices
    LEFT JOIN  BILLING.PUBLISHED_PROD.STRIPE_CUSTOMERS   AS Customer ON Invoices.Customer_Id = Customer.Id --cus_OlC6No4Lwxozg3
    LEFT JOIN
        BILLING.PUBLISHED_PROD.STRIPE_CUSTOMERS_METADATA  AS  B
        ON Invoices.Customer_Id = B.Customer_Id AND B.Key = 'Customer ID'
    GROUP BY
        Date_trunc(
            'month',
            Invoices.Date::date
        ), 
        Invoice_Date,
        Invoices.Id,
        Invoices.Customer_Id,
        B.Value,
        Invoices.Number,
        Invoices.DISCOUNT_COUPON_ID,
        Invoices.Discount_Start,
        Invoices.Discount_End,
        Invoices.Currency,
        Invoices.Discount_Customer_Id,
        Invoices.Discount_Subscription
) -- Joined the above with customers, subscriptions, coupons, and promotion codes


SELECT
    MI.InvoiceMonth,
    MI.InvoiceID,
    MI.Invoice_Date,
    MI.Invoice_Number,
    MI.Customer_Id,
    MI.Myob_Customer_Id,
    MI.AmountPaid,
    MI.AmountRemaining,
    MI.AmountDue,
    MI.TotalAmount,
    MI.SubtotalAmount,
    MI.Currency,
    MI.DiscountCustomerID,
    MI.DiscountSubscription,
    MI.Discount_Start,
    MI.Discount_End,
    CASE WHEN Coupons.Id IS NOT null THEN 1 ELSE 0 END AS HasCoupon,
    Coalesce(Coupons.Name, 'No Coupon') AS Reason,
    Coalesce(Coupons.Id, 'No Coupon') AS CouponID,
    Round(
        CASE
            WHEN MI.SubtotalAmount IS NOT null
                AND MI.SubtotalAmount != 0 THEN MI.SubtotalAmount * Coupons.PERCENT_OFF / 100
            ELSE 0
        END,
        2
    ) AS CouponAmountOff,
    Coalesce(Coupons.Percent_Off, 0) AS CouponPercentOff
FROM
    MonthlyInvoices AS MI
LEFT JOIN  PURCHASING.PUBLISHED_PROD.STRIPE_COUPONS  AS Coupons ON MI.CouponID = Coupons.Id
--Where Invoice_number IN ('3-854889603',  '3-854889642')
ORDER BY
    MI.InvoiceMonth,
    MI.InvoiceID
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_invoice_discounts", "node_alias": "stripe_invoice_discounts", "node_package_name": "stripe", "node_original_file_path": "models/stripe_invoice_discounts.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_invoice_discounts", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": [], "materialized": "view"} */;