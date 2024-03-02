create or replace view STRIPE_CUSTOMERS_WITH_METADATA(
	ID,
	CUSTOMER_NAME,
	STRIPE_CUSTOMER_ID,
	MYOB_CUSTOMER_ID,
	CUSTOMER_CRM_ID,
	ADDRESS_POSTAL_CODE,
	LEGAL_ENTITY,
	DELINQUENT_FLAG,
	CUSTOMER_SEGMENT,
	ACCOUNT_BALANCE,
	CUSTOMER_CREATED,
	CUSTOMERS_CREATED_LOCAL,
	CURRENCY,
	DEFAULT_SOURCE_ID,
	DELETED,
	DELINQUENT,
	INVOICE_SETTINGS_DEFAULT_PAYMENT_METHOD_ID,
	BUSINESS_VAT_ID,
	TAX_INFO_TYPE,
	ADDRESS_COUNTRY,
	TAX_INFO_TAX_ID,
	HUBSPOT_CUSTOMER_OBJECT_ID,
	HUBSPOT_CUSTOMER_MATCH_METHOD,
	HUBSPOT_CUSTOMER_GST_STATUS,
	HUBSPOT_CUSTOMER_CLIENT_ID,
	LATEST_PAYMENT_DATE,
	TOTAL_STRIPE_CREDIT_NOTES
) as (
    

/*
    Purpose:     Creates a  view of stripe customers including metadata
    Date:        20202402
    Description:  Creates a  view of stripe customers including metadata
*/


WITH last_payment_by_customer AS (
    SELECT
        pa.customer_id,
        Max(pa.created)::date AS latest_payment_date
    FROM  BILLING.PUBLISHED_PROD.STRIPE_PAYMENT_INTENTS  AS pa
    WHERE pa.status = 'succeeded'
    GROUP BY 1
),

stripe_credit_notes_per_customer AS (
    SELECT
        customer_id,
        Sum(amount) AS total_stripe_credit_notes
    FROM  BILLING.PUBLISHED_PROD.STRIPE_CREDIT_NOTES
    WHERE effective_at <= Current_date
    GROUP BY 1
)


SELECT
    customers.id,
    customers.name AS customer_name,
    customers.id AS stripe_customer_id,
    cmd_customerid.value::varchar AS myob_customer_id,
    cmd_crmid.value::varchar  AS customer_crm_id,
    customers.address_postal_code,
    customers.legal_entity,
    customers.delinquent AS delinquent_flag,
    cmd_segment.value AS customer_segment,
    customers.account_balance,
    customers.created AS customer_created,
    CASE WHEN
            customers.legal_entity = 'AU'
            THEN Convert_timezone('UTC','Australia/Melbourne', customers.created::datetime)
        WHEN customers.legal_entity = 'NZ' THEN Convert_timezone('UTC','Pacific/Auckland',  customers.created::datetime)
        ELSE customers.created::datetime
    END AS  customers_created_local,
    customers.currency,
    customers.default_source_id,
    customers.deleted,
    customers.delinquent,
    customers.invoice_settings_default_payment_method_id,
    customers.business_vat_id,
    customers.tax_info_type,
    customers.address_country,
    customers.tax_info_tax_id,
    CASE WHEN get_hubspot_customers.objectid IS NOT null THEN get_hubspot_customers.objectid
        WHEN get_hubspot_customers2.objectid IS NOT null THEN get_hubspot_customers2.objectid
        WHEN get_hubspot_customers3.objectid IS NOT null THEN get_hubspot_customers3.objectid
        WHEN get_hubspot_customers4.objectid IS NOT null THEN get_hubspot_customers4.objectid
    END AS hubspot_customer_object_id,
    CASE WHEN get_hubspot_customers.objectid IS NOT null THEN 'Stripe Customer Id'
        WHEN get_hubspot_customers2.objectid IS NOT null THEN 'MYOB Customer Id to Customer ID metadata'
        WHEN get_hubspot_customers3.objectid IS NOT null THEN 'CRM Id to MYOB Customer id'
        WHEN get_hubspot_customers4.objectid IS NOT null THEN 'CRM Id to Hubspot Object Id'
    END AS hubspot_customer_match_method,
    CASE WHEN get_hubspot_customers.property_gst_status  IS NOT null THEN get_hubspot_customers.property_gst_status
        WHEN get_hubspot_customers2.property_gst_status IS NOT null THEN get_hubspot_customers2.property_gst_status
        WHEN get_hubspot_customers3.property_gst_status IS NOT null THEN get_hubspot_customers3.property_gst_status
        WHEN get_hubspot_customers4.property_gst_status  IS NOT null THEN get_hubspot_customers4.property_gst_status
    END AS hubspot_customer_gst_status,
    CASE WHEN get_hubspot_customers.PROPERTY_CLIENT_ID__C  IS NOT null THEN get_hubspot_customers.PROPERTY_CLIENT_ID__C 
        WHEN get_hubspot_customers2.PROPERTY_CLIENT_ID__C IS NOT null THEN get_hubspot_customers2.PROPERTY_CLIENT_ID__C 
        WHEN get_hubspot_customers3.PROPERTY_CLIENT_ID__C IS NOT null THEN get_hubspot_customers3.PROPERTY_CLIENT_ID__C 
        WHEN get_hubspot_customers4.PROPERTY_CLIENT_ID__C  IS NOT null THEN get_hubspot_customers4.PROPERTY_CLIENT_ID__C 
    END AS hubspot_customer_client_id,
    last_payment.latest_payment_date,
    stripe_credit_notes_per_customer.total_stripe_credit_notes AS total_stripe_credit_notes
FROM  BILLING.PUBLISHED_PROD.STRIPE_CUSTOMERS AS customers
LEFT JOIN BILLING.PUBLISHED_PROD.STRIPE_CUSTOMERS_METADATA AS cmd_customerid
    ON customers.id = cmd_customerid.customer_id AND cmd_customerid.key = 'Customer ID'
LEFT JOIN BILLING.PUBLISHED_PROD.STRIPE_CUSTOMERS_METADATA AS cmd_segment
    ON customers.id= cmd_segment.customer_id AND cmd_segment.key = 'Customer Segment'
LEFT JOIN BILLING.PUBLISHED_PROD.STRIPE_CUSTOMERS_METADATA  AS cmd_crmid
    ON customers.id = cmd_crmid.customer_id AND cmd_crmid.key = 'CRMID'
LEFT JOIN CRM.RAW.HUBSPOT_OBJECTSCOMPANIES AS get_hubspot_customers ON get_hubspot_customers.property_stripe_customer_id::varchar = customers.id::varchar --join to hubspot customer based on stripe customer id
LEFT JOIN CRM.RAW.HUBSPOT_OBJECTSCOMPANIES  AS get_hubspot_customers2 ON get_hubspot_customers2.property_myob_customer_id::varchar = cmd_customerid.value::varchar AND cmd_customerid.value IS NOT null  ---join to hubspot customer on the myob customer id
LEFT JOIN CRM.RAW.HUBSPOT_OBJECTSCOMPANIES AS get_hubspot_customers3 ON get_hubspot_customers3.property_myob_customer_id::varchar = cmd_crmid.value::varchar AND cmd_crmid.value IS NOT null --join to hubspot customer based on crm id
LEFT JOIN CRM.RAW.HUBSPOT_OBJECTSCOMPANIES  AS get_hubspot_customers4 ON get_hubspot_customers4.objectid::varchar = cmd_crmid.value::varchar AND cmd_crmid.value IS NOT null --join to hubspot customer based on crm id
LEFT JOIN last_payment_by_customer AS last_payment ON customers.id = last_payment.customer_id
LEFT JOIN stripe_credit_notes_per_customer ON customers.id = stripe_credit_notes_per_customer.customer_id
QUALIFY Row_number() OVER (PARTITION BY  customers.id ORDER BY customers.id)  = 1
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_customers_with_metadata", "node_alias": "stripe_customers_with_metadata", "node_package_name": "stripe", "node_original_file_path": "models/stripe_customers_with_metadata.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_customers_with_metadata", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": [], "materialized": "view"} */;