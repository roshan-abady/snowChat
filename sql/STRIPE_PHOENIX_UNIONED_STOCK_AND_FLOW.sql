create or replace view STRIPE_PHOENIX_UNIONED_STOCK_AND_FLOW(
	ACCOUNT_NAME,
	AGREE_STATUS,
	ARCHIE_AGREE_NUM,
	ARCHIE_ASSET_NUM,
	ARCHIE_CLIENT_ID,
	ARR_ADDS_NEW_AND_MIGRATION,
	ARR_CANCELLED,
	ARR_DISCOUNT_EXPIRED,
	ARR_DOWNGRADE,
	ARR_EXPANSION,
	ARR_IMPORT,
	ARR_MIGRATION_ADDS,
	ARR_NEW_ADDS,
	ARR_OTHER_CHANGE,
	ARR_PRICE_CHANGE,
	CHURN_MONTH_ID,
	CLIENT_ID,
	CLOSE_ARR,
	CLOSE_EE_COUNT,
	CONTRACT_CANCEL_CODE,
	CONTRACT_CANCEL_DATE,
	CONTRACT_ID,
	CONTRACT_MAKLSYNCDATE,
	CONTRACT_NUM,
	CONTRACT_START_DT,
	COUNTRY,
	CUSTOMER_ID,
	DATA_EXTRACTION_DATE,
	DROPLET_LINKING_KEY,
	ENTERPRISE_CUSTOMER_SEGMENT,
	FSWD_FLAG,
	IMPORT_FLAG,
	INDUSTRY,
	INPUT_FILE_NAME,
	INSERT_DATE,
	ITEM_EDITION_DESCRIPTION,
	ITEM_EDITION_SHORT,
	MAX_GENDATE,
	MAX_GENMONTH,
	MIGRATION_PRACTICE,
	MIG_FLAG,
	MIG_FROM,
	MIG_TO,
	MONTH_ID,
	NET_ARR_CHANGE,
	NEW_LOGO_FLAG,
	NEXT_CHARGE_DATE,
	NO_ARR_FLAG,
	NUM_EMPLOYEE,
	OPEN_ARR,
	OPEN_EE_COUNT,
	PARQUET_FILE_NAME,
	PARTNER,
	PARTNER_ACCOUNT_FLAG,
	PARTNER_SWITCH_FLAG,
	PRODUCTCLASS,
	PRODUCT_CLASS,
	PRODUCT_SEGMENT,
	PROD_EDITION,
	PROD_ITEM_EDITION,
	PROJECT,
	REGION,
	REVISIONNBR,
	REVISION_NUMBER_END_MONTH,
	REV_AUTOGEN_ID,
	REV_COMMENTS,
	STATE,
	SUB_ACCOUNT,
	SUB_INDUSTRY,
	SOURCE
) as (
    WITH phoenix AS (
    SELECT
        account_name,
        agree_status,
        archie_agree_num,
        archie_asset_num,
        archie_client_id,
        arr_adds_new_and_migration,
        arr_cancelled,
        arr_discount_expired,
        arr_downgrade,
        arr_expansion,
        arr_import,
        arr_migration_adds,
        arr_new_adds,
        arr_other_change,
        arr_price_change,
        churn_month_id,
        client_id,
        close_arr,
        close_ee_count,
        contract_cancel_code,
        contract_cancel_date,
        contract_id,
        contract_maklsyncdate,
        contract_num,
        contract_start_dt,
        country,
        customer_id,
        data_extraction_date,
        droplet_linking_key,
        enterprise_customer_segment,
        fswd_flag,
        import_flag,
        industry,
        input_file_name,
        insert_date,
        item_edition_description,
        item_edition_short,
        max_gendate,
        max_genmonth,
        migration_practice,
        mig_flag,
        mig_from,
        mig_to,
        month_id,
        net_arr_change,
        new_logo_flag,
        next_charge_date,
        no_arr_flag,
        num_employee,
        open_arr,
        open_ee_count,
        parquet_file_name,
        partner,
        partner_account_flag,
        partner_switch_flag,
        productclass,
        product_class,
        product_segment,
        prod_edition,
        prod_item_edition,
        project,
        region,
        revisionnbr,
        revision_number_end_month,
        rev_autogen_id,
        rev_comments,
        state,
        sub_account,
        sub_industry,
        'PHOENIX' AS source
    FROM OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stg_es_erp_payroll_arr_and_movement
),

stripe AS (
    SELECT
        customer_name AS account_name,
        subscription_status AS agree_status,
        subscription_id AS archie_agree_num,
        null AS archie_asset_num,
        hubspot_company_archie_client_id AS archie_client_id,
        null AS arr_adds_new_and_migration,
        null AS arr_cancelled,
        null AS arr_discount_expired,
        null AS arr_downgrade,
        null AS arr_expansion,
        null AS arr_import,
        null AS arr_migration_adds,
        null AS arr_new_adds,
        null AS arr_other_change,
        null AS arr_price_change,
        To_char(canceled_at_date_local, 'YYYYMM') AS churn_month_id,
        subscription_id AS client_id,
        null AS close_arr,
        null AS close_ee_count,
        Coalesce(cancellation_reason, cancellation_reason_text) AS contract_cancel_code,
        canceled_at_date_local AS contract_cancel_date,
        subscription_id AS contract_id,
        null AS contract_maklsyncdate,
        subscription_id AS contract_num,
        subscription_start_date_time_local::date AS contract_start_dt,
        address_country AS country,
        customer_id,
        null AS data_extraction_date,
        null AS droplet_linking_key,
        customer_segment AS enterprise_customer_segment,
        null AS fswd_flag,
        null AS import_flag,
        null AS industry,
        null AS input_file_name,
        null AS insert_date,
        plan_nickname AS item_edition_description,
        Coalesce(price_myob_advanced_edition,plan_nickname) AS item_edition_short,
        null AS max_gendate,
        null AS max_genmonth,
        null AS migration_practice,
        null AS mig_flag,
        null AS mig_from,
        null AS mig_to,
        month_id,
        null AS net_arr_change,
        num_added AS new_logo_flag,
        Coalesce(current_period_end, current_period_start) AS next_charge_date,
        0 AS no_arr_flag,
        null AS num_employee,
        null AS open_arr,
        null AS open_ee_count,
        null AS parquet_file_name,
        null AS partner,
        0 AS partner_account_flag,
        0 AS partner_switch_flag,
        null AS productclass,
        null AS product_class,
        null AS product_segment,
        null AS prod_edition,
        null AS prod_item_edition,
        null AS project,
        null AS region,
        null AS revisionnbr,
        null AS revision_number_end_month,
        null AS rev_autogen_id,
        null AS rev_comments,
        null AS state,
        null AS sub_account,
        null AS sub_industry,
        'STRIPE' AS source
    FROM OPERATIONS_ANALYTICS.TRANSFORMED_PROD.stripe_stock_and_flow_by_subscription_item_id_by_month
)

SELECT *
FROM phoenix
UNION ALL
SELECT *
FROM stripe
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "stripe", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "1167d7ae-02a0-438c-a6d8-e30b4684dca6", "node_name": "stripe_phoenix_unioned_stock_and_flow", "node_alias": "stripe_phoenix_unioned_stock_and_flow", "node_package_name": "stripe", "node_original_file_path": "models/stripe_phoenix_unioned_stock_and_flow.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.stripe.stripe_phoenix_unioned_stock_and_flow", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": ["stg_es_erp_payroll_arr_and_movement", "stripe_stock_and_flow_by_subscription_item_id_by_month"], "materialized": "view"} */;