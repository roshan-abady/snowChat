create or replace view ES_ERP_CONTRACT_BY_PRODUCT_ITEM_EDITION_COMBINED(
	MONTH_ID,
	PRODUCT_SEGMENT,
	CUSTOMER_ID,
	ARCHIE_CLIENT_ID,
	CONTRACT_NUM,
	CONTRACT_ID,
	PRODUCT_CLASS,
	PROD_EDITION,
	PROD_ITEM_EDITION,
	ITEM_EDITION_DESCRIPTION,
	ITEM_EDITION_SHORT,
	OPEN_ARR,
	CLOSE_ARR,
	NET_ARR_CHANGE,
	ARR_NEW_ADDS,
	ARR_MIGRATION_ADDS,
	ARR_CANCELLED,
	ARR_PRICE_CHANGE,
	ARR_DISCOUNT_EXPIRED,
	ARR_EXPANSION,
	ARR_DOWNGRADE,
	ARR_OTHER_CHANGE,
	ARR_IMPORT,
	REGION,
	PROJECT,
	PARTNER,
	CLIENT_ID,
	ACCOUNT_NAME,
	NO_EMPLOYEE,
	INDUSTRY,
	SUB_INDUSTRY,
	CONTRACT_START_DT,
	COUNTRY,
	PRODUCTCLASS,
	NO_ARR_FLAG,
	CONTRACT_MAKLSYNCDATE,
	NEW_LOGO_FLAG,
	SUB_ACCOUNT,
	PARTNER_ACCOUNT_FLAG,
	MAX_GENDATE,
	MAX_GENMONTH,
	CHURN_MONTH_ID,
	REVISION_NUMBER_END_MONTH,
	REVISIONNBR,
	REV_COMMENTS,
	REV_AUTOGEN_ID,
	IMPORT_FLAG,
	PARTNER_SWITCH_FLAG,
	CONTRACT_CANCEL_DATE,
	CONTRACT_CANCEL_CODE,
	NEXT_CHARGE_DATE,
	ARCHIE_AGREE_NUM,
	ARCHIE_ASSET_NUM,
	INSERT_DATE,
	MIG_FLAG,
	MIG_TO,
	MIG_FROM,
	MIGRATION_PRACTICE,
	OPEN_EE_COUNT,
	CLOSE_EE_COUNT,
	AGREE_STATUS,
	FSWD_FLAG,
	COMMENT
) as(
SELECT
MONTH_ID,
	PRODUCT_SEGMENT,
	CUSTOMER_ID,
	ARCHIE_CLIENT_ID,
	CONTRACT_NUM,
	CONTRACT_ID,
	PRODUCT_CLASS,
	PROD_EDITION,
	PROD_ITEM_EDITION,
	ITEM_EDITION_DESCRIPTION,
	ITEM_EDITION_SHORT,
	OPEN_ARR,
	CLOSE_ARR,
	NET_ARR_CHANGE,
	ARR_NEW_ADDS,
	ARR_MIGRATION_ADDS,
	ARR_CANCELLED,
	ARR_PRICE_CHANGE,
	ARR_DISCOUNT_EXPIRED,
	ARR_EXPANSION,
	ARR_DOWNGRADE,
	ARR_OTHER_CHANGE,
	ARR_IMPORT,
	REGION,
	PROJECT,
	PARTNER,
	CLIENT_ID,
	ACCOUNT_NAME,
	NO_EMPLOYEE,
	INDUSTRY,
	SUB_INDUSTRY,
	CONTRACT_START_DT,
	COUNTRY,
	PRODUCTCLASS,
	NO_ARR_FLAG,
	CONTRACT_MAKLSYNCDATE,
	NEW_LOGO_FLAG,
	SUB_ACCOUNT,
	PARTNER_ACCOUNT_FLAG,
	MAX_GENDATE,
	MAX_GENMONTH,
	CHURN_MONTH_ID,
	REVISION_NUMBER_END_MONTH,
	REVISIONNBR,
	REV_COMMENTS,
	REV_AUTOGEN_ID,
	IMPORT_FLAG,
	PARTNER_SWITCH_FLAG,
	CONTRACT_CANCEL_DATE,
	CONTRACT_CANCEL_CODE,
	NEXT_CHARGE_DATE,
	ARCHIE_AGREE_NUM,
	ARCHIE_ASSET_NUM,
	INSERT_DATE,
	MIG_FLAG,
	MIG_TO,
	MIG_FROM,
	MIGRATION_PRACTICE,
	OPEN_EE_COUNT,
	CLOSE_EE_COUNT,
	AGREE_STATUS,
	FSWD_FLAG,
    null as COMMENT
   
from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.ES_ERP_CONTRACT_BY_PRODUCT_ITEM_EDITION

union all

select 
MONTH_ID,
	PRODUCT_SEGMENT,
	CUSTOMER_ID,
	ARCHIE_CLIENT_ID,
	CONTRACT_NUM,
	CONTRACT_ID,
	PRODUCT_CLASS,
	PROD_EDITION,
	PROD_ITEM_EDITION,
	ITEM_EDITION_DESCRIPTION,
	ITEM_EDITION_SHORT,
	OPEN_ARR,
	CLOSE_ARR,
	NET_ARR_CHANGE,
	ARR_NEW_ADDS,
	ARR_MIGRATION_ADDS,
	ARR_CANCELLED,
	ARR_PRICE_CHANGE,
	ARR_DISCOUNT_EXPIRED,
	ARR_EXPANSION,
	ARR_DOWNGRADE,
	ARR_OTHER_CHANGE,
	ARR_IMPORT,
	REGION,
	PROJECT,
	PARTNER,
	CLIENT_ID,
	ACCOUNT_NAME,
	NO_EMPLOYEE,
	INDUSTRY,
	SUB_INDUSTRY,
	CONTRACT_START_DT,
	COUNTRY,
	PRODUCTCLASS,
	NO_ARR_FLAG,
	CONTRACT_MAKLSYNCDATE,
	NEW_LOGO_FLAG,
	SUB_ACCOUNT,
	PARTNER_ACCOUNT_FLAG,
	MAX_GENDATE,
	MAX_GENMONTH,
	CHURN_MONTH_ID,
	REVISION_NUMBER_END_MONTH,
	REVISIONNBR,
	REV_COMMENTS,
	REV_AUTOGEN_ID,
	IMPORT_FLAG,
	PARTNER_SWITCH_FLAG,
	CONTRACT_CANCEL_DATE,
	CONTRACT_CANCEL_CODE,
	NEXT_CHARGE_DATE,
	ARCHIE_AGREE_NUM,
	ARCHIE_ASSET_NUM,
	INSERT_DATE,
	MIG_FLAG,
	MIG_TO,
	MIG_FROM,
	MIGRATION_PRACTICE,
	OPEN_EE_COUNT,
	CLOSE_EE_COUNT,
	AGREE_STATUS,
	FSWD_FLAG,
    COMMENT
    
    from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.CS_ENT_ARR_ADJ

);