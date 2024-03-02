create or replace TABLE ESERPARR_PROD_ITEM_EDITION_STATIC_2023 (
	MONTH_ID NUMBER(8,0),
	PRODUCT_SEGMENT VARCHAR(18),
	CUSTOMER_ID VARCHAR(16777216),
	ARCHIE_CLIENT_ID VARCHAR(16777216),
	CONTRACT_NUM VARCHAR(16777216),
	CONTRACT_ID NUMBER(38,0),
	PRODUCT_CLASS VARCHAR(16777216),
	PROD_EDITION VARCHAR(16777216),
	PROD_ITEM_EDITION VARCHAR(16777216),
	ITEM_EDITION_DESCRIPTION VARCHAR(16777216),
	ITEM_EDITION_SHORT VARCHAR(16777216),
	OPEN_ARR FLOAT,
	CLOSE_ARR FLOAT,
	NET_ARR_CHANGE FLOAT,
	ARR_NEW_ADDS FLOAT,
	ARR_MIGRATION_ADDS FLOAT,
	ARR_CANCELLED FLOAT,
	ARR_PRICE_CHANGE FLOAT,
	ARR_DISCOUNT_EXPIRED FLOAT,
	ARR_EXPANSION FLOAT,
	ARR_DOWNGRADE FLOAT,
	ARR_OTHER_CHANGE FLOAT,
	ARR_IMPORT FLOAT,
	REGION VARCHAR(16777216),
	PROJECT VARCHAR(16777216),
	PARTNER VARCHAR(16777216),
	CLIENT_ID VARCHAR(16777216),
	ACCOUNT_NAME VARCHAR(16777216),
	NO_EMPLOYEE NUMBER(38,0),
	INDUSTRY VARCHAR(16777216),
	SUB_INDUSTRY VARCHAR(16777216),
	CONTRACT_START_DT TIMESTAMP_NTZ(9),
	COUNTRY VARCHAR(16777216),
	PRODUCTCLASS VARCHAR(16777216),
	NO_ARR_FLAG NUMBER(1,0),
	CONTRACT_MAKLSYNCDATE TIMESTAMP_NTZ(9),
	NEW_LOGO_FLAG NUMBER(1,0),
	SUB_ACCOUNT NUMBER(38,0),
	PARTNER_ACCOUNT_FLAG NUMBER(38,0),
	MAX_GENDATE TIMESTAMP_NTZ(9),
	MAX_GENMONTH NUMBER(8,0),
	CHURN_MONTH_ID NUMBER(8,0),
	REVISION_NUMBER_END_MONTH NUMBER(38,0),
	REVISIONNBR NUMBER(38,0),
	REV_COMMENTS VARCHAR(16777216),
	REV_AUTOGEN_ID VARCHAR(16777216),
	IMPORT_FLAG NUMBER(1,0),
	PARTNER_SWITCH_FLAG NUMBER(1,0),
	CONTRACT_CANCEL_DATE TIMESTAMP_NTZ(9),
	CONTRACT_CANCEL_CODE VARCHAR(16777216),
	NEXT_CHARGE_DATE TIMESTAMP_NTZ(9),
	MIG_FLAG NUMBER(1,0),
	MIG_TO VARCHAR(16777216),
	MIG_FROM VARCHAR(16777216),
	MIGRATION_PRACTICE VARCHAR(1)
);