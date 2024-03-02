create or replace TABLE ES_ERP_MONTH_END_ARR_TABLE (
	CONTRACTNUM VARCHAR(16777216),
	DISCOUNT_EXPIRY_ARR FLOAT,
	CONTRACTID NUMBER(38,0),
	PRODUCT_CLASS VARCHAR(16777216),
	BIMONTHID NUMBER(8,0),
	CONTRACT_NEW_WITH_DUP_FLAG NUMBER(1,0),
	CONTRACT_REVISION_WITH_DUP_FLAG NUMBER(1,0),
	CONTRACT_REVISION_FLAG NUMBER(1,0),
	CONTRACT_NEW_FLAG NUMBER(1,0),
	SAME_MONTH_FLAG NUMBER(1,0),
	ITEM_PROD VARCHAR(16777216),
	USRMAKLEDITIONID NUMBER(38,0),
	REVISION_CHANGE_FLAG NUMBER(1,0),
	CUR_MID DATE,
	PREV_MID DATE,
	SALES_AMOUNT FLOAT,
	PRICE_CHANGE FLOAT,
	FIRST_ARR FLOAT,
	CLOSE_ARR FLOAT,
	CHURN_MONTH_FLAG NUMBER(1,0),
	MAX_REVISION_BM VARCHAR(16777216),
	CONTRACT_MONTH_END NUMBER(18,0),
	CONTRACT_ITEM_DESC NUMBER(18,0),
	SUB_ACCOUNT NUMBER(38,0),
	PARTNER_ACCOUNT_FLAG NUMBER(38,0),
	MAKLSYNCDATE TIMESTAMP_NTZ(9),
	FIRST_QTY FLOAT,
	CLOSE_QTY FLOAT,
	UOM VARCHAR(16777216)
);