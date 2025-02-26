create or replace TABLE ES_ERP_MONTH_END_ARR_TABLE_STG_ALL_CONTRACT (
	CONTRACTNUM VARCHAR(16777216),
	CONTRACTID NUMBER(38,0),
	ARREFNBR VARCHAR(16777216),
	PRODUCT_CLASS VARCHAR(16777216),
	BIMONTHID NUMBER(8,0),
	AUTOGENID VARCHAR(16777216),
	NEXT_GENID VARCHAR(16777216),
	GENDATE TIMESTAMP_NTZ(9),
	NEXT_GENDATE DATE,
	MANUALLYBOOKED BOOLEAN,
	LINENBR NUMBER(38,0),
	CONTRACT_MONTH_BEGIN NUMBER(18,0),
	CONTRACT_MONTH_END NUMBER(18,0),
	CONTRACT_START_TYPE VARCHAR(16777216),
	CONTRACT_NEW_FLAG NUMBER(1,0),
	CONTRACT_REVISION_FLAG NUMBER(1,0),
	CONTRACT_NEW_WITH_DUP_FLAG NUMBER(1,0),
	CONTRACT_REVISION_WITH_DUP_FLAG NUMBER(1,0),
	SAME_MONTH_FLAG NUMBER(1,0),
	ITEM_PROD VARCHAR(16777216),
	USRMAKLEDITIONID NUMBER(38,0),
	ARR FLOAT,
	SALESAMT FLOAT,
	EXPIREDDISCOUNT_ARR FLOAT,
	SUB_ACCOUNT NUMBER(38,0),
	PRICECHANGEANNUALISEDAMT FLOAT,
	CHURN_MONTHID NUMBER(8,0),
	REVISION_CHANGE_FLAG NUMBER(1,0),
	MAX_REVISION_BM VARCHAR(16777216),
	PARTNER_ACCOUNT_FLAG NUMBER(38,0),
	MAKLSYNCDATE TIMESTAMP_NTZ(9),
	QTY FLOAT,
	UOM VARCHAR(16777216),
	FIRST_ARR FLOAT,
	CLOSE_ARR FLOAT,
	FIRST_QTY FLOAT,
	CLOSE_QTY FLOAT,
	CHURN_MONTH_FLAG NUMBER(1,0),
	CONTRACT_ITEM_DESC NUMBER(18,0),
	SALES_AMOUNT_BY_PROD_ROLL_UP FLOAT,
	DISCOUNT_EXPIRY_BY_PROD_ROLL_UP FLOAT,
	PRICE_CHANGE_BY_PROD_ROLL_UP FLOAT
);