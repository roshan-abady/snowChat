create or replace TABLE ES_ERP_PAYROLL_ARR_AND_MOVEMENT_SUMMARY_BY_COUNTRY (
	MONTH_ID NUMBER(38,0),
	COUNTRY VARCHAR(16777216),
	CLOSE_ARR FLOAT,
	COUNT_CONTRACTS NUMBER(18,0),
	COUNT_CUSTOMERS NUMBER(18,0),
	ARPU FLOAT
);