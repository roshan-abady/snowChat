create or replace TRANSIENT TABLE STRIPE_INVOICES_UNPAID (
	CUSTOMER_ID VARCHAR(16777216),
	TOTAL_CREDIT_NOTES NUMBER(38,0),
	FIRST_30_DAYS NUMBER(38,0),
	FIRST_61_TO_90_DAYS NUMBER(38,0),
	OTHER_90_DAYS_PLUS NUMBER(38,0),
	TOTAL_SALES NUMBER(38,0),
	TOTAL_PAID NUMBER(38,0),
	LATEST_PAYMENT_DATE DATE,
	PRODUCT_NAMES VARCHAR(16777216),
	SIEBEL_BLOCKED VARCHAR(13),
	CURRENT_REMINDER_LEVEL VARCHAR(13),
	SALES_PERSON VARCHAR(19),
	STATE VARCHAR(19),
	CUSTOMER_DIMENSION VARCHAR(19),
	CREDIT_CONTROLLER VARCHAR(18)
);