create or replace view ES_ERP_MONTH_END_ARR(
	CONTRACTNUM,
	DISCOUNT_EXPIRY_ARR,
	CONTRACTID,
	PRODUCT_CLASS,
	BIMONTHID,
	CONTRACT_NEW_WITH_DUP_FLAG,
	CONTRACT_REVISION_WITH_DUP_FLAG,
	CONTRACT_REVISION_FLAG,
	CONTRACT_NEW_FLAG,
	SAME_MONTH_FLAG,
	ITEM_PROD,
	USRMAKLEDITIONID,
	REVISION_CHANGE_FLAG,
	CUR_MID,
	PREV_MID,
	SALES_AMOUNT,
	PRICE_CHANGE,
	FIRST_ARR,
	CLOSE_ARR,
	CHURN_MONTH_FLAG,
	MAX_REVISION_BM,
	CONTRACT_MONTH_END,
	CONTRACT_ITEM_DESC,
	SUB_ACCOUNT,
	PARTNER_ACCOUNT_FLAG,
	MAKLSYNCDATE,
	FIRST_QTY,
	CLOSE_QTY,
	UOM
) as (

SELECT
--Churn_Monthid,
all_contract.ContractNum
, sum(COALESCE(discount_expiry_by_prod_roll_up, 0)) discount_expiry_ARR
, contractid
, Product_class
, Bimonthid
, CONTRACT_NEW_WITH_DUP_FLAG
, CONTRACT_REVISION_WITH_DUP_FLAG
, CONTRACT_REVISION_FLAG
, CONTRACT_NEW_FLAG
, SAME_MONTH_FLAG
, Item_Prod
, usrmakleditionid
, REVISION_CHANGE_FLAG
, date(concat(substring(CAST(Bimonthid AS varchar), 1, 4), '-', substring(CAST(Bimonthid AS varchar), 5, 6), '-01')) CUR_MID
, dateadd('month', -1, date(concat(substring(CAST(Bimonthid AS varchar), 1, 4), '-', substring(CAST(Bimonthid AS varchar), 5, 6), '-01'))) PREV_MID
, sum(sales_amount_by_prod_roll_up) sales_amount
, sum(price_change_by_prod_roll_up) price_change
, sum(FIRST_ARR) FIRST_ARR
, sum(CASE WHEN CONTRACT_STATUS='HOLD'
      OR Churn_Monthid=Bimonthid
      THEN 0 ELSE CLOSE_ARR END) CLOSE_ARR
, CHURN_MONTH_FLAG
, MAX_REVISION_BM
, contract_month_end
, CONTRACT_ITEM_DESC
, SUB_ACCOUNT
, PARTNER_ACCOUNT_FLAG
, maklsyncdate
, sum(FIRST_QTY) FIRST_QTY
, sum(CLOSE_QTY) CLOSE_QTY
, uom UOM
FROM
(
SELECT
 *
, (CASE WHEN ((contract_month_begin = 1) AND (CONTRACT_NEW_FLAG = 1)) THEN ARR ELSE 0 END) FIRST_ARR
, (CASE WHEN ((contract_month_end = 1) AND (Churn_Monthid = BImonthid)) THEN 0 WHEN (contract_month_end = 1) THEN ARR ELSE 0 END) CLOSE_ARR
, (CASE WHEN ((contract_month_begin = 1) AND (CONTRACT_NEW_FLAG = 1)) THEN QTY ELSE 0 END) FIRST_QTY
, (CASE WHEN ((contract_month_end = 1) AND (Churn_Monthid = BImonthid)) THEN 0 WHEN (contract_month_end = 1) THEN QTY ELSE 0 END) CLOSE_QTY
, (CASE WHEN ((Churn_Monthid = BImonthid) AND (contract_month_end = 1)) THEN 1 ELSE 0 END) CHURN_MONTH_FLAG
, row_number() OVER (PARTITION BY ContractNum, Item_Prod, bimonthid, SUB_ACCOUNT, uom ORDER BY (CASE WHEN ((contract_month_end = 1) AND (Churn_Monthid = BImonthid)) THEN 0 WHEN (contract_month_end = 1) THEN abs(ARR) ELSE 0 END) DESC) CONTRACT_ITEM_DESC
, (CASE WHEN ((row_number() OVER (PARTITION BY ContractNum, Item_Prod, bimonthid ORDER BY (CASE WHEN ((contract_month_end = 1) AND (Churn_Monthid = BImonthid)) THEN 0 WHEN (contract_month_end = 1) THEN abs(ARR) ELSE 0 END) DESC) = 1) AND (SAME_MONTH_FLAG = 1)) THEN sum(salesamt) OVER (PARTITION BY ContractNum, BImonthid, Item_Prod) ELSE 0 END) sales_amount_by_prod_roll_up
, (CASE WHEN ((row_number() OVER (PARTITION BY ContractNum, Item_Prod, bimonthid ORDER BY (CASE WHEN ((contract_month_end = 1) AND (Churn_Monthid = BImonthid)) THEN 0 WHEN (contract_month_end = 1) THEN abs(ARR) ELSE 0 END) DESC) = 1) AND (SAME_MONTH_FLAG = 1)) THEN sum(ExpiredDiscount_ARR) OVER (PARTITION BY ContractNum, BImonthid, Item_Prod) ELSE 0 END) discount_expiry_by_prod_roll_up
, (CASE WHEN ((row_number() OVER (PARTITION BY ContractNum, Item_Prod, bimonthid ORDER BY (CASE WHEN ((contract_month_end = 1) AND (Churn_Monthid = BImonthid)) THEN 0 WHEN (contract_month_end = 1) THEN abs(ARR) ELSE 0 END) DESC) = 1) AND (SAME_MONTH_FLAG = 1)) THEN sum(pricechangeannualisedamt) OVER (PARTITION BY ContractNum, BImonthid, Item_Prod) ELSE 0 END) price_change_by_prod_roll_up
FROM
 (
  SELECT
	contract.contractcd ContractNum
  , contract_line.contractid
  , contract_line.arrefnbr
  , contract.classid Product_class
  , monthid.bimonthid
  , contract_line.autogenid
  , contract_line_next.autogenid next_genid
  , contract_line.gendate
  , contract_line_next.gendate next_gendate
  , contract_line.manuallybooked
  , contract_line.linenbr
  , dense_rank() OVER (PARTITION BY contract.contractcd, monthid.bimonthid ORDER BY monthid.bimonthid ASC, (CASE WHEN (contract_line.arrefnbr IS NULL) THEN 0 ELSE 1 END) DESC, contract_line.linenbr ASC, contract_line.gendate ASC) contract_month_begin
  , dense_rank() OVER (PARTITION BY contract.contractcd, monthid.bimonthid ORDER BY monthid.bimonthid DESC, ((year(contract_line.gendate) * 100) + month(contract_line.gendate)) DESC, (CASE WHEN (contract_line.gendate < contract_line.lastmodifieddatetime) THEN contract_line.gendate ELSE contract_line.lastmodifieddatetime END) DESC, contract_line.gendate DESC, contract_line.linenbr DESC) contract_month_end
  , max((CASE WHEN (contract_line.autogenid IN ('NEWSL', 'REVIS')) THEN contract_line.autogenid END)) OVER (PARTITION BY contract_line.contractid, monthid.bimonthid) CONTRACT_START_TYPE
  , (CASE WHEN (monthid.bimonthid = ((year(contract_line.gendate) * 100) + month(contract_line.gendate))) THEN max((CASE WHEN (contract_line.autogenid IN ('NEWSL')) THEN 1 ELSE 0 END)) OVER (PARTITION BY contract.contractcd, monthid.bimonthid) ELSE 0 END) CONTRACT_NEW_FLAG
  , (CASE WHEN (monthid.bimonthid = ((year(contract_line.gendate) * 100) + month(contract_line.gendate))) THEN max((CASE WHEN (contract_line.autogenid IN ('REVIS')) THEN 1 ELSE 0 END)) OVER (PARTITION BY contract.contractcd, monthid.bimonthid) ELSE 0 END) CONTRACT_REVISION_FLAG
  , max((CASE WHEN (contract_line.autogenid IN ('NEWSL')) THEN 1 ELSE 0 END)) OVER (PARTITION BY contract.contractcd, monthid.bimonthid) CONTRACT_NEW_WITH_DUP_FLAG
  , max((CASE WHEN (contract_line.autogenid IN ('REVIS')) THEN 1 ELSE 0 END)) OVER (PARTITION BY contract.contractcd, monthid.bimonthid) CONTRACT_REVISION_WITH_DUP_FLAG
  , (CASE WHEN (monthid.bimonthid = ((year(contract_line.gendate) * 100) + month(contract_line.gendate))) THEN 1 ELSE 0 END) SAME_MONTH_FLAG
  , inventoryitem.descr Item_Prod
  , inventoryitem.usrmakleditionid
  , annualised.annualisedamt ARR
  , annualised.salesamt
  , (CASE WHEN (contract_line.autogenid = 'RECUR') THEN annualised.salesamt END) ExpiredDiscount_ARR
  , annualised.subid SUB_ACCOUNT
  , annualised.pricechangeannualisedamt
 -- , (CASE WHEN (contract.status = 'X') THEN ((year(contract.cancelon) * 100) + month(contract.cancelon)) END) Churn_Monthid
 -- ,(year(contract.cancelon) * 100) + month(contract.cancelon)
   ,year(case when status='X' and CANCELON is null then contract.LASTMODIFIEDDATETIME else CANCELON end)*100+
month(case when status='X' and CANCELON is null then contract.LASTMODIFIEDDATETIME else CANCELON end) AS Churn_Monthid
  , (CASE WHEN (((CASE WHEN (monthid.bimonthid = ((year(contract_line.gendate) * 100) + month(contract_line.gendate))) THEN max((CASE WHEN (contract_line.autogenid IN ('REVIS')) THEN 1 ELSE 0 END)) OVER (PARTITION BY contract_line.contractid, monthid.bimonthid) ELSE 0 END) = 1) AND (max(revisionnbr) OVER (PARTITION BY contract.contractcd) <> min(revisionnbr) OVER (PARTITION BY contract.contractcd, monthid.bimonthid))) THEN 1 ELSE 0 END) REVISION_CHANGE_FLAG
  , max(revisionnbr) OVER (PARTITION BY contract.contractcd, monthid.bimonthid) MAX_REVISION_BM
  , partner_flg.baccountid PARTNER_ACCOUNT_FLAG
  , contract.maklsyncdate
  , annualised.qty
  , annualised.uom
  FROM
	((((((PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR contract
  LEFT JOIN (
	 SELECT
	   *
	 , max((CASE WHEN (autogenid IN ('NEWSL', 'REVIS', 'RECUR')) THEN 1 ELSE 0 END)) OVER (PARTITION BY contractid, year(gendate), month(gendate)) HAS_NON_UPSEL_CONTRACT_LINE_FLAG
	 , row_number() OVER (PARTITION BY contractid ORDER BY gendate ASC, linenbr ASC) contract_month_begin
	 FROM
	   PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLXRBCONTRDETPREVNEXT
  )  contract_line ON ((contract_line.contractid = contract.contractid) AND (contract.maklsyncdate = contract_line.maklsyncdate)))
  LEFT JOIN OPERATIONS_ANALYTICS.TRANSFORMED_PROD.ES_PHOENIX_CONTRACT_PREVIOUS_LINE contract_line_next ON (((contract.contractcd = contract_line_next.contractcd) AND (CAST(contract_line_next.prev_linenbr AS integer) = contract_line.linenbr)) AND (contract_line.maklsyncdate = contract_line_next.maklsyncdate)))
  LEFT JOIN PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLXRBANNUALISEDVALUE annualised ON (((contract_line.contractid = annualised.contractid) AND (contract_line.linenbr = annualised.detlinenbr)) AND (annualised.maklsyncdate = contract_line.maklsyncdate)))
  LEFT JOIN (
	 SELECT *
	 FROM
	   PURCHASING.PUBLISHED_PROD.PHOENIX_INVENTORYITEM
	 WHERE (maklsyncdate IN (SELECT max(maklsyncdate)
FROM
PURCHASING.PUBLISHED_PROD.PHOENIX_INVENTORYITEM
))
  )  inventoryitem ON (inventoryitem.inventoryid = annualised.inventoryid))
  LEFT JOIN (
	 SELECT baccountid
	 FROM
	   PURCHASING.PUBLISHED_PROD.PHOENIX_VENDOR
	 WHERE ((maklsyncdate = (SELECT max(maklsyncdate)
FROM
PURCHASING.PUBLISHED_PROD.PHOENIX_VENDOR
)) AND (baccountid <> 9259))
  )  partner_flg ON (contract.customerid = partner_flg.baccountid))
  INNER JOIN (
	 SELECT DISTINCT
	   year(bidate) BIYear
	 , month(bidate) BIMonth
	 , ((year(bidate) * 100) + month(bidate)) BImonthid
	 FROM
	   PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLBIDATE
  )  monthid ON (((monthid.bimonthid <= ((100 * year(contract.cancelon)) + month(contract.cancelon))) OR (contract.cancelon IS NULL)) AND ((monthid.bimonthid = ((year(contract_line.gendate) * 100) + month(contract_line.gendate))) OR ((monthid.bimonthid > ((year(contract_line.gendate) * 100) + month(contract_line.gendate))) AND ((monthid.bimonthid < ((year(contract_line_next.gendate) * 100) + month(contract_line_next.gendate))) OR (contract_line_next.gendate IS NULL))))))
  WHERE ((((contract_line.maklsyncdate = (SELECT max(maklsyncdate)
FROM
PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLXRBCONTRDETPREVNEXT
)) AND (((contract_line.manuallybooked = true) OR (monthid.bimonthid = ((year(current_date) * 100) + month(current_date)))) OR (monthid.bimonthid = ((100 * year(contract.cancelon)) + month(contract.cancelon))))) AND ((monthid.biyear <= year(GETDATE())) AND (monthid.bimonthid <= ((year(dateadd('month', 1, GETDATE())) * 100) + month(dateadd('month', 1, GETDATE())))))) AND (contract.maklsyncdate = (SELECT max(maklsyncdate)
FROM
PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR
)))
)  all_contract_item
)  all_contract
left join OPERATIONS_ANALYTICS.TRANSFORMED_NONPROD.ES_ERP_MONTHLY_CONTRACT_STATUS CONTRACT_STATUS
ON CONTRACT_STATUS.MONTH_ID=Bimonthid
AND CONTRACT_STATUS.CONTRACT_NUMBER=all_contract.ContractNum
WHERE ((Bimonthid >= 201901) AND (CONTRACT_ITEM_DESC = 1))
--AND CONTRACTNUM='CT00005988'
--AND Bimonthid IN ('202107')
-- AND CONTRACTNUM='CT00006781'--'CT00005687'
GROUP BY ContractNum, Bimonthid, CONTRACT_NEW_WITH_DUP_FLAG, CONTRACT_REVISION_WITH_DUP_FLAG, CONTRACT_REVISION_FLAG, CONTRACT_NEW_FLAG, SAME_MONTH_FLAG, contractid, Item_Prod, usrmakleditionid, REVISION_CHANGE_FLAG, MAX_REVISION_BM, contract_month_end, CONTRACT_ITEM_DESC, CHURN_MONTH_FLAG, Product_class, SUB_ACCOUNT, PARTNER_ACCOUNT_FLAG, maklsyncdate, uom

--,Churn_Monthid
);