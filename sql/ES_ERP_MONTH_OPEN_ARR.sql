create or replace view ES_ERP_MONTH_OPEN_ARR(
	CONTRACTNUM,
	BIMONTHID,
	CONTRACTID,
	ITEM_PROD,
	SALES_AMOUNT,
	PRICE_CHANGE,
	CLOSE_ARR,
	SUB_ACCOUNT,
	CLOSE_QTY,
	UOM,
	MAKLSYNCDATE,
	CHURN_MONTH_FLAG,
	PRODUCT_CLASS,
	USRMAKLEDITIONID,
	CUR_MID,
	PREV_MID,
	NEXT_MID
) as (


SELECT
ContractNum
, Bimonthid
, contractid
, Item_Prod
, sum(salesamt) sales_amount
, sum(pricechangeannualisedamt) price_change
, sum(CASE WHEN CONTRACT_STATUS='HOLD'
            OR Churn_Monthid=Bimonthid
      THEN 0 ELSE CLOSE_ARR END) CLOSE_ARR
, SUB_ACCOUNT
, sum(CLOSE_QTY) CLOSE_QTY
, uom UOM
,MAKLSYNCDATE
,CHURN_MONTH_FLAG
,PRODUCT_CLASS
,USRMAKLEDITIONID
, date(concat(substring(CAST(Bimonthid AS varchar), 1, 4), '-', substring(CAST(Bimonthid AS varchar), 5, 6), '-01')) CUR_MID
, dateadd('month', -1, date(concat(substring(CAST(Bimonthid AS varchar), 1, 4), '-', substring(CAST(Bimonthid AS varchar), 5, 6), '-01'))) PREV_MID
, dateadd('month', 1, date(concat(substring(CAST(Bimonthid AS varchar), 1, 4), '-', substring(CAST(Bimonthid AS varchar), 5, 6), '-01'))) NEXT_MID
FROM
(
SELECT distinct
*
, (CASE WHEN ((contract_month_end = 1) AND (Churn_Monthid = BImonthid)) THEN 0 WHEN (contract_month_end = 1) THEN ARR ELSE 0 END) CLOSE_ARR
, (CASE WHEN ((contract_month_end = 1) AND (Churn_Monthid = BImonthid)) THEN 0 WHEN (contract_month_end = 1) THEN QTY ELSE 0 END) CLOSE_QTY
, (CASE WHEN ((Churn_Monthid = BImonthid) AND (contract_month_end = 1)) THEN 1 ELSE 0 END) CHURN_MONTH_FLAG
FROM
(
SELECT
contract.contractcd ContractNum
, monthid.bimonthid
, contract_line.autogenid
, contract_line_next.autogenid next_genid
, contract_line.gendate
, contract_line_next.gendate next_gendate
, contract_line.manuallybooked
, contract_line.linenbr
, contract_line.comments
, dense_rank() OVER (PARTITION BY contract.contractcd, monthid.bimonthid ORDER BY monthid.bimonthid ASC, (CASE WHEN (contract_line.arrefnbr IS NULL) THEN 0 ELSE 1 END) DESC, contract_line.linenbr ASC, contract_line.gendate ASC) contract_month_begin
, dense_rank() OVER (PARTITION BY contract.contractcd, monthid.bimonthid ORDER BY monthid.bimonthid DESC, ((year(contract_line.gendate) * 100) + month(contract_line.gendate)) DESC, (CASE WHEN (contract_line.gendate < contract_line.lastmodifieddatetime) THEN contract_line.gendate ELSE contract_line.lastmodifieddatetime END) DESC, contract_line.gendate DESC, contract_line.linenbr DESC) contract_month_end
, max((CASE WHEN (contract_line.autogenid IN ('NEWSL', 'REVIS')) THEN contract_line.autogenid END)) OVER (PARTITION BY contract_line.contractid, monthid.bimonthid, contract_line.linenbr) CONTRACT_START_TYPE
, max((CASE WHEN (contract_line.autogenid IN ('NEWSL', 'REVIS')) THEN contract_line.comments END)) OVER (PARTITION BY contract_line.contractid, monthid.bimonthid, contract_line.linenbr) CONTRACT_START_REASON
, contract_line.contractid
, inventoryitem.descr Item_Prod
, annualised.annualisedamt ARR
, annualised.salesamt
, annualised.pricechangeannualisedamt
--, (CASE WHEN (contract.status = 'X') THEN ((year(contract.cancelon) * 100) + month(contract.cancelon)) END) Churn_Monthid
--,(year(contract.cancelon) * 100) + month(contract.cancelon) AS Churn_Monthid
     ,year(case when status='X' and CANCELON is null then contract.LASTMODIFIEDDATETIME else CANCELON end)*100+
month(case when status='X' and CANCELON is null then contract.LASTMODIFIEDDATETIME else CANCELON end) AS Churn_Monthid
, annualised.subid SUB_ACCOUNT
, annualised.qty
, annualised.uom
, contract.maklsyncdate
, contract.classid Product_class
, inventoryitem.usrmakleditionid
FROM
(((((PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR contract
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
)) AND ((contract_line.manuallybooked = true) OR (monthid.bimonthid = ((year(current_date) * 100) + month(current_date))))) AND ((monthid.biyear <= year(GETDATE())) AND (monthid.bimonthid <= ((year(DATEADD('month', 1, GETDATE())) * 100) + month(DATEADD('month', 1, GETDATE())))))) AND (contract.maklsyncdate = (SELECT max(maklsyncdate)
FROM
PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR
)))
)  all_contract_item
WHERE (contract_month_end = 1)
)  all_contract
left join OPERATIONS_ANALYTICS.TRANSFORMED_NONPROD.ES_ERP_MONTHLY_CONTRACT_STATUS CONTRACT_STATUS
ON CONTRACT_STATUS.MONTH_ID=Bimonthid
AND CONTRACT_STATUS.CONTRACT_NUMBER=all_contract.ContractNum
WHERE (Bimonthid >= 201901)
--AND CONTRACTNUM='CT00005988'
GROUP BY ContractNum, Bimonthid, contractid, Item_Prod, SUB_ACCOUNT, UOM,MAKLSYNCDATE,CHURN_MONTH_FLAG,PRODUCT_CLASS,USRMAKLEDITIONID
);