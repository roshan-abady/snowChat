create or replace view ES_ERP_CONTRACT_BY_PRODUCT(
	PROD_EDITION,
	PROD_ITEM_EDITION,
	CUSTOMERID,
	REVISIONNBR,
	REV_COMMENTS,
	REV_AUTOGEN_ID,
	IMPORT_FLAG,
	PARTNER_SWITCH_FLAG,
	REGION,
	PROJECT,
	PARTNER,
	NO_EMPLOYEE,
	CLIENT_ID,
	ACCOUNT_NAME,
	ARCHIE_CLIENT_ID,
	INDUSTRY,
	SUB_INDUSTRY,
	CONTRACT_START_DT,
	CONTRACT_CANCEL_DATE,
	CONTRACT_CANCEL_CODE,
	COUNTRY,
	PRODUCTCLASS,
	NEXT_CHARGE_DATE,
	CONTRACT_MAKLSYNCDATE,
	UNION_FLAG,
	MONTH_ID,
	CONTRACT_NUM,
	CONTRACT_ID,
	PRODUCT_CLASS,
	ITEM_PROD,
	ITEM_EDITION_ID,
	SALES_AMOUNT,
	DISCOUNT_EXPIRY_ARR,
	PRICE_CHANGE,
	CLOSE_ARR,
	OPEN_ARR,
	NEW_LOGO_FLAG,
	REVISION_FLAG,
	CHURN_MONTH_FLAG,
	REVISION_CHANGE_FLAG,
	CONTRACT_ITEM_MONTH_END_FLAG,
	REVISION_NUMBER_END_MONTH,
	SUB_ACCOUNT,
	PARTNER_ACCOUNT_FLAG,
	CLOSE_QTY,
	OPEN_QTY,
	UOM,
	ROW_UNION_FLAG,
	ITEM_EDITION_DESCRIPTION,
	ITEM_EDITION_SHORT,
	MAX_GENDATE,
	MAX_GENMONTH,
	CHURN_MONTH_ID
) as (
        --Last updated 20211126 Add VELIXO products
        --ERP MONTH OPEN  "OPERATIONS_ANALYTICS"."TRANSFORMED_PROD".ES_ERP_MONTH_OPEN_ARR
        --ERP MONTH END "OPERATIONS_ANALYTICS"."TRANSFORMED_PROD".ES_ERP_MONTH_END_ARR
        --END LEFT JOIN OPEN "OPERATIONS_ANALYTICS"."TRANSFORMED_PROD".ES_ERP_END_LEFT_JOIN_OPEN
        --MONTH_END RIGHT JOIN OPEN "OPERATIONS_ANALYTICS"."TRANSFORMED_PROD".ES_ERP_END_RIGHT_JOIN_OPEN
        --20211117 Split GTR to Payroll and WFM

SELECT *
FROM
  (
   SELECT
     (CASE WHEN ((PRODUCT_CLASS = 'ADV') AND (Item_Edition_Description IN ('ProfessionalServices', 'Manufacturing', 'Construction', 'Standard', 'Plus', 'Enterprise'))) THEN 'Advance Business'
         WHEN ((PRODUCT_CLASS = 'ADV') AND (Item_Edition_Description IN ('People Shared', 'People', 'Workforce Management'))) THEN 'Advance People'
         WHEN ((PRODUCT_CLASS = 'EXO') AND (Item_Edition_Description IN ('Business'))) THEN 'EXO Business' WHEN ((PRODUCT_CLASS = 'EXO') AND (Item_Edition_Description LIKE 'Employer Services%')) THEN 'EXO Employer Services'
         WHEN (PRODUCT_CLASS = 'GTR') THEN Item_Edition_Description
         WHEN (PRODUCT_CLASS = 'GRT') AND Item_Edition_Description IS NOT NULL THEN Item_Edition_Description
         WHEN (PRODUCT_CLASS = 'GRT') THEN 'Great Soft'
         ELSE 'Other' END) PROD_EDITION
   , (CASE
       WHEN (PRODUCT_CLASS = 'ADV') AND (UPPER(Item_Edition_Description) LIKE '%VELIXO%' ) THEN 'Distributed addons'
       WHEN ((PRODUCT_CLASS = 'ADV') AND (Item_Edition_Description IN ('ProfessionalServices', 'Manufacturing', 'Construction', 'Standard', 'Plus', 'Enterprise', 'Workforce Management')))  THEN Item_Edition_Description
       WHEN ((PRODUCT_CLASS = 'ADV') AND (Item_Edition_Description IN ('People Shared', 'People'))) THEN Item_Edition_Description
       WHEN ((PRODUCT_CLASS = 'EXO') AND (Item_Edition_Description IN ('Business'))) THEN 'EXO Business'
       WHEN ((PRODUCT_CLASS = 'EXO') AND (Item_Edition_Description LIKE 'Employer Services%')) THEN 'EXO Employer Services'
        WHEN PRODUCT_CLASS = 'GTR'
            and ITEM_PROD IN ('Mobile  Maintenance Timesheet Entry','Maintenance Timesheet','Maintenance HR Recruitment','Maintenance HR Leave Planning','Maintenance eTimesheets Users','Maintenance eTimesheets','HR Recruitment','HR Leave Planning','eTimesheets Users','eTimesheets')
        THEN 'Greentree WFM'
        WHEN PRODUCT_CLASS = 'GTR'
            and ITEM_PROD IN ('Payroll','Maintenance Payroll','Maintenance Human Resources Management','Maintenance HR Timecards','Maintenance HR','Maintenance Employee Development','Maintenance eHRM less 100','Maintenance eHRM','Maintenance API Human Resources','Maintenance Active # PR/HR Employees','Human Resources Management','HUMAN RESOURCES','HR Timecards','HR','Active # PR/HR Employees','Mobile  Maintenance Timesheet Entry')
        THEN 'Greentree Payroll'
        WHEN upper(ITEM_PROD) LIKE '%GREATSFOT%'         THEN Item_Edition_Description
        WHEN PRODUCT_CLASS = 'GRT' AND Item_Edition_Description IS not NULL                      THEN Item_Edition_Description
        WHEN PRODUCT_CLASS = 'GRT'                        THEN 'Great Soft'
       WHEN (PRODUCT_CLASS = 'GTR') THEN Item_Edition_Description ELSE 'Other' END) PROD_ITEM_EDITION
   , *
   FROM
     (
      SELECT
        contract.customerid
      , CAST(contract.revisionnbr AS integer) REVISIONNBR
      , CONTRACT_COMMENTS.comments REV_COMMENTS
      , CONTRACT_COMMENTS.autogenid REV_AUTOGEN_ID
      , CONTRACT_COMMENTS.import_flag
      , CONTRACT_COMMENTS.partner_switch_flag
      , (CASE project.usrmaklsalesregionid WHEN 'AUEST' THEN 'East' WHEN 'AUNTH' THEN 'North' WHEN 'AUSTH' THEN 'South' ELSE project.usrmaklsalesregionid END) REGION
      , project.description PROJECT
      , partner.acctname PARTNER
      , ba.usrmaklnumberofemployees NO_EMPLOYEE
      , ba.acctcd CLIENT_ID
      , BA.acctname ACCOUNT_NAME
      , BA.usrmaklarchieid ARCHIE_CLIENT_ID
      , (CASE WHEN (industry.industry IS NULL) THEN 'n/a' ELSE industry.industry END) INDUSTRY
      , (CASE WHEN (sub_industry."sub industry" IS NULL) THEN 'n/a' ELSE sub_industry."sub industry" END) SUB_INDUSTRY
      , contract.begdate CONTRACT_START_DT
      , contract.cancelon CONTRACT_CANCEL_DATE
      , contract.cancelcode CONTRACT_CANCEL_CODE
      , branch.countryid Country
      , (CASE contract.classid WHEN 'EXO' THEN 'Exo' WHEN 'GTR' THEN 'Greentree' WHEN 'GRT' THEN 'GreatSoft' WHEN 'ADV' THEN 'Advanced' END) ProductClass
      , NC_DATE.next_charge_date
      , base.*
      FROM
        (((

/**/

SELECT *
         FROM
           (
            SELECT
              keep_current_revision.*
            , row_number() OVER (PARTITION BY KEEP_CURRENT_REVISION.contract_num, KEEP_CURRENT_REVISION.month_id, KEEP_CURRENT_REVISION.item_prod, KEEP_CURRENT_REVISION.sub_account, KEEP_CURRENT_REVISION.uom ORDER BY UNION_FLAG DESC) ROW_UNION_FLAG
            , (CASE WHEN (upper(ITEM_PROD) LIKE '%ADVANCED WORKFORCE MANAGEMENT%') THEN 'Workforce Management' ELSE item_edition.descr END) Item_Edition_Description
            , (CASE WHEN (upper(ITEM_PROD) LIKE '%ADVANCED WORKFORCE MANAGEMENT%') THEN 'WFM' 
               WHEN PRODUCT_CLASS = 'GRT' THEN 'GREATSOFT'
               ELSE item_edition.editioncd END) Item_Edition_Short
            , MAX_GEN.max_gendate
            , MAX_GEN.max_genmonth
            , MAX_GEN.churn_month_id
            FROM
              ((
               SELECT DISTINCT *
               FROM
                 (

SELECT * FROM
OPERATIONS_ANALYTICS.TRANSFORMED_PROD.ES_ERP_END_LEFT_JOIN_OPEN

UNION ALL

SELECT * FROM
OPERATIONS_ANALYTICS.TRANSFORMED_PROD.ES_ERP_END_RIGHT_JOIN_OPEN


                              )  Combine_view
--WHERE MONTH_ID ='202110' AND CONTRACT_NUM='CT00005988'

            )  KEEP_CURRENT_REVISION


           LEFT JOIN PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLEDITION item_edition ON ((item_edition.editionid = KEEP_CURRENT_REVISION.item_edition_id)
            AND (item_edition.maklsyncdate = (SELECT max(maklsyncdate)FROM  PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLEDITION)))
            LEFT JOIN (
               SELECT
                 contract.contractcd ContractNum
               , contract.contractid
            --   , ((year(contract.cancelon) * 100) + month(contract.cancelon)) CHURN_MONTH_ID
              ,year(case when status='X' and CANCELON is null then contract.LASTMODIFIEDDATETIME else CANCELON end)*100+
month(case when status='X' and CANCELON is null then contract.LASTMODIFIEDDATETIME else CANCELON end) CHURN_MONTH_ID
               , max(((year((CASE WHEN (contract.cancelon IS NULL) THEN contract_line.gendate WHEN (contract.cancelon < contract_line.gendate) THEN contract.cancelon ELSE contract_line.gendate END)) * 100) + month((CASE WHEN (contract.cancelon IS NULL) THEN contract_line.gendate WHEN (contract.cancelon < contract_line.gendate) THEN contract.cancelon ELSE contract_line.gendate END)))) MAX_GENMONTH
               , max((CASE WHEN (contract.cancelon IS NULL) THEN contract_line.gendate WHEN (contract.cancelon < contract_line.gendate) THEN contract.cancelon ELSE contract_line.gendate END)) MAX_GENDATE
               ,max(contract.lastmodifieddatetime) as lastmodifieddatetime
              FROM
                 (PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR contract
               INNER JOIN PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRDET
                  contract_line ON ((contract_line.contractid = contract.contractid) AND (contract.maklsyncdate = contract_line.maklsyncdate)))
               WHERE (contract.maklsyncdate = (SELECT max(maklsyncdate) FROM PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR))
               GROUP BY contract.contractcd, contract.cancelon, contract.contractid,contract.status,contract.LASTMODIFIEDDATETIME
            )  MAX_GEN ON ((MAX_GEN.contractnum = KEEP_CURRENT_REVISION.contract_num) AND (MAX_GEN.contractid = KEEP_CURRENT_REVISION.contract_id)))
            WHERE (((KEEP_CURRENT_REVISION.month_id >= 202001) AND (((MONTH_ID <= MAX_GENMONTH) OR (MONTH_ID = CHURN_MONTH_ID)) OR
                 ((date(MAX_GENDATE) >= date(dateadd('day', -1, GETDATE()))) AND (date(MAX_GENDATE) < date(dateadd('month', 1, GETDATE()))))
             or MONTH_ID<=(year(MAX_GEN.lastmodifieddatetime)*100 +month(MAX_GEN.lastmodifieddatetime))
                                                                   )) AND (KEEP_CURRENT_REVISION.month_id <= ((year(GETDATE()) * 100) + month(GETDATE()))))
                                                                      AND (MONTH_ID <= CHURN_MONTH_ID OR CHURN_MONTH_ID IS NULL)

         )  DEDUP
         WHERE (ROW_UNION_FLAG = 1)

/**/

    )  BASE
LEFT JOIN PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR contract ON (((BASE.contract_num = contract.contractcd) AND (BASE.contract_maklsyncdate = contract.maklsyncdate)) AND (BASE.contract_id = contract.contractid)))
LEFT JOIN (SELECT * FROM PURCHASING.PUBLISHED_PROD.PHOENIX_BRANCH      WHERE (maklsyncdate = (SELECT max(maklsyncdate) FROM PURCHASING.PUBLISHED_PROD.PHOENIX_BRANCH)))  branch ON (contract.branchid = branch.branchid)
LEFT JOIN (SELECT * FROM PURCHASING.PUBLISHED_PROD.PHOENIX_PMPROJECT   WHERE (maklsyncdate = (SELECT max(maklsyncdate) FROM PURCHASING.PUBLISHED_PROD.PHOENIX_PMPROJECT)))  project ON (contract.projectid = project.contractid)
LEFT JOIN (SELECT * FROM PURCHASING.PUBLISHED_PROD.PHOENIX_VENDOR WHERE (maklsyncdate = (SELECT max(maklsyncdate) FROM PURCHASING.PUBLISHED_PROD.PHOENIX_VENDOR)))  partner ON (project.customerid = partner.baccountid)
LEFT JOIN (SELECT * FROM PURCHASING.PUBLISHED_PROD.PHOENIX_BACCOUNT WHERE (maklsyncdate = (SELECT max(maklsyncdate) FROM PURCHASING.PUBLISHED_PROD.PHOENIX_BACCOUNT)))  ba ON (contract.customerid = ba.baccountid)
LEFT JOIN (SELECT industryid, descr Industry FROM PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLINDUSTRY WHERE (maklsyncdate = (SELECT max(maklsyncdate) FROM PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLINDUSTRY)))  industry ON (ba.usrmaklindustry = industry.industryid)
LEFT JOIN (SELECT subindustryid, descr "sub industry" FROM PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLSUBINDUSTRY WHERE (maklsyncdate = (SELECT max(maklsyncdate) FROM PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLSUBINDUSTRY)))  sub_industry ON (ba.usrmaklsubindustry = sub_industry.subindustryid)
LEFT JOIN OPERATIONS_ANALYTICS.TRANSFORMED_PROD.ES_CONTRACT_LINE_REVISION_COMMENTS CONTRACT_COMMENTS ON ((BASE.contract_num = CONTRACT_COMMENTS.contractcd) AND (BASE.month_id = CONTRACT_COMMENTS.gen_month_id)))
LEFT JOIN (SELECT GENDATE NEXT_CHARGE_DATE, CONTRACT_NUM NC_CONTRACT_NUM, CONTRACT_ID NC_CONTRACT_ID FROM
           (SELECT
              contract.contractcd CONTRACT_NUM
            , contract_line.contractid CONTRACT_ID
            , contract_line.gendate GENDATE
            , rank() OVER (PARTITION BY contract.contractcd ORDER BY contract.revisionnbr DESC, contract_line.gendate DESC, contract_line.linenbr DESC) last_contract_line_flag
            FROM
              ((SELECT * FROM PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR WHERE (maklsyncdate IN (SELECT max(maklsyncdate) FROM PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR)))  contract
            LEFT JOIN PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLXRBCONTRDETPREVNEXT contract_line ON ((contract_line.contractid = contract.contractid) AND (contract.maklsyncdate = contract_line.maklsyncdate)))
         )  le1
         WHERE (last_contract_line_flag = 1)
      )  NC_DATE ON ((BASE.contract_num = NC_DATE.nc_contract_num) AND (BASE.contract_id = NC_DATE.nc_contract_id))
   )  ADD_ATTRIBUTE
)  ADD_EDITION

-- WHERE MONTH_ID ='202110' --AND CONTRACT_NUM='CT00005719'
-- and PRODUCT_CLASS = 'GTR' and PROD_ITEM_EDITION not in ('Greentree','GT 3rd Party','GT VAD')
);