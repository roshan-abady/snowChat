create or replace view ES_PHOENIX_CONTRACT_PREVIOUS_LINE(
	CONTRACTCD,
	GENDATE,
	LINENBR,
	MIN_DATE,
	PREV_LINENBR,
	AUTOGENID,
	CONTRACTID,
	CLASSID,
	MAKLSYNCDATE,
	LASTMODIFIEDDATETIME
) as (
    

SELECT  
  new_sort.contractcd
, new_sort.gendate
, new_sort.linenbr
, (CASE WHEN (new_sort.gendate < new_sort.lastmodifieddatetime) THEN new_sort.gendate ELSE new_sort.lastmodifieddatetime END) MIN_DATE
, new_sort_pre.linenbr prev_linenbr
, new_sort.autogenid
, new_sort.contractid
, new_sort.classid
, new_sort.maklsyncdate
, new_sort.lastmodifieddatetime
FROM
  ((
   SELECT
     contractcd
   , contractid
   , date(gendate) gendate
   , maklsyncdate
   , linenbr
   , autogenid
   , new_sort_asc new_sort
   , (new_sort_asc - 1) new_sort_prev
   , (new_sort_asc + 1) new_sort_next
   , usrmaklprevlinenbr
   , usrmaklnextlinenbr
   , lastmodifieddatetime
   , classid
   FROM
     (
      SELECT
        contract.contractcd
      , contract.maklsyncdate
      , contract_line.gendate
      , contract_line.linenbr
      , contract_line.autogenid
      , contract_line.contractid
      , contract_line.lastmodifieddatetime
      , contract.classid
      , row_number() OVER (PARTITION BY contract.contractcd ORDER BY year(contract_line.gendate) ASC, month(contract_line.gendate) ASC, (CASE WHEN (contract_line.gendate < contract_line.lastmodifieddatetime) THEN contract_line.gendate ELSE contract_line.lastmodifieddatetime END) ASC, contract_line.gendate ASC, contract_line.linenbr ASC) new_sort_asc
      , usrmaklprevlinenbr
      , usrmaklnextlinenbr
      FROM
        (
        
        PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR
           
        
         contract
      LEFT JOIN 
        
        PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLXRBCONTRDETPREVNEXT
         
      
      
      
      contract_line ON ((contract_line.contractid = contract.contractid) AND (contract.maklsyncdate = contract_line.maklsyncdate)))
      WHERE ((contract.maklsyncdate IN (SELECT max(maklsyncdate)
FROM

        
        PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLXRBCONTRDETPREVNEXT
         
      
)) AND (manuallybooked = true))
      ORDER BY gendate DESC, linenbr DESC
   )  A
)  new_sort
LEFT JOIN (
   SELECT
     contractcd
   , date(gendate) gendate
   , linenbr
   , autogenid
   , new_sort_asc new_sort
   , (new_sort_asc - 1) new_sort_prev
   , (new_sort_asc + 1) new_sort_next
   , usrmaklprevlinenbr
   , usrmaklnextlinenbr
   FROM
     (
      SELECT
        contract.contractcd
      , contract_line.gendate
      , contract_line.linenbr
      , contract_line.autogenid
      , row_number() OVER (PARTITION BY contract.contractcd ORDER BY year(contract_line.gendate) ASC, month(contract_line.gendate) ASC, (CASE WHEN (contract_line.gendate < contract_line.lastmodifieddatetime) THEN contract_line.gendate ELSE contract_line.lastmodifieddatetime END) ASC, contract_line.gendate ASC, contract_line.linenbr ASC) new_sort_asc
      , usrmaklprevlinenbr
      , usrmaklnextlinenbr
      FROM
        (
        PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR
     
        
        contract
      LEFT JOIN 
        PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLXRBCONTRDETPREVNEXT
        
      
      contract_line ON ((contract_line.contractid = contract.contractid) AND (contract.maklsyncdate = contract_line.maklsyncdate)))
      WHERE ((contract.maklsyncdate IN (SELECT max(maklsyncdate)
FROM
        PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLXRBCONTRDETPREVNEXT
)) AND (manuallybooked = true))
   )  A
)  new_sort_pre ON ((new_sort.new_sort = new_sort_pre.new_sort_next) AND (new_sort.contractcd = new_sort_pre.contractcd)))
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "PHOENIX", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "89bae525-c206-47fa-ade6-dd629f4ca44b", "node_name": "es_phoenix_contract_previous_line", "node_alias": "es_phoenix_contract_previous_line", "node_package_name": "PHOENIX", "node_original_file_path": "models/es_phoenix_contract_previous_line.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.PHOENIX.es_phoenix_contract_previous_line", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": [], "materialized": "view"} */;