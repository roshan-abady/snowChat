create or replace view ES_CONTRACT_LINE_REVISION_COMMENTS(
	CONTRACTCD,
	AUTOGENID,
	GEN_MONTH_ID,
	ROW_NUMBER,
	COMMENTS,
	IMPORT_FLAG,
	PARTNER_SWITCH_FLAG
) as (
    

 SELECT *
 FROM
   (
    SELECT DISTINCT
      contract.contractcd
    , contract_line.autogenid
    , (("year"(contract_line.gendate) * 100) + "month"(contract_line.gendate)) gen_month_id
    , "row_number"() OVER (PARTITION BY (("year"(contract_line.gendate) * 100) + "month"(contract_line.gendate)), contract.contractcd 
                           ORDER BY (CASE WHEN (upper(contract_line.comments) IN ('GT IMPORT', 'ARCHIE IMPORT')) 
                                     THEN 3 WHEN (contract_line.autogenid IN ('NEWSL')) 
                                     THEN 2 WHEN (contract_line.autogenid IN ('REVIS')) 
                                     THEN 1 ELSE 0 END) DESC) row_number
    , contract_line.comments
    , (CASE WHEN ("upper"(contract_line.comments) IN ('GT IMPORT', 'ARCHIE IMPORT')) THEN 1 ELSE 0 END) "IMPORT_FLAG"
    , (CASE WHEN ("upper"(contract_line.comments) IN ('PARTNER SWITCH')) THEN 1 ELSE 0 END) "PARTNER_SWITCH_FLAG"
    FROM
    
        PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLXRBCONTRDETPREVNEXT contract_line
    
    LEFT JOIN
    
        PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR
     
    contract ON ((contract_line.maklsyncdate = contract.maklsyncdate) AND (contract_line.contractid = contract.contractid))
    WHERE (((contract_line.maklsyncdate IN (SELECT "max"(maklsyncdate) 
    FROM
    
        PURCHASING.PUBLISHED_PROD.PHOENIX_MAKLXRBCONTRDETPREVNEXT
    
    ))
AND ((contract_line.autogenid IN ('REVIS', 'NEWSL')) OR ("upper"(contract_line.comments) IN ('PARTNER SWITCH', 'GT IMPORT', 'ARCHIE IMPORT')))) AND (contract_line.comments IS NOT NULL))
 )  dedup
 WHERE (row_number = 1)
  )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.2", "dbt_version": "1.1.0", "project_name": "PHOENIX", "target_name": "prod", "target_database": "OPERATIONS_ANALYTICS", "target_schema": "TRANSFORMED_PROD", "invocation_id": "89bae525-c206-47fa-ade6-dd629f4ca44b", "node_name": "es_contract_line_revision_comments", "node_alias": "es_contract_line_revision_comments", "node_package_name": "PHOENIX", "node_original_file_path": "models/es_contract_line_revision_comments.sql", "node_database": "OPERATIONS_ANALYTICS", "node_schema": "TRANSFORMED_PROD", "node_id": "model.PHOENIX.es_contract_line_revision_comments", "node_resource_type": "model", "node_meta": {}, "node_tags": [], "full_refresh": false, "which": "run", "node_refs": [], "materialized": "view"} */;