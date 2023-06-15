{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_SALESFORCE_ASSET_TMP',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'SAT_SALESFORCE_ASSET_INCR']
	)
}}
select * from (
	WITH "DIST_STG" AS 
	( 
		SELECT DISTINCT 
 			  "STG_DIS_SRC"."ASSET_HKEY" AS "ASSET_HKEY"
			, "STG_DIS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		FROM {{ ref('SALESFORCE_STG_ASSET') }} "STG_DIS_SRC"
		WHERE  "STG_DIS_SRC"."RECORD_TYPE" = 'S'
	)
	, "TEMP_TABLE_SET" AS 
	( 
		SELECT 
			  "STG_TEMP_SRC"."ASSET_HKEY" AS "ASSET_HKEY"
			, "STG_TEMP_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_TEMP_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_TEMP_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, 'STG' AS "SOURCE"
			, 1 AS "ORIGIN_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ACCOUNTROLLUPID"),'~'),'\#','\\' || 
				'\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ISCOMPETITORPRODUCT"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CREATEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTMODIFIEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SYSTEMMODSTAMP"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ISDELETED"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."NAME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SERIALNUMBER"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."INSTALLDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."PURCHASEDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."USAGEENDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."STATUS"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DIGITALASSETSTATUS"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."PRICE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."QUANTITY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DESCRIPTION"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ISINTERNAL"),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CASE WHEN "STG_TEMP_SRC"."JRN_FLAG" = 'D' THEN CAST('Y' AS VARCHAR) ELSE CAST('N' AS VARCHAR) END AS "DELETE_FLAG"
			, "STG_TEMP_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_TEMP_SRC"."ID" AS "ID"
			, "STG_TEMP_SRC"."ASSETSERVICEDBYID" AS "ASSETSERVICEDBYID"
			, "STG_TEMP_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "STG_TEMP_SRC"."CONTACTID" AS "CONTACTID"
			, "STG_TEMP_SRC"."ROOTASSETID" AS "ROOTASSETID"
			, "STG_TEMP_SRC"."ASSETPROVIDEDBYID" AS "ASSETPROVIDEDBYID"
			, "STG_TEMP_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "STG_TEMP_SRC"."PRODUCT2ID" AS "PRODUCT2ID"
			, "STG_TEMP_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "STG_TEMP_SRC"."OWNERID" AS "OWNERID"
			, "STG_TEMP_SRC"."PARENTID" AS "PARENTID"
			, "STG_TEMP_SRC"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
			, "STG_TEMP_SRC"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
			, "STG_TEMP_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "STG_TEMP_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "STG_TEMP_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "STG_TEMP_SRC"."ISDELETED" AS "ISDELETED"
			, "STG_TEMP_SRC"."NAME" AS "NAME"
			, "STG_TEMP_SRC"."SERIALNUMBER" AS "SERIALNUMBER"
			, "STG_TEMP_SRC"."INSTALLDATE" AS "INSTALLDATE"
			, "STG_TEMP_SRC"."PURCHASEDATE" AS "PURCHASEDATE"
			, "STG_TEMP_SRC"."USAGEENDDATE" AS "USAGEENDDATE"
			, "STG_TEMP_SRC"."STATUS" AS "STATUS"
			, "STG_TEMP_SRC"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
			, "STG_TEMP_SRC"."PRICE" AS "PRICE"
			, "STG_TEMP_SRC"."QUANTITY" AS "QUANTITY"
			, "STG_TEMP_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "STG_TEMP_SRC"."ISINTERNAL" AS "ISINTERNAL"
		FROM {{ ref('SALESFORCE_STG_ASSET') }} "STG_TEMP_SRC"
		WHERE  "STG_TEMP_SRC"."RECORD_TYPE" = 'S'
		UNION ALL 
		SELECT 
			  "SAT_SRC"."ASSET_HKEY" AS "ASSET_HKEY"
			, "SAT_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "SAT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "SAT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, 'SAT' AS "RECORD_TYPE"
			, 'SAT' AS "SOURCE"
			, 0 AS "ORIGIN_ID"
			, "SAT_SRC"."HASH_DIFF" AS "HASH_DIFF"
			, "SAT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
			, "SAT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "SAT_SRC"."ID" AS "ID"
			, "SAT_SRC"."ASSETSERVICEDBYID" AS "ASSETSERVICEDBYID"
			, "SAT_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "SAT_SRC"."CONTACTID" AS "CONTACTID"
			, "SAT_SRC"."ROOTASSETID" AS "ROOTASSETID"
			, "SAT_SRC"."ASSETPROVIDEDBYID" AS "ASSETPROVIDEDBYID"
			, "SAT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "SAT_SRC"."PRODUCT2ID" AS "PRODUCT2ID"
			, "SAT_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "SAT_SRC"."OWNERID" AS "OWNERID"
			, "SAT_SRC"."PARENTID" AS "PARENTID"
			, "SAT_SRC"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
			, "SAT_SRC"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
			, "SAT_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "SAT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "SAT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "SAT_SRC"."ISDELETED" AS "ISDELETED"
			, "SAT_SRC"."NAME" AS "NAME"
			, "SAT_SRC"."SERIALNUMBER" AS "SERIALNUMBER"
			, "SAT_SRC"."INSTALLDATE" AS "INSTALLDATE"
			, "SAT_SRC"."PURCHASEDATE" AS "PURCHASEDATE"
			, "SAT_SRC"."USAGEENDDATE" AS "USAGEENDDATE"
			, "SAT_SRC"."STATUS" AS "STATUS"
			, "SAT_SRC"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
			, "SAT_SRC"."PRICE" AS "PRICE"
			, "SAT_SRC"."QUANTITY" AS "QUANTITY"
			, "SAT_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "SAT_SRC"."ISINTERNAL" AS "ISINTERNAL"
		FROM {{ source('DEMO_FL', 'SAT_SALESFORCE_ASSET') }} "SAT_SRC"
		INNER JOIN "DIST_STG" "DIST_STG" ON  "SAT_SRC"."ASSET_HKEY" = "DIST_STG"."ASSET_HKEY"
		WHERE  "SAT_SRC"."LOAD_END_DATE" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS')
	)
	SELECT 
		  "TEMP_TABLE_SET"."ASSET_HKEY" AS "ASSET_HKEY"
		, "TEMP_TABLE_SET"."LOAD_DATE" AS "LOAD_DATE"
		, "TEMP_TABLE_SET"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "TEMP_TABLE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "TEMP_TABLE_SET"."RECORD_TYPE" AS "RECORD_TYPE"
		, "TEMP_TABLE_SET"."SOURCE" AS "SOURCE"
		, CASE WHEN "TEMP_TABLE_SET"."SOURCE" = 'STG' AND TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF")
			= LAG( TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF"),1)OVER(PARTITION BY "TEMP_TABLE_SET"."ASSET_HKEY" ORDER BY "TEMP_TABLE_SET"."LOAD_DATE","TEMP_TABLE_SET"."ORIGIN_ID")THEN 1 ELSE 0 END AS "EQUAL"
		, "TEMP_TABLE_SET"."HASH_DIFF" AS "HASH_DIFF"
		, "TEMP_TABLE_SET"."DELETE_FLAG" AS "DELETE_FLAG"
		, "TEMP_TABLE_SET"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "TEMP_TABLE_SET"."ID" AS "ID"
		, "TEMP_TABLE_SET"."ASSETSERVICEDBYID" AS "ASSETSERVICEDBYID"
		, "TEMP_TABLE_SET"."CREATEDBYID" AS "CREATEDBYID"
		, "TEMP_TABLE_SET"."CONTACTID" AS "CONTACTID"
		, "TEMP_TABLE_SET"."ROOTASSETID" AS "ROOTASSETID"
		, "TEMP_TABLE_SET"."ASSETPROVIDEDBYID" AS "ASSETPROVIDEDBYID"
		, "TEMP_TABLE_SET"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "TEMP_TABLE_SET"."PRODUCT2ID" AS "PRODUCT2ID"
		, "TEMP_TABLE_SET"."ACCOUNTID" AS "ACCOUNTID"
		, "TEMP_TABLE_SET"."OWNERID" AS "OWNERID"
		, "TEMP_TABLE_SET"."PARENTID" AS "PARENTID"
		, "TEMP_TABLE_SET"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
		, "TEMP_TABLE_SET"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
		, "TEMP_TABLE_SET"."CREATEDDATE" AS "CREATEDDATE"
		, "TEMP_TABLE_SET"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "TEMP_TABLE_SET"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "TEMP_TABLE_SET"."ISDELETED" AS "ISDELETED"
		, "TEMP_TABLE_SET"."NAME" AS "NAME"
		, "TEMP_TABLE_SET"."SERIALNUMBER" AS "SERIALNUMBER"
		, "TEMP_TABLE_SET"."INSTALLDATE" AS "INSTALLDATE"
		, "TEMP_TABLE_SET"."PURCHASEDATE" AS "PURCHASEDATE"
		, "TEMP_TABLE_SET"."USAGEENDDATE" AS "USAGEENDDATE"
		, "TEMP_TABLE_SET"."STATUS" AS "STATUS"
		, "TEMP_TABLE_SET"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
		, "TEMP_TABLE_SET"."PRICE" AS "PRICE"
		, "TEMP_TABLE_SET"."QUANTITY" AS "QUANTITY"
		, "TEMP_TABLE_SET"."DESCRIPTION" AS "DESCRIPTION"
		, "TEMP_TABLE_SET"."ISINTERNAL" AS "ISINTERNAL"
	FROM "TEMP_TABLE_SET" "TEMP_TABLE_SET"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'