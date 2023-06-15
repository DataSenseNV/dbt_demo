{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_SALESFORCE_PARTNER_TMP',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'SAT_SALESFORCE_PARTNER_INCR']
	)
}}
select * from (
	WITH "DIST_STG" AS 
	( 
		SELECT DISTINCT 
 			  "STG_DIS_SRC"."PARTNER_HKEY" AS "PARTNER_HKEY"
			, "STG_DIS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		FROM {{ ref('SALESFORCE_STG_PARTNER') }} "STG_DIS_SRC"
		WHERE  "STG_DIS_SRC"."RECORD_TYPE" = 'S'
	)
	, "TEMP_TABLE_SET" AS 
	( 
		SELECT 
			  "STG_TEMP_SRC"."PARTNER_HKEY" AS "PARTNER_HKEY"
			, "STG_TEMP_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_TEMP_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_TEMP_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, 'STG' AS "SOURCE"
			, 1 AS "ORIGIN_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ROLE"),'~'),'\#','\\' || '\#'))
				|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ISPRIMARY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CREATEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTMODIFIEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SYSTEMMODSTAMP"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ISDELETED"),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CASE WHEN "STG_TEMP_SRC"."JRN_FLAG" = 'D' THEN CAST('Y' AS VARCHAR) ELSE CAST('N' AS VARCHAR) END AS "DELETE_FLAG"
			, "STG_TEMP_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_TEMP_SRC"."ID" AS "ID"
			, "STG_TEMP_SRC"."ACCOUNTTOID" AS "ACCOUNTTOID"
			, "STG_TEMP_SRC"."REVERSEPARTNERID" AS "REVERSEPARTNERID"
			, "STG_TEMP_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "STG_TEMP_SRC"."OPPORTUNITYID" AS "OPPORTUNITYID"
			, "STG_TEMP_SRC"."ACCOUNTFROMID" AS "ACCOUNTFROMID"
			, "STG_TEMP_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "STG_TEMP_SRC"."ROLE" AS "ROLE"
			, "STG_TEMP_SRC"."ISPRIMARY" AS "ISPRIMARY"
			, "STG_TEMP_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "STG_TEMP_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "STG_TEMP_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "STG_TEMP_SRC"."ISDELETED" AS "ISDELETED"
		FROM {{ ref('SALESFORCE_STG_PARTNER') }} "STG_TEMP_SRC"
		WHERE  "STG_TEMP_SRC"."RECORD_TYPE" = 'S'
		UNION ALL 
		SELECT 
			  "SAT_SRC"."PARTNER_HKEY" AS "PARTNER_HKEY"
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
			, "SAT_SRC"."ACCOUNTTOID" AS "ACCOUNTTOID"
			, "SAT_SRC"."REVERSEPARTNERID" AS "REVERSEPARTNERID"
			, "SAT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "SAT_SRC"."OPPORTUNITYID" AS "OPPORTUNITYID"
			, "SAT_SRC"."ACCOUNTFROMID" AS "ACCOUNTFROMID"
			, "SAT_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "SAT_SRC"."ROLE" AS "ROLE"
			, "SAT_SRC"."ISPRIMARY" AS "ISPRIMARY"
			, "SAT_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "SAT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "SAT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "SAT_SRC"."ISDELETED" AS "ISDELETED"
		FROM {{ source('DEMO_FL', 'SAT_SALESFORCE_PARTNER') }} "SAT_SRC"
		INNER JOIN "DIST_STG" "DIST_STG" ON  "SAT_SRC"."PARTNER_HKEY" = "DIST_STG"."PARTNER_HKEY"
		WHERE  "SAT_SRC"."LOAD_END_DATE" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS')
	)
	SELECT 
		  "TEMP_TABLE_SET"."PARTNER_HKEY" AS "PARTNER_HKEY"
		, "TEMP_TABLE_SET"."LOAD_DATE" AS "LOAD_DATE"
		, "TEMP_TABLE_SET"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "TEMP_TABLE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "TEMP_TABLE_SET"."RECORD_TYPE" AS "RECORD_TYPE"
		, "TEMP_TABLE_SET"."SOURCE" AS "SOURCE"
		, CASE WHEN "TEMP_TABLE_SET"."SOURCE" = 'STG' AND TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF")
			= LAG( TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF"),1)OVER(PARTITION BY "TEMP_TABLE_SET"."PARTNER_HKEY" ORDER BY "TEMP_TABLE_SET"."LOAD_DATE","TEMP_TABLE_SET"."ORIGIN_ID")THEN 1 ELSE 0 END AS "EQUAL"
		, "TEMP_TABLE_SET"."HASH_DIFF" AS "HASH_DIFF"
		, "TEMP_TABLE_SET"."DELETE_FLAG" AS "DELETE_FLAG"
		, "TEMP_TABLE_SET"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "TEMP_TABLE_SET"."ID" AS "ID"
		, "TEMP_TABLE_SET"."ACCOUNTTOID" AS "ACCOUNTTOID"
		, "TEMP_TABLE_SET"."REVERSEPARTNERID" AS "REVERSEPARTNERID"
		, "TEMP_TABLE_SET"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "TEMP_TABLE_SET"."OPPORTUNITYID" AS "OPPORTUNITYID"
		, "TEMP_TABLE_SET"."ACCOUNTFROMID" AS "ACCOUNTFROMID"
		, "TEMP_TABLE_SET"."CREATEDBYID" AS "CREATEDBYID"
		, "TEMP_TABLE_SET"."ROLE" AS "ROLE"
		, "TEMP_TABLE_SET"."ISPRIMARY" AS "ISPRIMARY"
		, "TEMP_TABLE_SET"."CREATEDDATE" AS "CREATEDDATE"
		, "TEMP_TABLE_SET"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "TEMP_TABLE_SET"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "TEMP_TABLE_SET"."ISDELETED" AS "ISDELETED"
	FROM "TEMP_TABLE_SET" "TEMP_TABLE_SET"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'