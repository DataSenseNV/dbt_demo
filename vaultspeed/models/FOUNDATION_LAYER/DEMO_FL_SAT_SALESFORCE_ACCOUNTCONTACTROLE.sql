{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_SALESFORCE_ACCOUNTCONTACTROLE',
		schema='DEMO_FL',
		unique_key = ['"ACCOUNTCONTACTROLE_HKEY"', '"LOAD_DATE"'],
		merge_update_columns = ['"LOAD_END_DATE"', '"DELETE_FLAG"', '"CDC_TIMESTAMP"'],
		tags=['SALESFORCE', 'SAT_SALESFORCE_ACCOUNTCONTACTROLE_INCR', 'SAT_SALESFORCE_ACCOUNTCONTACTROLE_INIT']
	)
}}
select * from (
	WITH "END_DATING" AS 
	( 
		SELECT 
			  "SAT_TEMP_SRC_DK"."ACCOUNTCONTACTROLE_HKEY" AS "ACCOUNTCONTACTROLE_HKEY"
			, "SAT_TEMP_SRC_DK"."LOAD_DATE" AS "LOAD_DATE"
			, CASE WHEN LEAD( "SAT_TEMP_SRC_DK"."DELETE_FLAG" || "SAT_TEMP_SRC_DK"."SOURCE",1)OVER(PARTITION BY "SAT_TEMP_SRC_DK"."ACCOUNTCONTACTROLE_HKEY" ORDER BY "SAT_TEMP_SRC_DK"."LOAD_DATE",
				"SAT_TEMP_SRC_DK"."LOAD_CYCLE_ID")= 'Y' || 'STG' THEN CAST('Y' AS VARCHAR) ELSE "SAT_TEMP_SRC_DK"."DELETE_FLAG" END AS "DELETE_FLAG"
			, LEAD("SAT_TEMP_SRC_DK"."CDC_TIMESTAMP",1)OVER(PARTITION BY "SAT_TEMP_SRC_DK"."ACCOUNTCONTACTROLE_HKEY" ORDER BY "SAT_TEMP_SRC_DK"."LOAD_DATE") AS "CDC_TIMESTAMP"
		FROM {{ ref('SALESFORCE_STG_SAT_SALESFORCE_ACCOUNTCONTACTROLE_TMP') }} "SAT_TEMP_SRC_DK"
		WHERE  "SAT_TEMP_SRC_DK"."EQUAL" = 0
	)
	, "CALC_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "SAT_TEMP_SRC_US"."ACCOUNTCONTACTROLE_HKEY" AS "ACCOUNTCONTACTROLE_HKEY"
			, "SAT_TEMP_SRC_US"."LOAD_DATE" AS "LOAD_DATE"
			, "SAT_TEMP_SRC_US"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, COALESCE(LEAD("SAT_TEMP_SRC_US"."LOAD_DATE",1)OVER(PARTITION BY "SAT_TEMP_SRC_US"."ACCOUNTCONTACTROLE_HKEY" ORDER BY "SAT_TEMP_SRC_US"."LOAD_DATE")
				, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS')) AS "LOAD_END_DATE"
			, "END_DATING"."DELETE_FLAG" AS "DELETE_FLAG"
			, "SAT_TEMP_SRC_US"."SOURCE" AS "SOURCE"
			, "SAT_TEMP_SRC_US"."HASH_DIFF" AS "HASH_DIFF"
			, CASE WHEN "END_DATING"."DELETE_FLAG" = CAST('Y' AS VARCHAR) THEN "END_DATING"."CDC_TIMESTAMP" ELSE "SAT_TEMP_SRC_US"."CDC_TIMESTAMP" END AS "CDC_TIMESTAMP"
			, "SAT_TEMP_SRC_US"."ID" AS "ID"
			, "SAT_TEMP_SRC_US"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "SAT_TEMP_SRC_US"."CONTACTID" AS "CONTACTID"
			, "SAT_TEMP_SRC_US"."ACCOUNTID" AS "ACCOUNTID"
			, "SAT_TEMP_SRC_US"."CREATEDBYID" AS "CREATEDBYID"
			, "SAT_TEMP_SRC_US"."ISDELETED" AS "ISDELETED"
			, "SAT_TEMP_SRC_US"."CREATEDDATE" AS "CREATEDDATE"
			, "SAT_TEMP_SRC_US"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "SAT_TEMP_SRC_US"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "SAT_TEMP_SRC_US"."ROLE" AS "ROLE"
			, "SAT_TEMP_SRC_US"."ISPRIMARY" AS "ISPRIMARY"
		FROM {{ ref('SALESFORCE_STG_SAT_SALESFORCE_ACCOUNTCONTACTROLE_TMP') }} "SAT_TEMP_SRC_US"
		INNER JOIN "END_DATING" "END_DATING" ON  "SAT_TEMP_SRC_US"."ACCOUNTCONTACTROLE_HKEY" = "END_DATING"."ACCOUNTCONTACTROLE_HKEY" AND "SAT_TEMP_SRC_US"."LOAD_DATE" =
			 "END_DATING"."LOAD_DATE"
		WHERE  "SAT_TEMP_SRC_US"."EQUAL" = 0 AND NOT("SAT_TEMP_SRC_US"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "SAT_TEMP_SRC_US"."SOURCE" = 'STG')
	)
	, "FILTER_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "CALC_LOAD_END_DATE"."ACCOUNTCONTACTROLE_HKEY" AS "ACCOUNTCONTACTROLE_HKEY"
			, "CALC_LOAD_END_DATE"."LOAD_DATE" AS "LOAD_DATE"
			, "CALC_LOAD_END_DATE"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CALC_LOAD_END_DATE"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, "CALC_LOAD_END_DATE"."DELETE_FLAG" AS "DELETE_FLAG"
			, "CALC_LOAD_END_DATE"."HASH_DIFF" AS "HASH_DIFF"
			, "CALC_LOAD_END_DATE"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CALC_LOAD_END_DATE"."ID" AS "ID"
			, "CALC_LOAD_END_DATE"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CALC_LOAD_END_DATE"."CONTACTID" AS "CONTACTID"
			, "CALC_LOAD_END_DATE"."ACCOUNTID" AS "ACCOUNTID"
			, "CALC_LOAD_END_DATE"."CREATEDBYID" AS "CREATEDBYID"
			, "CALC_LOAD_END_DATE"."ISDELETED" AS "ISDELETED"
			, "CALC_LOAD_END_DATE"."CREATEDDATE" AS "CREATEDDATE"
			, "CALC_LOAD_END_DATE"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CALC_LOAD_END_DATE"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "CALC_LOAD_END_DATE"."ROLE" AS "ROLE"
			, "CALC_LOAD_END_DATE"."ISPRIMARY" AS "ISPRIMARY"
		FROM "CALC_LOAD_END_DATE" "CALC_LOAD_END_DATE"
		WHERE  "CALC_LOAD_END_DATE"."DELETE_FLAG" = CAST('Y' AS VARCHAR) OR "CALC_LOAD_END_DATE"."SOURCE" = 'STG' OR("CALC_LOAD_END_DATE"."SOURCE" = 'SAT' AND "CALC_LOAD_END_DATE"."LOAD_END_DATE" != TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'))
	)
	SELECT 
		  "FILTER_LOAD_END_DATE"."ACCOUNTCONTACTROLE_HKEY"
		, "FILTER_LOAD_END_DATE"."LOAD_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_END_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_CYCLE_ID"
		, "FILTER_LOAD_END_DATE"."HASH_DIFF"
		, "FILTER_LOAD_END_DATE"."DELETE_FLAG"
		, "FILTER_LOAD_END_DATE"."CDC_TIMESTAMP"
		, "FILTER_LOAD_END_DATE"."ID"
		, "FILTER_LOAD_END_DATE"."LASTMODIFIEDBYID"
		, "FILTER_LOAD_END_DATE"."CONTACTID"
		, "FILTER_LOAD_END_DATE"."ACCOUNTID"
		, "FILTER_LOAD_END_DATE"."CREATEDBYID"
		, "FILTER_LOAD_END_DATE"."ISDELETED"
		, "FILTER_LOAD_END_DATE"."CREATEDDATE"
		, "FILTER_LOAD_END_DATE"."LASTMODIFIEDDATE"
		, "FILTER_LOAD_END_DATE"."SYSTEMMODSTAMP"
		, "FILTER_LOAD_END_DATE"."ROLE"
		, "FILTER_LOAD_END_DATE"."ISPRIMARY"
	FROM "FILTER_LOAD_END_DATE"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	WITH "STG_SRC" AS 
	( 
		SELECT 
			  "STG_INR_SRC"."ACCOUNTCONTACTROLE_HKEY" AS "ACCOUNTCONTACTROLE_HKEY"
			, "STG_INR_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_INR_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ISDELETED"),'~'),'\#','\\' || '\#')
				)|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."CREATEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."LASTMODIFIEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."SYSTEMMODSTAMP"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ROLE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ISPRIMARY"),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CAST('N' AS VARCHAR) AS "DELETE_FLAG"
			, "STG_INR_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_INR_SRC"."ID" AS "ID"
			, "STG_INR_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "STG_INR_SRC"."CONTACTID" AS "CONTACTID"
			, "STG_INR_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "STG_INR_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "STG_INR_SRC"."ISDELETED" AS "ISDELETED"
			, "STG_INR_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "STG_INR_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "STG_INR_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "STG_INR_SRC"."ROLE" AS "ROLE"
			, "STG_INR_SRC"."ISPRIMARY" AS "ISPRIMARY"
			, ROW_NUMBER()OVER(PARTITION BY "STG_INR_SRC"."ACCOUNTCONTACTROLE_HKEY" ORDER BY "STG_INR_SRC"."LOAD_DATE") AS "DUMMY"
		FROM {{ ref('SALESFORCE_STG_ACCOUNTCONTACTROLE') }} "STG_INR_SRC"
	)
	SELECT 
		  "STG_SRC"."ACCOUNTCONTACTROLE_HKEY" AS "ACCOUNTCONTACTROLE_HKEY"
		, "STG_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "STG_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "STG_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "STG_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "STG_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "STG_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "STG_SRC"."ID" AS "ID"
		, "STG_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "STG_SRC"."CONTACTID" AS "CONTACTID"
		, "STG_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "STG_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "STG_SRC"."ISDELETED" AS "ISDELETED"
		, "STG_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "STG_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "STG_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "STG_SRC"."ROLE" AS "ROLE"
		, "STG_SRC"."ISPRIMARY" AS "ISPRIMARY"
	FROM "STG_SRC" "STG_SRC"
	WHERE  "STG_SRC"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'