{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_SALESFORCE_CUSTOMER__C_TMP',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'SAT_SALESFORCE_CUSTOMERC_INCR']
	)
}}
select * from (
	WITH "DIST_STG" AS 
	( 
		SELECT DISTINCT 
 			  "STG_DIS_SRC"."CUSTOMER__C_HKEY" AS "CUSTOMER__C_HKEY"
			, "STG_DIS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		FROM {{ ref('SALESFORCE_STG_CUSTOMER__C') }} "STG_DIS_SRC"
		WHERE  "STG_DIS_SRC"."RECORD_TYPE" = 'S'
	)
	, "TEMP_TABLE_SET" AS 
	( 
		SELECT 
			  "STG_TEMP_SRC"."CUSTOMER__C_HKEY" AS "CUSTOMER__C_HKEY"
			, "STG_TEMP_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_TEMP_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_TEMP_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, 'STG' AS "SOURCE"
			, 1 AS "ORIGIN_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."ISDELETED")),'~'),'\#',
				'\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."NAME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CREATEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTMODIFIEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SYSTEMMODSTAMP"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CUSTOMER_ID__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."GENDER__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."SENIOR_CITIZEN__C")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."PARTNER__C")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."DEPENDENTS__C")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."TENURE__C")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."PHONE_SERVICE__C")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MULTIPLE_LINES__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."INTERNET_SERVICE__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ONLINE_SECURITY__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ONLINE_BACKUP__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DEVICE_PROTECTION__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."TECH_SUPPORT__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."STREAMING_TV__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."STREAMING_MOVIES__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CONTRACT__C"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."PAPERLESS_BILLING__C")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."CHURN__C")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."PAYMENT_METHOD__C"),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CASE WHEN "STG_TEMP_SRC"."JRN_FLAG" = 'D' THEN CAST('Y' AS VARCHAR) ELSE CAST('N' AS VARCHAR) END AS "DELETE_FLAG"
			, "STG_TEMP_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_TEMP_SRC"."ID" AS "ID"
			, "STG_TEMP_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "STG_TEMP_SRC"."OWNERID" AS "OWNERID"
			, "STG_TEMP_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "STG_TEMP_SRC"."ISDELETED" AS "ISDELETED"
			, "STG_TEMP_SRC"."NAME" AS "NAME"
			, "STG_TEMP_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "STG_TEMP_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "STG_TEMP_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "STG_TEMP_SRC"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
			, "STG_TEMP_SRC"."GENDER__C" AS "GENDER__C"
			, "STG_TEMP_SRC"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
			, "STG_TEMP_SRC"."PARTNER__C" AS "PARTNER__C"
			, "STG_TEMP_SRC"."DEPENDENTS__C" AS "DEPENDENTS__C"
			, "STG_TEMP_SRC"."TENURE__C" AS "TENURE__C"
			, "STG_TEMP_SRC"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
			, "STG_TEMP_SRC"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
			, "STG_TEMP_SRC"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
			, "STG_TEMP_SRC"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
			, "STG_TEMP_SRC"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
			, "STG_TEMP_SRC"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
			, "STG_TEMP_SRC"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
			, "STG_TEMP_SRC"."STREAMING_TV__C" AS "STREAMING_TV__C"
			, "STG_TEMP_SRC"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
			, "STG_TEMP_SRC"."CONTRACT__C" AS "CONTRACT__C"
			, "STG_TEMP_SRC"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
			, "STG_TEMP_SRC"."CHURN__C" AS "CHURN__C"
			, "STG_TEMP_SRC"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
		FROM {{ ref('SALESFORCE_STG_CUSTOMER__C') }} "STG_TEMP_SRC"
		WHERE  "STG_TEMP_SRC"."RECORD_TYPE" = 'S'
		UNION ALL 
		SELECT 
			  "SAT_SRC"."CUSTOMER__C_HKEY" AS "CUSTOMER__C_HKEY"
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
			, "SAT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "SAT_SRC"."OWNERID" AS "OWNERID"
			, "SAT_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "SAT_SRC"."ISDELETED" AS "ISDELETED"
			, "SAT_SRC"."NAME" AS "NAME"
			, "SAT_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "SAT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "SAT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "SAT_SRC"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
			, "SAT_SRC"."GENDER__C" AS "GENDER__C"
			, "SAT_SRC"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
			, "SAT_SRC"."PARTNER__C" AS "PARTNER__C"
			, "SAT_SRC"."DEPENDENTS__C" AS "DEPENDENTS__C"
			, "SAT_SRC"."TENURE__C" AS "TENURE__C"
			, "SAT_SRC"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
			, "SAT_SRC"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
			, "SAT_SRC"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
			, "SAT_SRC"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
			, "SAT_SRC"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
			, "SAT_SRC"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
			, "SAT_SRC"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
			, "SAT_SRC"."STREAMING_TV__C" AS "STREAMING_TV__C"
			, "SAT_SRC"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
			, "SAT_SRC"."CONTRACT__C" AS "CONTRACT__C"
			, "SAT_SRC"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
			, "SAT_SRC"."CHURN__C" AS "CHURN__C"
			, "SAT_SRC"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
		FROM {{ source('DEMO_FL', 'SAT_SALESFORCE_CUSTOMER__C') }} "SAT_SRC"
		INNER JOIN "DIST_STG" "DIST_STG" ON  "SAT_SRC"."CUSTOMER__C_HKEY" = "DIST_STG"."CUSTOMER__C_HKEY"
		WHERE  "SAT_SRC"."LOAD_END_DATE" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS')
	)
	SELECT 
		  "TEMP_TABLE_SET"."CUSTOMER__C_HKEY" AS "CUSTOMER__C_HKEY"
		, "TEMP_TABLE_SET"."LOAD_DATE" AS "LOAD_DATE"
		, "TEMP_TABLE_SET"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "TEMP_TABLE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "TEMP_TABLE_SET"."RECORD_TYPE" AS "RECORD_TYPE"
		, "TEMP_TABLE_SET"."SOURCE" AS "SOURCE"
		, CASE WHEN "TEMP_TABLE_SET"."SOURCE" = 'STG' AND TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF")
			= LAG( TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF"),1)OVER(PARTITION BY "TEMP_TABLE_SET"."CUSTOMER__C_HKEY" ORDER BY "TEMP_TABLE_SET"."LOAD_DATE","TEMP_TABLE_SET"."ORIGIN_ID")THEN 1 ELSE 0 END AS "EQUAL"
		, "TEMP_TABLE_SET"."HASH_DIFF" AS "HASH_DIFF"
		, "TEMP_TABLE_SET"."DELETE_FLAG" AS "DELETE_FLAG"
		, "TEMP_TABLE_SET"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "TEMP_TABLE_SET"."ID" AS "ID"
		, "TEMP_TABLE_SET"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "TEMP_TABLE_SET"."OWNERID" AS "OWNERID"
		, "TEMP_TABLE_SET"."CREATEDBYID" AS "CREATEDBYID"
		, "TEMP_TABLE_SET"."ISDELETED" AS "ISDELETED"
		, "TEMP_TABLE_SET"."NAME" AS "NAME"
		, "TEMP_TABLE_SET"."CREATEDDATE" AS "CREATEDDATE"
		, "TEMP_TABLE_SET"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "TEMP_TABLE_SET"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "TEMP_TABLE_SET"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
		, "TEMP_TABLE_SET"."GENDER__C" AS "GENDER__C"
		, "TEMP_TABLE_SET"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
		, "TEMP_TABLE_SET"."PARTNER__C" AS "PARTNER__C"
		, "TEMP_TABLE_SET"."DEPENDENTS__C" AS "DEPENDENTS__C"
		, "TEMP_TABLE_SET"."TENURE__C" AS "TENURE__C"
		, "TEMP_TABLE_SET"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
		, "TEMP_TABLE_SET"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
		, "TEMP_TABLE_SET"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
		, "TEMP_TABLE_SET"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
		, "TEMP_TABLE_SET"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
		, "TEMP_TABLE_SET"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
		, "TEMP_TABLE_SET"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
		, "TEMP_TABLE_SET"."STREAMING_TV__C" AS "STREAMING_TV__C"
		, "TEMP_TABLE_SET"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
		, "TEMP_TABLE_SET"."CONTRACT__C" AS "CONTRACT__C"
		, "TEMP_TABLE_SET"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
		, "TEMP_TABLE_SET"."CHURN__C" AS "CHURN__C"
		, "TEMP_TABLE_SET"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
	FROM "TEMP_TABLE_SET" "TEMP_TABLE_SET"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'