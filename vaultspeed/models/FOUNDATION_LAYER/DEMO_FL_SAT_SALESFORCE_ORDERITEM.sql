{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_SALESFORCE_ORDERITEM',
		schema='DEMO_FL',
		unique_key = ['"ORDERITEM_HKEY"', '"LOAD_DATE"'],
		merge_update_columns = ['"LOAD_END_DATE"', '"DELETE_FLAG"', '"CDC_TIMESTAMP"'],
		tags=['SALESFORCE', 'SAT_SALESFORCE_ORDERITEM_INCR', 'SAT_SALESFORCE_ORDERITEM_INIT']
	)
}}
select * from (
	WITH "END_DATING" AS 
	( 
		SELECT 
			  "SAT_TEMP_SRC_DK"."ORDERITEM_HKEY" AS "ORDERITEM_HKEY"
			, "SAT_TEMP_SRC_DK"."LOAD_DATE" AS "LOAD_DATE"
			, CASE WHEN LEAD( "SAT_TEMP_SRC_DK"."DELETE_FLAG" || "SAT_TEMP_SRC_DK"."SOURCE",1)OVER(PARTITION BY "SAT_TEMP_SRC_DK"."ORDERITEM_HKEY" ORDER BY "SAT_TEMP_SRC_DK"."LOAD_DATE",
				"SAT_TEMP_SRC_DK"."LOAD_CYCLE_ID")= 'Y' || 'STG' THEN CAST('Y' AS VARCHAR) ELSE "SAT_TEMP_SRC_DK"."DELETE_FLAG" END AS "DELETE_FLAG"
			, LEAD("SAT_TEMP_SRC_DK"."CDC_TIMESTAMP",1)OVER(PARTITION BY "SAT_TEMP_SRC_DK"."ORDERITEM_HKEY" ORDER BY "SAT_TEMP_SRC_DK"."LOAD_DATE") AS "CDC_TIMESTAMP"
		FROM {{ ref('SALESFORCE_STG_SAT_SALESFORCE_ORDERITEM_TMP') }} "SAT_TEMP_SRC_DK"
		WHERE  "SAT_TEMP_SRC_DK"."EQUAL" = 0
	)
	, "CALC_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "SAT_TEMP_SRC_US"."ORDERITEM_HKEY" AS "ORDERITEM_HKEY"
			, "SAT_TEMP_SRC_US"."LOAD_DATE" AS "LOAD_DATE"
			, "SAT_TEMP_SRC_US"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, COALESCE(LEAD("SAT_TEMP_SRC_US"."LOAD_DATE",1)OVER(PARTITION BY "SAT_TEMP_SRC_US"."ORDERITEM_HKEY" ORDER BY "SAT_TEMP_SRC_US"."LOAD_DATE")
				, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS')) AS "LOAD_END_DATE"
			, "END_DATING"."DELETE_FLAG" AS "DELETE_FLAG"
			, "SAT_TEMP_SRC_US"."SOURCE" AS "SOURCE"
			, "SAT_TEMP_SRC_US"."HASH_DIFF" AS "HASH_DIFF"
			, CASE WHEN "END_DATING"."DELETE_FLAG" = CAST('Y' AS VARCHAR) THEN "END_DATING"."CDC_TIMESTAMP" ELSE "SAT_TEMP_SRC_US"."CDC_TIMESTAMP" END AS "CDC_TIMESTAMP"
			, "SAT_TEMP_SRC_US"."ID" AS "ID"
			, "SAT_TEMP_SRC_US"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "SAT_TEMP_SRC_US"."CREATEDBYID" AS "CREATEDBYID"
			, "SAT_TEMP_SRC_US"."ORIGINALORDERITEMID" AS "ORIGINALORDERITEMID"
			, "SAT_TEMP_SRC_US"."ORDERID" AS "ORDERID"
			, "SAT_TEMP_SRC_US"."PRICEBOOKENTRYID" AS "PRICEBOOKENTRYID"
			, "SAT_TEMP_SRC_US"."ISDELETED" AS "ISDELETED"
			, "SAT_TEMP_SRC_US"."AVAILABLEQUANTITY" AS "AVAILABLEQUANTITY"
			, "SAT_TEMP_SRC_US"."QUANTITY" AS "QUANTITY"
			, "SAT_TEMP_SRC_US"."UNITPRICE" AS "UNITPRICE"
			, "SAT_TEMP_SRC_US"."LISTPRICE" AS "LISTPRICE"
			, "SAT_TEMP_SRC_US"."SERVICEDATE" AS "SERVICEDATE"
			, "SAT_TEMP_SRC_US"."ENDDATE" AS "ENDDATE"
			, "SAT_TEMP_SRC_US"."DESCRIPTION" AS "DESCRIPTION"
			, "SAT_TEMP_SRC_US"."CREATEDDATE" AS "CREATEDDATE"
			, "SAT_TEMP_SRC_US"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "SAT_TEMP_SRC_US"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "SAT_TEMP_SRC_US"."ORDERITEMNUMBER" AS "ORDERITEMNUMBER"
		FROM {{ ref('SALESFORCE_STG_SAT_SALESFORCE_ORDERITEM_TMP') }} "SAT_TEMP_SRC_US"
		INNER JOIN "END_DATING" "END_DATING" ON  "SAT_TEMP_SRC_US"."ORDERITEM_HKEY" = "END_DATING"."ORDERITEM_HKEY" AND "SAT_TEMP_SRC_US"."LOAD_DATE" = 
			"END_DATING"."LOAD_DATE"
		WHERE  "SAT_TEMP_SRC_US"."EQUAL" = 0 AND NOT("SAT_TEMP_SRC_US"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "SAT_TEMP_SRC_US"."SOURCE" = 'STG')
	)
	, "FILTER_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "CALC_LOAD_END_DATE"."ORDERITEM_HKEY" AS "ORDERITEM_HKEY"
			, "CALC_LOAD_END_DATE"."LOAD_DATE" AS "LOAD_DATE"
			, "CALC_LOAD_END_DATE"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CALC_LOAD_END_DATE"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, "CALC_LOAD_END_DATE"."DELETE_FLAG" AS "DELETE_FLAG"
			, "CALC_LOAD_END_DATE"."HASH_DIFF" AS "HASH_DIFF"
			, "CALC_LOAD_END_DATE"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CALC_LOAD_END_DATE"."ID" AS "ID"
			, "CALC_LOAD_END_DATE"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CALC_LOAD_END_DATE"."CREATEDBYID" AS "CREATEDBYID"
			, "CALC_LOAD_END_DATE"."ORIGINALORDERITEMID" AS "ORIGINALORDERITEMID"
			, "CALC_LOAD_END_DATE"."ORDERID" AS "ORDERID"
			, "CALC_LOAD_END_DATE"."PRICEBOOKENTRYID" AS "PRICEBOOKENTRYID"
			, "CALC_LOAD_END_DATE"."ISDELETED" AS "ISDELETED"
			, "CALC_LOAD_END_DATE"."AVAILABLEQUANTITY" AS "AVAILABLEQUANTITY"
			, "CALC_LOAD_END_DATE"."QUANTITY" AS "QUANTITY"
			, "CALC_LOAD_END_DATE"."UNITPRICE" AS "UNITPRICE"
			, "CALC_LOAD_END_DATE"."LISTPRICE" AS "LISTPRICE"
			, "CALC_LOAD_END_DATE"."SERVICEDATE" AS "SERVICEDATE"
			, "CALC_LOAD_END_DATE"."ENDDATE" AS "ENDDATE"
			, "CALC_LOAD_END_DATE"."DESCRIPTION" AS "DESCRIPTION"
			, "CALC_LOAD_END_DATE"."CREATEDDATE" AS "CREATEDDATE"
			, "CALC_LOAD_END_DATE"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CALC_LOAD_END_DATE"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "CALC_LOAD_END_DATE"."ORDERITEMNUMBER" AS "ORDERITEMNUMBER"
		FROM "CALC_LOAD_END_DATE" "CALC_LOAD_END_DATE"
		WHERE  "CALC_LOAD_END_DATE"."DELETE_FLAG" = CAST('Y' AS VARCHAR) OR "CALC_LOAD_END_DATE"."SOURCE" = 'STG' OR("CALC_LOAD_END_DATE"."SOURCE" = 'SAT' AND "CALC_LOAD_END_DATE"."LOAD_END_DATE" != TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'))
	)
	SELECT 
		  "FILTER_LOAD_END_DATE"."ORDERITEM_HKEY"
		, "FILTER_LOAD_END_DATE"."LOAD_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_END_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_CYCLE_ID"
		, "FILTER_LOAD_END_DATE"."HASH_DIFF"
		, "FILTER_LOAD_END_DATE"."DELETE_FLAG"
		, "FILTER_LOAD_END_DATE"."CDC_TIMESTAMP"
		, "FILTER_LOAD_END_DATE"."ID"
		, "FILTER_LOAD_END_DATE"."LASTMODIFIEDBYID"
		, "FILTER_LOAD_END_DATE"."CREATEDBYID"
		, "FILTER_LOAD_END_DATE"."ORIGINALORDERITEMID"
		, "FILTER_LOAD_END_DATE"."ORDERID"
		, "FILTER_LOAD_END_DATE"."PRICEBOOKENTRYID"
		, "FILTER_LOAD_END_DATE"."ISDELETED"
		, "FILTER_LOAD_END_DATE"."AVAILABLEQUANTITY"
		, "FILTER_LOAD_END_DATE"."QUANTITY"
		, "FILTER_LOAD_END_DATE"."UNITPRICE"
		, "FILTER_LOAD_END_DATE"."LISTPRICE"
		, "FILTER_LOAD_END_DATE"."SERVICEDATE"
		, "FILTER_LOAD_END_DATE"."ENDDATE"
		, "FILTER_LOAD_END_DATE"."DESCRIPTION"
		, "FILTER_LOAD_END_DATE"."CREATEDDATE"
		, "FILTER_LOAD_END_DATE"."LASTMODIFIEDDATE"
		, "FILTER_LOAD_END_DATE"."SYSTEMMODSTAMP"
		, "FILTER_LOAD_END_DATE"."ORDERITEMNUMBER"
	FROM "FILTER_LOAD_END_DATE"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	WITH "STG_SRC" AS 
	( 
		SELECT 
			  "STG_INR_SRC"."ORDERITEM_HKEY" AS "ORDERITEM_HKEY"
			, "STG_INR_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_INR_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ISDELETED"),'~'),'\#','\\' || '\#')
				)|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."AVAILABLEQUANTITY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."QUANTITY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."UNITPRICE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."LISTPRICE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."SERVICEDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ENDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."DESCRIPTION"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."CREATEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."LASTMODIFIEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."SYSTEMMODSTAMP"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ORDERITEMNUMBER"),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CAST('N' AS VARCHAR) AS "DELETE_FLAG"
			, "STG_INR_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_INR_SRC"."ID" AS "ID"
			, "STG_INR_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "STG_INR_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "STG_INR_SRC"."ORIGINALORDERITEMID" AS "ORIGINALORDERITEMID"
			, "STG_INR_SRC"."ORDERID" AS "ORDERID"
			, "STG_INR_SRC"."PRICEBOOKENTRYID" AS "PRICEBOOKENTRYID"
			, "STG_INR_SRC"."ISDELETED" AS "ISDELETED"
			, "STG_INR_SRC"."AVAILABLEQUANTITY" AS "AVAILABLEQUANTITY"
			, "STG_INR_SRC"."QUANTITY" AS "QUANTITY"
			, "STG_INR_SRC"."UNITPRICE" AS "UNITPRICE"
			, "STG_INR_SRC"."LISTPRICE" AS "LISTPRICE"
			, "STG_INR_SRC"."SERVICEDATE" AS "SERVICEDATE"
			, "STG_INR_SRC"."ENDDATE" AS "ENDDATE"
			, "STG_INR_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "STG_INR_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "STG_INR_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "STG_INR_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "STG_INR_SRC"."ORDERITEMNUMBER" AS "ORDERITEMNUMBER"
			, ROW_NUMBER()OVER(PARTITION BY "STG_INR_SRC"."ORDERITEM_HKEY" ORDER BY "STG_INR_SRC"."LOAD_DATE") AS "DUMMY"
		FROM {{ ref('SALESFORCE_STG_ORDERITEM') }} "STG_INR_SRC"
	)
	SELECT 
		  "STG_SRC"."ORDERITEM_HKEY" AS "ORDERITEM_HKEY"
		, "STG_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "STG_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "STG_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "STG_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "STG_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "STG_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "STG_SRC"."ID" AS "ID"
		, "STG_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "STG_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "STG_SRC"."ORIGINALORDERITEMID" AS "ORIGINALORDERITEMID"
		, "STG_SRC"."ORDERID" AS "ORDERID"
		, "STG_SRC"."PRICEBOOKENTRYID" AS "PRICEBOOKENTRYID"
		, "STG_SRC"."ISDELETED" AS "ISDELETED"
		, "STG_SRC"."AVAILABLEQUANTITY" AS "AVAILABLEQUANTITY"
		, "STG_SRC"."QUANTITY" AS "QUANTITY"
		, "STG_SRC"."UNITPRICE" AS "UNITPRICE"
		, "STG_SRC"."LISTPRICE" AS "LISTPRICE"
		, "STG_SRC"."SERVICEDATE" AS "SERVICEDATE"
		, "STG_SRC"."ENDDATE" AS "ENDDATE"
		, "STG_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "STG_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "STG_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "STG_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "STG_SRC"."ORDERITEMNUMBER" AS "ORDERITEMNUMBER"
	FROM "STG_SRC" "STG_SRC"
	WHERE  "STG_SRC"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'