{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LKS_SALESFORCE_ORDERS_USER_ACTIVATEDBYID',
		schema='DEMO_FL',
		unique_key = ['"LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"', '"LOAD_DATE"'],
		merge_update_columns = ['"LOAD_END_DATE"', '"DELETE_FLAG"', '"CDC_TIMESTAMP"'],
		tags=['SALESFORCE', 'LKS_SALESFORCE_ORDERS_USER_ACTIVATEDBYID_INCR', 'LKS_SALESFORCE_ORDERS_USER_ACTIVATEDBYID_INIT']
	)
}}
select * from (
	WITH "END_DATING" AS 
	( 
		SELECT 
			  "LKS_TEMP_SRC_DK"."ORDERS_HKEY" AS "ORDERS_HKEY"
			, "LKS_TEMP_SRC_DK"."LOAD_DATE" AS "LOAD_DATE"
			, CASE WHEN LEAD( "LKS_TEMP_SRC_DK"."DELETE_FLAG" || "LKS_TEMP_SRC_DK"."SOURCE",1)OVER(PARTITION BY "LKS_TEMP_SRC_DK"."ORDERS_HKEY" ORDER BY "LKS_TEMP_SRC_DK"."LOAD_DATE",
				"LKS_TEMP_SRC_DK"."LOAD_CYCLE_ID")= 'Y' || 'STG' THEN CAST('Y' AS VARCHAR) ELSE "LKS_TEMP_SRC_DK"."DELETE_FLAG" END AS "DELETE_FLAG"
			, LEAD("LKS_TEMP_SRC_DK"."CDC_TIMESTAMP",1)OVER(PARTITION BY "LKS_TEMP_SRC_DK"."ORDERS_HKEY" ORDER BY "LKS_TEMP_SRC_DK"."LOAD_DATE",
				"LKS_TEMP_SRC_DK"."LOAD_CYCLE_ID") AS "CDC_TIMESTAMP"
		FROM {{ ref('SALESFORCE_STG_LKS_SALESFORCE_ORDERS_USER_ACTIVATEDBYID_TMP') }} "LKS_TEMP_SRC_DK"
		WHERE  "LKS_TEMP_SRC_DK"."EQUAL" = 0
	)
	, "CALC_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "LKS_TEMP_SRC_US"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
			, "LKS_TEMP_SRC_US"."LOAD_DATE" AS "LOAD_DATE"
			, "LKS_TEMP_SRC_US"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, COALESCE(LEAD("LKS_TEMP_SRC_US"."LOAD_DATE",1)OVER(PARTITION BY "LKS_TEMP_SRC_US"."ORDERS_HKEY" ORDER BY "LKS_TEMP_SRC_US"."LOAD_DATE",
				"LKS_TEMP_SRC_US"."LOAD_CYCLE_ID"), TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS')) AS "LOAD_END_DATE"
			, "END_DATING"."DELETE_FLAG" AS "DELETE_FLAG"
			, "LKS_TEMP_SRC_US"."SOURCE" AS "SOURCE"
			, CASE WHEN "END_DATING"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "LKS_TEMP_SRC_US"."DELETE_FLAG" = CAST('N' AS VARCHAR)
				THEN "END_DATING"."CDC_TIMESTAMP" ELSE "LKS_TEMP_SRC_US"."CDC_TIMESTAMP" END AS "CDC_TIMESTAMP"
			, "LKS_TEMP_SRC_US"."ID" AS "ID"
			, "LKS_TEMP_SRC_US"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		FROM {{ ref('SALESFORCE_STG_LKS_SALESFORCE_ORDERS_USER_ACTIVATEDBYID_TMP') }} "LKS_TEMP_SRC_US"
		INNER JOIN "END_DATING" "END_DATING" ON  "LKS_TEMP_SRC_US"."ORDERS_HKEY" = "END_DATING"."ORDERS_HKEY" AND "LKS_TEMP_SRC_US"."LOAD_DATE" = "END_DATING"."LOAD_DATE"
		WHERE  "LKS_TEMP_SRC_US"."EQUAL" = 0 AND NOT("LKS_TEMP_SRC_US"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "LKS_TEMP_SRC_US"."SOURCE" = 'STG')
	)
	, "FILTER_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "CALC_LOAD_END_DATE"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
			, "CALC_LOAD_END_DATE"."LOAD_DATE" AS "LOAD_DATE"
			, "CALC_LOAD_END_DATE"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CALC_LOAD_END_DATE"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, "CALC_LOAD_END_DATE"."DELETE_FLAG" AS "DELETE_FLAG"
			, "CALC_LOAD_END_DATE"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CALC_LOAD_END_DATE"."ID" AS "ID"
			, "CALC_LOAD_END_DATE"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		FROM "CALC_LOAD_END_DATE" "CALC_LOAD_END_DATE"
		WHERE  "CALC_LOAD_END_DATE"."DELETE_FLAG" = CAST('Y' AS VARCHAR) OR "CALC_LOAD_END_DATE"."SOURCE" = 'STG' OR("CALC_LOAD_END_DATE"."SOURCE" = 'LKS' AND "CALC_LOAD_END_DATE"."LOAD_END_DATE" != TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'))
	)
	SELECT 
		  "FILTER_LOAD_END_DATE"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
		, "FILTER_LOAD_END_DATE"."LOAD_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_CYCLE_ID"
		, "FILTER_LOAD_END_DATE"."LOAD_END_DATE"
		, "FILTER_LOAD_END_DATE"."DELETE_FLAG"
		, "FILTER_LOAD_END_DATE"."CDC_TIMESTAMP"
		, "FILTER_LOAD_END_DATE"."ID"
		, "FILTER_LOAD_END_DATE"."ACTIVATEDBYID"
	FROM "FILTER_LOAD_END_DATE"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	WITH "STG_SRC" AS 
	( 
		SELECT 
			  "STG_INR_SRC"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
			, "STG_INR_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_INR_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, CAST('N' AS VARCHAR) AS "DELETE_FLAG"
			, "STG_INR_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_INR_SRC"."ID" AS "ID"
			, "STG_INR_SRC"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, ROW_NUMBER()OVER(PARTITION BY "STG_INR_SRC"."ORDERS_HKEY" ORDER BY "STG_INR_SRC"."LOAD_DATE") AS "DUMMY"
		FROM {{ ref('SALESFORCE_STG_ORDERS') }} "STG_INR_SRC"
	)
	SELECT 
		  "STG_SRC"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
		, "STG_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "STG_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "STG_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "STG_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "STG_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "STG_SRC"."ID" AS "ID"
		, "STG_SRC"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
	FROM "STG_SRC" "STG_SRC"
	WHERE  "STG_SRC"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'