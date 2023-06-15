{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LKS_JDE_BUSINESSUNITMASTER_ACCOUNT_TMP',
		schema='JDEDWARDS_STG',
		tags=['JDEDWARDS', 'LKS_JDE_BUSINESSUNITMASTER_ACCOUNT_INCR']
	)
}}
select * from (
	WITH "DIST_STG" AS 
	( 
		SELECT 
			  "STG_DIS_SRC"."BUSINESSUNITMASTER_HKEY" AS "BUSINESSUNITMASTER_HKEY"
			, "STG_DIS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, MIN("STG_DIS_SRC"."LOAD_DATE") AS "MIN_LOAD_TIMESTAMP"
		FROM {{ ref('JDEDWARDS_STG_F0006') }} "STG_DIS_SRC"
		GROUP BY  "STG_DIS_SRC"."BUSINESSUNITMASTER_HKEY",  "STG_DIS_SRC"."LOAD_CYCLE_ID"
	)
	, "TEMP_TABLE_SET" AS 
	( 
		SELECT 
			  "STG_TEMP_SRC"."LNK_BUSINESSUNITMASTER_ACCOUNT_HKEY" AS "LNK_BUSINESSUNITMASTER_ACCOUNT_HKEY"
			, "STG_TEMP_SRC"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
			, "STG_TEMP_SRC"."BUSINESSUNITMASTER_HKEY" AS "BUSINESSUNITMASTER_HKEY"
			, "STG_TEMP_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_TEMP_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_TEMP_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, 'STG' AS "SOURCE"
			, 1 AS "ORIGIN_ID"
			, CASE WHEN "STG_TEMP_SRC"."JRN_FLAG" = 'D' THEN CAST('Y' AS VARCHAR) ELSE CAST('N' AS VARCHAR) END AS "DELETE_FLAG"
			, "STG_TEMP_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_TEMP_SRC"."MCMCU" AS "MCMCU"
			, "STG_TEMP_SRC"."MCAN8" AS "MCAN8"
		FROM {{ ref('JDEDWARDS_STG_F0006') }} "STG_TEMP_SRC"
		UNION 
		SELECT 
			  "LKS_SRC"."LNK_BUSINESSUNITMASTER_ACCOUNT_HKEY" AS "LNK_BUSINESSUNITMASTER_ACCOUNT_HKEY"
			, "LNK_SRC"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
			, "LNK_SRC"."BUSINESSUNITMASTER_HKEY" AS "BUSINESSUNITMASTER_HKEY"
			, "LKS_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "LKS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "LKS_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, 'SAT' AS "RECORD_TYPE"
			, 'LKS' AS "SOURCE"
			, 0 AS "ORIGIN_ID"
			, "LKS_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
			, "LKS_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "LKS_SRC"."MCMCU" AS "MCMCU"
			, "LKS_SRC"."MCAN8" AS "MCAN8"
		FROM {{ source('DEMO_FL', 'LKS_JDE_BUSINESSUNITMASTER_ACCOUNT') }} "LKS_SRC"
		INNER JOIN {{ source('DEMO_FL', 'LNK_BUSINESSUNITMASTER_ACCOUNT') }} "LNK_SRC" ON  "LKS_SRC"."LNK_BUSINESSUNITMASTER_ACCOUNT_HKEY" = "LNK_SRC"."LNK_BUSINESSUNITMASTER_ACCOUNT_HKEY"
		INNER JOIN "DIST_STG" "DIST_STG" ON  "LNK_SRC"."BUSINESSUNITMASTER_HKEY" = "DIST_STG"."BUSINESSUNITMASTER_HKEY"
		WHERE  "LKS_SRC"."LOAD_END_DATE" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS')
	)
	SELECT 
		  "TEMP_TABLE_SET"."LNK_BUSINESSUNITMASTER_ACCOUNT_HKEY" AS "LNK_BUSINESSUNITMASTER_ACCOUNT_HKEY"
		, "TEMP_TABLE_SET"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
		, "TEMP_TABLE_SET"."BUSINESSUNITMASTER_HKEY" AS "BUSINESSUNITMASTER_HKEY"
		, "TEMP_TABLE_SET"."LOAD_DATE" AS "LOAD_DATE"
		, "TEMP_TABLE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "TEMP_TABLE_SET"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "TEMP_TABLE_SET"."RECORD_TYPE" AS "RECORD_TYPE"
		, "TEMP_TABLE_SET"."SOURCE" AS "SOURCE"
		, CASE WHEN "TEMP_TABLE_SET"."SOURCE" = 'STG' AND TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."ACCOUNT_HKEY")
			= LAG( TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."ACCOUNT_HKEY"),1)OVER(PARTITION BY "TEMP_TABLE_SET"."BUSINESSUNITMASTER_HKEY" ORDER BY "TEMP_TABLE_SET"."LOAD_DATE","TEMP_TABLE_SET"."ORIGIN_ID")THEN 1 ELSE 0 END AS "EQUAL"
		, "TEMP_TABLE_SET"."DELETE_FLAG" AS "DELETE_FLAG"
		, "TEMP_TABLE_SET"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "TEMP_TABLE_SET"."MCMCU" AS "MCMCU"
		, "TEMP_TABLE_SET"."MCAN8" AS "MCAN8"
	FROM "TEMP_TABLE_SET" "TEMP_TABLE_SET"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'