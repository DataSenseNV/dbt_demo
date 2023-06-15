{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LKS_JDE_SOHEADERHIST_BUSINESSUNITMASTER_TMP',
		schema='JDEDWARDS_STG',
		tags=['JDEDWARDS', 'LKS_JDE_SOHEADERHIST_BUSINESSUNITMASTER_INCR']
	)
}}
select * from (
	WITH "DIST_STG" AS 
	( 
		SELECT 
			  "STG_DIS_SRC"."SALESORDERHEADERHISTORY_HKEY" AS "SALESORDERHEADERHISTORY_HKEY"
			, "STG_DIS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, MIN("STG_DIS_SRC"."LOAD_DATE") AS "MIN_LOAD_TIMESTAMP"
		FROM {{ ref('JDEDWARDS_STG_F42019') }} "STG_DIS_SRC"
		GROUP BY  "STG_DIS_SRC"."SALESORDERHEADERHISTORY_HKEY",  "STG_DIS_SRC"."LOAD_CYCLE_ID"
	)
	, "TEMP_TABLE_SET" AS 
	( 
		SELECT 
			  "STG_TEMP_SRC"."LNK_SOHEADERHIST_BUSINESSUNITMASTER_HKEY" AS "LNK_SOHEADERHIST_BUSINESSUNITMASTER_HKEY"
			, "STG_TEMP_SRC"."BUSINESSUNITMASTER_HKEY" AS "BUSINESSUNITMASTER_HKEY"
			, "STG_TEMP_SRC"."SALESORDERHEADERHISTORY_HKEY" AS "SALESORDERHEADERHISTORY_HKEY"
			, "STG_TEMP_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_TEMP_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_TEMP_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, 'STG' AS "SOURCE"
			, 1 AS "ORIGIN_ID"
			, CASE WHEN "STG_TEMP_SRC"."JRN_FLAG" = 'D' THEN CAST('Y' AS VARCHAR) ELSE CAST('N' AS VARCHAR) END AS "DELETE_FLAG"
			, "STG_TEMP_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_TEMP_SRC"."SHKCOO" AS "SHKCOO"
			, "STG_TEMP_SRC"."SHDCTO" AS "SHDCTO"
			, "STG_TEMP_SRC"."SHDOCO" AS "SHDOCO"
			, "STG_TEMP_SRC"."SHMCU" AS "SHMCU"
		FROM {{ ref('JDEDWARDS_STG_F42019') }} "STG_TEMP_SRC"
		UNION 
		SELECT 
			  "LKS_SRC"."LNK_SOHEADERHIST_BUSINESSUNITMASTER_HKEY" AS "LNK_SOHEADERHIST_BUSINESSUNITMASTER_HKEY"
			, "LNK_SRC"."BUSINESSUNITMASTER_HKEY" AS "BUSINESSUNITMASTER_HKEY"
			, "LNK_SRC"."SALESORDERHEADERHISTORY_HKEY" AS "SALESORDERHEADERHISTORY_HKEY"
			, "LKS_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "LKS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "LKS_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, 'SAT' AS "RECORD_TYPE"
			, 'LKS' AS "SOURCE"
			, 0 AS "ORIGIN_ID"
			, "LKS_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
			, "LKS_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "LKS_SRC"."SHKCOO" AS "SHKCOO"
			, "LKS_SRC"."SHDCTO" AS "SHDCTO"
			, "LKS_SRC"."SHDOCO" AS "SHDOCO"
			, "LKS_SRC"."SHMCU" AS "SHMCU"
		FROM {{ source('DEMO_FL', 'LKS_JDE_SOHEADERHIST_BUSINESSUNITMASTER') }} "LKS_SRC"
		INNER JOIN {{ source('DEMO_FL', 'LNK_SOHEADERHIST_BUSINESSUNITMASTER') }} "LNK_SRC" ON  "LKS_SRC"."LNK_SOHEADERHIST_BUSINESSUNITMASTER_HKEY" = "LNK_SRC"."LNK_SOHEADERHIST_BUSINESSUNITMASTER_HKEY"
		INNER JOIN "DIST_STG" "DIST_STG" ON  "LNK_SRC"."SALESORDERHEADERHISTORY_HKEY" = "DIST_STG"."SALESORDERHEADERHISTORY_HKEY"
		WHERE  "LKS_SRC"."LOAD_END_DATE" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS')
	)
	SELECT 
		  "TEMP_TABLE_SET"."LNK_SOHEADERHIST_BUSINESSUNITMASTER_HKEY" AS "LNK_SOHEADERHIST_BUSINESSUNITMASTER_HKEY"
		, "TEMP_TABLE_SET"."BUSINESSUNITMASTER_HKEY" AS "BUSINESSUNITMASTER_HKEY"
		, "TEMP_TABLE_SET"."SALESORDERHEADERHISTORY_HKEY" AS "SALESORDERHEADERHISTORY_HKEY"
		, "TEMP_TABLE_SET"."LOAD_DATE" AS "LOAD_DATE"
		, "TEMP_TABLE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "TEMP_TABLE_SET"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "TEMP_TABLE_SET"."RECORD_TYPE" AS "RECORD_TYPE"
		, "TEMP_TABLE_SET"."SOURCE" AS "SOURCE"
		, CASE WHEN "TEMP_TABLE_SET"."SOURCE" = 'STG' AND TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."BUSINESSUNITMASTER_HKEY")
			= LAG( TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."BUSINESSUNITMASTER_HKEY"),1)OVER(PARTITION BY "TEMP_TABLE_SET"."SALESORDERHEADERHISTORY_HKEY" ORDER BY "TEMP_TABLE_SET"."LOAD_DATE","TEMP_TABLE_SET"."ORIGIN_ID")THEN 1 ELSE 0 END AS "EQUAL"
		, "TEMP_TABLE_SET"."DELETE_FLAG" AS "DELETE_FLAG"
		, "TEMP_TABLE_SET"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "TEMP_TABLE_SET"."SHKCOO" AS "SHKCOO"
		, "TEMP_TABLE_SET"."SHDCTO" AS "SHDCTO"
		, "TEMP_TABLE_SET"."SHDOCO" AS "SHDOCO"
		, "TEMP_TABLE_SET"."SHMCU" AS "SHMCU"
	FROM "TEMP_TABLE_SET" "TEMP_TABLE_SET"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'