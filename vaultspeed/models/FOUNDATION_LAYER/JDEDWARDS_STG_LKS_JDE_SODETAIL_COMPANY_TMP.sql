{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LKS_JDE_SODETAIL_COMPANY_TMP',
		schema='JDEDWARDS_STG',
		tags=['JDEDWARDS', 'LKS_JDE_SODETAIL_COMPANY_INCR']
	)
}}
select * from (
	WITH "DIST_STG" AS 
	( 
		SELECT 
			  "STG_DIS_SRC"."SALESORDERDETAIL_HKEY" AS "SALESORDERDETAIL_HKEY"
			, "STG_DIS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, MIN("STG_DIS_SRC"."LOAD_DATE") AS "MIN_LOAD_TIMESTAMP"
		FROM {{ ref('JDEDWARDS_STG_F4211') }} "STG_DIS_SRC"
		GROUP BY  "STG_DIS_SRC"."SALESORDERDETAIL_HKEY",  "STG_DIS_SRC"."LOAD_CYCLE_ID"
	)
	, "TEMP_TABLE_SET" AS 
	( 
		SELECT 
			  "STG_TEMP_SRC"."LNK_SODETAIL_COMPANY_HKEY" AS "LNK_SODETAIL_COMPANY_HKEY"
			, "STG_TEMP_SRC"."COMPANY_HKEY" AS "COMPANY_HKEY"
			, "STG_TEMP_SRC"."SALESORDERDETAIL_HKEY" AS "SALESORDERDETAIL_HKEY"
			, "STG_TEMP_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_TEMP_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_TEMP_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, 'STG' AS "SOURCE"
			, 1 AS "ORIGIN_ID"
			, CASE WHEN "STG_TEMP_SRC"."JRN_FLAG" = 'D' THEN CAST('Y' AS VARCHAR) ELSE CAST('N' AS VARCHAR) END AS "DELETE_FLAG"
			, "STG_TEMP_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_TEMP_SRC"."SDLNID" AS "SDLNID"
			, "STG_TEMP_SRC"."SDDCTO" AS "SDDCTO"
			, "STG_TEMP_SRC"."SDDOCO" AS "SDDOCO"
			, "STG_TEMP_SRC"."SDKCOO" AS "SDKCOO"
			, "STG_TEMP_SRC"."SDCO" AS "SDCO"
		FROM {{ ref('JDEDWARDS_STG_F4211') }} "STG_TEMP_SRC"
		UNION 
		SELECT 
			  "LKS_SRC"."LNK_SODETAIL_COMPANY_HKEY" AS "LNK_SODETAIL_COMPANY_HKEY"
			, "LNK_SRC"."COMPANY_HKEY" AS "COMPANY_HKEY"
			, "LNK_SRC"."SALESORDERDETAIL_HKEY" AS "SALESORDERDETAIL_HKEY"
			, "LKS_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "LKS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "LKS_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, 'SAT' AS "RECORD_TYPE"
			, 'LKS' AS "SOURCE"
			, 0 AS "ORIGIN_ID"
			, "LKS_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
			, "LKS_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "LKS_SRC"."SDLNID" AS "SDLNID"
			, "LKS_SRC"."SDDCTO" AS "SDDCTO"
			, "LKS_SRC"."SDDOCO" AS "SDDOCO"
			, "LKS_SRC"."SDKCOO" AS "SDKCOO"
			, "LKS_SRC"."SDCO" AS "SDCO"
		FROM {{ source('DEMO_FL', 'LKS_JDE_SODETAIL_COMPANY') }} "LKS_SRC"
		INNER JOIN {{ source('DEMO_FL', 'LNK_SODETAIL_COMPANY') }} "LNK_SRC" ON  "LKS_SRC"."LNK_SODETAIL_COMPANY_HKEY" = "LNK_SRC"."LNK_SODETAIL_COMPANY_HKEY"
		INNER JOIN "DIST_STG" "DIST_STG" ON  "LNK_SRC"."SALESORDERDETAIL_HKEY" = "DIST_STG"."SALESORDERDETAIL_HKEY"
		WHERE  "LKS_SRC"."LOAD_END_DATE" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS')
	)
	SELECT 
		  "TEMP_TABLE_SET"."LNK_SODETAIL_COMPANY_HKEY" AS "LNK_SODETAIL_COMPANY_HKEY"
		, "TEMP_TABLE_SET"."COMPANY_HKEY" AS "COMPANY_HKEY"
		, "TEMP_TABLE_SET"."SALESORDERDETAIL_HKEY" AS "SALESORDERDETAIL_HKEY"
		, "TEMP_TABLE_SET"."LOAD_DATE" AS "LOAD_DATE"
		, "TEMP_TABLE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "TEMP_TABLE_SET"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "TEMP_TABLE_SET"."RECORD_TYPE" AS "RECORD_TYPE"
		, "TEMP_TABLE_SET"."SOURCE" AS "SOURCE"
		, CASE WHEN "TEMP_TABLE_SET"."SOURCE" = 'STG' AND TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."COMPANY_HKEY")
			= LAG( TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."COMPANY_HKEY"),1)OVER(PARTITION BY "TEMP_TABLE_SET"."SALESORDERDETAIL_HKEY" ORDER BY "TEMP_TABLE_SET"."LOAD_DATE","TEMP_TABLE_SET"."ORIGIN_ID")THEN 1 ELSE 0 END AS "EQUAL"
		, "TEMP_TABLE_SET"."DELETE_FLAG" AS "DELETE_FLAG"
		, "TEMP_TABLE_SET"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "TEMP_TABLE_SET"."SDLNID" AS "SDLNID"
		, "TEMP_TABLE_SET"."SDDCTO" AS "SDDCTO"
		, "TEMP_TABLE_SET"."SDDOCO" AS "SDDOCO"
		, "TEMP_TABLE_SET"."SDKCOO" AS "SDKCOO"
		, "TEMP_TABLE_SET"."SDCO" AS "SDCO"
	FROM "TEMP_TABLE_SET" "TEMP_TABLE_SET"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'