{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='HUB_SALESORDERHEADERHISTORY',
		schema='DEMO_FL',
		unique_key = ['"SALESORDERHEADERHISTORY_HKEY"'],
		merge_update_columns = [],
		tags=['JDEDWARDS', 'HUB_JDE_SALESORDERHEADERHISTORY_INCR', 'HUB_JDE_SALESORDERHEADERHISTORY_INIT']
	)
}}
select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."SALESORDERHEADERHISTORY_HKEY" AS "SALESORDERHEADERHISTORY_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."SHDOCO_BK" AS "SHDOCO_BK"
			, "STG_SRC1"."SHDCTO_BK" AS "SHDCTO_BK"
			, "STG_SRC1"."SHKCOO_BK" AS "SHKCOO_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('JDEDWARDS_STG_F42019') }} "STG_SRC1"
		WHERE  "STG_SRC1"."RECORD_TYPE" = 'S'
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."SALESORDERHEADERHISTORY_HKEY" AS "SALESORDERHEADERHISTORY_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."SHDOCO_BK" AS "SHDOCO_BK"
			, "CHANGE_SET"."SHDCTO_BK" AS "SHDCTO_BK"
			, "CHANGE_SET"."SHKCOO_BK" AS "SHKCOO_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."SALESORDERHEADERHISTORY_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER",
				"CHANGE_SET"."LOAD_DATE","CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	, "MIV" AS 
	( 
		SELECT 
			  "MIN_LOAD_TIME"."SALESORDERHEADERHISTORY_HKEY" AS "SALESORDERHEADERHISTORY_HKEY"
			, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
			, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "MIN_LOAD_TIME"."SHDOCO_BK" AS "SHDOCO_BK"
			, "MIN_LOAD_TIME"."SHDCTO_BK" AS "SHDCTO_BK"
			, "MIN_LOAD_TIME"."SHKCOO_BK" AS "SHKCOO_BK"
		FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
		WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	)
	SELECT 
		  "MIV"."SALESORDERHEADERHISTORY_HKEY"
		, "MIV"."LOAD_DATE"
		, "MIV"."LOAD_CYCLE_ID"
		, "MIV"."SHDOCO_BK"
		, "MIV"."SHDCTO_BK"
		, "MIV"."SHKCOO_BK"
	FROM "MIV"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."SALESORDERHEADERHISTORY_HKEY" AS "SALESORDERHEADERHISTORY_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."SHDOCO_BK" AS "SHDOCO_BK"
			, "STG_SRC1"."SHDCTO_BK" AS "SHDCTO_BK"
			, "STG_SRC1"."SHKCOO_BK" AS "SHKCOO_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('JDEDWARDS_STG_F42019') }} "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."SALESORDERHEADERHISTORY_HKEY" AS "SALESORDERHEADERHISTORY_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."SHDOCO_BK" AS "SHDOCO_BK"
			, "CHANGE_SET"."SHDCTO_BK" AS "SHDCTO_BK"
			, "CHANGE_SET"."SHKCOO_BK" AS "SHKCOO_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."SALESORDERHEADERHISTORY_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER",
				"CHANGE_SET"."LOAD_DATE","CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."SALESORDERHEADERHISTORY_HKEY" AS "SALESORDERHEADERHISTORY_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."SHDOCO_BK" AS "SHDOCO_BK"
		, "MIN_LOAD_TIME"."SHDCTO_BK" AS "SHDCTO_BK"
		, "MIN_LOAD_TIME"."SHKCOO_BK" AS "SHKCOO_BK"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'