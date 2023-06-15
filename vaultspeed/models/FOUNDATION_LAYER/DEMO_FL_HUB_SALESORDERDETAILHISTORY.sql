{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='HUB_SALESORDERDETAILHISTORY',
		schema='DEMO_FL',
		unique_key = ['"SALESORDERDETAILHISTORY_HKEY"'],
		merge_update_columns = [],
		tags=['JDEDWARDS', 'HUB_JDE_SALESORDERDETAILHISTORY_INCR', 'HUB_JDE_SALESORDERDETAILHISTORY_INIT']
	)
}}
select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."SALESORDERDETAILHISTORY_HKEY" AS "SALESORDERDETAILHISTORY_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."SDKCOO_BK" AS "SDKCOO_BK"
			, "STG_SRC1"."SDDOCO_BK" AS "SDDOCO_BK"
			, "STG_SRC1"."SDDCTO_BK" AS "SDDCTO_BK"
			, "STG_SRC1"."SDLNID_BK" AS "SDLNID_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('JDEDWARDS_STG_F42119') }} "STG_SRC1"
		WHERE  "STG_SRC1"."RECORD_TYPE" = 'S'
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."SALESORDERDETAILHISTORY_HKEY" AS "SALESORDERDETAILHISTORY_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."SDKCOO_BK" AS "SDKCOO_BK"
			, "CHANGE_SET"."SDDOCO_BK" AS "SDDOCO_BK"
			, "CHANGE_SET"."SDDCTO_BK" AS "SDDCTO_BK"
			, "CHANGE_SET"."SDLNID_BK" AS "SDLNID_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."SALESORDERDETAILHISTORY_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER",
				"CHANGE_SET"."LOAD_DATE","CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	, "MIV" AS 
	( 
		SELECT 
			  "MIN_LOAD_TIME"."SALESORDERDETAILHISTORY_HKEY" AS "SALESORDERDETAILHISTORY_HKEY"
			, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
			, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "MIN_LOAD_TIME"."SDKCOO_BK" AS "SDKCOO_BK"
			, "MIN_LOAD_TIME"."SDDOCO_BK" AS "SDDOCO_BK"
			, "MIN_LOAD_TIME"."SDDCTO_BK" AS "SDDCTO_BK"
			, "MIN_LOAD_TIME"."SDLNID_BK" AS "SDLNID_BK"
		FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
		WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	)
	SELECT 
		  "MIV"."SALESORDERDETAILHISTORY_HKEY"
		, "MIV"."LOAD_DATE"
		, "MIV"."LOAD_CYCLE_ID"
		, "MIV"."SDKCOO_BK"
		, "MIV"."SDDOCO_BK"
		, "MIV"."SDDCTO_BK"
		, "MIV"."SDLNID_BK"
	FROM "MIV"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."SALESORDERDETAILHISTORY_HKEY" AS "SALESORDERDETAILHISTORY_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."SDKCOO_BK" AS "SDKCOO_BK"
			, "STG_SRC1"."SDDOCO_BK" AS "SDDOCO_BK"
			, "STG_SRC1"."SDDCTO_BK" AS "SDDCTO_BK"
			, "STG_SRC1"."SDLNID_BK" AS "SDLNID_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('JDEDWARDS_STG_F42119') }} "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."SALESORDERDETAILHISTORY_HKEY" AS "SALESORDERDETAILHISTORY_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."SDKCOO_BK" AS "SDKCOO_BK"
			, "CHANGE_SET"."SDDOCO_BK" AS "SDDOCO_BK"
			, "CHANGE_SET"."SDDCTO_BK" AS "SDDCTO_BK"
			, "CHANGE_SET"."SDLNID_BK" AS "SDLNID_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."SALESORDERDETAILHISTORY_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER",
				"CHANGE_SET"."LOAD_DATE","CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."SALESORDERDETAILHISTORY_HKEY" AS "SALESORDERDETAILHISTORY_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."SDKCOO_BK" AS "SDKCOO_BK"
		, "MIN_LOAD_TIME"."SDDOCO_BK" AS "SDDOCO_BK"
		, "MIN_LOAD_TIME"."SDDCTO_BK" AS "SDDCTO_BK"
		, "MIN_LOAD_TIME"."SDLNID_BK" AS "SDLNID_BK"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'