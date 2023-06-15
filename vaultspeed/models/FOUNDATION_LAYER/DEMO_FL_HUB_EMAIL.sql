{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='HUB_EMAIL',
		schema='DEMO_FL',
		unique_key = ['"EMAIL_HKEY"'],
		merge_update_columns = [],
		tags=['JDEDWARDS', 'HUB_JDE_EMAIL_INCR', 'HUB_JDE_EMAIL_INIT']
	)
}}
select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."EMAIL_HKEY" AS "EMAIL_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."EAAN8_BK" AS "EAAN8_BK"
			, "STG_SRC1"."EAIDLN_BK" AS "EAIDLN_BK"
			, "STG_SRC1"."EARCK7_BK" AS "EARCK7_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('JDEDWARDS_STG_F01151') }} "STG_SRC1"
		WHERE  "STG_SRC1"."RECORD_TYPE" = 'S'
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."EMAIL_HKEY" AS "EMAIL_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."EAAN8_BK" AS "EAAN8_BK"
			, "CHANGE_SET"."EAIDLN_BK" AS "EAIDLN_BK"
			, "CHANGE_SET"."EARCK7_BK" AS "EARCK7_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."EMAIL_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER","CHANGE_SET"."LOAD_DATE",
				"CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	, "MIV" AS 
	( 
		SELECT 
			  "MIN_LOAD_TIME"."EMAIL_HKEY" AS "EMAIL_HKEY"
			, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
			, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "MIN_LOAD_TIME"."EAAN8_BK" AS "EAAN8_BK"
			, "MIN_LOAD_TIME"."EAIDLN_BK" AS "EAIDLN_BK"
			, "MIN_LOAD_TIME"."EARCK7_BK" AS "EARCK7_BK"
		FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
		WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	)
	SELECT 
		  "MIV"."EMAIL_HKEY"
		, "MIV"."LOAD_DATE"
		, "MIV"."LOAD_CYCLE_ID"
		, "MIV"."EAAN8_BK"
		, "MIV"."EAIDLN_BK"
		, "MIV"."EARCK7_BK"
	FROM "MIV"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."EMAIL_HKEY" AS "EMAIL_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."EAAN8_BK" AS "EAAN8_BK"
			, "STG_SRC1"."EAIDLN_BK" AS "EAIDLN_BK"
			, "STG_SRC1"."EARCK7_BK" AS "EARCK7_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('JDEDWARDS_STG_F01151') }} "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."EMAIL_HKEY" AS "EMAIL_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."EAAN8_BK" AS "EAAN8_BK"
			, "CHANGE_SET"."EAIDLN_BK" AS "EAIDLN_BK"
			, "CHANGE_SET"."EARCK7_BK" AS "EARCK7_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."EMAIL_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER","CHANGE_SET"."LOAD_DATE",
				"CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."EMAIL_HKEY" AS "EMAIL_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."EAAN8_BK" AS "EAAN8_BK"
		, "MIN_LOAD_TIME"."EAIDLN_BK" AS "EAIDLN_BK"
		, "MIN_LOAD_TIME"."EARCK7_BK" AS "EARCK7_BK"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'