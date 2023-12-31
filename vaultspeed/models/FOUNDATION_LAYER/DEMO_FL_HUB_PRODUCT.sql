{{
	config(
		materialized='incremental',
		alias='HUB_PRODUCT',
		schema='DEMO_FL',
		unique_key = ['"PRODUCT_HKEY"'],
		merge_update_columns = [],
		tags=['JDEDWARDS', 'HUB_JDE_PRODUCT_INCR', 'HUB_JDE_PRODUCT_INIT', 'SALESFORCE', 'HUB_SALESFORCE_PRODUCT_INCR', 'HUB_SALESFORCE_PRODUCT_INIT']
	)
}}
select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."PRODUCT_BK" AS "PRODUCT_BK"
			, "STG_SRC1"."SRC_BK" AS "SRC_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('JDEDWARDS_STG_F4101') }} "STG_SRC1"
		WHERE  "STG_SRC1"."RECORD_TYPE" = 'S'
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."PRODUCT_BK" AS "PRODUCT_BK"
			, "CHANGE_SET"."SRC_BK" AS "SRC_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."PRODUCT_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER","CHANGE_SET"."LOAD_DATE",
				"CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	, "MIV" AS 
	( 
		SELECT 
			  "MIN_LOAD_TIME"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
			, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
			, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "MIN_LOAD_TIME"."PRODUCT_BK" AS "PRODUCT_BK"
			, "MIN_LOAD_TIME"."SRC_BK" AS "SRC_BK"
		FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
		WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	)
	SELECT 
		  "MIV"."PRODUCT_HKEY"
		, "MIV"."LOAD_DATE"
		, "MIV"."LOAD_CYCLE_ID"
		, "MIV"."PRODUCT_BK"
		, "MIV"."SRC_BK"
	FROM "MIV"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."PRODUCT_BK" AS "PRODUCT_BK"
			, "STG_SRC1"."SRC_BK" AS "SRC_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('JDEDWARDS_STG_F4101') }} "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."PRODUCT_BK" AS "PRODUCT_BK"
			, "CHANGE_SET"."SRC_BK" AS "SRC_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."PRODUCT_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER","CHANGE_SET"."LOAD_DATE",
				"CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."PRODUCT_BK" AS "PRODUCT_BK"
		, "MIN_LOAD_TIME"."SRC_BK" AS "SRC_BK"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	LEFT OUTER JOIN {{ this }} "HUB_SRC" ON  "MIN_LOAD_TIME"."PRODUCT_HKEY" = "HUB_SRC"."PRODUCT_HKEY"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1 AND "HUB_SRC"."PRODUCT_HKEY" IS NULL

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."PRODUCT_BK" AS "PRODUCT_BK"
			, "STG_SRC1"."SRC_BK" AS "SRC_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('SALESFORCE_STG_PRODUCT2') }} "STG_SRC1"
		WHERE  "STG_SRC1"."RECORD_TYPE" = 'S'
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."PRODUCT_BK" AS "PRODUCT_BK"
			, "CHANGE_SET"."SRC_BK" AS "SRC_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."PRODUCT_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER","CHANGE_SET"."LOAD_DATE",
				"CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	, "MIV" AS 
	( 
		SELECT 
			  "MIN_LOAD_TIME"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
			, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
			, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "MIN_LOAD_TIME"."PRODUCT_BK" AS "PRODUCT_BK"
			, "MIN_LOAD_TIME"."SRC_BK" AS "SRC_BK"
		FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
		WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	)
	SELECT 
		  "MIV"."PRODUCT_HKEY"
		, "MIV"."LOAD_DATE"
		, "MIV"."LOAD_CYCLE_ID"
		, "MIV"."PRODUCT_BK"
		, "MIV"."SRC_BK"
	FROM "MIV"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."PRODUCT_BK" AS "PRODUCT_BK"
			, "STG_SRC1"."SRC_BK" AS "SRC_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('SALESFORCE_STG_PRODUCT2') }} "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."PRODUCT_BK" AS "PRODUCT_BK"
			, "CHANGE_SET"."SRC_BK" AS "SRC_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."PRODUCT_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER","CHANGE_SET"."LOAD_DATE",
				"CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."PRODUCT_BK" AS "PRODUCT_BK"
		, "MIN_LOAD_TIME"."SRC_BK" AS "SRC_BK"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	LEFT OUTER JOIN {{ this }} "HUB_SRC" ON  "MIN_LOAD_TIME"."PRODUCT_HKEY" = "HUB_SRC"."PRODUCT_HKEY"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1 AND "HUB_SRC"."PRODUCT_HKEY" IS NULL

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'