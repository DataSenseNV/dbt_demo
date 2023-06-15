{{
	config(
		materialized='view',
		alias='HUB_ASSET',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_ASSET_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ASSET_HKEY" AS "ASSET_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID_BK" AS "ID_BK"
	FROM {{ ref('DEMO_FL_HUB_ASSET') }} "DVT_SRC"
