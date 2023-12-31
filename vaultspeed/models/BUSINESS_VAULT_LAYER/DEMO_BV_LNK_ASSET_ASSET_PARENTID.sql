{{
	config(
		materialized='view',
		alias='LNK_ASSET_ASSET_PARENTID',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_ASSET_ASSET_PARENTID_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ASSET_HKEY" AS "ASSET_HKEY"
		, "DVT_SRC"."LNK_ASSET_ASSET_PARENTID_HKEY" AS "LNK_ASSET_ASSET_PARENTID_HKEY"
		, "DVT_SRC"."ASSET_PARENTID_HKEY" AS "ASSET_PARENTID_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_ASSET_ASSET_PARENTID') }} "DVT_SRC"
