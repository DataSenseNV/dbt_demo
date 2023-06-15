{{
	config(
		materialized='view',
		alias='HUB_ACCOUNT',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_ACCOUNT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ACCOUNT_BK" AS "ACCOUNT_BK"
		, "DVT_SRC"."SRC_BK" AS "SRC_BK"
	FROM {{ ref('DEMO_FL_HUB_ACCOUNT') }} "DVT_SRC"
