{{
	config(
		materialized='view',
		alias='HUB_WHOISWHO',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_WHOISWHO_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."WHOISWHO_HKEY" AS "WHOISWHO_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."WWAN8_BK" AS "WWAN8_BK"
		, "DVT_SRC"."WWIDLN_BK" AS "WWIDLN_BK"
	FROM {{ ref('DEMO_FL_HUB_WHOISWHO') }} "DVT_SRC"
