{{
	config(
		materialized='view',
		alias='HUB_PHONE',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_PHONE_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."PHONE_HKEY" AS "PHONE_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."WPAN8_BK" AS "WPAN8_BK"
		, "DVT_SRC"."WPIDLN_BK" AS "WPIDLN_BK"
		, "DVT_SRC"."WPCNLN_BK" AS "WPCNLN_BK"
		, "DVT_SRC"."WPRCK7_BK" AS "WPRCK7_BK"
	FROM {{ ref('DEMO_FL_HUB_PHONE') }} "DVT_SRC"
