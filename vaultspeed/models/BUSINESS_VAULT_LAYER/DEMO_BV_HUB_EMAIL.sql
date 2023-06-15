{{
	config(
		materialized='view',
		alias='HUB_EMAIL',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_EMAIL_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."EMAIL_HKEY" AS "EMAIL_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."EAAN8_BK" AS "EAAN8_BK"
		, "DVT_SRC"."EAIDLN_BK" AS "EAIDLN_BK"
		, "DVT_SRC"."EARCK7_BK" AS "EARCK7_BK"
	FROM {{ ref('DEMO_FL_HUB_EMAIL') }} "DVT_SRC"
