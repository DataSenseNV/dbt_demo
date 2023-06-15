{{
	config(
		materialized='view',
		alias='HUB_ADDRESS',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_ADDRESS_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ADDRESS_HKEY" AS "ADDRESS_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ALAN8_BK" AS "ALAN8_BK"
		, "DVT_SRC"."ALEFTB_BK" AS "ALEFTB_BK"
	FROM {{ ref('DEMO_FL_HUB_ADDRESS') }} "DVT_SRC"
