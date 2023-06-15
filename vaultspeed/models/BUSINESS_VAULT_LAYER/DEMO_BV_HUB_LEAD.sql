{{
	config(
		materialized='view',
		alias='HUB_LEAD',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_LEAD_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."LEAD_HKEY" AS "LEAD_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID_BK" AS "ID_BK"
	FROM {{ ref('DEMO_FL_HUB_LEAD') }} "DVT_SRC"
