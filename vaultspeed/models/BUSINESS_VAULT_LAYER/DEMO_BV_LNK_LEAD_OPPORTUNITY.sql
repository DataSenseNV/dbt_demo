{{
	config(
		materialized='view',
		alias='LNK_LEAD_OPPORTUNITY',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_LEAD_OPPORTUNITY_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."LEAD_HKEY" AS "LEAD_HKEY"
		, "DVT_SRC"."LNK_LEAD_OPPORTUNITY_HKEY" AS "LNK_LEAD_OPPORTUNITY_HKEY"
		, "DVT_SRC"."OPPORTUNITY_HKEY" AS "OPPORTUNITY_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_LEAD_OPPORTUNITY') }} "DVT_SRC"