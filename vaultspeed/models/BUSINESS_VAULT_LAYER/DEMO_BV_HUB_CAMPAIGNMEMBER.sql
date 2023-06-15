{{
	config(
		materialized='view',
		alias='HUB_CAMPAIGNMEMBER',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_CAMPAIGNMEMBER_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CAMPAIGNMEMBER_HKEY" AS "CAMPAIGNMEMBER_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID_BK" AS "ID_BK"
	FROM {{ ref('DEMO_FL_HUB_CAMPAIGNMEMBER') }} "DVT_SRC"
