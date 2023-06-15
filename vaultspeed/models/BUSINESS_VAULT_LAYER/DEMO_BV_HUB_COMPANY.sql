{{
	config(
		materialized='view',
		alias='HUB_COMPANY',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_COMPANY_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."COMPANY_HKEY" AS "COMPANY_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."CCCO_BK" AS "CCCO_BK"
	FROM {{ ref('DEMO_FL_HUB_COMPANY') }} "DVT_SRC"
