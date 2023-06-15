{{
	config(
		materialized='view',
		alias='HUB_CASE',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_CASE_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CASE_HKEY" AS "CASE_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID_BK" AS "ID_BK"
	FROM {{ ref('DEMO_FL_HUB_CASE') }} "DVT_SRC"
