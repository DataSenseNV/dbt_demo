{{
	config(
		materialized='view',
		alias='HUB_CUSTOMER__C',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_CUSTOMER__C_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CUSTOMER__C_HKEY" AS "CUSTOMER__C_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID_BK" AS "ID_BK"
	FROM {{ ref('DEMO_FL_HUB_CUSTOMER__C') }} "DVT_SRC"
