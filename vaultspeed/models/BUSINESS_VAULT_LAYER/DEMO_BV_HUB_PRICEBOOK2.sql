{{
	config(
		materialized='view',
		alias='HUB_PRICEBOOK2',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_PRICEBOOK2_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."PRICEBOOK2_HKEY" AS "PRICEBOOK2_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID_BK" AS "ID_BK"
	FROM {{ ref('DEMO_FL_HUB_PRICEBOOK2') }} "DVT_SRC"
