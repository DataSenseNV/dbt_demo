{{
	config(
		materialized='view',
		alias='HUB_PRODUCT2',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_PRODUCT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."PRODUCT_BK" AS "PRODUCT_BK"
		, "DVT_SRC"."SRC_BK" AS "SRC_BK"
	FROM {{ ref('DEMO_FL_HUB_PRODUCT') }} "DVT_SRC"
