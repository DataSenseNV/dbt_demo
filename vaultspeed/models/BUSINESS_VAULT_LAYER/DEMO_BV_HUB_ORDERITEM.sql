{{
	config(
		materialized='view',
		alias='HUB_ORDERITEM',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_ORDERITEM_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ORDERITEM_HKEY" AS "ORDERITEM_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID_BK" AS "ID_BK"
	FROM {{ ref('DEMO_FL_HUB_ORDERITEM') }} "DVT_SRC"
