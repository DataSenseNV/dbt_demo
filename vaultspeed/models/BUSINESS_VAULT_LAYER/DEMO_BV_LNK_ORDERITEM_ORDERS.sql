{{
	config(
		materialized='view',
		alias='LNK_ORDERITEM_ORDERS',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_ORDERITEM_ORDERS_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ORDERITEM_HKEY" AS "ORDERITEM_HKEY"
		, "DVT_SRC"."LNK_ORDERITEM_ORDERS_HKEY" AS "LNK_ORDERITEM_ORDERS_HKEY"
		, "DVT_SRC"."ORDERS_HKEY" AS "ORDERS_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_ORDERITEM_ORDERS') }} "DVT_SRC"
