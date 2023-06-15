{{
	config(
		materialized='view',
		alias='LNK_ORDERS_ORDERS_ORIGINALORDERID',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_ORDERS_ORDERS_ORIGINALORDERID_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ORDERS_HKEY" AS "ORDERS_HKEY"
		, "DVT_SRC"."LNK_ORDERS_ORDERS_ORIGINALORDERID_HKEY" AS "LNK_ORDERS_ORDERS_ORIGINALORDERID_HKEY"
		, "DVT_SRC"."ORDERS_ORIGINALORDERID_HKEY" AS "ORDERS_ORIGINALORDERID_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_ORDERS_ORDERS_ORIGINALORDERID') }} "DVT_SRC"
