{{
	config(
		materialized='view',
		alias='LNK_ITEMPRICE_ITEMCOST',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_ITEMPRICE_ITEMCOST_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ITEMPRICE_HKEY" AS "ITEMPRICE_HKEY"
		, "DVT_SRC"."LNK_ITEMPRICE_ITEMCOST_HKEY" AS "LNK_ITEMPRICE_ITEMCOST_HKEY"
		, "DVT_SRC"."ITEMCOST_HKEY" AS "ITEMCOST_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_ITEMPRICE_ITEMCOST') }} "DVT_SRC"
