{{
	config(
		materialized='view',
		alias='LNK_ITEMPRICE_BUSINESSUNITMASTER',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_ITEMPRICE_BUSINESSUNITMASTER_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ITEMPRICE_HKEY" AS "ITEMPRICE_HKEY"
		, "DVT_SRC"."LNK_ITEMPRICE_BUSINESSUNITMASTER_HKEY" AS "LNK_ITEMPRICE_BUSINESSUNITMASTER_HKEY"
		, "DVT_SRC"."BUSINESSUNITMASTER_HKEY" AS "BUSINESSUNITMASTER_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_ITEMPRICE_BUSINESSUNITMASTER') }} "DVT_SRC"
