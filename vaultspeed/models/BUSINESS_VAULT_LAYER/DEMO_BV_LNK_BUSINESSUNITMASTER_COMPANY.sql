{{
	config(
		materialized='view',
		alias='LNK_BUSINESSUNITMASTER_COMPANY',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_BUSINESSUNITMASTER_COMPANY_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."BUSINESSUNITMASTER_HKEY" AS "BUSINESSUNITMASTER_HKEY"
		, "DVT_SRC"."LNK_BUSINESSUNITMASTER_COMPANY_HKEY" AS "LNK_BUSINESSUNITMASTER_COMPANY_HKEY"
		, "DVT_SRC"."COMPANY_HKEY" AS "COMPANY_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_BUSINESSUNITMASTER_COMPANY') }} "DVT_SRC"
