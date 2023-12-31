{{
	config(
		materialized='view',
		alias='LKS_JDE_ACCOUNT_BUSINESSUNITMASTER',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LKS_JDE_ACCOUNT_BUSINESSUNITMASTER_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."LNK_ACCOUNT_BUSINESSUNITMASTER_HKEY" AS "LNK_ACCOUNT_BUSINESSUNITMASTER_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ABAN8" AS "ABAN8"
		, "DVT_SRC"."ABMCU" AS "ABMCU"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_LKS_JDE_ACCOUNT_BUSINESSUNITMASTER') }} "DVT_SRC"
