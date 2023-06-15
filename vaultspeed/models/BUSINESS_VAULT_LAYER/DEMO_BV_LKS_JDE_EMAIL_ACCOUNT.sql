{{
	config(
		materialized='view',
		alias='LKS_JDE_EMAIL_ACCOUNT',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LKS_JDE_EMAIL_ACCOUNT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."LNK_EMAIL_ACCOUNT_HKEY" AS "LNK_EMAIL_ACCOUNT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."EARCK7" AS "EARCK7"
		, "DVT_SRC"."EAIDLN" AS "EAIDLN"
		, "DVT_SRC"."EAAN8" AS "EAAN8"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_LKS_JDE_EMAIL_ACCOUNT') }} "DVT_SRC"
