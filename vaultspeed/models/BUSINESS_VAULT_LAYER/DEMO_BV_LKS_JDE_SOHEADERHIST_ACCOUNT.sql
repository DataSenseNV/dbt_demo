{{
	config(
		materialized='view',
		alias='LKS_JDE_SOHEADERHIST_ACCOUNT',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LKS_JDE_SOHEADERHIST_ACCOUNT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."LNK_SOHEADERHIST_ACCOUNT_HKEY" AS "LNK_SOHEADERHIST_ACCOUNT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."SHKCOO" AS "SHKCOO"
		, "DVT_SRC"."SHDCTO" AS "SHDCTO"
		, "DVT_SRC"."SHDOCO" AS "SHDOCO"
		, "DVT_SRC"."SHAN8" AS "SHAN8"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_LKS_JDE_SOHEADERHIST_ACCOUNT') }} "DVT_SRC"
