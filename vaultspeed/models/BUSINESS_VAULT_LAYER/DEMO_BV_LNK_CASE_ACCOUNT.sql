{{
	config(
		materialized='view',
		alias='LNK_CASE_ACCOUNT',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_CASE_ACCOUNT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CASE_HKEY" AS "CASE_HKEY"
		, "DVT_SRC"."LNK_CASE_ACCOUNT_HKEY" AS "LNK_CASE_ACCOUNT_HKEY"
		, "DVT_SRC"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_CASE_ACCOUNT') }} "DVT_SRC"
