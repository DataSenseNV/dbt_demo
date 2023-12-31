{{
	config(
		materialized='view',
		alias='LNK_EMAIL_ACCOUNT',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_EMAIL_ACCOUNT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."EMAIL_HKEY" AS "EMAIL_HKEY"
		, "DVT_SRC"."LNK_EMAIL_ACCOUNT_HKEY" AS "LNK_EMAIL_ACCOUNT_HKEY"
		, "DVT_SRC"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_EMAIL_ACCOUNT') }} "DVT_SRC"
