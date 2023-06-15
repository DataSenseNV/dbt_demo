{{
	config(
		materialized='view',
		alias='LNK_ACCOUNT_ACCOUNT_PARENTID',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_ACCOUNT_ACCOUNT_PARENTID_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
		, "DVT_SRC"."LNK_ACCOUNT_ACCOUNT_PARENTID_HKEY" AS "LNK_ACCOUNT_ACCOUNT_PARENTID_HKEY"
		, "DVT_SRC"."ACCOUNT_PARENTID_HKEY" AS "ACCOUNT_PARENTID_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_ACCOUNT_ACCOUNT_PARENTID') }} "DVT_SRC"