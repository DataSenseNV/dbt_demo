{{
	config(
		materialized='view',
		alias='LNK_SOHEADER_ACCOUNT',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_SOHEADER_ACCOUNT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."SALESORDERHEADER_HKEY" AS "SALESORDERHEADER_HKEY"
		, "DVT_SRC"."LNK_SOHEADER_ACCOUNT_HKEY" AS "LNK_SOHEADER_ACCOUNT_HKEY"
		, "DVT_SRC"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_SOHEADER_ACCOUNT') }} "DVT_SRC"