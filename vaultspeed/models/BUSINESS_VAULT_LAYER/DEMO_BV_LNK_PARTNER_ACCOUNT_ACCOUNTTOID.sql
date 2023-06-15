{{
	config(
		materialized='view',
		alias='LNK_PARTNER_ACCOUNT_ACCOUNTTOID',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_PARTNER_ACCOUNT_ACCOUNTTOID_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."PARTNER_HKEY" AS "PARTNER_HKEY"
		, "DVT_SRC"."LNK_PARTNER_ACCOUNT_ACCOUNTTOID_HKEY" AS "LNK_PARTNER_ACCOUNT_ACCOUNTTOID_HKEY"
		, "DVT_SRC"."ACCOUNT_ACCOUNTTOID_HKEY" AS "ACCOUNT_ACCOUNTTOID_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_PARTNER_ACCOUNT_ACCOUNTTOID') }} "DVT_SRC"
