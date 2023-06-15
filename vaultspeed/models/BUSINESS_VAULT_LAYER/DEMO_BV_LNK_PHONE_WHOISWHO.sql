{{
	config(
		materialized='view',
		alias='LNK_PHONE_WHOISWHO',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_PHONE_WHOISWHO_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."PHONE_HKEY" AS "PHONE_HKEY"
		, "DVT_SRC"."LNK_PHONE_WHOISWHO_HKEY" AS "LNK_PHONE_WHOISWHO_HKEY"
		, "DVT_SRC"."WHOISWHO_HKEY" AS "WHOISWHO_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_PHONE_WHOISWHO') }} "DVT_SRC"