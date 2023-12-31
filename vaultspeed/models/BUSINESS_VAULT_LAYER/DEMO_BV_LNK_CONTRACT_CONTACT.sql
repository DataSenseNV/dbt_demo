{{
	config(
		materialized='view',
		alias='LNK_CONTRACT_CONTACT',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_CONTRACT_CONTACT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CONTRACT_HKEY" AS "CONTRACT_HKEY"
		, "DVT_SRC"."LNK_CONTRACT_CONTACT_HKEY" AS "LNK_CONTRACT_CONTACT_HKEY"
		, "DVT_SRC"."CONTACT_HKEY" AS "CONTACT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_CONTRACT_CONTACT') }} "DVT_SRC"
