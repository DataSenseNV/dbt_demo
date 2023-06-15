{{
	config(
		materialized='view',
		alias='LNK_ACCOUNTCONTACTROLE_USER_CREATEDBYID',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_ACCOUNTCONTACTROLE_USER_CREATEDBYID_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ACCOUNTCONTACTROLE_HKEY" AS "ACCOUNTCONTACTROLE_HKEY"
		, "DVT_SRC"."LNK_ACCOUNTCONTACTROLE_USER_CREATEDBYID_HKEY" AS "LNK_ACCOUNTCONTACTROLE_USER_CREATEDBYID_HKEY"
		, "DVT_SRC"."USER_CREATEDBYID_HKEY" AS "USER_CREATEDBYID_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_ACCOUNTCONTACTROLE_USER_CREATEDBYID') }} "DVT_SRC"
