{{
	config(
		materialized='view',
		alias='LNK_CASE_USER_LASTMODIFIEDBYID',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_CASE_USER_LASTMODIFIEDBYID_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CASE_HKEY" AS "CASE_HKEY"
		, "DVT_SRC"."LNK_CASE_USER_LASTMODIFIEDBYID_HKEY" AS "LNK_CASE_USER_LASTMODIFIEDBYID_HKEY"
		, "DVT_SRC"."USER_LASTMODIFIEDBYID_HKEY" AS "USER_LASTMODIFIEDBYID_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_CASE_USER_LASTMODIFIEDBYID') }} "DVT_SRC"
