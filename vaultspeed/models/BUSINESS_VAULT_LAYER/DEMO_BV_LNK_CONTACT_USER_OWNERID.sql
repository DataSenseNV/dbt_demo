{{
	config(
		materialized='view',
		alias='LNK_CONTACT_USER_OWNERID',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_CONTACT_USER_OWNERID_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CONTACT_HKEY" AS "CONTACT_HKEY"
		, "DVT_SRC"."LNK_CONTACT_USER_OWNERID_HKEY" AS "LNK_CONTACT_USER_OWNERID_HKEY"
		, "DVT_SRC"."USER_OWNERID_HKEY" AS "USER_OWNERID_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_CONTACT_USER_OWNERID') }} "DVT_SRC"
