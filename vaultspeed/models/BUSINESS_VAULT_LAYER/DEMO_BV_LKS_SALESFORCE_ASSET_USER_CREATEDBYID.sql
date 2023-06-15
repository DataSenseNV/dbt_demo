{{
	config(
		materialized='view',
		alias='LKS_SALESFORCE_ASSET_USER_CREATEDBYID',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LKS_SALESFORCE_ASSET_USER_CREATEDBYID_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."LNK_ASSET_USER_CREATEDBYID_HKEY" AS "LNK_ASSET_USER_CREATEDBYID_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_LKS_SALESFORCE_ASSET_USER_CREATEDBYID') }} "DVT_SRC"
