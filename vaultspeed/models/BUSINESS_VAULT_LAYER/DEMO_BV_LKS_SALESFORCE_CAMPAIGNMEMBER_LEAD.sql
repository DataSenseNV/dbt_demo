{{
	config(
		materialized='view',
		alias='LKS_SALESFORCE_CAMPAIGNMEMBER_LEAD',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LKS_SALESFORCE_CAMPAIGNMEMBER_LEAD_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."LNK_CAMPAIGNMEMBER_LEAD_HKEY" AS "LNK_CAMPAIGNMEMBER_LEAD_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."LEADID" AS "LEADID"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_LKS_SALESFORCE_CAMPAIGNMEMBER_LEAD') }} "DVT_SRC"
