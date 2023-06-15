{{
	config(
		materialized='view',
		alias='LNK_LEAD_LEAD_MASTERRECORDID',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_LEAD_LEAD_MASTERRECORDID_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."LEAD_HKEY" AS "LEAD_HKEY"
		, "DVT_SRC"."LNK_LEAD_LEAD_MASTERRECORDID_HKEY" AS "LNK_LEAD_LEAD_MASTERRECORDID_HKEY"
		, "DVT_SRC"."LEAD_MASTERRECORDID_HKEY" AS "LEAD_MASTERRECORDID_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_LEAD_LEAD_MASTERRECORDID') }} "DVT_SRC"
