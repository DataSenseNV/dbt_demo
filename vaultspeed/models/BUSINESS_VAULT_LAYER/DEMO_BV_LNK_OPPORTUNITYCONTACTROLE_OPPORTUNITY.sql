{{
	config(
		materialized='view',
		alias='LNK_OPPORTUNITYCONTACTROLE_OPPORTUNITY',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_OPPORTUNITYCONTACTROLE_OPPORTUNITY_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."OPPORTUNITYCONTACTROLE_HKEY" AS "OPPORTUNITYCONTACTROLE_HKEY"
		, "DVT_SRC"."LNK_OPPORTUNITYCONTACTROLE_OPPORTUNITY_HKEY" AS "LNK_OPPORTUNITYCONTACTROLE_OPPORTUNITY_HKEY"
		, "DVT_SRC"."OPPORTUNITY_HKEY" AS "OPPORTUNITY_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_OPPORTUNITYCONTACTROLE_OPPORTUNITY') }} "DVT_SRC"
