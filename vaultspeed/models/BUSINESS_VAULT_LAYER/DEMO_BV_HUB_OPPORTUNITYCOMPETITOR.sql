{{
	config(
		materialized='view',
		alias='HUB_OPPORTUNITYCOMPETITOR',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_HUB_OPPORTUNITYCOMPETITOR_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."OPPORTUNITYCOMPETITOR_HKEY" AS "OPPORTUNITYCOMPETITOR_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID_BK" AS "ID_BK"
	FROM {{ ref('DEMO_FL_HUB_OPPORTUNITYCOMPETITOR') }} "DVT_SRC"
