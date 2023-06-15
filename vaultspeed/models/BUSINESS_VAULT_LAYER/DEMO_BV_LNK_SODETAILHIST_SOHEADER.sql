{{
	config(
		materialized='view',
		alias='LNK_SODETAILHIST_SOHEADER',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_SODETAILHIST_SOHEADER_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."SALESORDERDETAILHISTORY_HKEY" AS "SALESORDERDETAILHISTORY_HKEY"
		, "DVT_SRC"."LNK_SODETAILHIST_SOHEADER_HKEY" AS "LNK_SODETAILHIST_SOHEADER_HKEY"
		, "DVT_SRC"."SALESORDERHEADER_HKEY" AS "SALESORDERHEADER_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_SODETAILHIST_SOHEADER') }} "DVT_SRC"