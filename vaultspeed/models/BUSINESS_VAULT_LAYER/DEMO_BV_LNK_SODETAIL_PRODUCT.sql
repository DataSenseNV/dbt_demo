{{
	config(
		materialized='view',
		alias='LNK_SODETAIL_PRODUCT',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LNK_SODETAIL_PRODUCT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."SALESORDERDETAIL_HKEY" AS "SALESORDERDETAIL_HKEY"
		, "DVT_SRC"."LNK_SODETAIL_PRODUCT_HKEY" AS "LNK_SODETAIL_PRODUCT_HKEY"
		, "DVT_SRC"."PRODUCT_HKEY" AS "PRODUCT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
	FROM {{ ref('DEMO_FL_LNK_SODETAIL_PRODUCT') }} "DVT_SRC"
