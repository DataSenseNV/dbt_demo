{{
	config(
		materialized='view',
		alias='LKS_JDE_SODETAIL_PRODUCT',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LKS_JDE_SODETAIL_PRODUCT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."LNK_SODETAIL_PRODUCT_HKEY" AS "LNK_SODETAIL_PRODUCT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."SDLNID" AS "SDLNID"
		, "DVT_SRC"."SDDCTO" AS "SDDCTO"
		, "DVT_SRC"."SDDOCO" AS "SDDOCO"
		, "DVT_SRC"."SDKCOO" AS "SDKCOO"
		, "DVT_SRC"."SDITM" AS "SDITM"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_LKS_JDE_SODETAIL_PRODUCT') }} "DVT_SRC"
