{{
	config(
		materialized='view',
		alias='LKS_JDE_ITEMPRICE_ITEMCOST',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_LKS_JDE_ITEMPRICE_ITEMCOST_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."LNK_ITEMPRICE_ITEMCOST_HKEY" AS "LNK_ITEMPRICE_ITEMCOST_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."BPTDAY" AS "BPTDAY"
		, "DVT_SRC"."BPUPMJ" AS "BPUPMJ"
		, "DVT_SRC"."BPEXDJ" AS "BPEXDJ"
		, "DVT_SRC"."BPUOM" AS "BPUOM"
		, "DVT_SRC"."BPCRCD" AS "BPCRCD"
		, "DVT_SRC"."BPFRMP" AS "BPFRMP"
		, "DVT_SRC"."BPLOTG" AS "BPLOTG"
		, "DVT_SRC"."BPCGID" AS "BPCGID"
		, "DVT_SRC"."BPIGID" AS "BPIGID"
		, "DVT_SRC"."BPAN8" AS "BPAN8"
		, "DVT_SRC"."BPLOTN" AS "BPLOTN"
		, "DVT_SRC"."BPLOCN" AS "BPLOCN"
		, "DVT_SRC"."BPMCU" AS "BPMCU"
		, "DVT_SRC"."BPITM" AS "BPITM"
		, "DVT_SRC"."BPLEDG" AS "BPLEDG"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_LKS_JDE_ITEMPRICE_ITEMCOST') }} "DVT_SRC"
