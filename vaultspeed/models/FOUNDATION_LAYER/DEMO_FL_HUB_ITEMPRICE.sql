{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='HUB_ITEMPRICE',
		schema='DEMO_FL',
		unique_key = ['"ITEMPRICE_HKEY"'],
		merge_update_columns = [],
		tags=['JDEDWARDS', 'HUB_JDE_ITEMPRICE_INCR', 'HUB_JDE_ITEMPRICE_INIT']
	)
}}
select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."ITEMPRICE_HKEY" AS "ITEMPRICE_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."BPITM_BK" AS "BPITM_BK"
			, "STG_SRC1"."BPMCU_BK" AS "BPMCU_BK"
			, "STG_SRC1"."BPLOCN_BK" AS "BPLOCN_BK"
			, "STG_SRC1"."BPLOTN_BK" AS "BPLOTN_BK"
			, "STG_SRC1"."BPAN8_BK" AS "BPAN8_BK"
			, "STG_SRC1"."BPIGID_BK" AS "BPIGID_BK"
			, "STG_SRC1"."BPCGID_BK" AS "BPCGID_BK"
			, "STG_SRC1"."BPLOTG_BK" AS "BPLOTG_BK"
			, "STG_SRC1"."BPFRMP_BK" AS "BPFRMP_BK"
			, "STG_SRC1"."BPCRCD_BK" AS "BPCRCD_BK"
			, "STG_SRC1"."BPUOM_BK" AS "BPUOM_BK"
			, "STG_SRC1"."BPEXDJ_BK" AS "BPEXDJ_BK"
			, "STG_SRC1"."BPUPMJ_BK" AS "BPUPMJ_BK"
			, "STG_SRC1"."BPTDAY_BK" AS "BPTDAY_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('JDEDWARDS_STG_F4106') }} "STG_SRC1"
		WHERE  "STG_SRC1"."RECORD_TYPE" = 'S'
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."ITEMPRICE_HKEY" AS "ITEMPRICE_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."BPITM_BK" AS "BPITM_BK"
			, "CHANGE_SET"."BPMCU_BK" AS "BPMCU_BK"
			, "CHANGE_SET"."BPLOCN_BK" AS "BPLOCN_BK"
			, "CHANGE_SET"."BPLOTN_BK" AS "BPLOTN_BK"
			, "CHANGE_SET"."BPAN8_BK" AS "BPAN8_BK"
			, "CHANGE_SET"."BPIGID_BK" AS "BPIGID_BK"
			, "CHANGE_SET"."BPCGID_BK" AS "BPCGID_BK"
			, "CHANGE_SET"."BPLOTG_BK" AS "BPLOTG_BK"
			, "CHANGE_SET"."BPFRMP_BK" AS "BPFRMP_BK"
			, "CHANGE_SET"."BPCRCD_BK" AS "BPCRCD_BK"
			, "CHANGE_SET"."BPUOM_BK" AS "BPUOM_BK"
			, "CHANGE_SET"."BPEXDJ_BK" AS "BPEXDJ_BK"
			, "CHANGE_SET"."BPUPMJ_BK" AS "BPUPMJ_BK"
			, "CHANGE_SET"."BPTDAY_BK" AS "BPTDAY_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."ITEMPRICE_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER",
				"CHANGE_SET"."LOAD_DATE","CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	, "MIV" AS 
	( 
		SELECT 
			  "MIN_LOAD_TIME"."ITEMPRICE_HKEY" AS "ITEMPRICE_HKEY"
			, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
			, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "MIN_LOAD_TIME"."BPITM_BK" AS "BPITM_BK"
			, "MIN_LOAD_TIME"."BPMCU_BK" AS "BPMCU_BK"
			, "MIN_LOAD_TIME"."BPLOCN_BK" AS "BPLOCN_BK"
			, "MIN_LOAD_TIME"."BPLOTN_BK" AS "BPLOTN_BK"
			, "MIN_LOAD_TIME"."BPAN8_BK" AS "BPAN8_BK"
			, "MIN_LOAD_TIME"."BPIGID_BK" AS "BPIGID_BK"
			, "MIN_LOAD_TIME"."BPCGID_BK" AS "BPCGID_BK"
			, "MIN_LOAD_TIME"."BPLOTG_BK" AS "BPLOTG_BK"
			, "MIN_LOAD_TIME"."BPFRMP_BK" AS "BPFRMP_BK"
			, "MIN_LOAD_TIME"."BPCRCD_BK" AS "BPCRCD_BK"
			, "MIN_LOAD_TIME"."BPUOM_BK" AS "BPUOM_BK"
			, "MIN_LOAD_TIME"."BPEXDJ_BK" AS "BPEXDJ_BK"
			, "MIN_LOAD_TIME"."BPUPMJ_BK" AS "BPUPMJ_BK"
			, "MIN_LOAD_TIME"."BPTDAY_BK" AS "BPTDAY_BK"
		FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
		WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	)
	SELECT 
		  "MIV"."ITEMPRICE_HKEY"
		, "MIV"."LOAD_DATE"
		, "MIV"."LOAD_CYCLE_ID"
		, "MIV"."BPITM_BK"
		, "MIV"."BPMCU_BK"
		, "MIV"."BPLOCN_BK"
		, "MIV"."BPLOTN_BK"
		, "MIV"."BPAN8_BK"
		, "MIV"."BPIGID_BK"
		, "MIV"."BPCGID_BK"
		, "MIV"."BPLOTG_BK"
		, "MIV"."BPFRMP_BK"
		, "MIV"."BPCRCD_BK"
		, "MIV"."BPUOM_BK"
		, "MIV"."BPEXDJ_BK"
		, "MIV"."BPUPMJ_BK"
		, "MIV"."BPTDAY_BK"
	FROM "MIV"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."ITEMPRICE_HKEY" AS "ITEMPRICE_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, 0 AS "LOGPOSITION"
			, "STG_SRC1"."BPITM_BK" AS "BPITM_BK"
			, "STG_SRC1"."BPMCU_BK" AS "BPMCU_BK"
			, "STG_SRC1"."BPLOCN_BK" AS "BPLOCN_BK"
			, "STG_SRC1"."BPLOTN_BK" AS "BPLOTN_BK"
			, "STG_SRC1"."BPAN8_BK" AS "BPAN8_BK"
			, "STG_SRC1"."BPIGID_BK" AS "BPIGID_BK"
			, "STG_SRC1"."BPCGID_BK" AS "BPCGID_BK"
			, "STG_SRC1"."BPLOTG_BK" AS "BPLOTG_BK"
			, "STG_SRC1"."BPFRMP_BK" AS "BPFRMP_BK"
			, "STG_SRC1"."BPCRCD_BK" AS "BPCRCD_BK"
			, "STG_SRC1"."BPUOM_BK" AS "BPUOM_BK"
			, "STG_SRC1"."BPEXDJ_BK" AS "BPEXDJ_BK"
			, "STG_SRC1"."BPUPMJ_BK" AS "BPUPMJ_BK"
			, "STG_SRC1"."BPTDAY_BK" AS "BPTDAY_BK"
			, 0 AS "GENERAL_ORDER"
		FROM {{ ref('JDEDWARDS_STG_F4106') }} "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."ITEMPRICE_HKEY" AS "ITEMPRICE_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."BPITM_BK" AS "BPITM_BK"
			, "CHANGE_SET"."BPMCU_BK" AS "BPMCU_BK"
			, "CHANGE_SET"."BPLOCN_BK" AS "BPLOCN_BK"
			, "CHANGE_SET"."BPLOTN_BK" AS "BPLOTN_BK"
			, "CHANGE_SET"."BPAN8_BK" AS "BPAN8_BK"
			, "CHANGE_SET"."BPIGID_BK" AS "BPIGID_BK"
			, "CHANGE_SET"."BPCGID_BK" AS "BPCGID_BK"
			, "CHANGE_SET"."BPLOTG_BK" AS "BPLOTG_BK"
			, "CHANGE_SET"."BPFRMP_BK" AS "BPFRMP_BK"
			, "CHANGE_SET"."BPCRCD_BK" AS "BPCRCD_BK"
			, "CHANGE_SET"."BPUOM_BK" AS "BPUOM_BK"
			, "CHANGE_SET"."BPEXDJ_BK" AS "BPEXDJ_BK"
			, "CHANGE_SET"."BPUPMJ_BK" AS "BPUPMJ_BK"
			, "CHANGE_SET"."BPTDAY_BK" AS "BPTDAY_BK"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."ITEMPRICE_HKEY" ORDER BY "CHANGE_SET"."GENERAL_ORDER",
				"CHANGE_SET"."LOAD_DATE","CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."ITEMPRICE_HKEY" AS "ITEMPRICE_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."BPITM_BK" AS "BPITM_BK"
		, "MIN_LOAD_TIME"."BPMCU_BK" AS "BPMCU_BK"
		, "MIN_LOAD_TIME"."BPLOCN_BK" AS "BPLOCN_BK"
		, "MIN_LOAD_TIME"."BPLOTN_BK" AS "BPLOTN_BK"
		, "MIN_LOAD_TIME"."BPAN8_BK" AS "BPAN8_BK"
		, "MIN_LOAD_TIME"."BPIGID_BK" AS "BPIGID_BK"
		, "MIN_LOAD_TIME"."BPCGID_BK" AS "BPCGID_BK"
		, "MIN_LOAD_TIME"."BPLOTG_BK" AS "BPLOTG_BK"
		, "MIN_LOAD_TIME"."BPFRMP_BK" AS "BPFRMP_BK"
		, "MIN_LOAD_TIME"."BPCRCD_BK" AS "BPCRCD_BK"
		, "MIN_LOAD_TIME"."BPUOM_BK" AS "BPUOM_BK"
		, "MIN_LOAD_TIME"."BPEXDJ_BK" AS "BPEXDJ_BK"
		, "MIN_LOAD_TIME"."BPUPMJ_BK" AS "BPUPMJ_BK"
		, "MIN_LOAD_TIME"."BPTDAY_BK" AS "BPTDAY_BK"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'